import asyncio
import copy

import pika
from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection
from aioretry import retry
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

from .constants import (
    DEAD_LETTER_QUEUE_NAME,
    DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
    UNROUTABLE_EXCHANGE_NAME,
    UNROUTABLE_QUEUE_NAME,
)
from .dto import Exchange, Queue
from .utils import cleanup_and_normalize_queue_name, retry_policy


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        self._url: str = broker_url

        self._connection: BlockingConnection = None
        self._channel: BlockingChannel = None
        self._channel_number: int = channel_number

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: U100
        self.disconnect()

    @property
    def connection(self):
        if self._connection is None or self._connection.is_closed:
            self._connection = pika.BlockingConnection(parameters=pika.URLParameters(self._url))
            logger.info('AMQP connection established')

        return self._connection

    def get_channel(self) -> BlockingChannel:
        if self._channel is None or self._channel.is_closed:
            self._channel = self.connection.channel(channel_number=self._channel_number)
            logger.info('Channel connection established')

        return self._channel

    def connect(self):
        self._channel = self.connection.channel(channel_number=self._channel_number)
        logger.info('Connection established')

        self._run_routines(self._channel)

    def disconnect(self):
        if self._channel is not None and self._channel.is_open:
            self._channel.close()

        if self._connection is not None and self._connection.is_open:
            self._connection.close()

        logger.info('AMQP Connection disconnected')

    def declare_exchange(self, channel: BlockingChannel, exchange: Exchange, ignore_unroutable: bool = False):
        arguments = copy.deepcopy(exchange.arguments)
        if not ignore_unroutable:
            arguments.setdefault('alternate-exchange', UNROUTABLE_EXCHANGE_NAME)

        channel.exchange_declare(
            exchange=exchange.name,
            exchange_type=exchange.type,
            arguments=arguments,
            **exchange.as_dict(exclude_none=True, exclude=['arguments', 'name', 'type', 'timeout']),
        )

        logger.info(f'Exchange {exchange.name} declared')

    def declare_queue(self, channel: BlockingChannel, *, name: str, queue: Queue | None = None):
        if queue is None:
            queue = Queue()

        arguments: dict = queue.arguments
        if dead_letter_exchange := arguments.get('x-dead-letter-exchange'):
            queue_name = cleanup_and_normalize_queue_name(dead_letter_exchange)
            self._declare_dead_letter(
                channel,
                exchange_name=dead_letter_exchange,
                queue_name=queue_name,
                queue_arguments=queue.dead_letter_arguments,
            )
        else:
            arguments.setdefault('x-dead-letter-exchange', DEFAULT_DEAD_LETTER_EXCHANGE_NAME)

        channel.queue_declare(
            queue=name,
            passive=queue.passive,
            durable=queue.durable,
            exclusive=queue.exclusive,
            auto_delete=queue.auto_delete,
            arguments=arguments,
        )
        logger.info(f'Queue {name} declared')

    def _declare_default_unroutable(self, channel: BlockingChannel):
        channel.exchange_declare(exchange=UNROUTABLE_EXCHANGE_NAME, exchange_type='fanout', durable=True)

        arguments = {'x-queue-type': 'quorum', 'x-dead-letter-exchange': DEFAULT_DEAD_LETTER_EXCHANGE_NAME}
        channel.queue_declare(queue=UNROUTABLE_QUEUE_NAME, durable=True, arguments=arguments)

        channel.queue_bind(queue=UNROUTABLE_QUEUE_NAME, exchange=UNROUTABLE_EXCHANGE_NAME, routing_key='')

        logger.info('Unrouteable exchange and queue declared')

    def _declare_dead_letter(
        self,
        channel: BlockingChannel,
        *,
        exchange_name: str,
        queue_name: str,
        queue_arguments: dict | None = None,
    ):
        channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

        if queue_arguments:
            arguments = copy.deepcopy(queue_arguments)
        else:
            arguments = {'x-queue-type': 'quorum'}

        channel.queue_declare(queue=queue_name, durable=True, arguments=arguments)
        channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key='')

        logger.info(f'Dead letter exchange "{exchange_name}" and queue "{queue_name}" declared')

    def _run_routines(self, channel: BlockingChannel):
        self._declare_default_unroutable(channel)
        self._declare_dead_letter(
            channel,
            exchange_name=DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
            queue_name=DEAD_LETTER_QUEUE_NAME,
        )


class AsyncConnection:
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        self._url = broker_url

        self._connection = None
        self._channel = None
        self._channel_number = channel_number

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: U100
        await self.disconnect()

    @property
    async def connection(self) -> AbstractRobustConnection:
        if self._connection is None or self._connection.is_closed:
            self._connection = await connect_robust(url=self._url)
            logger.info('AMQP connection established')

        return self._connection

    async def get_channel(self) -> AbstractRobustChannel:
        if self._channel is None or self._channel.is_closed:
            self._channel = await (await self.connection).channel(channel_number=self._channel_number)
            logger.info('Channel connection established')

        return self._channel

    @retry(retry_policy)
    async def connect(self):
        self._channel = await (await self.connection).channel(channel_number=self._channel_number)
        logger.info('Connection established')

        await self._run_routines(self._channel)

    async def disconnect(self):
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()

        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()

        logger.info('AMQP Connection disconnected')

    async def declare_exchange(
        self,
        channel: AbstractRobustChannel,
        exchange: Exchange,
        ignore_unroutable: bool = False,
    ):
        arguments = copy.deepcopy(exchange.arguments)

        if not ignore_unroutable:
            arguments.setdefault('alternate-exchange', UNROUTABLE_EXCHANGE_NAME)

        declared_exchange = await channel.declare_exchange(
            name=exchange.name,
            arguments=arguments,
            **exchange.as_dict(exclude_none=True, exclude=['arguments', 'name']),
        )

        logger.info(f'Exchange {exchange.name} declared')

        return declared_exchange

    async def declare_queue(self, channel: AbstractRobustChannel, *, name: str, queue: Queue | None = None):
        if queue is None:
            queue = Queue()

        arguments: dict = queue.arguments
        if dead_letter_exchange := arguments.get('x-dead-letter-exchange'):
            queue_name = cleanup_and_normalize_queue_name(dead_letter_exchange)
            await self._declare_dead_letter(
                channel,
                exchange_name=dead_letter_exchange,
                queue_name=queue_name,
                queue_arguments=queue.dead_letter_arguments,
            )
        else:
            arguments.setdefault('x-dead-letter-exchange', DEFAULT_DEAD_LETTER_EXCHANGE_NAME)

        declared_queue = await channel.declare_queue(
            name,
            durable=queue.durable,
            exclusive=queue.exclusive,
            passive=queue.passive,
            auto_delete=queue.auto_delete,
            arguments=arguments,
            timeout=queue.timeout,
            robust=queue.robust,
        )
        logger.info(f'Queue {name} declared')

        return declared_queue

    async def _declare_default_unroutable(self, channel: AbstractRobustChannel):
        exchange = await channel.declare_exchange(UNROUTABLE_EXCHANGE_NAME, type='fanout', durable=True)

        arguments = {'x-queue-type': 'quorum', 'x-dead-letter-exchange': DEFAULT_DEAD_LETTER_EXCHANGE_NAME}
        queue = await channel.declare_queue(UNROUTABLE_QUEUE_NAME, durable=True, arguments=arguments)

        await queue.bind(exchange=exchange, routing_key='')

        logger.info('Unrouteable exchange and queue declared')

    async def _declare_dead_letter(
        self,
        channel: AbstractRobustChannel,
        *,
        exchange_name: str,
        queue_name: str,
        queue_arguments: dict | None = None,
    ):
        exchange = await channel.declare_exchange(exchange_name, type='fanout', durable=True)
        if queue_arguments:
            arguments = copy.deepcopy(queue_arguments)
        else:
            arguments = {'x-queue-type': 'quorum'}

        queue = await channel.declare_queue(queue_name, durable=True, arguments=arguments)
        await queue.bind(exchange=exchange, routing_key='')

        logger.info(f'Dead letter exchange "{exchange_name}" and queue "{queue_name}" declared')

    async def _run_routines(self, channel: AbstractRobustChannel):
        await asyncio.gather(
            self._declare_default_unroutable(channel),
            self._declare_dead_letter(
                channel,
                exchange_name=DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
                queue_name=DEAD_LETTER_QUEUE_NAME,
            ),
            return_exceptions=True,
        )
