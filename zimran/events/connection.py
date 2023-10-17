import aio_pika
import pika
from aioretry import retry
from pika.adapters.blocking_connection import BlockingChannel

from zimran.events.constants import (
    DEAD_LETTER_QUEUE_NAME,
    DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
    UNROUTABLE_EXCHANGE_NAME,
    UNROUTABLE_QUEUE_NAME,
)
from zimran.events.utils import retry_policy


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        self._url = broker_url

        self._connection = None
        self._channel = None
        self._channel_number = channel_number

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
        logger.info('Channel connection established')

    def disconnect(self):
        if self._channel is not None and self._channel.is_open:
            self._channel.close()

        if self._connection is not None and self._connection.is_open:
            self._connection.close()

        logger.info('AMQP Connection disconnected')

    def _declare_unroutable_queue(self, channel: BlockingChannel):
        channel.exchange_declare(exchange=UNROUTABLE_EXCHANGE_NAME, exchange_type='fanout', durable=True)
        channel.queue_declare(queue=UNROUTABLE_QUEUE_NAME, durable=True)
        channel.queue_bind(queue=UNROUTABLE_QUEUE_NAME, exchange=UNROUTABLE_EXCHANGE_NAME, routing_key='')

    def _declare_dead_letter_exchange(self, channel: BlockingChannel):
        channel.exchange_declare(exchange=DEFAULT_DEAD_LETTER_EXCHANGE_NAME, exchange_type='fanout', durable=True)
        channel.queue_declare(queue=DEAD_LETTER_QUEUE_NAME, durable=True)
        channel.queue_bind(queue=DEAD_LETTER_QUEUE_NAME, exchange=DEFAULT_DEAD_LETTER_EXCHANGE_NAME, routing_key='')


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
    async def connection(self) -> aio_pika.abc.AbstractRobustConnection:
        if self._connection is None or self._connection.is_closed:
            self._connection = await aio_pika.connect_robust(url=self._url)
            logger.info('AMQP connection established')

        return self._connection

    async def get_channel(self):
        if self._channel is None or self._channel.is_closed:
            self._channel = await (await self.connection).channel(channel_number=self._channel_number)
            logger.info('Channel connection established')

        return self._channel

    @retry(retry_policy)
    async def connect(self):
        self._channel = await (await self.connection).channel(channel_number=self._channel_number)
        logger.info('Channel connection established')

    async def disconnect(self):
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()

        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()

        logger.info('AMQP Connection disconnected')

    async def _declare_unroutable_queue(self, channel: aio_pika.abc.AbstractRobustChannel):
        exchange = await channel.declare_exchange(name=UNROUTABLE_EXCHANGE_NAME, type='fanout', durable=True)
        queue = await channel.declare_queue(name=UNROUTABLE_QUEUE_NAME, durable=True)
        await queue.bind(exchange=exchange, routing_key='')

    async def _declare_dead_letter_exchange(self, channel: aio_pika.abc.AbstractRobustChannel):
        exchange = await channel.declare_exchange(
            name=DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
            type='fanout',
            durable=True,
        )
        queue = await channel.declare_queue(name=DEAD_LETTER_QUEUE_NAME, durable=True)
        await queue.bind(exchange=exchange, routing_key='')
