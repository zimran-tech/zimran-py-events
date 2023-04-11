import json

import aio_pika


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


from zimran.events.connection import AsyncConnection, Connection
from zimran.events.constants import UNROUTABLE_EXCHANGE_NAME, UNROUTABLE_QUEUE_NAME
from zimran.events.schemas import ContextScheme
from zimran.events.utils import validate_context, validate_exchange


class Producer(Connection):
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

    def publish(self, routing_key: str, *, payload: dict, context: ContextScheme | None = None):
        if context is None:
            context = ContextScheme()
        else:
            validate_context(context)

        body = json.dumps(payload, default=str)
        if context.exchange is None:
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=body)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        self._declare_unroutable_queue()

        validate_exchange(context.exchange)
        self.channel.exchange_declare(
            exchange=context.exchange.name,
            exchange_type=context.exchange.type,
            **context.exchange.as_dict(exclude=['name', 'type', 'timeout'], exclude_none=True),
        )

        self.channel.basic_publish(exchange=context.exchange.name, routing_key=routing_key, body=body)
        logger.info(f'Message published to {context.exchange.name} exchange | routing_key: {routing_key}')

    def _declare_unroutable_queue(self):
        self.channel.exchange_declare(exchange=UNROUTABLE_EXCHANGE_NAME, exchange_type='fanout', durable=True)
        self.channel.queue_declare(queue=UNROUTABLE_QUEUE_NAME, durable=True)
        self.channel.queue_bind(queue=UNROUTABLE_QUEUE_NAME, exchange=UNROUTABLE_EXCHANGE_NAME, routing_key='')


class AsyncProducer(AsyncConnection):
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

    async def publish(self, routing_key: str, *, payload: dict, context: ContextScheme | None = None):
        if context is None:
            context = ContextScheme()
        else:
            validate_context(context)

        message = self._get_message(context=context, payload=payload)

        channel = await self.channel
        if context.exchange is None:
            await channel.default_exchange.publish(message=message, routing_key=routing_key)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        validate_exchange(context.exchange)
        await self._declare_unroutable_queue(channel)

        exchange = await channel.declare_exchange(**context.exchange.as_dict(exclude_none=True))
        await exchange.publish(message=message, routing_key=routing_key)
        logger.info(f'Message published to {context.exchange.name} exchange | routing_key: {routing_key}')

    @staticmethod
    def _get_message(context: ContextScheme, payload: dict):
        return aio_pika.Message(
            body=json.dumps(payload, default=str).encode(),
            headers=context.headers,
            content_type='application/json',
            correlation_id=context.correlation_id,
        )

    async def _declare_unroutable_queue(self, channel: aio_pika.abc.AbstractRobustChannel):
        exchange = await channel.declare_exchange(name=UNROUTABLE_EXCHANGE_NAME, type='fanout', durable=True)
        queue = await channel.declare_queue(name=UNROUTABLE_QUEUE_NAME, durable=True)
        await queue.bind(exchange=exchange, routing_key='')
