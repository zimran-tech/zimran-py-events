import asyncio
import json
import uuid

import aio_pika
import pika
from aioretry import retry


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


from .connection import AsyncConnection, Connection
from .dto import ChannelProperties, Exchange
from .utils import retry_policy, validate_channel_properties, validate_exchange


class Producer(Connection):
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

    def publish(
        self,
        routing_key: str,
        *,
        payload: dict,
        exchange: Exchange | None = None,
        properties: ChannelProperties | None = None,
    ):
        if properties is None:
            properties = ChannelProperties()
        else:
            validate_channel_properties(properties)

        basic_properties = pika.BasicProperties(**properties.as_dict(exclude_none=True))
        body = json.dumps(payload, default=str)
        if exchange is None:
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=body, properties=basic_properties)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        validate_exchange(exchange)
        self._declare_unroutable_queue(channel=self.channel)
        self._declare_default_dead_letter_exchange(channel=self.channel)

        self.channel.exchange_declare(
            exchange=exchange.name,
            exchange_type=exchange.type,
            **exchange.as_dict(exclude=['name', 'type', 'timeout'], exclude_none=True),
        )

        self.channel.basic_publish(
            exchange=exchange.name,
            routing_key=routing_key,
            body=body,
            properties=basic_properties,
        )
        logger.info(f'Message published to {exchange.name} exchange | routing_key: {routing_key}')


class AsyncProducer(AsyncConnection):
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        super().__init__(broker_url=broker_url, channel_number=channel_number)
        self._futures = {}
        self.callback_queues = {}

    async def disconnect(self):
        await super().disconnect()
        self._futures = {}
        self.callback_queues = {}

    def on_response(self, message: aio_pika.IncomingMessage) -> None:
        if message.correlation_id is None:
            return

        future = self._futures.pop(message.correlation_id)
        future.set_result(message)

    @retry(retry_policy)
    async def publish(
        self,
        routing_key: str,
        *,
        payload: dict,
        exchange: Exchange | None = None,
        properties: ChannelProperties | None = None,
    ):
        if properties is None:
            properties = ChannelProperties()
        else:
            validate_channel_properties(properties)

        message = self._get_message(properties=properties, payload=payload)

        channel = await self.channel
        if exchange is None:
            await channel.default_exchange.publish(message=message, routing_key=routing_key)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        validate_exchange(exchange)
        declared_exchange, *_ = await asyncio.gather(
            channel.declare_exchange(**exchange.as_dict(exclude_none=True)),
            self._declare_unroutable_queue(channel=channel),
            self._declare_default_dead_letter_exchange(channel=channel),
            return_exceptions=True,
        )

        await declared_exchange.publish(message=message, routing_key=routing_key)
        logger.info(f'Message published to {exchange.name} exchange | routing_key: {routing_key}')

    async def rpc_publish(
        self,
        routing_key: str,
        payload: dict,
        exchange: Exchange,
        timeout: int = 5,
    ) -> aio_pika.IncomingMessage:
        channel = await self.channel

        correlation_id = str(uuid.uuid4())
        future = asyncio.Future()

        self._futures[correlation_id] = future

        validate_exchange(exchange)
        declared_exchange, *_ = await asyncio.gather(
            channel.declare_exchange(**exchange.as_dict(exclude_none=True)),
            self._declare_unroutable_queue(channel=channel),
            self._declare_default_dead_letter_exchange(channel=channel),
            return_exceptions=True,
        )

        callback_queue_key = f'{exchange.name}_callback_q'
        callback_queue = self.callback_queues.get(callback_queue_key)

        if callback_queue is None:
            callback_queue = await channel.declare_queue(exclusive=True)
            self.callback_queues[callback_queue_key] = callback_queue
            await callback_queue.bind(declared_exchange)
            await callback_queue.consume(self.on_response, no_ack=True)

        rpc_properties = ChannelProperties(
            correlation_id=correlation_id, reply_to=callback_queue.name,
        )
        validate_channel_properties(rpc_properties)
        message = self._get_message(properties=rpc_properties, payload=payload)

        await declared_exchange.publish(message=message, routing_key=routing_key)
        logger.info(
            f'Message published to {exchange.name} exchange | routing_key: {routing_key}',
        )

        try:
            async with asyncio.timeout(timeout):
                return await future
        except TimeoutError:
            self._futures.pop(correlation_id)
            raise

    @staticmethod
    def _get_message(properties: ChannelProperties, payload: dict):
        return aio_pika.Message(body=json.dumps(payload, default=str).encode(), **properties.as_dict(exclude_none=True))
