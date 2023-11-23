import json

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
        ignore_unroutable: bool = False,
    ):
        if properties is None:
            properties = ChannelProperties()
        else:
            validate_channel_properties(properties)

        basic_properties = pika.BasicProperties(**properties.as_dict(exclude_none=True))
        if isinstance(payload, dict):
            body = json.dumps(payload, default=str)
        else:
            body = payload

        channel = self.get_channel()
        if exchange is None:
            channel.basic_publish(exchange='', routing_key=routing_key, body=body, properties=basic_properties)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        validate_exchange(exchange)

        self.declare_exchange(channel=channel, exchange=exchange, ignore_unroutable=ignore_unroutable)

        channel.basic_publish(exchange=exchange.name, routing_key=routing_key, body=body, properties=basic_properties)

        logger.info(f'Message published to {exchange.name} exchange | routing_key: {routing_key}')


class AsyncProducer(AsyncConnection):
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

    @retry(retry_policy)
    async def publish(
        self,
        routing_key: str,
        *,
        payload: dict,
        exchange: Exchange | None = None,
        properties: ChannelProperties | None = None,
        ignore_unroutable: bool = False,
    ):
        if properties is None:
            properties = ChannelProperties()
        else:
            validate_channel_properties(properties)

        message = self._get_message(properties=properties, payload=payload)

        channel = await self.get_channel()
        if exchange is None:
            await channel.default_exchange.publish(message=message, routing_key=routing_key)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        validate_exchange(exchange)

        declared_exchange = await self.declare_exchange(channel, exchange, ignore_unroutable)

        await declared_exchange.publish(message=message, routing_key=routing_key)

        logger.info(f'Message published to {exchange.name} exchange | routing_key: {routing_key}')

    @staticmethod
    def _get_message(properties: ChannelProperties, payload: dict):
        if isinstance(payload, dict):
            body = json.dumps(payload, default=str)
        else:
            body = payload

        return aio_pika.Message(body=body.encode(), **properties.as_dict(exclude_none=True))
