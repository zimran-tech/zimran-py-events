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
from .schemas import ChannelPropertiesScheme, ExchangeScheme
from .utils import retry_policy, validate_channel_properties, validate_exchange


class Producer(Connection):
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

    def publish(
        self,
        routing_key: str,
        *,
        payload: dict,
        exchange: ExchangeScheme | None = None,
        properties: ChannelPropertiesScheme | None = None,
    ):
        if properties is None:
            properties = ChannelPropertiesScheme()
        else:
            validate_channel_properties(properties)

        basic_properties = pika.BasicProperties(**properties.as_dict(exclude_none=True))
        body = json.dumps(payload, default=str)
        if exchange is None:
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=body, properties=basic_properties)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        validate_exchange(exchange)

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

    @retry(retry_policy)
    async def publish(
        self,
        routing_key: str,
        *,
        payload: dict,
        exchange: ExchangeScheme | None = None,
        properties: ChannelPropertiesScheme | None = None,
    ):
        if properties is None:
            properties = ChannelPropertiesScheme()
        else:
            validate_channel_properties(properties)

        message = self._get_message(properties=properties, payload=payload)

        channel = await self.channel
        if exchange is None:
            await channel.default_exchange.publish(message=message, routing_key=routing_key)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return

        validate_exchange(exchange)

        declared_exchange = await channel.declare_exchange(**exchange.as_dict(exclude_none=True))
        await declared_exchange.publish(message=message, routing_key=routing_key)
        logger.info(f'Message published to {exchange.name} exchange | routing_key: {routing_key}')

    @staticmethod
    def _get_message(properties: ChannelPropertiesScheme, payload: dict):
        return aio_pika.Message(body=json.dumps(payload, default=str).encode(), **properties.as_dict(exclude_none=True))
