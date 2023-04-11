import asyncio

import aio_pika
import pika


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

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @property
    def connection(self):
        if self._connection is None or self._connection.is_closed:
            self._connection = pika.BlockingConnection(parameters=pika.URLParameters(self._url))
            logger.info('AMQP connection established')

        return self._connection

    @property
    def channel(self):
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


class AsyncConnection:
    def __init__(self, *, broker_url: str, loop=None, channel_number: int = 1):
        self._url = broker_url
        self._loop = loop or asyncio.get_event_loop()

        self._connection = None
        self._channel = None
        self._channel_number = channel_number

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    @property
    async def connection(self) -> aio_pika.abc.AbstractRobustConnection:
        if self._connection is None or self._connection.is_closed:
            self._connection = await aio_pika.connect_robust(url=self._url, loop=self._loop)
            logger.info('AMQP connection established')

        return self._connection

    @property
    async def channel(self):
        if self._channel is None or self._channel.is_closed:
            self._channel = await (await self.connection).channel(channel_number=self._channel_number)
            logger.info('Channel connection established')

        return self._channel

    async def connect(self):
        self._channel = await (await self.connection).channel(channel_number=self._channel_number)
        logger.info('Channel connection established')

    async def disconnect(self):
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()

        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()

        logger.info('AMQP Connection disconnected')
