import asyncio
import json

import aio_pika
import pika

from zimran.events.mixins import EventMixin
from zimran.events.schemas import ContextScheme

from ._abstracts import AbstractProducer


class AsyncProducer(EventMixin, AbstractProducer):
    def __init__(self, *, broker_url: str, loop=None):
        super().__init__(broker_url=broker_url)

        self._loop = loop or asyncio.get_event_loop()
        self._connection: aio_pika.abc.AbstractRobustConnection = None
        self._channel: aio_pika.abc.AbstractRobustChannel = None

        self._queue = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    @property
    async def connection(self):
        if self._connection is None or self._connection.is_closed:
            self._connection = await aio_pika.connect_robust(url=self.url, loop=self._loop)

        return self._connection

    @property
    async def channel(self):
        if self._channel is None or self._channel.is_closed:
            self._channel = await (await self.connection).channel()

        return self._channel

    async def connect(self):
        self._channel = await (await self.connection).channel()

    async def disconnect(self):
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()

        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()

    async def publish(self, routing_key: str, *, payload: dict, context: ContextScheme | None = None):
        if context is None:
            context = ContextScheme()
        else:
            self._validate_context(context)

        message = self._get_message(context=context, payload=payload)

        channel = await self.channel

        if context.exchange is None:
            await channel.default_exchange.publish(message=message, routing_key=routing_key)
            return

        self._validate_exchange(context.exchange)
        exchange = await channel.declare_exchange(**context.exchange.as_dict(exclude_none=True))

        await exchange.publish(message=message, routing_key=routing_key)

    @staticmethod
    def _get_message(context: ContextScheme, payload: dict):
        return aio_pika.Message(
            body=json.dumps(payload, default=str).encode(),
            headers=context.headers,
            content_type='application/json',
            correlation_id=context.correlation_id,
        )


class Producer(EventMixin, AbstractProducer):
    def __init__(self, *, broker_url: str):
        super().__init__(broker_url=broker_url)

        self._connection = None
        self._channel = None

        self._queue = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @property
    def connection(self):
        if self._connection is None or self._connection.is_closed:
            self._connection = pika.BlockingConnection(parameters=pika.URLParameters(self.url))

        return self._connection

    @property
    def channel(self):
        if self._channel is None or self._channel.is_closed:
            self._channel = self.connection.channel()

        return self._channel

    def connect(self):
        self._channel = self.connection.channel()

    def disconnect(self):
        if self._channel is not None and self._channel.is_open:
            self._channel.close()

        if self._connection is not None and self._connection.is_open:
            self._connection.close()

    def publish(self, routing_key: str, *, payload: dict, context: ContextScheme | None = None):
        if context is None:
            context = ContextScheme()
        else:
            self._validate_context(context)

        body = json.dumps(payload, default=str).encode()
        if context.exchange is None:
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=body)
            return

        self._validate_exchange(context.exchange)
        self.channel.exchange_declare(
            exchange=context.exchange.name,
            exchange_type=context.exchange.type,
            **context.exchange.as_dict(exclude=['name', 'type', 'timeout'], exclude_none=True),
        )

        self.channel.basic_publish(exchange=context.exchange.name, routing_key=routing_key, body=body)
