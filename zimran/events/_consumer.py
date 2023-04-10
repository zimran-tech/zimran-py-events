import asyncio

import aio_pika
import pika
from pika.channel import Channel

from zimran.events.constants import UNROUTABLE_EXCHANGE_NAME, UNROUTABLE_QUEUE_NAME
from zimran.events.schemas import ExchangeScheme, QueueScheme

from ._abstracts import AbstractConsumer


class AsyncConsumer(AbstractConsumer):
    def __init__(
        self,
        *,
        broker_url: str,
        service_name: str,
        loop: asyncio.AbstractEventLoop | None = None,
        channel_number: int = 1,
        prefetch_count: int = 10,
    ) -> None:
        super().__init__(service_name, channel_number, prefetch_count)

        self.url = broker_url
        self._loop = loop or asyncio.get_event_loop()

    async def run(self):
        connection = await aio_pika.connect(self.url)
        async with connection:
            self._channel: aio_pika.Channel = await connection.channel(channel_number=self._channel_number)
            await self._declare_unroutable_queue(channel=self._channel)

            await self._channel.set_qos(prefetch_count=self._prefetch_count)

            consumers = []
            for event_name, data in self._event_handlers.items():
                queue_: QueueScheme = data['queue']
                queue_name = self._cleanup_and_normalize_queue_name(queue_.name or event_name)
                queue = await self._channel.declare_queue(
                    name=queue_name,
                    **queue_.as_dict(exclude_none=True, exclude=['name']),
                )
                if exchange_data := data['exchange']:
                    exchange = await self._channel.declare_exchange(**exchange_data.as_dict(exclude_none=True))
                    await queue.bind(exchange=exchange, routing_key=event_name)
                else:
                    exchange = self._channel.default_exchange

                await queue.consume(data['handler'])
                consumers.append({'name': event_name, 'data': data})

            print(f'[x] Registered {len(consumers)} consumers of {self.__class__.__name__}: {consumers}')
            await asyncio.Future()

    async def _declare_unroutable_queue(self, channel: aio_pika.abc.AbstractChannel):
        exchange = await channel.declare_exchange(UNROUTABLE_EXCHANGE_NAME, type='fanout', durable=True)
        queue = await channel.declare_queue(UNROUTABLE_QUEUE_NAME, durable=True)
        await queue.bind(exchange=exchange, routing_key='')


class Consumer(AbstractConsumer):
    def __init__(
        self,
        *,
        service_name: str,
        broker_url: str,
        channel_number: int = 1,
        prefetch_count: int = 10,
    ) -> None:
        super().__init__(service_name, channel_number, prefetch_count)

        self._parameters = pika.URLParameters(broker_url)

    def run(self):
        with pika.BlockingConnection(self._parameters) as connection:
            self._channel: Channel = connection.channel(channel_number=self._channel_number)
            self._channel.basic_qos(prefetch_count=self._prefetch_count)

            consumers = []
            for event_name, data in self._event_handlers.items():
                queue_: QueueScheme = data['queue']
                queue_name = self._cleanup_and_normalize_queue_name(queue_.name or event_name)
                self._channel.queue_declare(
                    queue_name,
                    **queue_.as_dict(exclude=['name', 'timeout'], exclude_none=True),
                )
                self._create_exchange_if_provided(event_name, queue_name, data['exchange'])
                self._channel.basic_consume(queue_name, data['handler'])

                consumers.append({'name': queue_name, 'data': data})

            print(f'[x] Registered {len(consumers)} consumers of {self.__class__.__name__}: {consumers}')
            self._channel.start_consuming()

    def _create_exchange_if_provided(self, routing_key: str, queue_name: str, exchange: ExchangeScheme | None = None):
        if exchange is None:
            return

        self._channel.exchange_declare(
            exchange.name,
            exchange.type,
            **exchange.as_dict(
                exclude=['name', 'type', 'timeout'],
                exclude_none=True,
            ),
        )
        self._channel.queue_bind(queue=queue_name, exchange=exchange.name, routing_key=routing_key)

    def _declare_unroutable_queue(self):
        self._channel.exchange_declare(exchange=UNROUTABLE_EXCHANGE_NAME, exchange_type='fanout', durable=True)
        self._channel.queue_declare(UNROUTABLE_QUEUE_NAME, durable=True)
        self._channel.queue_bind(UNROUTABLE_QUEUE_NAME, UNROUTABLE_EXCHANGE_NAME, '')
