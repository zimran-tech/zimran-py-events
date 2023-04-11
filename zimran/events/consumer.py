import asyncio

import aio_pika
from aioretry import retry


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


from zimran.events.connection import AsyncConnection, Connection
from zimran.events.constants import UNROUTABLE_EXCHANGE_NAME, UNROUTABLE_QUEUE_NAME
from zimran.events.schemas import ExchangeScheme
from zimran.events.utils import cleanup_and_normalize_queue_name, retry_policy, validate_exchange


class ConsumerMixin:
    def handle_event(self, name: str, *, exchange: ExchangeScheme | None = None):
        if exchange is not None:
            validate_exchange(exchange)

        def wrapper(func):
            self._event_handlers[name] = {
                'exchange': exchange,
                'handler': func,
            }

        return wrapper

    def add_event_handler(
        self,
        name: str,
        handler: callable,
        *,
        exchange: ExchangeScheme | None = None,
    ):
        if exchange is not None:
            validate_exchange(exchange)

        self._event_handlers[name] = {
            'exchange': exchange,
            'handler': handler,
        }


class Consumer(Connection, ConsumerMixin):
    def __init__(self, *, service_name: str, broker_url: str, channel_number: int = 1, prefetch_count: int = 10):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

        self._service_name = service_name
        self._prefetch_count = prefetch_count

        self._event_handlers = {}

    def run(self):
        try:
            self.channel.basic_qos(prefetch_count=self._prefetch_count)

            self._declare_unroutable_exchange()

            consumer_amount = 0
            for event_name, data in self._event_handlers.items():
                queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{event_name}')
                self.channel.queue_declare(queue_name, durable=True)

                if exchange := data['exchange']:
                    self.channel.exchange_declare(
                        exchange=exchange.name,
                        exchange_type=exchange.type,
                        **exchange.as_dict(exclude=['name', 'type', 'timeout']),
                    )
                    self.channel.queue_bind(queue=queue_name, exchange=exchange.name, routing_key=event_name)

                self.channel.basic_consume(queue_name, data['handler'])
                logger.info(f'Registering consumer | queue: {queue_name} | routing_key: {event_name}')
                consumer_amount += 1

            logger.info(f'Registered {consumer_amount} consumers')
            self.channel.start_consuming()
        except Exception as exc:
            logger.error(f'Exception occured | error: {exc} | type: {type(exc)}')
        finally:
            self.disconnect()

    def _declare_unroutable_exchange(self):
        self.channel.exchange_declare(exchange=UNROUTABLE_EXCHANGE_NAME, exchange_type='fanout', durable=True)
        self.channel.queue_declare(queue=UNROUTABLE_QUEUE_NAME, durable=True)
        self.channel.queue_bind(queue=UNROUTABLE_QUEUE_NAME, exchange=UNROUTABLE_EXCHANGE_NAME, routing_key='')

        logger.info('Declared unroutable events exchange')


class AsyncConsumer(AsyncConnection, ConsumerMixin):
    def __init__(
        self,
        *,
        service_name: str,
        broker_url: str,
        channel_number: int = 1,
        prefetch_count: int = 10,
        loop=None,
    ):
        super().__init__(broker_url=broker_url, loop=loop, channel_number=channel_number)

        self._service_name = service_name
        self._prefetch_count = prefetch_count

        self._event_handlers = {}

    @retry(retry_policy)
    async def run(self):
        try:
            channel = await self.channel
            tasks = [self._declare_unroutable_queue(channel), channel.set_qos(prefetch_count=self._prefetch_count)]
            await asyncio.gather(*tasks)

            consumer_amount = 0
            for event_name, data in self._event_handlers.items():
                queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{event_name}')
                queue = await channel.declare_queue(queue_name, durable=True)
                if _exchange := data['exchange']:
                    exchange = await channel.declare_exchange(**_exchange.as_dict(exclude_none=True))
                    await queue.bind(exchange=exchange, routing_key=event_name)

                await queue.consume(data['handler'])

                logger.info(f'Registering consumer | queue: {queue_name} | routing_key: {event_name}')
                consumer_amount += 1

            logger.info(f'Registered {consumer_amount} consumers')
            await asyncio.Future()
        except Exception as exc:
            logger.error(f'Exception occured | error: {exc}')
            raise exc
        finally:
            await self.disconnect()

    async def _declare_unroutable_queue(self, channel: aio_pika.abc.AbstractRobustChannel):
        exchange = await channel.declare_exchange(name=UNROUTABLE_EXCHANGE_NAME, type='fanout', durable=True)
        queue = await channel.declare_queue(name=UNROUTABLE_QUEUE_NAME, durable=True)
        await queue.bind(exchange=exchange, routing_key='')
