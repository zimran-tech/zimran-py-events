import asyncio

import aioretry
import retry


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


from zimran.events.connection import AsyncConnection, Connection
from zimran.events.constants import DEFAULT_DEAD_LETTER_EXCHANGE_NAME
from zimran.events.dto import Exchange
from zimran.events.utils import cleanup_and_normalize_queue_name, retry_policy, validate_exchange


class ConsumerMixin:
    def handle_event(self, name: str, *, exchange: Exchange | None = None):
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
        exchange: Exchange | None = None,
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

        self._service_name = service_name.replace('-', '_').lower()
        self._prefetch_count = prefetch_count

        self._event_handlers = {}

    @retry.retry(tries=3, delay=1)
    def run(self):
        try:
            channel = self.channel
            channel.basic_qos(prefetch_count=self._prefetch_count)

            self._declare_unroutable_queue(channel)
            self._declare_default_dead_letter_exchange(channel)

            consumer_amount = 0
            for event_name, data in self._event_handlers.items():
                queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{event_name}')
                channel.queue_declare(
                    queue_name,
                    durable=True,
                    arguments={'x-dead-letter-exchange': DEFAULT_DEAD_LETTER_EXCHANGE_NAME},
                )

                if exchange := data['exchange']:
                    channel.exchange_declare(
                        exchange=exchange.name,
                        exchange_type=exchange.type,
                        **exchange.as_dict(exclude=['name', 'type', 'timeout']),
                    )
                    channel.queue_bind(queue=queue_name, exchange=exchange.name, routing_key=event_name)

                channel.basic_consume(queue_name, data['handler'])
                logger.info(f'Registering consumer | queue: {queue_name} | routing_key: {event_name}')
                consumer_amount += 1

            logger.info(f'Registered {consumer_amount} consumers')
            channel.start_consuming()
        except Exception as exc:
            logger.error(f'Exception occured | error: {exc} | type: {type(exc)}')
            raise exc
        finally:
            self.disconnect()


class AsyncConsumer(AsyncConnection, ConsumerMixin):
    def __init__(
        self,
        *,
        service_name: str,
        broker_url: str,
        channel_number: int = 1,
        prefetch_count: int = 10,
    ):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

        self._service_name = service_name.replace('-', '_').lower()
        self._prefetch_count = prefetch_count

        self._event_handlers = {}

    @aioretry.retry(retry_policy)
    async def run(self):
        try:
            channel = await self.channel
            await channel.set_qos(prefetch_count=self._prefetch_count)
            await self._declare_unroutable_queue(channel)

            await asyncio.gather(
                self._declare_unroutable_queue(channel),
                self._declare_default_dead_letter_exchange(channel),
                return_exceptions=True,
            )

            consumer_amount = 0
            for event_name, data in self._event_handlers.items():
                queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{event_name}')
                queue = await channel.declare_queue(
                    queue_name,
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
                    },
                )
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
