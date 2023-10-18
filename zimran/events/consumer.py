import asyncio
import time

import aio_pika
from pika.adapters.blocking_connection import BlockingChannel


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


from zimran.events.connection import AsyncConnection, Connection
from zimran.events.constants import DEFAULT_DEAD_LETTER_EXCHANGE_NAME
from zimran.events.router import Router
from zimran.events.utils import cleanup_and_normalize_queue_name


class Consumer(Connection):
    def __init__(
        self,
        *,
        service_name: str,
        broker_url: str,
        router: Router,
        channel_number: int = 1,
        prefetch_count: int = 10,
    ):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

        self._service_name = service_name.replace('-', '_').lower()
        self._prefetch_count = prefetch_count
        self._router = router

    def run(self, *, max_retries: int = 5, retry_delay: int = 3):
        retries = 0
        while retries <= max_retries:
            try:
                self._run()
            except Exception as e:
                logger.error(f'Error connecting to RabbitMQ: {e}')
                retries += 1
                if retries <= max_retries:
                    logger.info(f'Retrying in {retry_delay} seconds...')
                    time.sleep(retry_delay)
                else:
                    logger.error('Max retries exceeded, giving up')
                    break

    def _run(self):
        channel: BlockingChannel = self.get_channel()
        channel.basic_qos(prefetch_count=self._prefetch_count)
        self._run_routines(channel)

        for routing_key, event in self._router.handlers.items():
            queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{routing_key}')
            channel.queue_declare(
                queue_name,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
                    'x-queue-type': event.queue_type,
                },
            )

            if exchange := event.exchange:
                channel.exchange_declare(
                    exchange=exchange.name,
                    exchange_type=exchange.type,
                    **exchange.as_dict(exclude=['name', 'type', 'timeout']),
                )
                channel.queue_bind(queue=queue_name, exchange=exchange.name, routing_key=routing_key)

            channel.basic_consume(queue_name, event.handler)
            logger.info(f'Registering consumer | queue: {queue_name} | routing_key: {routing_key}')

        channel.start_consuming()

    def _run_routines(self, channel: BlockingChannel):
        self._declare_unroutable(channel)
        self._declare_dead_letter(channel)


class AsyncConsumer(AsyncConnection):
    def __init__(
        self,
        *,
        service_name: str,
        broker_url: str,
        router: Router,
        channel_number: int = 1,
        prefetch_count: int = 10,
    ):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

        self._service_name = service_name.replace('-', '_').lower()
        self._prefetch_count = prefetch_count
        self._router = router

    async def run(self, *, max_retries: int = 5, retry_delay: int = 3):
        retries = 0
        while retries <= max_retries:
            try:
                await self._run()
            except asyncio.CancelledError as e:
                logger.error(f'Consumer cancelled: {e}')
                break
            except Exception as e:
                logger.error(f'Error connecting to RabbitMQ: {e}')
                retries += 1
                if retries <= max_retries:
                    logger.info(f'Retrying in {retry_delay} seconds...')
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error('Max retries exceeded, giving up')
                    break

    async def _run(self):
        channel = await self.get_channel()
        await channel.set_qos(prefetch_count=self._prefetch_count)
        await self._run_routines(channel)

        for routing_key, event in self._router.handlers.items():
            queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{routing_key}')
            queue = await channel.declare_queue(
                queue_name,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
                    'x-queue-type': event.queue_type,
                },
            )

            if _exchange := event.exchange:
                exchange = await channel.declare_exchange(**_exchange.as_dict(exclude_none=True))
                await queue.bind(exchange=exchange, routing_key=routing_key)

            await queue.consume(event.handler)
            logger.info(f'Registering consumer | queue: {queue_name} | routing_key: {routing_key}')

        try:
            await asyncio.Future()
        except asyncio.CancelledError as error:
            logger.error('Consumer cancelled')
            raise error

    async def _run_routines(self, channel: aio_pika.Channel):
        await asyncio.gather(
            self._declare_unroutable(channel),
            self._declare_dead_letter(channel),
            return_exceptions=True,
        )
