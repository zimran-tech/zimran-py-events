import asyncio
import time

from aio_pika.abc import AbstractRobustChannel
from pika.adapters.blocking_connection import BlockingChannel


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


from .connection import AsyncConnection, Connection
from .router import Router
from .utils import cleanup_and_normalize_queue_name


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

        for routing_key, event in self._router.handlers.items():
            queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{routing_key}')
            self.declare_queue(channel, name=queue_name, queue=event.queue)

            if exchange := event.exchange:
                self.declare_exchange(channel, exchange, event.ignore_unroutable)
                channel.queue_bind(queue=queue_name, exchange=exchange.name, routing_key=routing_key)

            channel.basic_consume(queue_name, event.handler)
            logger.info(f'Registering consumer | queue: {queue_name} | routing_key: {routing_key}')

        channel.start_consuming()


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
        channel: AbstractRobustChannel = await self.get_channel()
        await channel.set_qos(prefetch_count=self._prefetch_count)

        for routing_key, event in self._router.handlers.items():
            queue_name = cleanup_and_normalize_queue_name(f'{self._service_name}.{routing_key}')
            queue = await self.declare_queue(channel, name=queue_name, queue=event.queue)

            if _exchange := event.exchange:
                exchange = await self.declare_exchange(channel, _exchange, event.ignore_unroutable)

                await queue.bind(exchange=exchange, routing_key=routing_key)

            await queue.consume(event.handler)
            logger.info(f'Registering consumer | queue: {queue_name} | routing_key: {routing_key}')

        try:
            await asyncio.Future()
        except asyncio.CancelledError as error:
            logger.error('Consumer cancelled')
            raise error
