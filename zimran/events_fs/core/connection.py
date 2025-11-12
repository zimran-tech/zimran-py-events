from zimran.events_fs.constants import 
DEFAULT_DEAD_LETTER_EXCHANGE_NAME, UNROUTABLE_EXCHANGE_NAME, UNROUTABLE_QUEUE_NAME, DEAD_LETTER_QUEUE_NAME

class AsyncConnection:
    def __init__(self, *, broker_url: str, channel_number: int = 1, prefetch_count: int = 10):
        self.url = broker_url
        self.broker = RabbitBroker(broker_url, channel_number=channel_number, prefetch_count=prefetch_count)
        self._started = False

    async def start(self):
        if self._started:
            return
        logger.info("Starting broker connection...")
        await self.broker.start()
        await self._declare_system_exchanges()
        self._started = True

    async def _declare_system_exchanges(self):
        """Создание системных exchange и DLX очередей."""
        await self.broker.declare_exchange(UNROUTABLE_EXCHANGE_NAME, type="fanout", durable=True)
        await self.broker.declare_exchange(DEFAULT_DEAD_LETTER_EXCHANGE_NAME, type="fanout", durable=True)
 
        await self.broker.declare_queue(
            UNROUTABLE_QUEUE_NAME,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
            },
        )
        await self.broker.declare_queue(
            DEAD_LETTER_QUEUE_NAME,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        await self.broker.bind_queue(UNROUTABLE_QUEUE_NAME, UNROUTABLE_EXCHANGE_NAME)
        await self.broker.bind_queue(DEAD_LETTER_QUEUE_NAME, DEFAULT_DEAD_LETTER_EXCHANGE_NAME)
        