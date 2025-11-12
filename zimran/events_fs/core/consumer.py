class AsyncConsumer(AsyncConnection): 
    def __init__(
        self, 
        *, 
        broker_url: str, 
        channel_number: int = 1, 
        router: Router, 
        channel_number: int = 1,
        prefetch_count: int = 10,
        service_name: str,
    ):
        super().__init__(broker_url=broker_url, channel_number=channel_number, prefetch_count=prefetch_count)

        self._router = router
        self._service_name = service_name.replace('-', '_').lower()

    async def run(self, *, max_retries: int = 5, retry_delay: int = 3):
        retries = 0
        while retries <= max_retries:
            try:
                await self.start() 
                await self._setup_handlers()
            except Exception as e:
                logger.error(f'Error setting up handlers: {e}')
                retries += 1
                if retries <= max_retries:
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error('Max retries exceeded, giving up')
                    break

    async def _setup_handlers(self): 
        for routing_key, event in self._router.handlers.items():  
            exchange_name = event.exchange.name

            