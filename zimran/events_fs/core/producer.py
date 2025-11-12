from zimran.events_fs.core.connection import AsyncConnection

class AsyncProducer(AsyncConnection):
    def __init__(self, *, broker_url: str, channel_number: int = 1):
        super().__init__(broker_url=broker_url, channel_number=channel_number)

    async def connect(self):
        await self.start()

    async def publish(self, routing_key: str, *, payload: dict, exchange: Exchange | None = None, properties: ChannelProperties | None = None, ignore_unroutable: bool = False):
        # format payload 
        # add retry policy
        
        if exchange is None:
            await self.broker.publish(routing_key, payload=payload)
            logger.info(f'Message published to basic exchange | routing_key: {routing_key}')
            return 

        await self.broker.declare_exchange(exchange.name, exchange.type, exchange.durable, exchange.arguments)
        await self.broker.publish(routing_key, payload=payload, exchange=exchange)

        logger.info(f'Message published to {exchange.name} exchange | routing_key: {routing_key}')
    