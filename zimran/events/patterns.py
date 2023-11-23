from .dto import ChannelProperties, Exchange
from .producer import AsyncProducer, Producer


class AsyncProducerSingleton:
    _producer: AsyncProducer = None

    @classmethod
    async def connect(cls, broker_url: str):
        if cls._producer is None:
            cls._producer = AsyncProducer(broker_url=broker_url)
            await cls._producer.connect()

    @classmethod
    async def disconnect(cls):
        await cls._producer.disconnect()

    @classmethod
    async def publish(
        cls,
        routing_key: str,
        *,
        payload: dict,
        exchange_name: str,
        exchange_type: str = 'topic',
        channel_properties: ChannelProperties = None,
        ignore_unroutable: bool = False,
    ):
        exchange = Exchange(name=exchange_name, type=exchange_type)
        await cls._producer.publish(
            routing_key,
            payload=payload,
            exchange=exchange,
            properties=channel_properties,
            ignore_unroutable=ignore_unroutable,
        )


class ProducerSingleton:
    _producer: Producer = None

    @classmethod
    def connect(cls, broker_url: str):
        if cls._producer is None:
            cls._producer = Producer(broker_url=broker_url)
            cls._producer.connect()

    @classmethod
    def disconnect(cls):
        cls._producer.disconnect()

    @classmethod
    def publish(
        cls,
        routing_key: str,
        *,
        payload: dict,
        exchange_name: str,
        exchange_type: str = 'topic',
        channel_properties: ChannelProperties = None,
        ignore_unroutable: bool = False,
    ):
        exchange = Exchange(name=exchange_name, type=exchange_type)
        cls._producer.publish(
            routing_key,
            payload=payload,
            exchange=exchange,
            properties=channel_properties,
            ignore_unroutable=ignore_unroutable,
        )
