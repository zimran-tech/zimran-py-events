from abc import ABC, abstractmethod

from zimran.events.mixins import EventMixin
from zimran.events.schemas import ContextScheme, ExchangeScheme, QueueScheme


class AbstractConsumer(ABC, EventMixin):
    def __init__(self, service_name: str, channel_number: int = 1, prefetch_count: int = 10) -> None:
        self._service_name: str = service_name.lower().replace('-', '_')

        self._channel = None

        self._prefetch_count: int = prefetch_count
        self._channel_number: int = channel_number

        self._event_handlers: dict = {}

    @abstractmethod
    def run(self):
        ...

    def handle_event(self, name: str, *, queue: QueueScheme | None = None, exchange: ExchangeScheme | None = None):
        if queue is None:
            queue = QueueScheme(name=self._service_name)
        else:
            self._validate_queue(queue)

        if exchange is not None:
            self._validate_exchange(exchange)

        def wrapper(func):
            self._event_handlers[name] = {
                'queue': queue,
                'exchange': exchange,
                'handler': func,
            }

        return wrapper

    def add_event_handler(
        self,
        name: str,
        handler: callable,
        *,
        queue: QueueScheme | None = None,
        exchange: ExchangeScheme | None = None,
    ):
        if queue is None:
            queue = QueueScheme()
        else:
            self._validate_queue(queue)

        if exchange is not None:
            self._validate_exchange(exchange)

        self._event_handlers[name] = {
            'queue': queue,
            'exchange': exchange,
            'handler': handler,
        }


class AbstractProducer(ABC):
    def __init__(self, *, broker_url: str):
        self.url = broker_url

    @abstractmethod
    def publish(self, routing_key: str, *, payload: dict, context: ContextScheme | None = None):
        ...

    @abstractmethod
    def connect(self):
        ...

    @abstractmethod
    def disconnect(self):
        ...
