import datetime
import uuid
from dataclasses import asdict, dataclass
from typing import Literal

from .constants import DEFAULT_DEAD_LETTER_EXCHANGE_NAME
from .exceptions import ExchangeTypeError, QueueTypeError


class Base:
    def as_dict(self, exclude: list | None = None, exclude_none: bool = False) -> dict:
        excluded_keys = exclude if isinstance(exclude, (list, tuple)) else []
        data = {key: val for key, val in asdict(self).items() if key not in excluded_keys}

        if exclude_none:
            data = {key: val for key, val in data.items() if val is not None}

        return data


@dataclass(kw_only=True)
class Exchange(Base):
    name: str
    durable: bool = True
    type: str = 'direct'  # noqa: A003
    internal: bool = False
    passive: bool = False
    auto_delete: bool = False
    arguments: dict | None = None
    timeout: float | int | None = None

    def __post_init__(self):
        if self.arguments is None:
            self.arguments = {}


@dataclass(kw_only=True)
class Queue(Base):
    type: Literal['quorum', 'classic'] = 'quorum'  # noqa: A003
    durable: bool = True
    exclusive: bool = False
    passive: bool = False
    auto_delete: bool = False
    arguments: dict | None = None
    timeout: float | int | None = None
    robust: bool = True

    dead_letter_arguments: dict | None = None

    def __post_init__(self):
        if self.arguments is None:
            self.arguments = {'x-queue-type': self.type}
        else:
            self.arguments.setdefault('x-queue-type', self.type)

        if self.dead_letter_arguments is None:
            self.dead_letter_arguments = {
                'x-queue-type': 'quorum',
                'x-dead-letter-exchange': DEFAULT_DEAD_LETTER_EXCHANGE_NAME,
            }
        else:
            self.dead_letter_arguments.setdefault('x-queue-type', 'quorum')
            self.dead_letter_arguments.setdefault('x-dead-letter-exchange', DEFAULT_DEAD_LETTER_EXCHANGE_NAME)


@dataclass(kw_only=True)
class ChannelProperties(Base):
    correlation_id: str | None = None
    content_type: str = 'application/json'
    delivery_mode: int = 2  # Persistent
    headers: dict | None = None
    priority: int | None = None
    reply_to: str | None = None
    expiration: datetime.datetime | None = None
    message_id: str | None = None
    timestamp: datetime.datetime | None = None
    type: str | None = None  # noqa: A003
    user_id: str | None = None
    app_id: str | None = None

    def __post_init__(self):
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())

        if self.headers is None:
            self.headers = {}


@dataclass(kw_only=True)
class EventHandler(Base):
    """
    :param exchange: Exchange for event.

    :param handler: Callable that will be called when event received.
    """

    handler: callable
    exchange: Exchange | None = None
    queue: Queue | None = None
    ignore_unroutable: bool = False

    def __post_init__(self):
        if self.exchange is not None and not isinstance(self.exchange, Exchange):
            raise ExchangeTypeError('ExchangeTypeError: <exchange> must be instance of <zimran.events.dto.Exchange>')

        if self.queue is not None and not isinstance(self.queue, Queue):
            raise QueueTypeError('QueueTypeError: <queue> must be instance of <zimran.events.dto.Queue>')
