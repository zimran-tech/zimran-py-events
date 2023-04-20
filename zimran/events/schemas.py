import datetime
import uuid
from dataclasses import asdict, dataclass


class SchemeBase:
    def as_dict(self, exclude: list | None = None, exclude_none: bool = False) -> dict:
        excluded_keys = exclude if isinstance(exclude, (list, tuple)) else []
        data = {key: val for key, val in asdict(self).items() if key not in excluded_keys}

        if exclude_none:
            data = {key: val for key, val in data.items() if val is not None}

        return data


@dataclass(kw_only=True)
class ExchangeScheme(SchemeBase):
    name: str
    durable: bool = True
    type: str = 'direct'  # noqa: A003
    internal: bool = False
    durable: bool = True
    passive: bool = False
    auto_delete: bool = False
    arguments: dict | None = None
    timeout: float | int | None = None

    def __post_init__(self):
        if self.arguments is None:
            self.arguments = {}


@dataclass(kw_only=True)
class ChannelPropertiesScheme(SchemeBase):
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
