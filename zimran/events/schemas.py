import uuid
from dataclasses import asdict, dataclass

from zimran.events.constants import UNROUTABLE_EXCHANGE_NAME


@dataclass(kw_only=True)
class ExchangeScheme:
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
        if isinstance(self.arguments, dict):
            self.arguments.setdefault('alternate-exchange', UNROUTABLE_EXCHANGE_NAME)

    def as_dict(self, exclude: list | None = None, exclude_none: bool = False) -> dict:
        excluded_keys = exclude if isinstance(exclude, (list, tuple)) else []
        data = {key: val for key, val in asdict(self).items() if key not in excluded_keys}

        if exclude_none:
            data = {key: val for key, val in data.items() if val is not None}

        return data


@dataclass(kw_only=True)
class ContextScheme:
    correlation_id: str | None = None
    exchange: ExchangeScheme | None = None
    headers: dict | None = None

    def __post_init__(self):
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())

        if self.headers is None:
            self.headers = {}
