from .exceptions import ContextTypeError, ExchangeTypeError, QueueTypeError
from .schemas import ContextScheme, ExchangeScheme, QueueScheme


class EventMixin:
    @staticmethod
    def _validate_exchange(exchange: ExchangeScheme):
        if not isinstance(exchange, ExchangeScheme):
            raise ExchangeTypeError('ExchangeTypeError: <exchange> must be instance of <ExchangeScheme>')

    @staticmethod
    def _validate_queue(queue: QueueScheme):
        if not isinstance(queue, QueueScheme):
            raise QueueTypeError('QueueTypeError: <queue> must be instance of <QueueScheme>')

    @staticmethod
    def _validate_context(context: ContextScheme):
        if not isinstance(context, ContextScheme):
            raise ContextTypeError('ContextTypeError: <context> must be instance of <ContextScheme>')

    @staticmethod
    def _cleanup_and_normalize_queue_name(queue_name: str):
        if '*' in queue_name:
            queue_name = queue_name.replace('*', '')

        if '#' in queue_name:
            queue_name = queue_name.replace('#', '')

        if queue_name.endswith('.'):
            queue_name = queue_name[:-1]

        return f'{queue_name}_q'
