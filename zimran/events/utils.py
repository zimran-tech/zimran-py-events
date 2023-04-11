from aiormq.exceptions import AMQPChannelError, AMQPConnectionError


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

from .exceptions import ContextTypeError, ExchangeTypeError
from .schemas import ContextScheme, ExchangeScheme


def validate_exchange(exchange: ExchangeScheme):
    if not isinstance(exchange, ExchangeScheme):
        raise ExchangeTypeError('ExchangeTypeError: <exchange> must be instance of <ExchangeScheme>')


def validate_context(context: ContextScheme):
    if not isinstance(context, ContextScheme):
        raise ContextTypeError('ContextTypeError: <context> must be instance of <ContextScheme>')


def cleanup_and_normalize_queue_name(queue_name: str):
    if '*' in queue_name:
        queue_name = queue_name.replace('*', '')

    if '#' in queue_name:
        queue_name = queue_name.replace('#', '')

    if queue_name.endswith('.'):
        queue_name = queue_name[:-1]

    return f'{queue_name}_q'


def retry_policy(info):
    if isinstance(info.exception, (AMQPConnectionError, AMQPChannelError)):
        logger.warning(f'Retrying connection... | attempt amount: {info.fails}')
        return info.fails > 3, (info.fails - 1) * 2

    return True, 0
