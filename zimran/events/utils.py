from aiormq.exceptions import AMQPChannelError, AMQPConnectionError


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

from .exceptions import ChannelPropertiesTypeError, ExchangeTypeError
from .schemas import ChannelPropertiesScheme, ExchangeScheme


def validate_exchange(exchange: ExchangeScheme):
    if not isinstance(exchange, ExchangeScheme):
        raise ExchangeTypeError('ExchangeTypeError: <exchange> must be instance of <ExchangeScheme>')


def validate_channel_properties(properties: ChannelPropertiesScheme):
    if not isinstance(properties, ChannelPropertiesScheme):
        raise ChannelPropertiesTypeError(
            'ChannelPropertiesTypeError: <properties> must be instance of <ChannelPropertiesScheme>',
        )


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
