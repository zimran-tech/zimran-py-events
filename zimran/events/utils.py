import aioretry
from aiormq.exceptions import AMQPChannelError, AMQPConnectionError


try:
    from loguru import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

from .dto import ChannelProperties, Exchange
from .exceptions import ChannelPropertiesTypeError, ExchangeTypeError


def validate_exchange(exchange: Exchange):
    if not isinstance(exchange, Exchange):
        raise ExchangeTypeError('ExchangeTypeError: <exchange> must be instance of <Exchange>')


def validate_channel_properties(properties: ChannelProperties):
    if not isinstance(properties, ChannelProperties):
        raise ChannelPropertiesTypeError(
            'ChannelPropertiesTypeError: <properties> must be instance of <ChannelProperties>',
        )


def cleanup_and_normalize_queue_name(queue_name: str):
    if '*' in queue_name:
        queue_name = queue_name.replace('*', '')

    if '#' in queue_name:
        queue_name = queue_name.replace('#', '')

    if '..' in queue_name:
        queue_name = queue_name.replace('..', '.')

    if queue_name.endswith('.'):
        queue_name = queue_name[:-1]

    if '-' in queue_name:
        queue_name = queue_name.replace('-', '_')

    return f'{queue_name}_q'


def retry_policy(info: aioretry.RetryInfo):
    if isinstance(info.exception, (AMQPConnectionError, AMQPChannelError)):
        logger.warning(f'Retrying connection... | attempt amount: {info.fails}')
        return info.fails > 3, (info.fails - 1) * 2

    return True, 0
