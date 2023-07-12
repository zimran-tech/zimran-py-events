import pytest

from aiormq.exceptions import AMQPConnectionError

from zimran.events import utils
from zimran.events.dto import ChannelProperties, Exchange
from zimran.events.exceptions import ChannelPropertiesTypeError, ExchangeTypeError


@pytest.mark.parametrize(
    'case, expected',
    [
        ['payments.orders.oneclick.*.created', 'payments.orders.oneclick.created_q'],
        ['payments.orders.oneclick.#.', 'payments.orders.oneclick_q'],
    ],
)
def test_queue_name_cleaner(case, expected):
    got = utils.cleanup_and_normalize_queue_name(case)

    assert expected == got


def test_validate_exchange():
    utils.validate_exchange(Exchange(name='exchange'))

    with pytest.raises(ExchangeTypeError):
        utils.validate_exchange({'name': 'exchange'})


def test_validate_channel_props():
    utils.validate_channel_properties(ChannelProperties())

    with pytest.raises(ChannelPropertiesTypeError):
        utils.validate_channel_properties({})


def test_retry_policy():
    class Info:
        fails = 1

        @property
        def exception(self):
            return AMQPConnectionError('test error')

    info = Info()
    success, retry_attempt = utils.retry_policy(info=info)

    assert success is False
    assert retry_attempt == 0
