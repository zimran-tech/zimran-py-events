from zimran.events.dto import ChannelProperties, Exchange
from zimran.events.schemas import ChannelPropertiesScheme, ExchangeScheme


def test_legacy_scheme_import():
    assert isinstance(ChannelPropertiesScheme(), ChannelProperties)
    assert isinstance(ExchangeScheme(name='example'), Exchange)
