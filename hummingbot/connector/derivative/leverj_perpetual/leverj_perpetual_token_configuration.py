import aiohttp
# import asyncio
# import logging
from decimal import Decimal
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    # Optional
)

from hummingbot.core.event.events import TradeType
from hummingbot.core.utils.async_utils import safe_ensure_future

from hummingbot.connector.derivative.leverj_perpetual.constants import (
    PERPETUAL_BASE_URL,
    TESTNET_BASE_URL
)
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_utils import convert_from_exchange_trading_pair

MARKET_CONFIGURATIONS_URL = '/all/config'


class LeverjPerpetualAPITokenConfigurationDataSource():
    """ Gets the token configuration on creation.

        Use LoopringAPITokenConfigurationDataSource.create() to create.
    """

    def __init__(self, domain: str = "kovan"):
        self.marketid_lookup: Dict[str, int] = {}
        self.symbol_lookup: Dict[int, str] = {}
        self._decimals: Dict[str, Decimal] = {}
        self._decimals_by_currency: Dict[str, Decimal] = {}
        self._base_url = PERPETUAL_BASE_URL if domain == "leverj_perpetual" else TESTNET_BASE_URL
        self._addresses: Dict[str, str] = {}

    @classmethod
    def create(cls):
        configuration_data_source = cls()
        safe_ensure_future(configuration_data_source._configure())

        return configuration_data_source

    async def _configure(self):
        async with aiohttp.ClientSession() as client:
            response: aiohttp.ClientResponse = await client.get(
                f"{self._base_url}{MARKET_CONFIGURATIONS_URL}"
            )

            if response.status >= 300:
                raise IOError(f"Error fetching active loopring token configurations. HTTP status is {response.status}.")

            response_dict: Dict[str, Any] = await response.json()
            instruments = response_dict["instruments"]

            for instrument_id, config in instruments.items():
                base: str = config['baseSymbol']
                quote: str = config['quoteSymbol']
                exchange_symbol: str = f"{base}-{quote}"
                self.marketid_lookup[exchange_symbol] = instrument_id
                self.symbol_lookup[instrument_id] = exchange_symbol
                if instrument_id not in self._decimals:
                    self._decimals[instrument_id] = int(config['quote']['decimals'])
                    if quote not in self._decimals_by_currency:
                        self._decimals_by_currency[quote] = int(config['quote']['decimals'])
                if quote not in self._addresses:
                    self._addresses[quote] = config['quote']['address']

    def get_bq(self, symbol: str) -> List[str]:
        """ Returns the base and quote of a trading pair """
        return symbol.split('-')

    def get_marketid(self, symbol: str) -> int:
        """ Returns the token id for the given token symbol """
        return self.marketid_lookup.get(symbol)

    def get_symbol(self, marketid: int) -> str:
        """Returns the symbol for the given tokenid """
        return self.symbol_lookup.get(marketid)

    def get_decimals(self, marketid: int) -> int:
        return self._decimals[marketid]

    def get_decimals_by_currency(self, currency: str) -> int:
        if currency in self._decimals_by_currency:
            return self._decimals_by_currency[currency]
        else:
            return 0

    def get_address(self, quote: str) -> str:
        return self._addresses[quote]
