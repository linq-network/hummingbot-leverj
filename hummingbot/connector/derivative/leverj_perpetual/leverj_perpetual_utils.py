import re

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange


CENTRALIZED = True


EXAMPLE_PAIR = "BTC-USD"


DEFAULT_FEES = [0.04, 0.02]


KEYS = {
    "leverj_perpetual_api_key":
        ConfigVar(key="leverj_perpetual_api_key",
                  prompt="Enter your leverj Perpetual API key >>> ",
                  required_if=using_exchange("leverj_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
    "leverj_perpetual_api_secret":
        ConfigVar(key="leverj_perpetual_api_secret",
                  prompt="Enter your leverj Perpetual API secret >>> ",
                  required_if=using_exchange("leverj_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
    "leverj_perpetual_account_number":
        ConfigVar(key="leverj_perpetual_account_number",
                  prompt="Enter your leverj Perpetual API account_number >>> ",
                  required_if=using_exchange("leverj_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
}

PAIR_REGEX = r"([A-Z]*)(USD|DEFI|USDT)"
MATCHER = re.compile(PAIR_REGEX)

def convert_from_exchange_trading_pair(leverj_trading_pair: str):
    m = MATCHER.match(leverj_trading_pair)
    quote = 'DAI' if m.group(2) == 'USD' else m.group(2)
    return f"{m.group(1)}-{quote}"

def convert_to_exchange_trading_pair(hb_trading_pair: str):
    return hb_trading_pair.replace('-', '')