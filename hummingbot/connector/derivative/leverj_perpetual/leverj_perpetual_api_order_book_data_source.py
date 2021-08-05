#!/usr/bin/env python

import asyncio
from decimal import Decimal

import aiohttp
import logging
# import pandas as pd
# import math

import requests
import cachetools.func

from typing import AsyncIterable, Dict, List, Optional, Any

import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_order_book import LeverjPerpetualOrderBook
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_token_configuration import LeverjPerpetualAPITokenConfigurationDataSource
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_socketio_client import LeverjPerpetualSocketIOClient
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow

from hummingbot.connector.derivative.leverj_perpetual.constants import (
    PERPETUAL_BASE_URL,
    TESTNET_BASE_URL
)

MARKETS_URL = "/all/config"
TICKER_URL = "/all/info"
SNAPSHOT_URL = "/instrument/:marketid/orderbook/"
TRADES_URL = "/instrument/:marketid/trade"


class LeverjPerpetualAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    __daobds__logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.__daobds__logger is None:
            cls.__daobds__logger = logging.getLogger(__name__)
        return cls.__daobds__logger

    def __init__(self, trading_pairs: List[str] = None, domain: str = "kovan", token_configuration = None):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()
        self._base_url = TESTNET_BASE_URL if domain == "kovan" else PERPETUAL_BASE_URL
        self._domain = domain
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()
        self.order_book_create_function = lambda: OrderBook()
        self.token_config: LeverjPerpetualAPITokenConfigurationDataSource = token_configuration

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{PERPETUAL_BASE_URL}{TICKER_URL}")
            resp_json = await resp.json()
            retval = {}
            #for key, value in resp_json.items():
            #    symbol = cls.token_config.get_symbol(key)
            #    if symbol in trading_pairs: 
            #        retval[symbol] = float(value['lastPrice'])
            return retval

    @property
    def order_book_class(self) -> LeverjPerpetualOrderBook:
        return LeverjPerpetualOrderBook

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str, level: int = 0) -> Dict[str, any]:
        market_id = self.token_config.get_marketid(trading_pair)
        async with client.get(f"{self._base_url}{SNAPSHOT_URL}".replace(":marketid", str(market_id))) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(
                    f"Error fetching leverj market snapshot for {trading_pair}. " f"HTTP status is {response.status}."
                )
            data: Dict[str, Any] = await response.json()
            data["trading_pair"] = trading_pair
            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = LeverjPerpetualOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"id": trading_pair, "rest": True}
            )
            order_book: OrderBook = self.order_book_create_function()
            bids = [ClientOrderBookRow(Decimal(bid["price"]), Decimal(bid["amount"]), snapshot_msg.update_id) for bid in snapshot_msg.bids]
            asks = [ClientOrderBookRow(Decimal(ask["price"]), Decimal(ask["amount"]), snapshot_msg.update_id) for ask in snapshot_msg.asks]
            order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
            return order_book

    '''
    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()
    '''

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        resp = requests.get(f"{PERPETUAL_BASE_URL}{TICKER_URL}")
        trading_pair_str = f"index_{convert_to_exchange_trading_pair(trading_pair)}"
        resp_json = resp.json()
        for key, value in resp_json:
            if value["index"]["topic"] == trading_pair_str:
                mid_price = value['vol24H']['high'] + value['vol24H']['low'] / 2
                return mid_price

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{LEVERJ_API_URL}{MARKETS_URL}", timeout=5) as response:
                    if response.status == 200:
                        res_json: Dict[str, Any] = await response.json()
                        all_trading_pairs = res_json["instruments"]
                        valid_trading_pairs: list = []
                        for key, val in all_trading_pairs.items():
                            trading_pair = convert_from_exchange_trading_pair(val['symbol'])
                            valid_trading_pairs.append(key)
                        return valid_trading_pairs
        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for dydx trading pairs
            pass

        return []

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass
        '''
        while True:
            try:
                trade_message_queue: asyncio.Queue = asyncio.Queue()
                client = LeverjPerpetualSocketIOClient(trade_message_queue, self._base_url, [('GET /instrument/1/trade', {})])
                await client.run_client()
                while True:
                    msg = await trade_message_queue.get()
                    if msg[0] not in ["orderbook", "difforderbook"]:
                        print(msg)
                        continue
                        async for raw_msg in self._inner_messages(ws):
                            msg = ujson.loads(raw_msg)
                            if "contents" in msg:
                                if "trades" in msg["contents"]:
                                    if msg["type"] == "channel_data":
                                        for datum in msg["contents"]["trades"]:
                                            msg["ts"] = time.time()
                                            trade_msg: OrderBookMessage = LeverjPerpetualOrderBook.trade_message_from_exchange(datum, msg)
                                            output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)
        '''
    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                diff_message_queue: asyncio.Queue = asyncio.Queue()
                client = LeverjPerpetualSocketIOClient(diff_message_queue, self._base_url)
                await client.run_client()
                while True:
                    msg = await diff_message_queue.get()
                    if msg[0] in ['difforderbook']: #'orderbook']:
                        if msg[0] == "difforderbook":
                            trading_pair = self.token_config.get_symbol(msg[1]["instrument"])
                            if trading_pair in self._trading_pairs:
                                msg[1]["trading_pair"] = trading_pair
                                market_id = self.token_config.get_marketid(trading_pair)
                                ts = time.time()
                                order_msg: OrderBookMessage = LeverjPerpetualOrderBook.diff_message_from_exchange(msg[1], ts, msg[1])
                                output.put_nowait(order_msg)
                        else:
                            for trading_pair in self._trading_pairs:
                                put_msg = {}
                                market_id = self.token_config.get_marketid(trading_pair)
                                put_msg["trading_pair"] = trading_pair

                                put_msg["rest"] = False
                                ts = time.time()
                                order_msg: OrderBookMessage = LeverjPerpetualOrderBook.snapshot_message_from_exchange(msg[1][market_id], ts, put_msg)
                                output.put_nowait(order_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass