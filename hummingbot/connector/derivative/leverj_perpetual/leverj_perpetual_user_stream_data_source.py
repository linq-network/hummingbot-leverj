#!/usr/bin/env python

import asyncio
import aiohttp
import json
import logging
import time
from typing import (
    AsyncIterable,
    Optional
)
import ujson
import websockets
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_auth import LeverjPerpetualAuth
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_api_order_book_data_source import LeverjPerpetualAPIOrderBookDataSource
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_order_book import LeverjPerpetualOrderBook
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_socketio_client import LeverjPerpetualSocketIOClient

from hummingbot.connector.derivative.leverj_perpetual.constants import (
    PERPETUAL_BASE_URL,
    TESTNET_BASE_URL
)

class LeverjPerpetualUserStreamDataSource(UserStreamTrackerDataSource):

    _krausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._krausds_logger is None:
            cls._krausds_logger = logging.getLogger(__name__)
        return cls._krausds_logger

    def __init__(self, orderbook_tracker_data_source: LeverjPerpetualAPIOrderBookDataSource, leverj_auth: LeverjPerpetualAuth, domain: str = "perpetual"):
        self._leverj_auth: LeverjPerpetualAuth = leverj_auth
        self._orderbook_tracker_data_source: LeverjPerpetualAPIOrderBookDataSource = orderbook_tracker_data_source
        self._shared_client: Optional[aiohttp.ClientSession] = None
        self._last_recv_time: float = 0
        self._request_message_queue: asyncio.Queue = asyncio.Queue()
        self._account_message_queue: asyncio.Queue = asyncio.Queue()
        self._base_url = TESTNET_BASE_URL if domain == 'kovan' else PERPETUAL_BASE_URL
        super().__init__()

    @property
    def order_book_class(self):
        return LeverjPerpetualOrderBook

    @property
    def last_recv_time(self):
        return self._last_recv_time

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                headers = self._leverj_auth.generate_request_headers('GET', '/register', {})
                req_data = {
                    "headers": headers,
                    "method": 'GET',
                    "uri": "/register",
                    "params": {},
                    "body": {},
                    "retry": False
                }
                client = LeverjPerpetualSocketIOClient(self._account_message_queue,
                                                       self._base_url,
                                                       self._leverj_auth)
                await client.run_client()
                while True:
                    msg = await self._account_message_queue.get()
                    self._last_recv_time = time.time()

                    if msg[0] in ['order_add',
                                  'order_update',
                                  'order_del',
                                  'account_balance',
                                  'order_execution', 
                                  'position',
                                  'order_cancelled']:
                        output.put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with leverj WebSocket connection. "
                                    "Retrying after 30 seconds...", exc_info=True)
                await asyncio.sleep(30.0)
