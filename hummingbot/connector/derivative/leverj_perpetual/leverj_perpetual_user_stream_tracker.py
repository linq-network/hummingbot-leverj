#!/usr/bin/env python

import asyncio
import logging
from typing import (
    Optional
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_api_order_book_data_source import LeverjPerpetualAPIOrderBookDataSource
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_user_stream_data_source import LeverjPerpetualUserStreamDataSource
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_auth import LeverjPerpetualAuth


class LeverjPerpetualUserStreamTracker(UserStreamTracker):
    _krust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._krust_logger is None:
            cls._krust_logger = logging.getLogger(__name__)
        return cls._krust_logger

    def __init__(self,
                 orderbook_tracker_data_source: LeverjPerpetualAPIOrderBookDataSource,
                 leverj_auth: LeverjPerpetualAuth,
                 domain: str = "kovan"):
        super().__init__()
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None
        self._orderbook_tracker_data_source = orderbook_tracker_data_source
        self._leverj_auth: LeverjPerpetualAuth = leverj_auth
        self._domain = domain

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        if not self._data_source:
            self._data_source = LeverjPerpetualUserStreamDataSource(orderbook_tracker_data_source=self._orderbook_tracker_data_source,
                                                                    leverj_auth=self._leverj_auth,
                                                                    domain=self._domain)
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "leverj_perpetual"

    async def start(self):
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)