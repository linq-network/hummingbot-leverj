import asyncio
import logging
# import sys
from collections import deque, defaultdict
from typing import (
    Optional,
    Deque,
    List,
    Dict,
    # Set
)
from decimal import Decimal
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_order_book import LeverjPerpetualOrderBook
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_order_book_message import LeverjPerpetualOrderBookMessage
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_api_order_book_data_source import LeverjPerpetualAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow


class LeverjPerpetualOrderBookTracker(OrderBookTracker):
    _dobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._dobt_logger is None:
            cls._dobt_logger = logging.getLogger(__name__)
        return cls._dobt_logger

    def __init__(
        self,
        trading_pairs: Optional[List[str]] = None,
        domain: str = None,
        leverj_auth: str = "",
        token_configuration = None
    ):
        super().__init__(
           LeverjPerpetualAPIOrderBookDataSource(
                trading_pairs=trading_pairs,
                token_configuration=token_configuration,
                domain=domain,
            ),
            trading_pairs)

        self._order_books: Dict[str, LeverjPerpetualOrderBook] = {}
        self._saved_message_queues: Dict[str, Deque[LeverjPerpetualOrderBookMessage]] = defaultdict(lambda: deque(maxlen=1000))
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()

    @property
    def exchange_name(self) -> str:
        return "leverj_perpetual"

    async def _track_single_book(self, trading_pair: str):
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: LeverjPerpetualOrderBook = self._order_books[trading_pair]
        while True:
            try:
                message: LeverjPerpetualOrderBookMessage = None
                saved_messages: Deque[LeverjPerpetualOrderBookMessage] = self._saved_message_queues[trading_pair]
                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()
                if message.type is OrderBookMessageType.DIFF:
                    bids = [ClientOrderBookRow(Decimal(bid["price"]), Decimal(bid["amount"]), message.update_id) for bid in message.bids]
                    asks = [ClientOrderBookRow(Decimal(ask["price"]), Decimal(ask["amount"]), message.update_id) for ask in message.asks]
                    order_book.apply_diffs(bids, asks, int(message.timestamp))

                elif message.type is OrderBookMessageType.SNAPSHOT:
                    bids = [ClientOrderBookRow(Decimal(bid["price"]), Decimal(bid["amount"]), message.update_id) for bid in message.bids]
                    asks = [ClientOrderBookRow(Decimal(ask["price"]), Decimal(ask["amount"]), message.update_id) for ask in message.asks]
                    order_book.apply_snapshot(bids, asks, int(message.timestamp))
                    self.logger().debug("Processed order book snapshot for %s.", trading_pair)

            except asyncio.CancelledError:
                raise
            except KeyError:
                pass
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds.",
                )
                await asyncio.sleep(5.0)