import aiohttp
import asyncio
from datetime import datetime
import json
import time
import logging
from collections import defaultdict
from decimal import Decimal
from typing import (
    Any,
    Dict,
    List,
    Optional,
    AsyncIterable
)
from dateutil.parser import parse as dataparse

from hummingbot.client.config.fee_overrides_config_map import fee_overrides_config_map
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.core.event.event_listener import EventListener
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.perpetual_trading import PerpetualTrading
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_auth import LeverjPerpetualAuth
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_fill_report import LeverjPerpetualFillReport
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_in_flight_order import LeverjPerpetualInFlightOrder
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_order_book_tracker import LeverjPerpetualOrderBookTracker
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_position import LeverjPerpetualPosition
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_user_stream_tracker import LeverjPerpetualUserStreamTracker
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_token_configuration import LeverjPerpetualAPITokenConfigurationDataSource
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_socketio_client import LeverjPerpetualSocketIOClient
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_order_signer import sign_order

from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
)
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    MarketOrderFailureEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    FundingPaymentCompletedEvent,
    TradeType,
    OrderType,
    TradeFee,
    PositionAction,
    PositionSide,
    PositionMode
)
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.utils.estimate_fee import estimate_fee
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

from hummingbot.connector.derivative.leverj_perpetual.constants import (
    PERPETUAL_BASE_URL,
    TESTNET_BASE_URL
)

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


def now():
    return int(time.time()) * 1000


BUY_ORDER_COMPLETED_EVENT = MarketEvent.BuyOrderCompleted
SELL_ORDER_COMPLETED_EVENT = MarketEvent.SellOrderCompleted
ORDER_CANCELLED_EVENT = MarketEvent.OrderCancelled
ORDER_EXPIRED_EVENT = MarketEvent.OrderExpired
ORDER_FILLED_EVENT = MarketEvent.OrderFilled
ORDER_FAILURE_EVENT = MarketEvent.OrderFailure
MARKET_FUNDING_PAYMENT_COMPLETED_EVENT_TAG = MarketEvent.FundingPaymentCompleted
BUY_ORDER_CREATED_EVENT = MarketEvent.BuyOrderCreated
SELL_ORDER_CREATED_EVENT = MarketEvent.SellOrderCreated
API_CALL_TIMEOUT = 10.0

# ==========================================================

MARKETS_INFO_ROUTE = '/all/info'
UNRECOGNIZED_ORDER_DEBOUCE = 10


class LatchingEventResponder(EventListener):
    def __init__(self, callback: any, num_expected: int):
        super().__init__()
        self._callback = callback
        self._completed = asyncio.Event()
        self._num_remaining = num_expected

    def __call__(self, arg: any):
        if self._callback(arg):
            self._reduce()

    def _reduce(self):
        self._num_remaining -= 1
        if self._num_remaining <= 0:
            self._completed.set()

    async def wait_for_completion(self, timeout: float):
        try:
            await asyncio.wait_for(self._completed.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return self._completed.is_set()

    def cancel_one(self):
        self._reduce()


class LeverjPerpetualDerivativeTransactionTracker(TransactionTracker):

    def __init__(self, owner):
        super().__init__()
        self._owner = owner

    def did_timeout_tx(self, tx_id: str):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.did_timeout_tx(tx_id)


class LeverjPerpetualDerivative(ExchangeBase, PerpetualTrading):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self,
                 leverj_perpetual_api_key: str,
                 leverj_perpetual_api_secret: str,
                 leverj_perpetual_account_number: int,
                 poll_interval: float = 10.0,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = "leverj_perpetual"):

        super().__init__()

        self._real_time_balance_update = True
        self._token_configuration = LeverjPerpetualAPITokenConfigurationDataSource(domain=domain)
        self.API_REST_ENDPOINT = TESTNET_BASE_URL if (domain == "kovan") else PERPETUAL_BASE_URL
        self._order_book_tracker = LeverjPerpetualOrderBookTracker(
            trading_pairs=trading_pairs,
            token_configuration=self._token_configuration,
            domain=domain,
        )
        self._tx_tracker = LeverjPerpetualDerivativeTransactionTracker(self)
        self._trading_required = trading_required
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._poll_interval = poll_interval
        self._shared_client = None
        self._polling_update_task = None

        # State
        self._leverj_auth = LeverjPerpetualAuth(leverj_perpetual_api_key,
                                                leverj_perpetual_account_number,
                                                leverj_perpetual_api_secret)
        self._user_stream_tracker = LeverjPerpetualUserStreamTracker(
            orderbook_tracker_data_source=self._order_book_tracker.data_source,
            leverj_auth=self._leverj_auth,
            domain=domain
        )

        self._user_stream_event_listener_task = None
        self._user_stream_tracker_task = None
        self._lock = asyncio.Lock()
        self._trading_rules = {}
        self._in_flight_orders = {}
        self._trading_pairs = trading_pairs
        self._fee_rules = {}
        self._fee_override = ("leverj_maker_fee_amount" in fee_overrides_config_map)
        self._reserved_balances = {}
        self._unclaimed_fills = defaultdict(set)
        self._in_flight_orders_by_exchange_id = {}
        self._orders_pending_ack = set()
        self._account_positions = {}
        self._position_mode = PositionMode.ONEWAY
        self._margin_fractions = {}
        self._funding_info = {}
        self._leverage = {}

    @property
    def name(self) -> str:
        return "leverj_perpetual"

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balances": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self._trading_required else True,
            "funding_info_available": len(self._funding_info) > 0 if self._trading_required else True,
        }

    # ----------------------------------------
    # Markets & Order Books

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    def get_order_book(self, trading_pair: str):
        order_books = self._order_book_tracker.order_books
        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    @property
    def limit_orders(self) -> List[LimitOrder]:
        retval = []

        for in_flight_order in self._in_flight_orders.values():
            leverj_flight_order = in_flight_order
            if leverj_flight_order.order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]:
                retval.append(leverj_flight_order.to_limit_order())
        return retval

    # ----------------------------------------
    # Account Balances

    def get_balance(self, currency: str):
        return self._account_balances.get(currency, Decimal(0))

    def get_available_balance(self, currency: str):
        return self._account_available_balances.get(currency, Decimal(0))

    # ==========================================================
    # Order Submission
    # ----------------------------------------------------------

    @property
    def in_flight_orders(self) -> Dict[str, LeverjPerpetualInFlightOrder]:
        return self._in_flight_orders

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _set_exchange_id(self, in_flight_order, exchange_order_id):
        in_flight_order.update_exchange_order_id(exchange_order_id)
        self._in_flight_orders_by_exchange_id[exchange_order_id] = in_flight_order

        # Claim any fill reports for this order that came in while we awaited this exchange id
        if exchange_order_id in self._unclaimed_fills:
            for fill in self._unclaimed_fills[exchange_order_id]:
                in_flight_order.register_fill(fill.id, fill.amount, fill.price)
                pair = in_flight_order.trading_pair
                position = self._account_positions.get(pair, None)
                if position is not None:
                    position.update_from_fill(in_flight_order, fill.price, fill.amount)
                    if (position.position_side == PositionSide.SHORT and position.amount < Decimal('0')) or (position.position_side == PositionSide.LONG and position.amount > Decimal('0')):
                        continue
                    else:
                        del self._account_positions[pair]
                        continue
                else:
                    self._account_positions[pair] = LeverjPerpetualPosition.from_leverj_fill(
                       in_flight_order,
                       fill.amount,
                       fill.price,
                       self._leverage[pair] 
                    )


            del self._unclaimed_fills[exchange_order_id]

        self._orders_pending_ack.discard(in_flight_order.client_order_id)
        if len(self._orders_pending_ack) == 0:
            # We are no longer waiting on any exchange order ids, so all uncalimed fills can be discarded
            self._unclaimed_fills.clear()

    def get_margin_per_fraction(self, trading_pair, price):
        instrument_id = self._token_configuration.get_marketid(trading_pair)
        decimals = self._token_configuration.get_decimals(instrument_id)

        multiplier = Decimal(pow(Decimal(10), decimals))

        return int((price * multiplier) / self._leverage[trading_pair])


    async def place_order(self,
                          client_order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal) -> Dict[str, Any]:

        order_side = 'buy' if is_buy else 'sell'
        post_only = False
        if order_type is OrderType.LIMIT_MAKER:
            post_only = True
        leverj_order_type = 'LMT' if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER] else 'MKT'

        base, quote = trading_pair.split('-')
        instrument_id = self._token_configuration.get_marketid(trading_pair)
        data = {
            'accountId': str(self._leverj_auth.account_id),
            'originator': str(self._leverj_auth.api_key),
            'instrument': str(instrument_id),
            'price': float(price),
            'triggerPrice': "",
            'quantity': float(amount),
            'marginPerFraction': str(self.get_margin_per_fraction(trading_pair, price)),
            'side': order_side,
            'orderType': leverj_order_type,
            'timestamp': str(int(time.time()*1000000)),
            'quote': self._token_configuration.get_address(quote),
            'isPostOnly': False,
            'reduceOnly': False,
            'clientOrderId': client_order_id,
        }
        order_signature = sign_order(data,
                                     instrument_id,
                                     self._token_configuration.get_decimals(instrument_id),
                                     self._leverj_auth.secret_key)
        data['signature'] = order_signature

        return await self.api_request('POST', '/order', data=json.dumps([data], separators=(',', ':')))

    async def execute_order(self, order_side, client_order_id, trading_pair, amount, order_type, position_action, price):
        """
        Completes the common tasks from execute_buy and execute_sell.  Quantizes the order's amount and price, and
        validates the order against the trading rules before placing this order.
        """
        if position_action not in [PositionAction.OPEN, PositionAction.CLOSE]:
            raise ValueError("Specify either OPEN_POSITION or CLOSE_POSITION position_action.")
        # Quantize order

        amount = self.quantize_order_amount(trading_pair, amount)
        price = self.quantize_order_price(trading_pair, price)

        # Check trading rules
        if order_type.is_limit_type():
            trading_rule = self._trading_rules[trading_pair]
            if amount < trading_rule.min_order_size:
                amount = s_decimal_0
        elif order_type == OrderType.MARKET:
            trading_rule = self._trading_rules[trading_pair]
        if order_type.is_limit_type() and trading_rule.supports_limit_orders is False:
            raise ValueError("LIMIT orders are not supported")
        elif order_type == OrderType.MARKET and trading_rule.supports_market_orders is False:
            raise ValueError("MARKET orders are not supported")

        if amount < trading_rule.min_order_size:
            raise ValueError(f"Order amount({str(amount)}) is less than the minimum allowable amount({str(trading_rule.min_order_size)})")
        if amount > trading_rule.max_order_size:
            raise ValueError(f"Order amount({str(amount)}) is greater than the maximum allowable amount({str(trading_rule.max_order_size)})")
        if amount * price < trading_rule.min_notional_size:
            raise ValueError(f"Order notional value({str(amount*price)}) is less than the minimum allowable notional value for an order ({str(trading_rule.min_notional_size)})")

        try:
            created_at: int = int(time.time())
            self.start_tracking_order(order_side, client_order_id, order_type, created_at, None, trading_pair, price, amount, 1, position_action.name)
            try:
                creation_response = await self.place_order(client_order_id, trading_pair, amount, order_side is TradeType.BUY, order_type, price)
            except asyncio.TimeoutError:
                # We timed out while placing this order. We may have successfully submitted the order, or we may have had connection
                # issues that prevented the submission from taking place.

                # Note that if this order is live and we never recieved the exchange_order_id, we have no way of re-linking with this order
                # TODO: we can use the /v2/orders endpoint to get a list of orders that match the parameters of the lost orders and that will contain
                # the clientId that we have set. This can resync orders, but wouldn't be a garuntee of finding them in the list and would require a fair amout
                # of work in handling this re-syncing process
                # This would be somthing like
                # self._lost_orders.append(client_order_id) # add this here
                # ...
                # some polling loop:
                #   get_orders()
                #   see if any lost orders are in the returned orders and set the exchange id if so
                # ...

                # TODO: ensure this is the right exception from place_order with our wrapped library call...
                return
            except Exception as e:
                print(e)

            if "error" in creation_response:
                raise Exception(creation_response['error'])

            order = creation_response[0]
            status = order["status"]
            if status not in ['pending', 'open']:
                raise Exception(status)

            leverj_order_id = order["uuid"]
            in_flight_order = self._in_flight_orders.get(client_order_id)
            if in_flight_order is not None:
                self._set_exchange_id(in_flight_order, leverj_order_id)

                # Begin tracking order
                self.logger().info(
                    f"Created {in_flight_order.description} order {client_order_id} for {amount} {trading_pair}.")
            else:
                self.logger().info(
                    f"Created order {client_order_id} for {amount} {trading_pair}.")

        except Exception as e:
            self.logger().warning(f"Error submitting {order_side.name} {order_type.name} order to leverj for "
                                  f"{amount} {trading_pair} at {price}.")
            self.logger().info(e, exc_info=True)

            # Stop tracking this order
            self.stop_tracking_order(client_order_id)
            self.trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), client_order_id, order_type))

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          position_action: PositionAction,
                          price: Optional[Decimal] = Decimal('NaN')):
        try:
            await self.execute_order(TradeType.BUY, order_id, trading_pair, amount, order_type, position_action, price)
            self.trigger_event(BUY_ORDER_CREATED_EVENT,
                               BuyOrderCreatedEvent(now(), order_type, trading_pair, Decimal(amount), Decimal(price), order_id))

            # Issue any other events (fills) for this order that arrived while waiting for the exchange id
            tracked_order = self.in_flight_orders.get(order_id)
            if tracked_order is not None:
                self._issue_order_events(tracked_order)
        except ValueError as e:
            # never tracked, so no need to stop tracking
            self.trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), order_id, order_type))
            self.logger().warning(f"Failed to place {order_id} on leverj. {str(e)}")

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           position_action: PositionAction,
                           price: Optional[Decimal] = Decimal('NaN')):
        try:
            await self.execute_order(TradeType.SELL, order_id, trading_pair, amount, order_type, position_action, price)
            self.trigger_event(SELL_ORDER_CREATED_EVENT,
                               SellOrderCreatedEvent(now(), order_type, trading_pair, Decimal(amount), Decimal(price), order_id))

            # Issue any other events (fills) for this order that arrived while waiting for the exchange id
            tracked_order = self.in_flight_orders.get(order_id)
            if tracked_order is not None:
                self._issue_order_events(tracked_order)
        except ValueError as e:
            # never tracked, so no need to stop tracking
            self.trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), order_id, order_type))
            self.logger().warning(f"Failed to place {order_id} on leverj. {str(e)}")

    # ----------------------------------------
    # Cancellation

    async def cancel_order(self, client_order_id: str):
        in_flight_order = self._in_flight_orders.get(client_order_id)
        cancellation_event = OrderCancelledEvent(now(), client_order_id)
        exchange_order_id = in_flight_order.exchange_order_id

        if in_flight_order is None:
            self.logger().warning("Cancelled an untracked order {client_order_id}")
            self.trigger_event(ORDER_CANCELLED_EVENT, cancellation_event)
            return False

        try:
            if exchange_order_id is None:
                # Note, we have no way of canceling an order or querying for information about the order
                # without an exchange_order_id
                if in_flight_order.created_at < (int(time.time()) - UNRECOGNIZED_ORDER_DEBOUCE):
                    # We'll just have to assume that this order doesn't exist
                    self.stop_tracking_order(in_flight_order.client_order_id)
                    self.trigger_event(ORDER_CANCELLED_EVENT, cancellation_event)
                    return False
                else:
                    raise Exception(f"order {client_order_id} has no exchange id")
            cancel_res = await self.api_request('DELETE', f'/order/{exchange_order_id}')
            return True

        except Exception as e:
            if f"Order with specified id: {exchange_order_id} could not be found" in str(e):
                if in_flight_order.created_at < (int(time.time()) - UNRECOGNIZED_ORDER_DEBOUCE):
                    # Order didn't exist on exchange, mark this as canceled
                    self.stop_tracking_order(in_flight_order.client_order_id)
                    self.trigger_event(ORDER_CANCELLED_EVENT, cancellation_event)
                    return False
                else:
                    raise Exception(f"order {client_order_id} does not yet exist on the exchange and could not be cancelled.")
            else:
                self.logger().warning(f"Unable to cancel order {exchange_order_id}: {str(e)}")
                return False
        except Exception as e:
            self.logger().warning(f"Failed to cancel order {client_order_id}")
            self.logger().info(e)
            return False

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        cancellation_queue = self._in_flight_orders.copy()
        if len(cancellation_queue) == 0:
            return []

        order_status = {o.client_order_id: o.is_done for o in cancellation_queue.values()}

        def set_cancellation_status(oce: OrderCancelledEvent):
            if oce.order_id in order_status:
                order_status[oce.order_id] = True
                return True
            return False

        cancel_verifier = LatchingEventResponder(set_cancellation_status, len(cancellation_queue))
        self.add_listener(ORDER_CANCELLED_EVENT, cancel_verifier)

        for order_id, in_flight in cancellation_queue.items():
            try:
                if order_status[order_id]:
                    cancel_verifier.cancel_one()
                elif not await self.cancel_order(order_id):
                    # this order did not exist on the exchange
                    cancel_verifier.cancel_one()
                    order_status[order_id] = True
            except Exception:
                cancel_verifier.cancel_one()
                order_status[order_id] = True

        await cancel_verifier.wait_for_completion(timeout_seconds)
        self.remove_listener(ORDER_CANCELLED_EVENT, cancel_verifier)

        return [CancellationResult(order_id=order_id, success=success) for order_id, success in order_status.items()]

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal):
        is_maker = order_type is OrderType.LIMIT
        return estimate_fee("leverj_perpetual", is_maker)

    # ==========================================================
    # Runtime
    # ----------------------------------------------------------

    def start(self, clock: Clock, timestamp: float):
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        super().stop(clock)

    async def start_network(self):
        await self.stop_network()
        await self._token_configuration._configure()
        self._order_book_tracker.start()
        if self._trading_required:
            self._polling_update_task = safe_ensure_future(self._polling_update())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._polling_update_task is not None:
            self._polling_update_task.cancel()
            self._polling_update_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        try:
            await self.api_request("GET", MARKETS_INFO_ROUTE)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    # ----------------------------------------
    # State Management

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        for order_id, in_flight_repr in saved_states.items():
            in_flight_json: Dict[str, Any] = json.loads(in_flight_repr)
            order = LeverjPerpetualInFlightOrder.from_json(in_flight_json)
            if not order.is_done:
                self._in_flight_orders[order_id] = order

    def start_tracking_order(self,
                             order_side: TradeType,
                             client_order_id: str,
                             order_type: OrderType,
                             created_at: int,
                             hash: str,
                             trading_pair: str,
                             price: Decimal,
                             amount: Decimal,
                             leverage: int,
                             position: str):
        in_flight_order = LeverjPerpetualInFlightOrder.from_leverj_order(
            order_side,
            client_order_id,
            order_type,
            created_at,
            None,
            trading_pair,
            price,
            amount,
            leverage,
            position)
        self._in_flight_orders[in_flight_order.client_order_id] = in_flight_order
        self._orders_pending_ack.add(client_order_id)

        old_reserved = self._reserved_balances.get(in_flight_order.reserved_asset, Decimal(0))
        new_reserved = old_reserved + in_flight_order.reserved_balance
        self._reserved_balances[in_flight_order.reserved_asset] = new_reserved
        self._account_available_balances[in_flight_order.reserved_asset] = \
            max(self._account_balances.get(in_flight_order.reserved_asset, Decimal(0)) - new_reserved, Decimal(0))

    def stop_tracking_order(self, client_order_id: str):
        in_flight_order = self._in_flight_orders.get(client_order_id)
        if in_flight_order is not None:
            old_reserved = self._reserved_balances.get(in_flight_order.reserved_asset, Decimal(0))
            new_reserved = max(old_reserved - in_flight_order.reserved_balance, Decimal(0))
            self._reserved_balances[in_flight_order.reserved_asset] = new_reserved
            self._account_available_balances[in_flight_order.reserved_asset] = \
                max(self._account_balances.get(in_flight_order.reserved_asset, Decimal(0)) - new_reserved, Decimal(0))
            if in_flight_order.exchange_order_id is not None and in_flight_order.exchange_order_id in self._in_flight_orders_by_exchange_id:
                del self._in_flight_orders_by_exchange_id[in_flight_order.exchange_order_id]
            if client_order_id in self._in_flight_orders:
                del self._in_flight_orders[client_order_id]
            if client_order_id in self._orders_pending_ack:
                self._orders_pending_ack.remove(client_order_id)

    def get_order_by_exchange_id(self, exchange_order_id: str):
        if exchange_order_id in self._in_flight_orders_by_exchange_id:
            return self._in_flight_orders_by_exchange_id[exchange_order_id]

        for o in self._in_flight_orders.values():
            if o.exchange_order_id == exchange_order_id:
                return o

        return None

    # ----------------------------------------
    # updates to orders and balances

    def _issue_order_events(self, tracked_order: LeverjPerpetualInFlightOrder):
        issuable_events: List[MarketEvent] = tracked_order.get_issuable_events()
        # Issue relevent events
        for (market_event, new_amount, new_price, new_fee) in issuable_events:
            if new_amount is not None:
                base, quote = (tracked_order.trading_pair).split('-')
                trade_fee = self.get_fee(
                    base,
                    quote,
                    tracked_order.order_type,
                    tracked_order.trade_type,
                    new_amount,
                    new_price
                )
                new_fee = new_price * new_amount * trade_fee.percent
            if market_event == MarketEvent.OrderCancelled:
                self.logger().info(f"Successfully cancelled order {tracked_order.client_order_id}")
                self.stop_tracking_order(tracked_order.client_order_id)
                self.trigger_event(ORDER_CANCELLED_EVENT,
                                   OrderCancelledEvent(self.current_timestamp,
                                                       tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderFilled:
                self.trigger_event(ORDER_FILLED_EVENT,
                                   OrderFilledEvent(self.current_timestamp,
                                                    tracked_order.client_order_id,
                                                    tracked_order.trading_pair,
                                                    tracked_order.trade_type,
                                                    tracked_order.order_type,
                                                    new_price,
                                                    new_amount,
                                                    #TODO Fix TradeFee. Use default
                                                    TradeFee(Decimal(0), [(tracked_order.fee_asset, new_fee)]),
                                                    tracked_order.client_order_id,
                                                    self._leverage.get(tracked_order.trading_pair, 1),
                                                    tracked_order.position))
            elif market_event == MarketEvent.OrderExpired:
                self.logger().info(f"The market order {tracked_order.client_order_id} has expired according to "
                                   f"order status API.")
                self.stop_tracking_order(tracked_order.client_order_id)
                self.trigger_event(ORDER_EXPIRED_EVENT,
                                   OrderExpiredEvent(self.current_timestamp,
                                                     tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderFailure:
                self.logger().info(f"The market order {tracked_order.client_order_id} has failed according to "
                                   f"order status API.")
                self.stop_tracking_order(tracked_order.client_order_id)
                self.trigger_event(ORDER_FAILURE_EVENT,
                                   MarketOrderFailureEvent(self.current_timestamp,
                                                           tracked_order.client_order_id,
                                                           tracked_order.order_type))
            elif market_event == MarketEvent.BuyOrderCompleted:
                self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                   f"according to user stream.")
                self.stop_tracking_order(tracked_order.client_order_id)
                self.trigger_event(BUY_ORDER_COMPLETED_EVENT,
                                   BuyOrderCompletedEvent(self.current_timestamp,
                                                          tracked_order.client_order_id,
                                                          tracked_order.base_asset,
                                                          tracked_order.quote_asset,
                                                          tracked_order.fee_asset,
                                                          tracked_order.executed_amount_base,
                                                          tracked_order.executed_amount_quote,
                                                          tracked_order.fee_paid,
                                                          tracked_order.order_type))
            elif market_event == MarketEvent.SellOrderCompleted:
                self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                   f"according to user stream.")
                self.stop_tracking_order(tracked_order.client_order_id)
                self.trigger_event(SELL_ORDER_COMPLETED_EVENT,
                                   SellOrderCompletedEvent(self.current_timestamp,
                                                           tracked_order.client_order_id,
                                                           tracked_order.base_asset,
                                                           tracked_order.quote_asset,
                                                           tracked_order.fee_asset,
                                                           tracked_order.executed_amount_base,
                                                           tracked_order.executed_amount_quote,
                                                           tracked_order.fee_paid,
                                                           tracked_order.order_type))

    async def _get_funding_info(self, trading_pair):
        markets_info = (await self.api_request('GET', '/all/info'))
        market_id = self._token_configuration.get_marketid(trading_pair)
        decimals = self._token_configuration.get_decimals(market_id)

        multiplier = Decimal(pow(Decimal(10), -decimals))

        self._funding_info[trading_pair] = {"indexPrice": markets_info[market_id]['index']['price'],
                                            "nextFundingTime": markets_info[market_id]['fundingRate']['end'],
                                            "rate": multiplier * Decimal(markets_info[market_id]['fundingRate']['rate'])}

    async def _update_funding_rates(self):
        try:
            for trading_pair in self._trading_pairs:
                await self._get_funding_info(trading_pair)
        except Exception as e:
            print(e)
            self.logger().warning(
                "Unknown error. Retrying after 1 seconds.",
                exc_info=True,
                app_warning_msg=f"Could not fetch funding_rate for {trading_pair}. Check API key and network connection."
            )

    def get_funding_info(self, trading_pair):
        return self._funding_info[trading_pair]

    def set_hedge_mode(self, position_mode: PositionMode):
        # leverj only allows one-way futures
        pass

    async def _set_balances(self, updates, is_snapshot=False):
        try:
            async with self._lock:
                for key, value in updates.items():
                    currency = value['symbol']
                    decimals = self._token_configuration.get_decimals_by_currency(currency)
                    mult: Decimal = Decimal(pow(Decimal(10), -decimals))
                    self._account_balances[value['symbol']] = Decimal(value['plasma']) * mult
                    self._account_available_balances[value['symbol']] = Decimal(value['available']) * mult
        except Exception as e:
            self.logger().error(f"Could not set balance {repr(e)}", exc_info=True)

    # ----------------------------------------
    # User stream updates

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from leverj. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event: Dict[str, Any] = event_message[0]
                data: Dict[str, Any] = event_message[1]
                if event == 'account_balance':
                    await self._set_balances(data, is_snapshot=False)
                elif event == 'order_add':
                    for order in data['result']:
                        exchange_order_id: str = order['uuid']

                        tracked_order: LeverjPerpetualInFlightOrder = self.get_order_by_exchange_id(exchange_order_id)

                        if tracked_order is None:
                            self.logger().debug(f"Unrecognized order ID from user stream: {exchange_order_id}.")
                            self.logger().debug(f"Event: {event_message}")
                            continue

                        # update the tracked order
                        tracked_order.update(order)
                        if not tracked_order.fills_covered():
                            # We're missing fill reports for this order, so poll for them as well
                            await self._update_fills(tracked_order)
                        self._issue_order_events(tracked_order)
                elif event == 'order_del':
                    for order in data['result']:
                        exchange_order_id: str = order

                        tracked_order: LeverjPerpetualInFlightOrder = self.get_order_by_exchange_id(exchange_order_id)

                        if tracked_order is None:
                            self.logger().debug(f"Unrecognized order ID from user stream: {exchange_order_id}.")
                            self.logger().debug(f"Event: {event_message}")
                            continue

                        await self._update_fills(tracked_order)
                        if not tracked_order.is_done:
                            tracked_order.order_deleted()
                        self._issue_order_events(tracked_order)
                elif event == 'position':
                    self._set_account_positions([data])
                elif event == 'order_execution':
                    exchange_order_id: str = data['orderId']

                    tracked_order: LeverjPerpetualInFlightOrder = self.get_order_by_exchange_id(exchange_order_id)

                    if tracked_order is None:
                        self.logger().debug(f"Unrecognized order ID from user stream: {exchange_order_id}.")
                        self.logger().debug(f"Event: {event_message}")
                        continue

                    self._set_fills([data], tracked_order)
                    self._issue_order_events(tracked_order)
                elif event == 'order_cancelled':
                    for order in data:
                        exchange_order_id: str = order['uuid']

                        tracked_order: LeverjPerpetualInFlightOrder = self.get_order_by_exchange_id(exchange_order_id)

                        if tracked_order is None:
                            self.logger().debug(f"Unrecognized order ID from user stream: {exchange_order_id}.")
                            self.logger().debug(f"Event: {event_message}")
                            continue

                        tracked_order.update(order)
                        if not tracked_order.fills_covered():
                            # We're missing fill reports for this order, so poll for them as well
                            await self._update_fills(tracked_order)
                        self._issue_order_events(tracked_order)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    # ----------------------------------------
    # Polling Updates

    async def _polling_update(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await asyncio.gather(
                    self._update_balances(),
                    self._update_trading_rules(),
                    self._update_order_status(),
                    self._update_account_positions(),
                    self._update_funding_rates(),
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning("Failed to fetch updates on leverj. Check network connection.")
                self.logger().warning(e)

    async def _update_account_positions(self):
        current_positions = await self.api_request('GET', '/account/position')
        self._set_account_positions(current_positions)
        
    def _set_account_positions(self, current_positions):
        for position in current_positions:
            instrument_id = position["instrument"]
            position_str = self._token_configuration.get_symbol(instrument_id)
            size: Decimal = Decimal(position['size'])
            # TODO figure out how to use this info

    async def _update_balances(self):
        current_balances = await self.api_request('GET', '/account/balance')
        await self._set_balances(current_balances, True)

    async def _update_trading_rules(self):
        markets_info = (await self.api_request('GET', '/all/config'))["instruments"]
        for market_id, info in markets_info.items():
            market = self._token_configuration.get_symbol(market_id)
            try:
                self._trading_rules[market] = TradingRule(
                    trading_pair = market,
                    min_order_size = Decimal(f"10e{-(info['baseSignificantDigits'] + 1)}"),
                    min_price_increment = Decimal(str(info['tickSize'])),
                    min_base_amount_increment = Decimal(f"10e{-(info['baseSignificantDigits'] + 1)}"),
                    min_notional_size = Decimal(f"10e{-(info['baseSignificantDigits'] + 1)}") * Decimal(info['tickSize']),
                    supports_limit_orders = True,
                    supports_market_orders = True
                )
                if market in self._leverage:
                    if self._leverage[market] > int(info['maxLeverage']):
                        self._leverage[market] = int(info['maxLeverage'])
            except Exception as e:
                self.logger().warning("Error updating trading rules")
                self.logger().warning(str(e))

    async def _update_order_status(self):
        tracked_orders = self._in_flight_orders.copy()
        for client_order_id, tracked_order in tracked_orders.items():
            leverj_order_id = tracked_order.exchange_order_id
            if leverj_order_id is None:
                # This order is still pending acknowledgement from the exchange
                if tracked_order.created_at < (int(time.time()) - UNRECOGNIZED_ORDER_DEBOUCE):
                    # this order should have a leverj_order_id at this point. If it doesn't, we should cancel it
                    # as we won't be able to poll for updates
                    try:
                        self.cancel_order(client_order_id)
                    except Exception:
                        pass
                continue

            leverj_order_request = None
            try:
                leverj_order_request = await self.api_request('GET', f"/order/{leverj_order_id}")
                if leverj_order_request == 'Not Found':
                    # we need to check for fills on this order that may not have been caught on the
                    # websocket
                    if tracked_order.created_at < (int(time.time()) - UNRECOGNIZED_ORDER_DEBOUCE):
                        try:
                            await self._update_fills(tracked_order)
                            if not tracked_order.is_done:
                                tracked_order.order_deleted()
                            self._issue_order_events(tracked_order)
                        except Exception:
                            continue
                data = leverj_order_request[0]
            except Exception:
                self.logger().warning(f"Failed to fetch tracked leverj order "
                                      f"{client_order_id }({tracked_order.exchange_order_id}) from api "
                                      f"(code: {leverj_order_request['resultInfo']['code'] if leverj_order_request is not None else 'None'})")

                # check if this error is because the api cliams to be unaware of this order. If so, and this order
                # is reasonably old, mark the order as cancelled
                continue

            try:
                if isinstance(data, dict):
                    tracked_order.update(data)
                    if not tracked_order.fills_covered():
                        # We're missing fill reports for this order, so poll for them as well
                        await self._update_fills(tracked_order)
                    self._issue_order_events(tracked_order)
            except Exception as e:
                self.logger().warning(f"Failed to update leverj order {tracked_order.exchange_order_id}")
                self.logger().warning(e)

    async def _update_fills(self, tracked_order: LeverjPerpetualInFlightOrder):
        try:
            data = await self.api_request('GET', '/account/execution')
            self._set_fills(data, tracked_order)

        except Exception as e:
            print(e)
            self.logger().warning(f"Unable to poll for fills for order {tracked_order.client_order_id}"
                                  f"(tracked_order.exchange_order_id): {e.status} {e.msg}")
        except KeyError:
            self.logger().warning(f"Unable to poll for fills for order {tracked_order.client_order_id}"
                                  f"(tracked_order.exchange_order_id): unexpected response data {data}")

    def _set_fills(self, data, tracked_order):
        for fill in data:
            if fill['orderId'] == tracked_order.exchange_order_id:
                id = fill['executionId']
                amount = Decimal(str(fill['quantity']))
                price = Decimal(str(fill['price']))
                fill_registered = tracked_order.register_fill(id, amount, price)
                if fill_registered:
                    pair = tracked_order.trading_pair
                    position = self._account_positions.get(pair, None)
                    if position is not None:
                        position.update_from_fill(tracked_order, price, amount)
                        if (position.position_side == PositionSide.SHORT and position.amount < Decimal('0')) or (position.position_side == PositionSide.LONG and position.amount > Decimal('0')):
                            continue
                        else:
                            del self._account_positions[pair]
                            continue
                    else:
                        self._account_positions[pair] = LeverjPerpetualPosition.from_leverj_fill(
                           tracked_order,
                           amount,
                           price,
                           self._leverage[pair] 
                        )

    def set_leverage(self, trading_pair: str, leverage: int = 1):
        safe_ensure_future(self._set_leverage(trading_pair, leverage))

    async def _set_leverage(self, trading_pair: str, leverage: int = 1):
        self._leverage[trading_pair] = leverage
        # necessary adjustments will be made in updates to trading rules

    async def _get_position_mode(self):
        self._position_mode = PositionMode.ONEWAY

        return self._position_mode

    def supported_position_modes(self):
        return [PositionMode.ONEWAY]

    def set_position_mode(self, position_mode: PositionMode):
        self._position_mode = PositionMode.ONEWAY

    # ==========================================================
    # Miscellaneous
    # ----------------------------------------------------------

    def get_order_price_quantum(self, trading_pair: str, price: Decimal):
        return self._trading_rules[trading_pair].min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal):
        return self._trading_rules[trading_pair].min_base_amount_increment

    def quantize_order_price(self, trading_pair: str, price: Decimal):
        return price.quantize(self.get_order_price_quantum(trading_pair, price))

    def quantize_order_amount(self, trading_pair: str, amount: Decimal, price: Decimal = Decimal('0')):
        quantized_amount = amount.quantize(self.get_order_size_quantum(trading_pair, amount))

        rules = self._trading_rules[trading_pair]

        if quantized_amount < rules.min_order_size:
            return s_decimal_0

        if price > 0 and price * quantized_amount < rules.min_notional_size:
            return s_decimal_0

        return quantized_amount

    def tick(self, timestamp: float):
        last_tick = self._last_timestamp / self._poll_interval
        current_tick = timestamp / self._poll_interval
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def api_request(self,
                          http_method: str,
                          url: str,
                          data: Optional[Dict[str, Any]] = None,
                          params: Optional[Dict[str, Any]] = None,
                          headers: Optional[Dict[str, str]] = {},
                          secure: bool = False) -> Dict[str, Any]:

        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()

        headers = self._leverj_auth.generate_request_headers(http_method, url, headers, data, params)

        full_url = f"{self.API_REST_ENDPOINT}{url}"

        async with self._shared_client.request(http_method, url=full_url,
                                               timeout=API_CALL_TIMEOUT,
                                               data=data, params=params, headers=headers) as response:
            if response.status > 299:
                if (await response.text() == 'Not Found'):
                    return 'Not Found'
                if url == '/order' and http_method == 'POST':
                    return await response.json()
                self.logger().info(f"Issue with leverj API {http_method} to {url}, response: ")
                self.logger().info(await response.text())
                raise IOError(f"Error fetching data from {full_url}. HTTP status is {response.status}.")
            data = await response.json()
            return data

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        tracking_nonce = get_tracking_nonce()
        client_order_id: str = str(f"buy-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_buy(client_order_id, trading_pair, amount, order_type, kwargs["position_action"], price))
        return client_order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        tracking_nonce = get_tracking_nonce()
        client_order_id: str = str(f"sell-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_sell(client_order_id, trading_pair, amount, order_type, kwargs["position_action"], price))
        return client_order_id

    def cancel(self, trading_pair: str, client_order_id: str):
        return safe_ensure_future(self.cancel_order(client_order_id))

    # TODO: Implement
    async def close_position(self, trading_pair: str):
        pass