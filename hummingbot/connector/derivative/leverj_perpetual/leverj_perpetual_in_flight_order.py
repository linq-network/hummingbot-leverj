import json
from typing import (Any, Dict, List)
from decimal import Decimal
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_order_status import LeverjPerpetualOrderStatus
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_fill_report import LeverjPerpetualFillReport
from hummingbot.connector.in_flight_order_base import InFlightOrderBase
from hummingbot.core.event.events import (TradeType, OrderType, MarketEvent)


class LeverjPerpetualInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: LeverjPerpetualOrderStatus,
                 filled_size: Decimal,
                 filled_volume: Decimal,
                 filled_fee: Decimal,
                 created_at: int,
                 leverage: int,
                 position: str):

        super().__init__(client_order_id=client_order_id,
                         exchange_order_id=exchange_order_id,
                         trading_pair=trading_pair,
                         order_type=order_type,
                         trade_type=trade_type,
                         price=price,
                         amount=amount,
                         initial_state = str(initial_state))
        self.status = initial_state
        self.created_at = created_at
        self._last_executed_amount_from_order_status = Decimal(0)
        self.executed_amount_base = filled_size
        self.executed_amount_quote = filled_volume
        self.fee_paid = filled_fee
        self.fills = set()
        self._queued_events = []
        self._queued_fill_events = []
        self._completion_sent = False
        self.leverage = leverage
        self.position = position

        (base, quote) = trading_pair.split('-')
        self.fee_asset = quote
        self.reserved_asset = quote if trade_type is TradeType.BUY else base

    @property
    def is_done(self) -> bool:
        return self.status >= LeverjPerpetualOrderStatus.done

    @property
    def is_cancelled(self) -> bool:
        return self.status in [LeverjPerpetualOrderStatus.deleted, LeverjPerpetualOrderStatus.expired]

    @property
    def is_failure(self) -> bool:
        return self.status >= LeverjPerpetualOrderStatus.failed

    @property
    def description(self):
        return f"{str(self.order_type).lower()} {str(self.trade_type).lower()}"

    @property
    def amount_remaining(self):
        return self.amount - self.executed_amount_base

    @property
    def reserved_balance(self):
        if self.trade_type is TradeType.SELL:
            return self.amount_remaining
        else:
            return self.amount_remaining * self.price

    def to_json(self):
        return json.dumps({
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "trading_pair": self.trading_pair,
            "order_type": self.order_type.name,
            "trade_type": self.trade_type.name,
            "price": str(self.price),
            "amount": str(self.amount),
            "status": self.status.name,
            "executed_amount_base": str(self.executed_amount_base),
            "executed_amount_quote": str(self.executed_amount_quote),
            "fee_paid": str(self.fee_paid),
            "created_at": self.created_at,
            "leverage": self.leverage,
            "position": self.position,
            "fills": [f.as_dict() for f in self.fills],
            "_last_executed_amount_from_order_status": str(self._last_executed_amount_from_order_status),
        })

    @classmethod
    def from_json(cls, data: Dict[str, Any]):
        order = LeverjPerpetualInFlightOrder(
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            OrderType[data["order_type"]],
            TradeType[data["trade_type"]],
            Decimal(data["price"]),
            Decimal(data["amount"]),
            LeverjPerpetualOrderStatus[data["status"]],
            Decimal(data["executed_amount_base"]),
            Decimal(data["executed_amount_quote"]),
            Decimal(data["fee_paid"]),
            data["created_at"],
            data["leverage"],
            data["position"]
        )
        for fill in data["fills"]:
            order.fills.add(LeverjPerpetualFillReport(fill['id'], Decimal(fill['amount']), Decimal(fill['price'])))
        order._last_executed_amount_from_order_status = Decimal(data['_last_executed_amount_from_order_status'])

        return order

    @classmethod
    def from_leverj_order(cls,
                        side: TradeType,
                        client_order_id: str,
                        order_type: OrderType,
                        created_at: int,
                        hash: str,
                        trading_pair: str,
                        price: Decimal,
                        amount: Decimal,
                        leverage: int,
                        position: str):
        return LeverjPerpetualInFlightOrder(
            client_order_id,
            hash,
            trading_pair,
            order_type,
            side,
            price,
            amount,
            LeverjPerpetualOrderStatus.PENDING,
            Decimal(0),
            Decimal(0),
            Decimal(0),
            created_at,
            leverage,
            position
        )

    def fills_covered(self) -> bool:
        return self.executed_amount_base == self._last_executed_amount_from_order_status

    def _enqueue_completion_event(self):
        if (not self._completion_sent and
                self.executed_amount_base == self.amount):
            self._queued_events.append((MarketEvent.BuyOrderCompleted if self.trade_type is TradeType.BUY else MarketEvent.SellOrderCompleted,
                                        self.executed_amount_base,
                                        self.executed_amount_quote,
                                        self.fee_paid))
            self.status = LeverjPerpetualOrderStatus.FILLED
            self._completion_sent = True

    # returns true if fill is new false otherwise
    def register_fill(self, id: str, amount: Decimal, price: Decimal):
        fill_ids = [x.id for x in self.fills]
        if id not in fill_ids:
            report = LeverjPerpetualFillReport(id, amount, price)
            self.fills.add(report)
            self.executed_amount_base += report.amount
            self.executed_amount_quote += report.value
            # enqueue the relevent events caused by this fill report
            self._queued_fill_events.append((MarketEvent.OrderFilled, amount, price, None))
            self._enqueue_completion_event()
            return True
        return False

    def get_issuable_events(self) -> List[Any]:
        # We can always issue our fill events
        events: List[Any] = self._queued_fill_events.copy()
        self._queued_fill_events.clear()

        if self.executed_amount_base >= self._last_executed_amount_from_order_status:
            # We have all the fill reports up to our observed order status, so we can issue all
            # order status update related events.
            events.extend(self._queued_events)
            self._queued_events.clear()
        return events

    def update(self, data: Dict[str, Any]) -> List[Any]:
        new_status: LeverjPerpetualOrderStatus = LeverjPerpetualOrderStatus[data["status"]]
        new_executed_amount_base: Decimal = Decimal(str(data["filled"]))
        average_price: Decimal = Decimal(str(data["averagePrice"]))
        new_executed_amount_quote: Decimal = average_price * new_executed_amount_base

        if new_executed_amount_base > self.executed_amount_base or new_executed_amount_quote > self.executed_amount_quote:
            diff_base: Decimal = new_executed_amount_base - self.executed_amount_base
            diff_quote: Decimal = new_executed_amount_quote - self.executed_amount_quote
            if diff_quote > Decimal(0):
                price: Decimal = diff_quote / diff_base
            else:
                price: Decimal = self.executed_amount_quote / self.executed_amount_base

        if not self.is_done and new_status == LeverjPerpetualOrderStatus.deleted:
            self._queued_events.append((MarketEvent.OrderCancelled, None, None, None))

        if not self.is_done and new_status == LeverjPerpetualOrderStatus.expired:
            self._queued_events.append((MarketEvent.OrderExpired, None, None, None))

        if not self.is_done and new_status == LeverjPerpetualOrderStatus.failed:
            self._queued_events.append( (MarketEvent.OrderFailure, None, None, None) )

        self.status = new_status
        self.last_state = str(new_status)

        self._last_executed_amount_from_order_status = new_executed_amount_base

        # check and enqueue our completion event if it is time to do so
        self._enqueue_completion_event()

        if self.exchange_order_id is None:
            self.update_exchange_order_id(str(data["uuid"]))

    def order_deleted(self):
          self.status = LeverjPerpetualOrderStatus['deleted']
          self._queued_events.append((MarketEvent.OrderCancelled, None, None, None))