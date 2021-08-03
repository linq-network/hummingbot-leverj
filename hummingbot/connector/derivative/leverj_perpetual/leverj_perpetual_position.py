from decimal import Decimal

from hummingbot.connector.derivative.position import Position
from hummingbot.core.event.events import (
    PositionSide,
    TradeType
)
from hummingbot.connector.derivative.leverj_perpetual.leverj_perpetual_in_flight_order import LeverjPerpetualInFlightOrder


class LeverjPerpetualPosition(Position):
    def __init__(self,
                 trading_pair: str,
                 position_side: PositionSide,
                 unrealized_pnl: Decimal,
                 entry_price: Decimal,
                 amount: Decimal,
                 leverage: Decimal,
                 is_open: bool = True):
        super().__init__(
            trading_pair,
            position_side,
            unrealized_pnl,
            entry_price,
            amount,
            leverage
        )
        self.is_open = is_open

    @property
    def leverage(self):
        return round(self._leverage, 2)

    @classmethod
    def from_leverj_fill(cls,
                         in_flight_order: LeverjPerpetualInFlightOrder,
                         amount: Decimal,
                         price: Decimal,
                         leverage: int):
        position_side, signed_amount = (PositionSide.LONG, amount) if in_flight_order.trade_type == TradeType.BUY else (PositionSide.SHORT, -amount)
        return LeverjPerpetualPosition(
            in_flight_order.trading_pair,
            position_side,
            Decimal('0'),
            price,
            signed_amount,
            leverage,
            True)

    def update_from_fill(self,
                         in_flight_order: LeverjPerpetualInFlightOrder,
                         price: Decimal,
                         amount: Decimal):
        if self.position_side == PositionSide.SHORT:
            if in_flight_order.trade_type == TradeType.BUY:
                self._amount += amount
            elif in_flight_order.trade_type == TradeType.SELL:
                total_quote: Decimal = (self.entry_price * abs(self.amount)) + (price * amount)
                self._amount -= amount
                self._entry_price: Decimal = total_quote / abs(self.amount)
        elif self.position_side == PositionSide.LONG:
            if in_flight_order.trade_type == TradeType.BUY:
                total_quote: Decimal = (self.entry_price * self.amount) + (price * amount)
                self._amount += amount
                self._entry_price: Decimal = total_quote / self.amount
            elif in_flight_order.trade_type == TradeType.SELL:
                self._amount -= amount

    def update_position(self,
                        entry_price,
                        amount,
                        leverage):
        self._leverage = leverage
        self._amount = amount
        self._entry_price = entry_price
