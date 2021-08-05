from decimal import Decimal


class LeverjPerpetualFillReport:
    def __init__(self, id: str, amount: Decimal, price: Decimal):
        self.id = id
        self.amount = amount
        self.price = price

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    def as_dict(self):
        return {
            "id": self.id,
            "amount": str(self.amount),
            "price": str(self.price)
        }

    @property
    def value(self) -> Decimal:
        return self.amount * self.price