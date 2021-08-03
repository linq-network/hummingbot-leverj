from enum import Enum


class LeverjPerpetualOrderStatus(Enum):
    PENDING = 0
    ACTIVE = 100
    open = 101
    cancelled = 200
    done = 300
    FILLED = 301
    failed = 400
    deleted = 402
    expired = 403

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented
