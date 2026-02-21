from abc import ABC


class Sliceable(ABC):
    @property
    def slice(self) -> object:
        return
