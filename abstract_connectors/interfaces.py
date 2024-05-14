from abc import ABC, abstractmethod


class AbstractExtract(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def extract(self, data: list) -> list:
        pass


class AbstractTransform(ABC):
    @abstractmethod
    def __init__(self, data: list, **kwargs):
        self.data = data

    @abstractmethod
    def apply(self) -> list:
        pass


class AbstractLoad(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def load(self):
        pass
