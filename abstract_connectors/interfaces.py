from abc import ABC, abstractmethod


class AbstractExtract(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def extract(self) -> list:
        pass


class AbstractTransform(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

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
