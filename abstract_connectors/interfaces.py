from abc import ABC, abstractmethod
from pandas import DataFrame


class AbstractExtract(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data and return it as a DataFrame.
        """
        pass


class AbstractTransform(ABC):
    @abstractmethod
    def __init__(self, data: DataFrame, **kwargs):
        """
        Initialize with a DataFrame containing the data to transform.
        """
        self.data = data

    @abstractmethod
    def apply(self) -> DataFrame:
        """
        Apply transformations and return a transformed DataFrame.
        """
        pass


class AbstractLoad(ABC):
    @abstractmethod
    def __init__(self, data: DataFrame, **kwargs):
        """
        Initialize with a DataFrame containing the data to load.
        """
        self.data = data

    @abstractmethod
    def load(self):
        """
        Load the DataFrame data into the target destination.
        """
        pass
