from abc import ABC, abstractmethod
from typing import List

from pandas import DataFrame


class Converter(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def to_df(self, data: object) -> List[DataFrame]:
        """convert data to a DataFrame

        Args:
            data (object): _description_

        Raises:
            NotImplementedError: if method is not implemented

        Returns:
            DataFrame: data in a DataFrame object
        """
        raise NotImplementedError
