from typing import Any, Dict, List
from pandas import DataFrame


def insert_column(
    dfs: Dict[str, DataFrame], column_name: str, value: List[Any] | Any, loc: int = 0
):
    for key in dfs.keys():
        dfs[key].insert(loc, column_name, value)
    return dfs
