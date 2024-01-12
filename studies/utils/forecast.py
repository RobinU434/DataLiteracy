from datetime import datetime
import glob
from typing import Dict, Iterable, List
from pandas import DataFrame
import pandas as pd


def set_errors_to_zeros(value: float):
    """_summary_

    Args:
        value (float): _description_

    Returns:
        _type_: _description_
    """
    if value < -100:
        return 0
    return value


def to_int(l: Iterable) -> List[int]:
    """_summary_

    Args:
        l (Iterable): _description_

    Returns:
        List[int]: _description_
    """
    return list(map(lambda x: int(x), l))


def serialized_to_datetime(dt: str):
    """_summary_

    Args:
        dt (str): _description_

    Returns:
        _type_: _description_
    """
    date, time = dt.split(" ")
    year, month, day = to_int(date.split("-"))
    hour, minute, second = to_int(time.split(":"))
    return datetime(year, month, day, hour, minute, second)


def get_dwd_forecast(feature: str, source_path: str = "../data/dwd") -> DataFrame:
    """_summary_

    Args:
        feature (str): _description_

    Returns:
        DataFrame: _description_
    """
    files = glob.glob(f"{source_path}/csv/*/{feature}.csv")
    frames: List[pd.DataFrame] = []
    for file in files:
        temp_df = pd.read_csv(file)
        frames.append(temp_df)

    df = pd.concat(frames)

    # convert time to datetime
    df["call_time"] = df["call_time"].apply(serialized_to_datetime)
    df["time"] = df["time"].apply(serialized_to_datetime)
    df[feature] = df[feature].apply(set_errors_to_zeros)
    return df
