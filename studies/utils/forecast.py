

from datetime import datetime
import glob
from typing import Dict, Iterable, List
from pandas import DataFrame
import pandas as pd

def set_errors_to_zeros(value: float):
    if value < -100:
        return 0
    return value

def to_int(l: Iterable) -> List[int]:
    return list(map(lambda x: int(x), l))

def serialized_to_datetime(dt: str):
    date, time = dt.split(" ")
    year, month, day = to_int(date.split("-"))
    hour, minute, second = to_int(time.split(":"))
    return datetime(year, month, day, hour, minute, second)

def get_dwd_forecast(feature: str) -> DataFrame:
    files = glob.glob(f"../data/dwd/forecast/*/{feature}.csv")
    frames: List[pd.DataFrame] = []
    for file in files:
        temp_df = pd.read_csv(file)
        frames.append(temp_df)

    df = pd.concat(frames)

    #convert time to datetime
    df["call_time"] = df["call_time"].apply(serialized_to_datetime)
    df["time"] = df["time"].apply(serialized_to_datetime)
    df[feature] = df[feature].apply(set_errors_to_zeros)
    return df