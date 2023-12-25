from datetime import datetime
import glob
from typing import List
import pandas as pd
from pandas import DataFrame
from project.process.utils.download_dwd_data import FEATURE_STATION_PROPERTY_MAP

DATA_ROOT_DIRECTORY = "../data/dwd/recent"

def to_date_time(dt: str):
    dt = str(dt)
    year = int(dt[:4])
    month = int(dt[4:6])
    day = int(dt[6:8])
    hour = int(dt[8:])
    return datetime(year, month, day, hour, 0)


def get_recent(feature: str):
    path = (
        DATA_ROOT_DIRECTORY + f"/*{FEATURE_STATION_PROPERTY_MAP[feature]}*/produkt*.txt"
    )
    data_files = glob.glob(path)
    dfs: List[DataFrame] = []
    for file in data_files:
        # print(pd.read_csv(data_files[0], sep=";"))
        dfs.append(pd.read_csv(file, sep=";"))
    df = pd.concat(dfs)

    df["MESS_DATUM"] = df["MESS_DATUM"].apply(to_date_time)

    return df


def set_errors_to_zeros(value: float):
    if value < -100:
        return 0
    return value