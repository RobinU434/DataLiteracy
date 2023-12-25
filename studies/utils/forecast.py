

import glob
from typing import List
from pandas import DataFrame
import pandas as pd

def get_dwd_forecast(feature: str) -> DataFrame:
    files = glob.glob(f"../data/dwd/forecast/*/{feature}.csv")
    frames: List[pd.DataFrame] = []
    for file in files:
        temp_df = pd.read_csv(file)
        frames.append(temp_df)

    df = pd.concat(frames)
    return df