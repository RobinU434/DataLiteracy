import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from pandas import DataFrame

from project.convert.converter import Converter
from project.convert.utils import insert_column


class DWDJsonConverter(Converter):
    def __init__(self) -> None:
        super().__init__()
        self._features_of_interest = [
            "temperature",
            "windSpeed",
            "windDirection",
            "precipitationTotal",
            "surfacePressure",
            "humidity",
            "temperatureStd",
            "precipitationProbablity",
        ]

    def to_df(self, data: Dict[str, Any]) -> List[DataFrame]:
        frames = []
        for station_name, station_data in data.items():
            frames.append(self._station_to_df(station_name, station_data))

        result = self._merge_dfs(*frames)
        return result

    def _station_to_df(
        self, station_id: str, station_data: Dict[str, Any]
    ) -> List[DataFrame]:
        forecast1 = self._forecast_to_df(station_data["forecast1"])
        forecast1 = insert_column(forecast1, column_name="provider", value="DWD_1")
        forecast2 = self._forecast_to_df(station_data["forecast2"])
        forecast2 = insert_column(forecast2, column_name="provider", value="DWD_2")
        dwd_forecast = self._merge_dfs(forecast1, forecast2)
        dwd_forecast = insert_column(dwd_forecast, "station_id", station_id)
        return dwd_forecast

    def _forecast_to_df(self, forecast_data: Dict[str, Any]) -> List[DataFrame]:
        start = forecast_data["start"]
        time_step = forecast_data["timeStep"]
        frames = {}
        for feature_name in self._features_of_interest:
            feature = forecast_data[feature_name]

            if feature is None:
                frames[feature_name] = DataFrame(columns=("time", feature_name))
                continue

            length = len(feature)
            time = np.arange(start, start + length * time_step, time_step) / 1000
            time = np.array(
                [datetime.datetime.utcfromtimestamp(time_stamp) for time_stamp in time]
            )
            frames[feature_name] = DataFrame({"time": time, feature_name: feature})

        return frames

    def _merge_dfs(self, *df_dicts: Dict[str, DataFrame]):
        result = {}
        for feature_name in self._features_of_interest:
            frames = [dfs[feature_name] for dfs in df_dicts]
            result[feature_name] = pd.concat(frames)
        return result
