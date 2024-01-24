import logging
from enum import Enum
from typing import List
import numpy as np
import pandas as pd
import polars as pl
import datetime

from studies.utils.forecast import get_dwd_forecast, set_errors_to_zeros
from studies.utils.recent import get_recent
from typing import Iterable

def accumulate_timeseries(
    historical: pl.DataFrame,
    accumulate_time_step: datetime.timedelta,
    actual_time_step: datetime.timedelta,
    col_to_aggregate: pl.functions.col,
    aggregation_op: pl.Expr,
    grouped_by: pl.Expr | Iterable[pl.Expr] = pl.col("station_id"),
):
    "this has all kinds of horrible naming, but I cant spend time properly naming everything in here. So be warned."
    historical = historical.sort("time")

    # print(historical
    #     .filter(pl.col("station_id") == 259)
    #     .filter(pl.col("time") > datetime.datetime.fromisoformat("2023-12-08 04:00:00"))
    # )

    # this will assume 0 for values outside of the timeframe, and the time will be at the end of the accumulation window
    # corrected later
    historical_window_sum_missing_data = historical.rolling(
        "time", period=accumulate_time_step, by=grouped_by, closed="left"
    ).agg(aggregation_op)

    datapoints_to_drop = int(accumulate_time_step / actual_time_step - 1)

    # drop first rows per group and adjust time so it fits forecasts
    historical_window_sum = (
        historical_window_sum_missing_data
        # aggregate to drop data, we will explode afterwards.
        # a waste of compute but only doubles total aggregations and should still be fast enough.
        .group_by(grouped_by)
        .agg(
            (
                (
                    pl.col("time").slice(datapoints_to_drop)
                    # forecasts predict weather for the next time delta, aggregated accordingly.
                    # since polars looks into the past we correct for that.
                    - (accumulate_time_step - actual_time_step)
                )
                # without this cast the datetime switches to microsecs
                .dt.cast_time_unit("ns")
            ),
            col_to_aggregate.slice(datapoints_to_drop),
            pl.col("time").first().alias("original_start_time"),
        )
        .explode([pl.col("time"), col_to_aggregate])
    )

    # print(historical_window_sum
    #     .filter(pl.col("station_id") == 259)
    #     .filter(pl.col("time") > datetime.datetime.fromisoformat("2023-12-08 04:00:00"))
    # )

    # thats the pain with polars: trying to do anything but data wrangling is annoying.
    # amongst these is asserting correct data comes in, which has to be rephrased in terms of data conversions.
    assert_correctness = historical_window_sum.group_by(grouped_by).agg(
        (pl.col("time").first() == pl.col("original_start_time"))
        .alias("correct_calc")
        .all()
    )

    assert assert_correctness.select(pl.col("correct_calc").all())["correct_calc"][0]

    historical_window_sum = historical_window_sum.drop("original_start_time")

    return historical_window_sum

def accumulate_and_merge_different_timeseries_periodes(
    forecast: pd.DataFrame,
    historical: pd.DataFrame,
    forecast_time_step: datetime.timedelta,
    col_to_aggregate: pl.functions.col,
    aggregation_op: pl.Expr,
) -> pd.DataFrame:
    forecast = pl.from_pandas(forecast)
    historical = pl.from_pandas(historical)

    actual_time_step = datetime.timedelta(hours=1)

    historical_window_sum = accumulate_timeseries(
        historical,
        forecast_time_step,
        actual_time_step,
        col_to_aggregate,
        aggregation_op
    )

    joined = forecast.join(historical_window_sum, on=["station_id", "time"], how="left")

    return joined.to_pandas()


class Feature(Enum):
    """Listing of possible features to choose from"""

    ALL = 0
    PRECIPITATION = 1
    TEMPERATURE = 2


FORECAST_FEATURE_TRANSLATOR = {
    Feature.PRECIPITATION: ("precipitationTotal", "precipitation_forecast"),
    Feature.TEMPERATURE: ("temperature", "air_temperature_forecast"),
}
RECENT_FEATURE_TRANSLATOR = {
    Feature.PRECIPITATION: (
        "precipitation",
        "precipitation_real",
        "  R1",
        ["QN_8", "RS_IND", "WRTR", "eor"],
    ),
    Feature.TEMPERATURE: (
        "air_temperature",
        "air_temperature_real",
        "TT_TU",
        ["QN_9", "RF_TU", "eor"],
    ),
}


class DWD_Dataset:
    """container for dwd dataset"""

    def __init__(self, source_path: str, feature: Feature = None, model: int = 1):
        """_summary_

        Args:
            source_path (str): _description_
            feature (Feature, optional): _description_. Defaults to None.
            model (int, optional): from which dwd model. 1: short term. 2: long term. 0: both. Defaults to 1.

        """
        if model not in [1, 2]:
            logging.error("Choose either model 1 or 2. But you chose model = " + model)
            return
        else:
            self._model = model

        self._source_path = source_path
        self._feature = feature if feature is not None else Feature.ALL
        # load station meta information
        self._stations = pd.read_csv(self._source_path + "/stations.tsv", sep="\t")

        if self._feature == Feature.ALL:
            # load all specified features in Features enum
            precipitation_forecast = self._load_forecast(Feature.PRECIPITATION)
            temperature_forecast = self._load_forecast(Feature.TEMPERATURE)
            # merge all feature; There are more temperature values than forecast values -> left outter join
            self._forecast = pd.merge(
                precipitation_forecast,
                temperature_forecast,
                how="left",
                on=["call_time", "station_id", "provider", "time"],
            )

            # get recent
            precipitation_real = self._load_recent(Feature.PRECIPITATION)
            temperature_real = self._load_recent(Feature.TEMPERATURE)
            self._real_data = pd.merge(
                precipitation_real,
                temperature_real,
                how="left",
                on=["station_id", "time"],
            )

        elif isinstance(self._feature, Feature):
            # load a specific feature
            self._forecast = self._load_forecast(self._feature)
            # get recent
            self._real_data = self._load_recent(self._feature)

        else:
            msg = f"the given feature: {self._feature} is neither is None nor an element of Feature enum."
            logging.error(msg)
            return

        self._forecast = self._filter_df(self._forecast, model=self._model)
        # remove samples with out fitting forecast or vise versa a fitting historical sample
        self._trim_datasets()

        # create a merge dataset where we link forecasts and recent data
        self._merge = self._create_merge(self._forecast, self._real_data)

    def get_forecast(self, station_id: int = 0, columns: List[str] = []) -> pd.DataFrame:
        """get only forecast data

        Args:
            station_id (int, optional): from which data. If set to 0 -> all stations. Defaults to 0.
            columns (List[str], optional): which columns do you want to select?. [] -> all columns. Defaults to []

        Returns:
            pd.DataFrame: processed forecast DataFrame
        """
        forecast = self._forecast.copy()
        forecast = self._filter_df(df=forecast, station_id=station_id)
        forecast = self._get_columns(df=forecast, columns=columns)
        return forecast.copy()

    def get_merge(self, station_id: int = 0, columns: List[str] = []) -> pd.DataFrame:
        """get merge data with forecast and historical data

        Args:
            station_id (int, optional): from which data. If set to 0 -> all stations. Defaults to 0.
            columns (List[str], optional): which columns do you want to select?. [] -> all columns. Defaults to []

        Returns:
            pd.DataFrame: processed DataFrame with forecast and historical data inside
        """
        merge = self._merge.copy()
        merge = self._filter_df(df=merge, station_id=station_id)
        merge = self._get_columns(df=merge, columns=columns)
        return merge.copy()

    def get_historical(self, station_id: int = 0, columns: List[str] = []) -> pd.DataFrame:
        """get only forecast data

        Args:
            station_id (int, optional): from which data. If set to 0 -> all stations. Defaults to 0.
            columns (List[str], optional): which columns do you want to select?. [] -> all columns. Defaults to []
        Returns:
            pd.DataFrame: processed forecast DataFrame
        """
        real_data = self._real_data.copy()
        real_data = self._filter_df(df=real_data, station_id=station_id)
        real_data = self._get_columns(df=real_data, columns=columns)
        return real_data.copy()

    def get_matrix(self, data_column: str):
        """shape: (num_api_calls, num_stations, num_predictions_into_future)

        Args:
            data_column (str): _description_

        Returns:
            _type_: _description_
        """
        if data_column not in self._merge.columns:
            msg = f"{data_column} not in {self._merge.columns}. Not possible to create matrix."
            logging.error(msg)
            return
        errors = []
        merge = self._merge[self._merge["provider"] == "DWD_1"]
        station_ids = merge["station_id"].unique()
        for call_time in sorted(merge["call_time"].unique()):
            same_call_time = merge[merge["call_time"] == call_time]
            call_time_errors = []
            for station_id in station_ids:
                call_time_error = same_call_time[
                    same_call_time["station_id"] == station_id
                ]["precipitation_error"].values
                # This is a HACK and needs to be resolved
                # THIS ONLY WORKS FOR MODEL 1
                call_time_error = call_time_error[:72]
                if len(call_time_error) < 72:
                    continue
                call_time_errors.append(call_time_error)
            if len(call_time_errors):
                errors.append(np.stack(call_time_errors))

        # shape: (num_api_calls, num_stations, num_predictions_into_future)
        errors = np.stack(errors)
        return errors

    def _load_forecast(self, feature: Feature) -> pd.DataFrame:
        forecast_feature_name, forecast_column_name = FORECAST_FEATURE_TRANSLATOR[
            feature
        ]
        # get forecast
        forecast = get_dwd_forecast(forecast_feature_name, self._source_path)
        # drop unnamed colum
        forecast = forecast.drop(columns="Unnamed: 0")
        # rename forecast_column
        forecast = forecast.rename(
            columns={forecast_feature_name: forecast_column_name}
        )
        # convert kennungen to real ids
        # huge speedup to turn the data-structure in the lightweight dict
        kennung_id_dict = dict(
            zip(self._stations["Stations-kennung"], self._stations["Stations_ID"])
        )
        forecast["station_id"] = forecast["station_id"].apply(
            kennung_id_dict.__getitem__
        )

        forecast[forecast_column_name] = forecast[forecast_column_name] / 10
        # set to GMT + 1
        forecast["time"] = forecast["time"] + pd.Timedelta(hours=1)

        return forecast

    def _load_recent(self, feature: Feature) -> pd.DataFrame:
        (
            property_name,
            recent_column_name,
            data_column_name,
            columns_to_drop,
        ) = RECENT_FEATURE_TRANSLATOR[feature]
        precipitation = get_recent(
            feature=property_name, data_root_dir=f"{self._source_path}/recent"
        )
        # clean columns
        real_data = precipitation.drop(columns=columns_to_drop)
        # rename columns
        real_data = real_data.rename(
            columns={
                "MESS_DATUM": "time",
                data_column_name: recent_column_name,
                "STATIONS_ID": "station_id",
            }
        )
        # TODO: correct this. ASSIGNING 0 TO MEASUREMENT ERRORS IS PURELY WRONG
        real_data[recent_column_name] = real_data[recent_column_name].apply(
            set_errors_to_zeros
        )

        return real_data

    def _create_merge(
        self, forecast: pd.DataFrame, historical: pd.DataFrame
    ) -> pd.DataFrame:
        timestep: datetime.timedelta
        match self._model:
            case 1:
                timestep = datetime.timedelta(hours=1)
            case 2:
                timestep = datetime.timedelta(hours=3)
            case _:
                raise RuntimeError("unreachable")
            
        if self._model != 1 and (self._feature == Feature.ALL or self._feature == Feature.TEMPERATURE):
            logging.warning("There is not forecast data for temperature at model = 2. You will find Nan")

        match self._feature:
            case Feature.PRECIPITATION:
                merge = accumulate_and_merge_different_timeseries_periodes(
                    forecast,
                    historical,
                    forecast_time_step=timestep,
                    col_to_aggregate=pl.col("precipitation_real"),
                    aggregation_op=pl.col("precipitation_real").sum(),
                )
            case Feature.TEMPERATURE:
                merge = accumulate_and_merge_different_timeseries_periodes(
                    forecast,
                    historical,
                    forecast_time_step=timestep,
                    col_to_aggregate=pl.col("air_temperature_real"),
                    aggregation_op=pl.col("air_temperature_real").mean(),
                )
            case Feature.ALL:    
                merge_precipitation = accumulate_and_merge_different_timeseries_periodes(
                    forecast,
                    historical,
                    forecast_time_step=timestep,
                    col_to_aggregate=pl.col("precipitation_real"),
                    aggregation_op=pl.col("precipitation_real").sum(),
                )
                
                merge_temperature = accumulate_and_merge_different_timeseries_periodes(
                    forecast,
                    historical,
                    forecast_time_step=timestep,
                    col_to_aggregate=pl.col("air_temperature_real"),
                    aggregation_op=pl.col("air_temperature_real").mean(),
                )
                
                merge = pd.merge(
                    merge_precipitation,
                    merge_temperature,
                    how="left",
                    on=["station_id", "call_time", "time", "air_temperature_forecast", "precipitation_forecast"],
                )

        # add difference
        if self._feature == Feature.ALL:
            merge.insert(
                len(merge.columns),
                "precipitation_error",
                merge["precipitation_forecast"] - merge["precipitation_real"],
            )
            merge.insert(
                len(merge.columns),
                "air_temperature_error",
                merge["air_temperature_forecast"] - merge["air_temperature_real"],
            )
        elif isinstance(self._feature, Feature):
            name, _, _, _ = RECENT_FEATURE_TRANSLATOR[self._feature]
            merge.insert(
                len(merge.columns),
                f"{name}_error",
                merge[f"{name}_forecast"] - merge[f"{name}_real"],
            )

        return merge

    def _trim_datasets(self):
        """clean data by removing every point which is outside every forecast
        and also vise versa on forecasts we can't evaluate because we don't
        have a reference
        """
        # clean data by removing every point which is outside every forecast
        min_time = min(self._forecast["time"])
        self._real_data = self._real_data[self._real_data["time"] >= min_time]
        # ... and also vise versa on forecasts we can't evaluate because we don't have a reference
        max_time = max(self._real_data["time"])
        self._forecast = self._forecast[self._forecast["time"] <= max_time]

    def _filter_errors(self, threshold: float = 900):
        """_summary_

        Args:
            threshold (float, optional): disable it with values > 999. Defaults to 900.
        """
        # find mistakes in forecasts
        mask_1 = self._forecast["precipitation_forecast"].abs() > threshold
        mask_2 = self._real_data["precipitation_real"].abs() > threshold
        mask = mask_1 & mask_2

        self._forecast = self._forecast[mask]
        self._real_data = self._real_data[mask]
        self._merge = self._merge[mask]

    @staticmethod
    def _get_columns(df, columns: List[str] = []) -> pd.DataFrame:
        if len(columns) == 0:
            return df
        
        sub_df = df[columns]
        return sub_df

    def __str__(self) -> str:
        return str(self._merge)

    @staticmethod
    def _filter_df(
        df: pd.DataFrame, station_id: int = 0, model: int = 1
    ) -> pd.DataFrame:
        if station_id > 0:
            df = df[df["station_id"] == station_id]

        if "provider" not in df.columns:
            pass
        elif model > 2 or model <= 0:
            raise ValueError("Choose either model 1 or 2.")
        else:
            df = df[df["provider"] == f"DWD_{model}"]
            df = df.drop(columns=["provider"])

        return df
    
    @property
    def station_meta(self) -> pd.DataFrame:
        """returns all active stations we are accessing through forecasts

        Returns:
            pd.DataFrame: _description_
        """
        active_stations = self._merge["station_id"].unique()
        filtered_stations = self._stations[self._stations["Stations_ID"].isin(active_stations) & self._stations["Kennung"] == "MN"].copy()
        return filtered_stations


if __name__ == "__main__":
    DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=1)
    DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=2)
