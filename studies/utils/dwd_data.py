import logging
from enum import Enum
from tqdm import tqdm
import itertools
import numpy as np
import pandas as pd
import datetime
from project.utils.array import operate_on_same_index
from project.utils.cast import datetime2timestamp_int

from studies.utils.forecast import get_dwd_forecast, set_errors_to_zeros
from studies.utils.recent import get_recent


class Feature(Enum):
    """Listing of possible features to choose from"""

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
        self._feature = feature
        # load station meta information
        self._stations = pd.read_csv(self._source_path + "/stations.tsv", sep="\t")

        if self._feature is None:
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

        # TODO: correct this. ASSIGNING 0 TO MEASUREMENT ERRORS IS PURELY WRONG

    def get_forecast(self, station_id: int = 0) -> pd.DataFrame:
        """get only forecast data

        Args:
            station_id (int, optional): from which data. If set to 0 -> all stations. Defaults to 0.

        Returns:
            pd.DataFrame: processed forecast DataFrame
        """
        forecast = self._filter_df(df=self._forecast, station_id=station_id)
        return forecast

    def get_merge(self, station_id: int = 0) -> pd.DataFrame:
        """get merge data with forecast and historical data

        Args:
            station_id (int, optional): from which data. If set to 0 -> all stations. Defaults to 0.

        Returns:
            pd.DataFrame: processed DataFrame with forecast and historical data inside
        """
        merge = self._filter_df(df=self._merge, station_id=station_id)
        return merge

    def get_historical(self, station_id: int = 0) -> pd.DataFrame:
        """get only forecast data

        Args:
            station_id (int, optional): from which data. If set to 0 -> all stations. Defaults to 0.

        Returns:
            pd.DataFrame: processed forecast DataFrame
        """
        real_data = self._filter_df(df=self._real_data, station_id=station_id)
        return real_data

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

        # if temperature do rescale
        # if feature == Feature.TEMPERATURE or feature is  None:
        #     print("correct temperature")
        #     forecast[forecast_column_name] = (forecast[forecast_column_name] - 32) * 5 / 9

        return forecast

    def _load_recent(self, feature: Feature) -> pd.DataFrame:
        (
            property_name,
            recent_column_name,
            data_column_name,
            columns_to_drop,
        ) = RECENT_FEATURE_TRANSLATOR[feature]
        precipitation = get_recent(feature=property_name)
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
        forecast = forecast.copy()
        historical = historical.copy()
        if self._model == 1:
            merge = pd.merge(
                forecast, historical, on=["time", "station_id"], how="left"
            )
        # TODO: make this faster
        elif self._model == 2:
            # NOTE: assert that the forecast for self._model=2 is for every three hours
            # therefor sum the historical data in buckets with a width of three hours
            station_ids = forecast["station_id"].unique()
            call_times = forecast["call_time"].unique()
            for station_id, call_time in tqdm(list(itertools.product(station_ids, call_times)), desc="correct for 3h scope of model 2"):  # list for tqdm bar
                sub_df_forecast = forecast[
                    (forecast["call_time"] == call_time)
                    & (forecast["station_id"] == station_id)
                ]
                forecast_times = sorted(sub_df_forecast["time"].unique())
                min_time = datetime2timestamp_int(forecast_times[0])
                max_time = datetime2timestamp_int(
                    forecast_times[-1] + datetime.timedelta(hours=3)
                )
                buckets = np.array(
                    [datetime2timestamp_int(dt) for dt in forecast_times]
                )
                # buckets = np.array([dt for dt in forecast_times])

                sub_df_historical = historical[
                    (historical["station_id"] == station_id)
                    & (historical["time"].apply(datetime2timestamp_int) >= min_time)
                    & (historical["time"].apply(datetime2timestamp_int) <= max_time)
                ]
                bucket_idx = np.digitize(
                    sub_df_historical["time"].apply(datetime2timestamp_int), buckets
                )
                # correct index
                bucket_idx -= 1
                if self._feature == Feature.PRECIPITATION:
                    operation = np.sum
                elif self._feature == Feature.TEMPERATURE:
                    operation = np.mean
                else:
                    operation = np.mean
                    msg = f"No aggregation function implemented for feature: {self._feature}. Fall back to {operation}"
                    logging.warning(msg)

                
                # sum the content of each bucket
                accs = operate_on_same_index(
                    sub_df_historical["precipitation_real"].values, bucket_idx, operation
                )
            
                # replace values in historical
                for time, value in zip(forecast_times, accs):
                    historical.loc[(historical["time"] == time) & (historical["station_id"] == station_id), "precipitation_real"] = value
                
                merge = pd.merge(
                    forecast, historical, on=["time", "station_id"], how="left"
                )
        else:
            pass

        # add difference
        if self._feature is None:
            merge.insert(
                len(merge.columns),
                "precipitation_error",
                merge["precipitation_forecast"]
                - merge["precipitation_real"],
            )
            merge.insert(
                len(merge.columns),
                "air_temperature_error",
                merge["air_temperature_forecast"]
                - merge["air_temperature_real"],
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