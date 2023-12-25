import logging
from typing import Iterable, List
import pandas as pd
import numpy as np
from numpy import ndarray

FEATURE_STATION_PROPERTY_MAP = {
    "air_temperature": "TU",
    "cloud_type": "CS",
    "cloudiness": "N",
    "dew_point": "TD",
    "extreme_wind": "FX",
    "moisture": "TF",
    "precipitation": "RR",
    "pressure": "P0",
    "soil_temperature": "EB",
    "solar": "ST",
    "sun": "SD",
    "visibility": "VV",
    "weather_phenomena": "WW",
    "wind": "FF",
    "wind_synop": "F",
}


def check_station_ids(station_ids: List[str]) -> ndarray:
    """checks if given station ids are valid and returns a bool array where True means valid and False not valid

    Args:
        station_ids (List[str]): station ids to check

    Returns:
        List[bool]: filter mask for station ids
    """
    # load existing station ids:
    real_stations = pd.read_csv("project/data/dwd/stations.tsv", sep="\t")

    result = np.empty(len(station_ids), dtype=bool)
    for idx, station_id in enumerate(station_ids):
        station_id = int(station_id)
        if station_id in real_stations["Stations_ID"]:
            result[idx] = True
        else:
            result[idx] = False

    return result


def filter_features(features: List[str]) -> Iterable[str]:
    """check if the list of given features is supported by dwd features for recent and historical data

    Args:
        features (List[str]): list of features

    Returns:
        Iterable[str]: True if all given features are supported
    """
    dwd_features = set(FEATURE_STATION_PROPERTY_MAP.keys())
    features = set(features)
    intersection = features.intersection(dwd_features)
    if intersection != features:
        logging.warning(f"Only {intersection} out of {features} are supported features")
    return features


def build_recent_url(feature: str, station_id: str):
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/"
    url += feature
    url += "/recent/"
    url += f"stundenwerte_{FEATURE_STATION_PROPERTY_MAP[feature]}_{str(station_id).zfill(5)}_akt.zip"
    return url
