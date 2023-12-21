import os
import json
import polars as pl
import numpy as np
from datetime import datetime
from numpy import nan

def read_json_files(directory_path: str):
    result_dict = {}

    # Check if the directory exists
    if not os.path.exists(directory_path):
        raise ValueError("Directory does not exist")

    # Iterate over each file in the directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        # Check if it's a file and has a JSON extension
        if os.path.isfile(file_path) and filename.endswith(".json"):
            with open(file_path, "r") as file:
                # Parse JSON content
                json_content = json.load(file)
                # Use the filename (without extension) as the key
                key = os.path.splitext(filename)[0]
                result_dict[key] = json_content

    return result_dict

def read_measurement_txt(path_glob: str):
    measurements = pl.scan_csv(path_glob, separator=';')
    measurements: pl.LazyFrame = measurements.drop(["eor"])
    datecol = "MESS_DATUM"
    # for some reason the parser requires either both minutes and hours or none of them.
    # So I fake minutes in the timestamp.
    measurements = measurements.with_columns(
        pl.col(datecol).cast(str) + "00"
    )
    measurements = measurements.with_columns(
        pl.col(datecol).cast(str).str.to_datetime(
            format="%Y%m%d%H%M",
            # ambiguous = 'earliest',
        ),
        (pl.when(pl.col("  R1") == -999).then(pl.lit(nan)).otherwise(pl.col("  R1"))).alias("precipitation_with_correct_nan"),
    )
    print(measurements.collect())

    # btw, theres is no datapoint so far where WRTR isnt -999
    # print(measurements.filter(pl.col("WRTR") != -999).collect())
    return measurements

def parse_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def get_measurements(measurement_metadata: dict, measurements: pl.LazyFrame):
    measurement_key = measurement_metadata["Stations_ID"]
    return measurements.filter(pl.col("STATIONS_ID") == measurement_key)

def same_date(date_str: str, timestamp_ms: int):
    parsed_date = parse_date(date_str)
    timestamp_date = datetime.utcfromtimestamp(timestamp_ms).date()

    return parsed_date == timestamp_date

def daily_forecasts_to_time_series(forecast: dict):
    # the forecasts seem to predict up to x days in the future
    forecasts_timeseries: list[dict[datetime, int]] = [{} for _ in range(10)]
    "shall contain the forecasts for index days in the future, every forecast is a dict{daydate, totalPrecipitation}"
    for (record_timestamp, itm) in forecast.items():
        try:
            if not same_date(date_str=itm[0]["dayDate"], timestamp_ms=int(record_timestamp)):
                print("skipped one", itm[0]["dayDate"])
            else:
                print("Success")            
        except ValueError:
            # parsing with int causes issues on my formatted json, perhaps solve this more elegant or simply remove the formatted json/move it somewhere else
            print("skipped one", itm[0]["dayDate"])
            continue
        start_day = parse_date(itm[0]["dayDate"])
        for day in itm:
            forecast_day = parse_date(day["dayDate"])
            days_in_future = (parse_date(day["dayDate"]) - start_day).days
            forecasts_timeseries[days_in_future][forecast_day] = day["precipitation"]
    return forecasts_timeseries

def hourly_forecasts_to_time_series(forecast: dict):
    """
    DO NOT USE, this isnt the way i followd in the end
    """
    for (record_timestamp, itm) in forecast.items():
        start_timestamp = itm["start"]
        timestep = itm["timeStep"]
        # should be one hour
        assert timestep == (60**2 * 1000)
        total_hourly_precipitation = np.array(itm["precipitationTotal"])
        # group over one day
        # Reshape the array to have 24 columns
        reshaped_array = total_hourly_precipitation.reshape(-1, 24)

        # Sum along the second axis (axis=1) to get the sum of every 24 datapoints
        total_daily_precipitation = np.sum(reshaped_array, axis=1)

        print(total_daily_precipitation)

        pass

def measurements_to_day_time_series(measurements: pl.LazyFrame):
    # measurement_timeseries: dict[datetime, int] = {}
    measurements:pl.LazyFrame = measurements.with_columns(
        pl.col("MESS_DATUM").dt.date().alias("date")
    )
    measurements = measurements.group_by("date").agg(
        pl.col("precipitation_with_correct_nan").sum().alias("total_precipitation_mm"),
        pl.col("STATIONS_ID").first(),
    )
    print(
        "measurement failures:",
        measurements.filter(pl.col("total_precipitation_mm") == nan).collect(),
    )
    return measurements

def get_forecasts(measurement_metadata: dict, forecasts: dict):
    """
    DO NOT USE, this isnt the way i followd in the end
    """
    station_key = measurement_metadata["Stations-kennung"]
    results = {}
    for (key, itm) in forecasts.items():
        try:
            # TODO: also check how this behaves with forecast2...
            results[key] = itm[station_key]["forecast2"]
        except Exception as err:
            # print(err)
            pass
    return results

def get_forecast_days(measurement_metadata: dict, forecasts: dict):
    station_key = measurement_metadata["Stations-kennung"]
    results = {}
    for (key, itm) in forecasts.items():
        try:
            # note: forecast1 is hourly, forecast 2 is 3 hourly, days is daily.
            results[key] = itm[station_key]["days"]
        except Exception as err:
            # print(err)
            pass
    return results


def main():
    with open("project/data/dwd/aggregated_station_info.json") as fp:
        metadata = json.load(fp)

    measurements_raw = read_measurement_txt("data/dwd/actual_observations/raw/*/produkt_rr_stunde_*.txt")

    forecasts_raw = read_json_files("data/dwd/test")

    for pos in metadata:
        accepted_measurements = [measurement for measurement in pos["data_avail"] if "MN" == measurement["Kennung"]]

        match len(accepted_measurements):
            case 0:
                continue
            case 1: 
                pass
            case _: 
                raise RuntimeError("unreachable")

        forecasts = get_forecast_days(accepted_measurements[0], forecasts_raw)

        measurements = get_measurements(accepted_measurements[0], measurements_raw)

        forecast_timeseries = daily_forecasts_to_time_series(forecasts)

        measurements_timeseries = measurements_to_day_time_series(measurements)

        print(measurements_timeseries.collect())

        days = [day for day in forecast_timeseries[0].keys()][9:]

        forecast_dataframe = pl.DataFrame({
            "date": days,
            ** {f"forecast_-{idx}_day": [forecast_timeseries[idx][day] for day in days] for idx in range(10)},
        })

        forecast_dataframe = forecast_dataframe.with_columns(
            pl.col("date").cast(pl.Date)
        ).lazy()

        print(forecast_dataframe)

        joined = forecast_dataframe.join(measurements_timeseries, how='left', on='date')

        print(joined.collect())

        joined: pl.DataFrame = joined.collect()

        os.makedirs("data/dwd/correlated/", exist_ok=True)

        joined.write_csv(f"data/dwd/correlated/{pos['Stationsname'][0]}.csv")

if __name__=="__main__":
    main()
