import datetime
# from datetime import datetime

import matplotlib.pyplot as plt

from studies.utils.dwd_data import DWD_Dataset, Feature, accumulate_timeseries

import polars as pl

from studies.utils.setup_pyplot import SIDEEFFECTS_setup_tueplot
import matplotlib

import tueplots.constants.color.palettes as tue_palettes

choosen_palette = tue_palettes.tue_plot

FIG_SAVE_BASE_PATH = "./docs/report/fig/"


STATION_ID = 5688

def plot_raw_data(
    model_1: DWD_Dataset,
    model_2: DWD_Dataset,
):
    # START_NUM_FOR_SOME_REASON = 10
    NUM_FORECASTS = 2

    sel_col = "call_time"

    historical = pl.from_pandas(model_1.get_historical())

    forecast_1 = pl.from_pandas(model_1.get_forecast())
    forecast_2 = pl.from_pandas(model_2.get_forecast())
    
    filter_station = (pl.col("station_id") == STATION_ID)

    start_date: datetime.datetime = forecast_1.sort(sel_col).filter(filter_station)[sel_col].unique()[10]

    filter_dates = (pl.col(sel_col) > (start_date - datetime.timedelta(hours=1))) & (pl.col(sel_col) < (start_date + NUM_FORECASTS * datetime.timedelta(days=1) - datetime.timedelta(hours=1)))

    filter_all = filter_dates & filter_station

    time_col = "time"

    shown_forecasts_1 = (
        forecast_1
            .filter(filter_all)
            # TODO: yeah, border conditions here are... screwy. 
            # we do want both precipitations in a similar scale, not one 3 times as high.
            # but for real data, we would have to accumulate the T=1h forecast every 3 steps, 
            # but that means that either we have to assume a border condition for the beginning (since we have fixed call times)
            # or we need to multiply every T=1h forecast by 3.
            # OPTION1 (TODO)
            # .rolling(time_col, period=datetime.timedelta(hours=3), closed="left", by = sel_col)
            # OPTION2
            .with_columns(pl.col("precipitation_forecast") * 3)
        )
    shown_forecasts_2 = forecast_2.filter(filter_all)

    start_date_hist = shown_forecasts_1.sort(time_col)[time_col][0] - datetime.timedelta(minutes=20)
    end_date_hist = shown_forecasts_2.sort(time_col, descending=True)[time_col][0] + datetime.timedelta(minutes=20) + datetime.timedelta(hours=3)

    filter_forecast_date = (start_date_hist < pl.col(time_col)) & (pl.col(time_col) < end_date_hist)

    shown_historical = historical.filter(filter_station).sort(filter_station)

    shown_historical = accumulate_timeseries(
        shown_historical,
        accumulate_time_step=datetime.timedelta(hours=3),
        actual_time_step=datetime.timedelta(hours=1),
        col_to_aggregate=pl.col("precipitation_real"),
        aggregation_op=pl.col("precipitation_real").sum(),
    ).filter(filter_forecast_date)

    normalize_to_1h = pl.col("^precipitation_.*$").cast(pl.datatypes.Float64) / 3

    shown_historical = shown_historical.with_columns(normalize_to_1h)
    shown_forecasts_1 = shown_forecasts_1.with_columns(normalize_to_1h)
    shown_forecasts_2 = shown_forecasts_2.with_columns(normalize_to_1h)

    SIDEEFFECTS_setup_tueplot(relative_path_to_root=".")

    fig, ax = plt.subplots()

    ax.plot(shown_historical[time_col].to_numpy(), shown_historical["precipitation_real"].to_numpy(), label="reference", c = choosen_palette[0])

    call_dates: list[datetime.datetime] = [(start_date + datetime.timedelta(days=num)).date() for num in range(NUM_FORECASTS)]

    for (idx, call_date) in enumerate(call_dates):
        shown_forecasts_1_filtered = shown_forecasts_1.filter(pl.col(sel_col).dt.date() == call_date)
        ax.plot(shown_forecasts_1_filtered[time_col].to_numpy(), shown_forecasts_1_filtered["precipitation_forecast"].to_numpy(), label=f"{call_date.isoformat()} - T = 1h", c = choosen_palette[1 + idx])

    for (idx, call_date) in enumerate(call_dates):
        shown_forecasts_2_filtered = shown_forecasts_2.filter(pl.col(sel_col).dt.date() == call_date)
        ax.plot(shown_forecasts_2_filtered[time_col].to_numpy(),shown_forecasts_2_filtered["precipitation_forecast"].to_numpy(), label=f"{call_date.isoformat()} - T = 3h", c = matplotlib.colors.to_rgba(choosen_palette[1 + idx], 0.5))

    # ax.set_yscale('log')

    ax.set_xticks(ax.get_xticks(), ax.get_xticklabels(), rotation=90)
    ax.set_ylabel("[l/m²]", rotation = 90)

    fig.legend()
    fig.suptitle("Precipitation in December")

    formatter = matplotlib.dates.DateFormatter('%d')
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_formatter(formatter)


    fig.savefig(f"{FIG_SAVE_BASE_PATH}/raw_data.pdf")

if __name__ == "__main__":
    model_1 = DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=1)
    model_2 = DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=2)

    plot_raw_data(model_1, model_2)
