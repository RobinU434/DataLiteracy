import datetime
# from datetime import datetime

import matplotlib.pyplot as plt

from studies.utils.dwd_data import DWD_Dataset, Feature, accumulate_timeseries

import polars as pl

from studies.utils.setup_pyplot import SIDEEFFECTS_setup_tueplot

import tueplots.constants.color.palettes as tue_palettes

choosen_palette = tue_palettes.tue_plot

# class ForecastClass(Enum):
#     """
#     DWD offers 2 different forecasts, 
#     either with 1h period with 72 datapoints (=> 3 days)
#     or with 3h period with x datapoints (=> y days)
#     """
#     Forecast1H = 1,
#     Forecast3H = 2,

FIG_SAVE_BASE_PATH = "./docs/report/fig/"

def plot_accuracy_masked(
    model_1: DWD_Dataset,
    model_2: DWD_Dataset,
):
    join_cols = ["station_id", "time"]

    historical = pl.from_pandas(model_1.get_historical()).sort(join_cols)

    forecast_1 = pl.from_pandas(model_1.get_forecast()).sort(join_cols)
    forecast_2 = pl.from_pandas(model_2.get_forecast()).sort(join_cols)

    historical_accu = accumulate_timeseries(
        historical,
        accumulate_time_step=datetime.timedelta(hours=12),
        actual_time_step=datetime.timedelta(hours=1),
        col_to_aggregate=pl.col("precipitation_real"),
        aggregation_op=pl.col("precipitation_real").sum(),
        grouped_by=[pl.col("station_id")],
    )

    forecast_1_accu = accumulate_timeseries(
        forecast_1,
        accumulate_time_step=datetime.timedelta(hours=12),
        actual_time_step=datetime.timedelta(hours=1),
        col_to_aggregate=pl.col("precipitation_forecast"),
        aggregation_op=pl.col("precipitation_forecast").sum(),
        grouped_by=[pl.col("station_id"), pl.col("call_time")],
    )

    forecast_2_accu = accumulate_timeseries(
        forecast_2,
        accumulate_time_step=datetime.timedelta(hours=12),
        actual_time_step=datetime.timedelta(hours=3),
        col_to_aggregate=pl.col("precipitation_forecast"),
        aggregation_op=pl.col("precipitation_forecast").sum(),
        grouped_by=[pl.col("station_id"), pl.col("call_time")],
    )

    forecast_accu = forecast_1_accu.vstack(forecast_2_accu)

    joined = forecast_accu.join(historical_accu, on = join_cols, how = "left")

    # forecast_time_delta_expr = (pl.col("time") - pl.col("call_time")).dt.round(every = datetime.timedelta(hours=1)).alias("forecast_time_delta")
    forecast_time_delta_expr = (((pl.col("time") - pl.col("call_time")) / datetime.timedelta(hours=1)).round()).cast(pl.datatypes.Int32).alias("forecast_time_delta_hours")

    joined = joined.with_columns(
        forecast_time_delta_expr
    )

    # TODO: perhaps try it with logscale for different values

    mask_vals = [0.0, 1.0, 2.0, 5.0]

    joined = joined.select(["station_id", "forecast_time_delta_hours", "time", "call_time", "precipitation_real", "precipitation_forecast"]).sort(["station_id", "forecast_time_delta_hours", "time"])

    # print(joined.filter(pl.col("forecast_time_delta_hours") == 60))

    time_delt = pl.col("forecast_time_delta_hours")
    # (pl.col("station_id") == 5688) & 
    print(joined.filter((61 <= time_delt) & (time_delt <= 71)))

    exit(0)

    # we have no prediction for hour 71.
    # this is at the border.
    # TODO: validate that this is expected
    # joined = joined.filter(pl.col("forecast_time_delta_hours") != 71)

    print(joined)

    for mask_val in mask_vals:
        # we always mask actual rain with 0.0!
        # otherwise the predictions improve, easy to realize with the  limit case of infinity:
        # since we never predict infinitely much rain our prediction is always correct.
        test_expr = ((pl.col("precipitation_forecast") > mask_val) == (pl.col("precipitation_real") > 0.0)).alias(f"over_{mask_val}_masked_forecast_correct")

        joined = joined.with_columns(
            test_expr,
        )

    test_cols = pl.col("^over_.*_masked_forecast_correct$")

    correct_pred = (
        joined
            .group_by(["forecast_time_delta_hours"])
            .agg((test_cols.filter(test_cols).count().cast(pl.datatypes.Float64) 
                / test_cols.count()).name.prefix("part_"))
            .sort("forecast_time_delta_hours")
    )

    print(correct_pred)

    SIDEEFFECTS_setup_tueplot(relative_path_to_root=".")

    fig, ax = plt.subplots()
    ax: plt.Axes

    for mask_val in mask_vals:
        ax.plot(correct_pred["forecast_time_delta_hours"].to_numpy(), correct_pred[f"part_over_{mask_val}_masked_forecast_correct"].to_numpy(), label=f"considering under {mask_val} as no rain")

    ax.set_xlabel("hours into the future")
    ax.set_ylabel("accuracy over all stations and call times")

    fig.legend()

    fig.savefig(f"{FIG_SAVE_BASE_PATH}/error.pdf")

if __name__ == "__main__":
    model_1 = DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=1)
    model_2 = DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=2)

    plot_accuracy_masked(model_1, model_2)
