import datetime
# from datetime import datetime

import matplotlib.pyplot as plt

from studies.utils.dwd_data import DWD_Dataset, Feature

import polars as pl

from studies.utils.setup_pyplot import (
    SIDEEFFECTS_setup_tueplot,
    SIDEEFFECTS_choose_color_palette,
    FIG_SAVE_BASE_PATH,
)

# class ForecastClass(Enum):
#     """
#     DWD offers 2 different forecasts,
#     either with 1h period with 72 datapoints (=> 3 days)
#     or with 3h period with x datapoints (=> y days)
#     """
#     Forecast1H = 1,
#     Forecast3H = 2,


def plot_accuracy_masked(
    model_1: DWD_Dataset,
    model_2: DWD_Dataset,
):
    join_cols = ["station_id", "time"]

    historical = pl.from_pandas(model_1.get_historical()).sort(join_cols)

    # historical_accu = accumulate_timeseries(
    #     historical,
    #     accumulate_time_step=datetime.timedelta(hours=12),
    #     actual_time_step=datetime.timedelta(hours=1),
    #     col_to_aggregate=pl.col("precipitation_real"),
    #     aggregation_op=pl.col("precipitation_real").sum(),
    #     grouped_by=[pl.col("station_id")],
    # )
    time_col = pl.col("time")

    forecast_1 = (
        pl.from_pandas(model_1.get_forecast())
        .sort(join_cols)
        .with_columns((time_col + datetime.timedelta(hours=1)).alias("time_end"))
    )
    forecast_2 = (
        pl.from_pandas(model_2.get_forecast())
        .sort(join_cols)
        .with_columns((time_col + datetime.timedelta(hours=3)).alias("time_end"))
    )

    # print(historical.sort(join_cols).write_csv("whatever_or.csv"))

    forecast = forecast_1.vstack(forecast_2)

    forecast_time_delta_expr = (
        (((pl.col("time") - pl.col("call_time")) / datetime.timedelta(hours=1)).round())
        .cast(pl.datatypes.Int32)
        .alias("forecast_time_delta_hours")
    )

    accumulation_timewindow = datetime.timedelta(hours=12)

    # for some reason floating point accuracy messed up our 0.0 for some accumulations.
    # so we remove that issue. Originally both forecast and historical data were given as integer as original value times 10.
    # we go there for accumulation, and then back again
    integerify_precipitation = (pl.col("^precipitation_.*$") * 10).cast(
        pl.datatypes.Int32
    )
    floatify_precipitation = (
        pl.col("^precipitation_.*$").cast(pl.datatypes.Float64) / 10
    )
    forecast = forecast.with_columns(integerify_precipitation)

    forecast = (
        forecast.rolling(
            time_col,
            period=accumulation_timewindow,
            by=["station_id", "call_time"],
            closed="left",
            offset=datetime.timedelta(),
        )
        .agg(pl.col("precipitation_forecast").sum())
        # we drop the last datapoints, where the accumulation goes over
        # the actually measured time window and assumes 0 for all values in there
        .with_columns(forecast_time_delta_expr)
        # .sort("forecast_time_delta_hours")
        # .slice(offset=0, length=-int(12 / 3))
    )

    forecast = forecast.with_columns(floatify_precipitation)

    historical = historical.with_columns(integerify_precipitation)

    historical = (
        historical.rolling(
            time_col,
            period=accumulation_timewindow,
            by=["station_id"],
            closed="left",
            offset=datetime.timedelta(),
        ).agg(pl.col("precipitation_real").sum())
        # .slice(offset=0, length=-int(12 / 1))
    )

    historical = historical.with_columns(floatify_precipitation)

    # print(
    # historical.sort(join_cols).write_csv("whatever.csv"),#[13:],
    # historical_accu.sort(join_cols).write_csv("whatever2.csv"),
    # )

    joined = forecast.join(historical, on=join_cols, how="left")

    mask_vals = [
        0.0,
        # 0.1,
        # 0.2,
        0.5,
        2.0,
    ]

    joined = joined.select(
        [
            "station_id",
            "forecast_time_delta_hours",
            "time",
            "call_time",
            "precipitation_real",
            "precipitation_forecast",
        ]
    ).sort(["station_id", "forecast_time_delta_hours", "time"])

    # print(joined.filter(pl.col("forecast_time_delta_hours") == 60))

    # time_delt = pl.col("forecast_time_delta_hours")
    # # (pl.col("station_id") == 5688) &
    # print(joined.filter((61 <= time_delt) & (time_delt <= 71)))

    # exit(0)

    # we have no prediction for hour 71.
    # this is at the border.
    # TODO: validate that this is expected
    # joined = joined.filter(pl.col("forecast_time_delta_hours") != 71)

    # print(
    # joined.write_csv("all_data.csv")
    # )

    for mask_val in mask_vals:
        # we always mask actual rain with 0.0!
        # otherwise the predictions improve, easy to realize with the  limit case of infinity:
        # since we never predict infinitely much rain our prediction is always correct.
        test_expr = (
            (pl.col("precipitation_forecast") > mask_val)
            == (pl.col("precipitation_real") > 0.0)
        ).alias(f"over_{mask_val}_masked_forecast_correct")

        joined = joined.with_columns(
            test_expr,
        )

    test_cols = pl.col("^over_.*_masked_forecast_correct$")

    correct_pred = (
        joined.group_by(["forecast_time_delta_hours"])
        .agg(
            (
                test_cols.filter(test_cols).count().cast(pl.datatypes.Float64)
                / test_cols.count()
            ).name.prefix("part_")
        )
        .sort("forecast_time_delta_hours")
    )

    print(correct_pred)

    SIDEEFFECTS_setup_tueplot(relative_path_to_root=".")

    SIDEEFFECTS_choose_color_palette()

    fig, ax = plt.subplots()
    ax: plt.Axes

    for mask_val in mask_vals:
        ax.plot(
            correct_pred["forecast_time_delta_hours"].to_numpy(),
            correct_pred[f"part_over_{mask_val}_masked_forecast_correct"].to_numpy(),
            label=f"T = {mask_val}",
            zorder=1 if mask_val == 0.0 else -1,
            # c = choosen_palette[idx],
        )

    ax.set_xlabel("hours ahead")
    ax.set_ylabel("Accuracy")  # over all stations and call times

    fig.legend()

    fig.savefig(f"{FIG_SAVE_BASE_PATH}/fig_accuracy_thresholds.pdf")


if __name__ == "__main__":
    model_1 = DWD_Dataset(
        source_path="./data/dwd", feature=Feature.PRECIPITATION, model=1
    )
    model_2 = DWD_Dataset(
        source_path="./data/dwd", feature=Feature.PRECIPITATION, model=2
    )

    plot_accuracy_masked(model_1, model_2)
