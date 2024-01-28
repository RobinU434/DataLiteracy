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


def plot_full_statistics(
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

    forecast_rain: pl.Expr = (pl.col("precipitation_forecast") > 0.0)
    # we always mask actual rain with 0.0!        
    actual_rain: pl.Expr = (pl.col("precipitation_real") > 0.0)

    test_exprs_1 = {
        "true positive": (forecast_rain & actual_rain).sum() / actual_rain.count(),
        "false positive": (forecast_rain & actual_rain.not_()).sum() / actual_rain.count(),
        "false negative": (forecast_rain.not_() & actual_rain).sum() / actual_rain.count(),
        "true negative": (forecast_rain.not_() & actual_rain.not_()).sum() / actual_rain.count(),
        "accuracy": ((forecast_rain == actual_rain).sum() / forecast_rain.count()),
        "precision": ((forecast_rain & actual_rain).sum() / forecast_rain.sum()),
        "recall": ((forecast_rain & actual_rain).sum() / actual_rain.sum()),
    }

    test_exprs_2 = {
        "F-score": 2 / (1/pl.col("accuracy") + 1/pl.col("recall")),
    }

    joined = joined.group_by(["station_id", "forecast_time_delta_hours"]).agg(
        [*[expr.alias(label) for (label, expr) in test_exprs_1.items()]]
    )

    joined = joined.sort("forecast_time_delta_hours")

    joined = joined.with_columns([expr.alias(label) for (label, expr) in test_exprs_2.items()])

    all_test_exprs = [
        *test_exprs_1.keys(),
        *test_exprs_2.keys(),
    ]

    joined_agg = joined.group_by("forecast_time_delta_hours").agg(
        pl.col(all_test_exprs).mean(),
    )

    print(joined_agg)

    SIDEEFFECTS_setup_tueplot(relative_path_to_root=".")

    SIDEEFFECTS_choose_color_palette()

    fig, ax = plt.subplots()
    ax: plt.Axes

    for label in all_test_exprs:
        ax.plot(
            joined_agg["forecast_time_delta_hours"].to_numpy(),
            joined_agg.select(label).to_numpy(),
            label=label,
            # zorder=1 if mask_val == 0.0 else -1,
            # c = choosen_palette[idx],
        )

    ax.set_xlabel("$\Delta t$ [h]")
    ax.set_ylabel("value [1]")  # over all stations and call times

    fig.legend()

    fig.savefig(f"{FIG_SAVE_BASE_PATH}/fig_full_statistics.pdf")


if __name__ == "__main__":
    model_1 = DWD_Dataset(
        source_path="./data/dwd", feature=Feature.PRECIPITATION, model=1
    )
    model_2 = DWD_Dataset(
        source_path="./data/dwd", feature=Feature.PRECIPITATION, model=2
    )

    plot_full_statistics(model_1, model_2)
