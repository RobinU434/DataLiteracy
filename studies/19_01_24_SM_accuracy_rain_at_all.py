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

    historical_accu = historical_accu.with_columns(
        pl.col("precipitation_real").alias("prec_over_zero_real") > 0.0
    )

    # forecast_time_delta_expr = (pl.col("time") - pl.col("call_time")).dt.round(every = datetime.timedelta(hours=1)).alias("forecast_time_delta")
    forecast_time_delta_expr = (((pl.col("time") - pl.col("call_time")) / datetime.timedelta(hours=1)).round()).cast(pl.datatypes.Int32).alias("forecast_time_delta_hours")

    forecast_1_accu = forecast_1_accu.with_columns(
        forecast_time_delta_expr
    )

    forecast_2_accu = forecast_2_accu.with_columns(
        forecast_time_delta_expr
    )

    mask_val = 0.0

    test_expr = ((pl.col("precipitation_forecast") > mask_val) == (pl.col("precipitation_real") > mask_val)).alias("prec_over_zero_forecast_correct")

    # print(forecast_1_accu.sort("forecast_time_delta", descending=True))
    # print(forecast_2_accu.sort("forecast_time_delta"))

    # exit(0)

    # print(historical_accu, forecast_1_accu, forecast_2_accu)

    forecast_1_with_hist = forecast_1_accu.join(historical_accu, on = join_cols, how = "left")
    forecast_2_with_hist = forecast_2_accu.join(historical_accu, on = join_cols, how = "left")

    forecast_1_with_hist = forecast_1_with_hist.with_columns(test_expr)

    forecast_2_with_hist = forecast_2_with_hist.with_columns(test_expr)

    stacked = forecast_1_with_hist.vstack(forecast_2_with_hist).select(["station_id", "forecast_time_delta_hours", "time", "prec_over_zero_forecast_correct", "precipitation_real", "precipitation_forecast"]).sort(["station_id", "forecast_time_delta_hours", "time"])

    test_col = pl.col("prec_over_zero_forecast_correct")

    correct_pred = (
        stacked
            .group_by(["forecast_time_delta_hours"])
            .agg((
                test_col.filter(test_col).count().cast(pl.datatypes.Float64) 
                / test_col.count()
            ).alias("forecast_correct_classified"))
            .sort("forecast_time_delta_hours")
    )

    SIDEEFFECTS_setup_tueplot(relative_path_to_root=".")

    fig, ax = plt.subplots()

    ax.plot(correct_pred["forecast_time_delta_hours"].to_numpy(), correct_pred["forecast_correct_classified"].to_numpy())

    fig.savefig(f"{FIG_SAVE_BASE_PATH}/error.pdf")

if __name__ == "__main__":
    model_1 = DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=1)
    model_2 = DWD_Dataset(source_path="./data/dwd", feature=Feature.PRECIPITATION, model=2)

    plot_accuracy_masked(model_1, model_2)
