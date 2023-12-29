from matplotlib.axes import Axes
from numpy import ndarray
from pandas import DataFrame


def plot_bw(ax: Axes, color: str, alpha: float) -> Axes:
    raise NotImplementedError


def plot_precipitation(
    ax: Axes, data: DataFrame | ndarray, color: str, alpha: float = 0.5
) -> Axes:
    raise NotImplementedError