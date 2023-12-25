

from typing import Callable
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure


def polar_plot(plot_func: Callable):

    def wrapper(fig: Figure = None, ax: Axes = None, title: str = "", grid: bool = False, legend: bool = False,*args, **kwargs)
        if fig is None or ax is None:
            fig, ax = plt.subplots(projection="polar")

        plot_func(ax, *args, **kwargs)

        if grid:
            ax.grid()
        if legend:
            ax.legend()
        
        return fig, ax

    return wrapper
