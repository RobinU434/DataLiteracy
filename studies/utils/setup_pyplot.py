from tueplots import bundles
import matplotlib.font_manager
import matplotlib.pyplot as plt
import os
import typing
from tueplots import cycler, markers,figsizes, fonts
from tueplots.constants import markers as marker_constants
from tueplots.constants.color import palettes


def SIDEEFFECTS_setup_tueplot(
    relative_path_to_root: str = "..", 
    column: typing.Literal["half"] | typing.Literal["full"] = "half",
):
    distr_font = f"{relative_path_to_root}/docs/report/fonts/liberation-fonts-ttf-2.1.5/LiberationSerif-Regular.ttf"
    assert os.path.exists(distr_font)

    matplotlib.font_manager.fontManager.addfont(distr_font)
    props = matplotlib.font_manager.FontProperties(fname=distr_font)

    plt.rcParams.update(bundles.icml2022(usetex=False, column=column))
    plt.rcParams.update({
        'font.serif': [props.get_name()]
    })

def SIDEEFFECTS_choose_color_palette(
    color_palette = palettes.paultol_muted,
):
    plt.rcParams.update(cycler.cycler(color=color_palette))
    return color_palette

FIG_SAVE_BASE_PATH = "./docs/report/fig/"
"path (from root) to the spot we save our figures. Not used everywhere!"