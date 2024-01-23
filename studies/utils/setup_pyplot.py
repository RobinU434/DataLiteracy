from tueplots import bundles
import matplotlib.font_manager
import matplotlib.pyplot as plt
import os

def SIDEEFFECTS_setup_tueplot(relative_path_to_root: str = ".."):
    distr_font = f"{relative_path_to_root}/docs/report/fonts/liberation-fonts-ttf-2.1.5/LiberationSerif-Regular.ttf"
    assert os.path.exists(distr_font)

    matplotlib.font_manager.fontManager.addfont(distr_font)
    props = matplotlib.font_manager.FontProperties(fname=distr_font)

    plt.rcParams.update(bundles.icml2022(usetex=False))
    plt.rcParams.update({
        'font.serif': [props.get_name()]
    })