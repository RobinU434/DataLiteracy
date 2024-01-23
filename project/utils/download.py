import logging
from urllib.error import HTTPError
import wget
from wget import bar_adaptive


def get_zips(url, path, verbose: bool = False) -> str:
    """download zip files

    Args:
        url (_type_): _description_
        path (_type_): _description_
        verbose (bool): If you would like to show a download bar. Defaults to False

    Returns:
        str: _description_
    """
    if verbose:
        bar_func = bar_adaptive
    else:
        bar_func = None

    try:
        file_name = wget.download(url, out=path, bar=bar_func)
        return file_name
    except HTTPError:
        msg = f"Not possible to download {url} because auf HTTP-Error"
        logging.error(msg)
        return ""
