import logging
from urllib.error import HTTPError
import wget


def get_zips(url, path) -> str:
    """download zip files

    Args:
        url (_type_): _description_
        path (_type_): _description_

    Returns:
        str: _description_
    """
    try:
        file_name = wget.download(url, out=path)
        return file_name
    except HTTPError:
        logging.error(f"Not possible to download {url} because auf HTTP-Error")
        return ""
