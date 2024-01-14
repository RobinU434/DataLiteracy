

from datetime import datetime


def datetime2timestamp_int(dt: datetime) -> int:
    """converts a given datetime object to an timestamp integer

    Args:
        dt (datetime): datetime object

    Returns:
        int: timestamp
    """
    return int(round(dt.timestamp()))