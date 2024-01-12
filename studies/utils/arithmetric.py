
from numpy import ndarray


def normalized(array: ndarray, axis: int = None) -> ndarray:
    if axis is None:
        mean = array.mean()
        std = array.std()
    else:
        mean = array.mean(axis=None)
        std = array.std(axis=None)

    return (array - mean) / std