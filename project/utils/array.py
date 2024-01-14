
from typing import Callable

import numpy as np
from numpy import ndarray


def operate_on_same_index(a: ndarray, indices: ndarray, operation: Callable) -> ndarray:
    """_summary_

    Args:
        a (ndarray): _description_
        indices (ndarray): _description_
        operation (Callable): _description_

    Returns:
        ndarray: _description_
    """
    result = []
    for index in np.unique(indices):
        a_idx = np.where(index == indices)
        result.append(operation(a[a_idx]))
    result = np.array(result)
    return result