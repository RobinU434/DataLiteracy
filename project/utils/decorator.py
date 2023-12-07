import os
from typing import Callable

def create_path(write_func: Callable):
    def inner(file_path: str, content: object, **kwargs):
        
        dir_path = "/".join(file_path.split("/")[:-1])
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        write_func(file_path=file_path, content=content)

    return inner