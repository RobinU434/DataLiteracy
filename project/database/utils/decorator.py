

from typing import Callable


def check_connection(db_func: Callable):
    def inner(self, *args, **kwargs):
        if hasattr(self, "_engine"):
            self.connect()
        return db_func(self, *args, **kwargs)
    return inner