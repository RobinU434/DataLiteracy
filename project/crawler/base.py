from abc import ABC, abstractmethod
import json
from typing import Dict
import time

class BaseCrawler(ABC):
    def __init__(self) -> None:
        super().__init__()  

        self._save_dir: str

    @abstractmethod
    def get(self):
        raise NotImplementedError
    

    def write(self, content: Dict[str, any]):
        path = self._save_dir + "/" + str(time.time()) + ".json"

        with open(path, "w") as f:
            json.dump(content, f)
