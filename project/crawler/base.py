from abc import ABC, abstractmethod
import json
from typing import Any, Dict
import time

from project.utils.file_system import write_json


class BaseCrawler(ABC):
    def __init__(self) -> None:
        super().__init__()

        self._save_dir: str

    def get(self, save: bool = False):
        content = self._get()

        if save:
            path = self._save_dir + "/" + str(time.time()) + ".json"
            write_json(path, content)

        return content
    
    @abstractmethod
    def _build_url(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _get(self) -> Dict[str, Any]:
        raise NotImplementedError
    

