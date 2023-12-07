from typing import Any, Dict, List
from project.crawler.base import BaseCrawler
import requests as rq
import json


class DWDCrawler(BaseCrawler):
    def __init__(self, api_identifier: str, station_ids: List[int]) -> None:
        super().__init__()

        # TODO: remove test
        self._save_dir = "./project/data/dwd/test"

        self._url = self._build_url(api_identifier, station_ids)

    @staticmethod
    def _build_url(api_identifier: str, station_ids: List[str]):
        url = "https://dwd.api.proxy.bund.dev/v30/"
        url += api_identifier

        url += "?stationIds="
        url += str(station_ids).strip("[]").replace(" ", "").replace("'", "")
        return url

    def _get(self) -> Dict[str, Any]:
        response = rq.get(self._url)
        content = json.loads(response.content.decode("utf-8"))
        return content
