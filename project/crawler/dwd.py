

from typing import List
from crawler.base import BaseCrawler
import requests as rq
import json


class DWDCrawler(BaseCrawler):
    def __init__(self, station_ids: List[int]) -> None:
        super().__init__()

        # TODO: remove test
        self._save_dir = "./data/dwd/test"

        self._url = f'https://dwd.api.proxy.bund.dev/v30/stationOverviewExtended?stationIds={str(station_ids).strip("[]")}'

    def get(self):
        response = rq.get(self._url)
        content = json.loads(response.content.decode("utf-8"))
        return content
