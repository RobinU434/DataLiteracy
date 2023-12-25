from typing import Any, Dict
from project.crawler.base import BaseCrawler


class MeteoBlueCrawler(BaseCrawler):
    def __init__(self, key: str, package: str, format: str = "json") -> None:
        super().__init__()

    def _build_url(self) -> str:
        url = """https://my.meteoblue.com/packages/"basic-1h_basic-day?apikey=CjDcReKosZWltBfI&lat=48.5227&lon=9.05222&asl=333&format=json"""
        return super()._build_url()

    def _get(self) -> Dict[str, Any]:
        return super()._get()
