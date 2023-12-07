from datetime import datetime
from typing import Dict, List
from project.crawler.base import BaseCrawler
import schedule
import time


class CrawlerManager:
    def __init__(
        self,
        crawler: List[BaseCrawler],
        pull_time: datetime,
    ) -> None:
        self._crawler = crawler
        self._pull_time = pull_time

    def start(self):
        schedule.every().day(self._pull_time).do(self.process)

        while True:
            schedule.run_pending()
            time.sleep(self._sleep_interval)

    def process(self):
        for crawler in self._crawler:
            content = crawler.get()
            crawler.write()
