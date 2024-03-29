from datetime import datetime
from typing import List
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
        self._sleep_interval = 1

    def start(self):
        job = schedule.every().day.at(self._pull_time).do(self.process)
        # schedule.every(3).seconds.do(self.process)

        print("Job: runs .....")
        print(job)
        while True:
            schedule.run_pending()
            time.sleep(self._sleep_interval)

    def process(self):
        for crawler in self._crawler:
            content = crawler.get(save=True)

    def __str__(self) -> str:
        s = "included crawler: [\n "
        for c in self._crawler:
            s += str(c) + "\n"
        s += "] \n"
        s += "query time: " + self._pull_time
        return s

