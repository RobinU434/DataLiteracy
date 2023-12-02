from datetime import datetime
from typing import Dict, List
from crawler.base import BaseCrawler
import schedule
import time

class CrawlerManager:
    def __init__(
        self, crawler: List[BaseCrawler], pull_time: datetime, sleep_interval: 3600
    ) -> None:
        self._crawler = crawler
        self._pull_time = pull_time
        self._sleep_interval = sleep_interval

    def start(self):
        schedule.every().day("01:00").do(self.process)
        
        while True:
            schedule.run_pending()
            time.sleep(self._sleep_interval) 

    def process(self):
        for crawler in self._crawler:
            content = crawler.get()
            crawler.write()


    def _write(self, content: Dict[str, any]):

