from typing import List
from project.crawler.base import BaseCrawler
import project.crawler as crawler
from project.crawler.manager import CrawlerManager
from project.database.database import Database
from project.utils.file_system import load_yaml


class DataProcess:
    """
    process for data literacy class
    """

    def __init__(self) -> None:
        """_summary_"""

        self._db = Database(
            user_name="root",
            password="example",
            db_name="WeatherData",
            host_ip="172.19.0.4",
            # host_ip="mariadb.data_literacy_network",
            port=3306,
        )

        self._crawler: List[BaseCrawler]

    def _build_crawler(self):
        self._crawler = []
        for crawler_config in load_yaml("project/config/crawler.config.yaml"):
            name = crawler_config.pop("name")
            crawler_class = getattr(crawler, name)
            self._crawler.append(crawler_class(**crawler_config))

    def build_db(self):
        """
        generate table in SQL data base
        """
        self._db.build_tables()

    def start_crawler(self, crawler_config_path: str):
        """
        start crawler to collect weather data from APIs specified in crawler config

        Args:
            crawler_config_path (str): crawler config for individual apis
        """
        self._build_crawler()
        crawler_manager = CrawlerManager(self._crawler, "00:10")
        crawler_manager.start()

    def analyse(self):
        """
        start analysis pipeline
        """
        pass

    def get(self, save: bool = True):
        """
        send request to every embedded crawler and return pandas data frame heads onto terminal
        """
        self._build_crawler()
        for crawler in self._crawler:
            content = crawler.get(save=save)
            print(content)
