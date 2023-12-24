from typing import List

import wget
from project.crawler.base import BaseCrawler
import project.crawler as crawler
from project.crawler.manager import CrawlerManager
from project.database.database import Database
from project.process.utils.download_dwd_data import build_recent_url, check_station_ids
from project.utils.download import get_zips
from project.utils.file_system import load_yaml
from tqdm import tqdm

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
        """generate table in SQL data base"""
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

    def analyse(self, num_samples: int):
        """get historical (precipitation, pressure, air temperature) data from the dwd database

        Args:
            num_samples (int): _description_
        """
        pass

    def get(self, save: bool = True):
        """send request to every embedded crawler and return pandas data frame heads onto terminal

        Args:
            save (bool, optional): _description_. Defaults to True.
        """
        self._build_crawler()
        for crawler in self._crawler:
            content = crawler.get(save=save)
            print(f"===== {type(crawler).__name__} ====")
            print(content)

    def get_historical(self, station_ids: list, save_path: str):
        """get historical (precipitation, pressure, air temperature) data from the dwd database


        Args:
            station_ids (List[str]): _description_
            save_path (str): _description_

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError


    def get_recent(self, station_ids: list, save_path: str):
        """get recent (precipitation, pressure, air temperature) data from the dwd database

        Args:
            station_ids (List[str]): station ids from DWD
            save_path (str): where you want to store the collected information
        """
        # hard-coded
        station_ids = [4189, 13965, 755, 757, 5688, 1197, 1214, 1224, 1255, 6258, 1584, 6259, 2074, 7331, 2575, 2814, 259, 3402, 5562, 6275, 3734, 1602, 3925, 3927, 4160, 4169, 4300, 4349, 6262, 4703, 6263, 5229, 4094, 5664, 5731] 

        mask = check_station_ids(station_ids)
        success = 0
        for station_id, valid in tqdm(zip(station_ids, mask)):
            if not valid:
                continue
            for feature in ["precipitation"]:
                url = build_recent_url(feature, station_id)
                file_name = get_zips(url, save_path)

            if len(file_name):
                success += 1

        print(f"({success}/{len(station_ids)}) where successful.")
