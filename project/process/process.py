from typing import List
from project.process.utils.unpack_zip import unzip

import wget
from project.crawler.base import BaseCrawler
import project.crawler as crawler
from project.crawler.manager import CrawlerManager
from project.database.database import Database
from project.process.utils.download_dwd_data import (
    build_recent_url,
    filter_features,
    check_station_ids,
)
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

    def get_historical(self, station_ids: List[int], save_path: str):
        """get historical (precipitation, pressure, air temperature) data from the dwd database


        Args:
            station_ids (List[int]): _description_
            save_path (str): _description_

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError

    def get_recent(
        self,
        station_ids: List[int],
        save_path: str,
        features: List[str],
        unpack: bool = True,
    ):
        """get recent (precipitation, pressure, air temperature) data from the dwd database

        Args:
            station_ids (List[int]): station ids from DWD
            save_path (str): where you want to store the collected information
            unpack (bool): if set to true we will also unpack the downloaded zips
            features (List[str]): features you want to extract from DWD API
        """
        features = filter_features(features)
        
        # convert elements of station ids to ints
        station_ids = list(map(lambda x: int(x), station_ids))

        mask = check_station_ids(station_ids)
        success_counter = 0
        file_names = []
        for station_id, valid in tqdm(zip(station_ids, mask)):
            if not valid:
                continue
            file_name = ""
            for feature in features:
                url = build_recent_url(feature, station_id)
                file_name = get_zips(url, save_path)
                if len(file_name):
                    file_names.append(file_name)

            if len(file_name):
                success_counter += 1

        print(f"({success_counter}/{len(station_ids)}) where successful.")

        if unpack:
            unzip(*file_names)
