import datetime
import glob
import os
from os import makedirs
from typing import List

from tqdm import tqdm

import project.crawler as crawler
from project.analysis.pipeline import JupyterPipeline
from project.convert.dwd_converter import DWDJsonConverter
from project.convert.utils import insert_column
from project.crawler.base import BaseCrawler
from project.crawler.manager import CrawlerManager
from project.process.utils.download_dwd_data import (
    build_recent_url,
    check_station_ids,
    filter_features,
)
from project.process.utils.unpack_zip import unzip
from project.utils.download import get_zips
from project.utils.file_system import load_json, load_yaml, remove


class DataProcess:
    """
    process for data literacy class
    """

    def __init__(self) -> None:
        """_summary_"""

        self._crawler: List[BaseCrawler]

    def _build_crawler(
        self,
        output_dir: str = "./data",
        crawler_config_path: str = "project/config/crawler.config.yaml",
    ):
        self._crawler = []
        for crawler_config in load_yaml(crawler_config_path):
            name = crawler_config.pop("name")
            crawler_class = getattr(crawler, name)
            crawler_class: BaseCrawler
            self._crawler.append(crawler_class(save_dir=output_dir, **crawler_config))

    def start_crawler(
        self,
        output_dir: str = "./data",
        crawler_config_path: str = "project/config/crawler.config.yaml",
        query_time: str = "00:10"
    ):
        """
        start crawler to collect weather data from APIs specified in crawler config

        Args:
            crawler_config_path (str): crawler config for individual apis
            output_dir (str): base path for all downloaded crawler data. For each crawler you will get a subdir of this base output dir
            query_time (str): at which time a day you would like to collect data from the forecast models. Format "HH:MM". Defaults to 00:10 
        """
        self._build_crawler(output_dir, crawler_config_path)
        crawler_manager = CrawlerManager(self._crawler, query_time)
        print(crawler_manager)
        crawler_manager.start()

    def analyse(
        self,
        use_active_venv: bool = False,
    ):
        """start pipeline to analyse data.

        Args:
            use_active_venv (bool, optional): set this flag if you have poetry installed and would like to run the analysis in with the active python env
        """
        jupyter_pipeline = JupyterPipeline(
            use_active_venv=use_active_venv,
            note_book_listing_path="project/config/analysation_scripts.yaml",
        )
        jupyter_pipeline.run()

    def get(self, save: bool = True):
        """send request to every embedded crawler and return pandas data frame heads onto terminal

        Args:
            save (bool, optional): if set to true save results. Defaults to True.
        """
        self._build_crawler()
        for single_crawler in self._crawler:
            content = single_crawler.get(save=save)
            print(f"===== {type(single_crawler).__name__} ====")
            print(content)

    def get_historical(self, station_ids: List[int], save_path: str):
        """get historical (precipitation, pressure, air temperature) data from the dwd database


        Args:
            station_ids (List[int]): _description_
            save_path (str): _description_

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError(
            "please implement this call your self. You can orientated the code at self.get_recent()"
        )

    def get_recent(
        self,
        station_ids: List[int],
        save_path: str,
        features: List[str],
        unpack: bool = False,
        force: bool = False,
    ):
        """get recent (precipitation, pressure, air temperature) data from the dwd database

        Args:
            station_ids (List[int]): station ids from DWD
            save_path (str): where you want to store the collected information. It will create this directory if it does not exist already.
            unpack (bool): if set to true we will also unpack the downloaded zips
            features (List[str]): features you want to extract from DWD API
            force (bool): force overwrite the existing files. Defaults to False
        """
        reference_dirs = glob.glob(save_path + "/*")
        if len(reference_dirs) > 0 and not force:
            result = input(
                f"There is already reference data {save_path}. Would you like to overwrite them? [Y, n]: "
            )
            force = result.lower() in ["", "y", "yes"]

        if not force and len(reference_dirs) > 0:
            print("Do not overwrite files -> Exit.")
            return

        # remove existing data
        if force:
            print("remove existing directories")
            remove(reference_dirs)
        makedirs(save_path, exist_ok=True)

        features = filter_features(features)

        # convert elements of station ids to ints
        station_ids = list(map(lambda x: int(x), station_ids))

        mask = check_station_ids(station_ids)
        success_counter = 0
        file_names = []
        for station_id, valid in tqdm(
            list(zip(station_ids, mask)), desc="Download files: "
        ):
            if not valid:
                continue
            file_name = ""
            for feature in features:
                url = build_recent_url(feature, station_id)
                file_name = get_zips(url, save_path, verbose=False)
                if len(file_name) > 0:
                    file_names.append(file_name)

            if len(file_name) > 0:
                success_counter += 1

        print(f"({success_counter}/{len(station_ids)}) where successful.")

        if unpack:
            unzip(*file_names)

    def convert_to_csv(
        self,
        input_dir: str = "data/dwd/raw",
        output_dir: str = "data/dwd/csv",
        force: bool = False,
    ):
        """convert the forecast data into csv format

        Args:
            input_dir (str): root folder of all forecast json files. Defaults to "data/dwd/json/raw
            output_dir (str): where to output_dir the csv file structure. Defaults to "data/dwd/raw
            force (bool): force overwrite the existing files. Defaults to False
        """
        input_dir = input_dir.rstrip("/")
        output_dir = output_dir.rstrip("/") + "/"

        # check for existing csv files
        csv_files = glob.glob(output_dir + "*/*.csv")
        if len(csv_files) > 0 and not force:
            result = input(
                f"There are already multiple csv files in {output_dir}. Would you like to overwrite them? [Y, n]: "
            )
            force = result.lower() in ["", "y", "yes"]

        csv_folder = glob.glob(output_dir + "*")
        csv_folder_time_stamps = list(map(lambda x: int(x.split("/")[-1]), csv_folder))

        stats = {"overwrite": 0, "skip": 0, "new": 0}

        # ensure a clean output directory
        # if os.path.exists(output):
        #     rmtree(output)

        # check for existing output directory
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        converter = DWDJsonConverter()
        for file_name in tqdm(
            glob.glob(input_dir + "/*.json"), desc="processing files"
        ):
            call_time_utc = int(file_name.split("/")[-1].split(".")[0])
            data = load_json(file_name)

            dfs = converter.to_df(data)
            dfs = insert_column(
                dfs,
                column_name="call_time",
                value=datetime.datetime.utcfromtimestamp(call_time_utc),
            )

            if call_time_utc not in csv_folder_time_stamps:
                os.makedirs(output_dir + str(call_time_utc))

            for key, df in filter(lambda x: bool(len(x[1])), dfs.items()):
                path = output_dir + str(call_time_utc) + "/" + key + ".csv"
                file_exists = path in csv_files
                if file_exists and force:
                    # overwrite the file
                    stats["overwrite"] += 1
                    os.remove(path)
                    df.to_csv(path)
                    continue
                if file_exists and not force:
                    stats["skip"] += 1
                    continue

                stats["new"] += 1
                df.to_csv(path)

        print("stats: ", stats)
