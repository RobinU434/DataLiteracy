from argparse import ArgumentParser


def add_convert_to_csv_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--input-dir",
        help='root folder of all forecast json files. Defaults to "data/dwd/json/raw',
        dest="input_dir",
        type=str,
        default="data/dwd/raw",
    )
    parser.add_argument(
        "--output-dir",
        help='where to output_dir the csv file structure. Defaults to "data/dwd/raw',
        dest="output_dir",
        type=str,
        default="data/dwd/csv",
    )
    parser.add_argument(
        "--force",
        help="force overwrite the existing files. Defaults to False",
        dest="force",
        action="store_true",
    )
    return parser


def add_get_recent_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--station-ids",
        help="station ids from DWD",
        dest="station_ids",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--save-path",
        help="where you want to store the collected information. It will create this directory if it does not exist already.",
        dest="save_path",
        type=str,
    )
    parser.add_argument(
        "--features",
        help="if set to true we will also unpack the downloaded zips",
        dest="features",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--unpack",
        help="features you want to extract from DWD API",
        dest="unpack",
        action="store_true",
    )
    parser.add_argument(
        "--force",
        help="force overwrite the existing files. Defaults to False",
        dest="force",
        action="store_true",
    )
    return parser


def add_get_historical_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--station-ids",
        help="_description_",
        dest="station_ids",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--save-path",
        help="_description_",
        dest="save_path",
        type=str,
    )
    return parser


def add_get_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--save",
        help="if set to true save results. Defaults to True.",
        dest="save",
        action="store_true",
    )
    return parser


def add_analyse_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--use-active-venv",
        help="set this flag if you have poetry installed and would like to run the analysis in with the active python env",
        dest="use_active_venv",
        action="store_true",
        default="False",
    )
    return parser


def add_start_crawler_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--output-dir",
        help="crawler config for individual apis",
        dest="output_dir",
        type=str,
        default="./data"
    )
    parser.add_argument(
        "--crawler-config-path",
        help="base path for all downloaded crawler data. For each crawler you will get a subdir of this base output dir",
        dest="crawler_config_path",
        type=str,
        default="project/config/crawler.config.yaml",
    )
    parser.add_argument(
        "--query-time",
        help="at which time you would like to collect data from the forecast models",
        dest="query_time",
        type=str,
        default="00:10"
    )
    return parser


def setup_dataprocess_parser(parser: ArgumentParser) -> ArgumentParser:
    command_subparser = parser.add_subparsers(dest="command", title="command")
    start_crawler = command_subparser.add_parser(
        "start-crawler",
        help="start crawler to collect weather data from APIs specified in crawler config",
    )
    start_crawler = add_start_crawler_args(start_crawler)
    analyse = command_subparser.add_parser(
        "analyse", help="start pipeline to analyse data."
    )
    analyse = add_analyse_args(analyse)
    get = command_subparser.add_parser(
        "get",
        help="send request to every embedded crawler and return pandas data frame heads onto terminal",
    )
    get = add_get_args(get)
    get_historical = command_subparser.add_parser(
        "get-historical",
        help="get historical (precipitation, pressure, air temperature) data from the dwd database",
    )
    get_historical = add_get_historical_args(get_historical)
    get_recent = command_subparser.add_parser(
        "get-recent",
        help="get recent (precipitation, pressure, air temperature) data from the dwd database",
    )
    get_recent = add_get_recent_args(get_recent)
    convert_to_csv = command_subparser.add_parser(
        "convert-to-csv", help="convert the forecast data into csv format"
    )
    convert_to_csv = add_convert_to_csv_args(convert_to_csv)
    return parser


def setup_parser(parser: ArgumentParser) -> ArgumentParser:
    parser = setup_dataprocess_parser(parser)
    return parser
