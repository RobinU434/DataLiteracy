from argparse import ArgumentParser


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
        help="where you want to store the collected information",
        dest="save_path",
        type=str,
    )
    parser.add_argument(
        "--unpack",
        help="if set to true we will also unpack the downloaded zips",
        dest="unpack",
        type=bool,
        default="True",
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
        help="_description_. Defaults to True.",
        dest="save",
        type=bool,
        default="True",
    )
    return parser


def add_analyse_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--num-samples",
        help="_description_",
        dest="num_samples",
        type=int,
    )
    return parser


def add_start_crawler_args(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "--crawler-config-path",
        help="crawler config for individual apis",
        dest="crawler_config_path",
        type=str,
    )
    return parser


def add_build_db_args(parser: ArgumentParser) -> ArgumentParser:
    return parser


def setup_dataprocess_parser(parser: ArgumentParser) -> ArgumentParser:
    command_subparser = parser.add_subparsers(dest="command", title="command")
    build_db = command_subparser.add_parser(
        "build-db", help="generate table in SQL data base"
    )
    build_db = add_build_db_args(build_db)
    start_crawler = command_subparser.add_parser(
        "start-crawler",
        help="start crawler to collect weather data from APIs specified in crawler config",
    )
    start_crawler = add_start_crawler_args(start_crawler)
    analyse = command_subparser.add_parser(
        "analyse",
        help="get historical (precipitation, pressure, air temperature) data from the dwd database",
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
    return parser


def setup_parser(parser: ArgumentParser) -> ArgumentParser:
    parser = setup_dataprocess_parser(parser)
    return parser
