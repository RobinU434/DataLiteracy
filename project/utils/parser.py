from argparse import ArgumentParser


def add_get_args(parser: ArgumentParser) -> ArgumentParser:
    return parser


def add_analyse_args(parser: ArgumentParser) -> ArgumentParser:
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
    analyse = command_subparser.add_parser("analyse", help="start analysis pipeline")
    analyse = add_analyse_args(analyse)
    get = command_subparser.add_parser(
        "get",
        help="send request to every embedded crawler and return pandas data frame heads onto terminal",
    )
    get = add_get_args(get)
    return parser


def setup_parser(parser: ArgumentParser) -> ArgumentParser:
    parser = setup_dataprocess_parser(parser)
    return parser
