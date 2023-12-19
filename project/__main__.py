from argparse import ArgumentParser
from project.process.process import DataProcess
from project.utils.parser import setup_parser


def execute(args: dict) -> bool:
    module = DataProcess()
    match args["command"]:
        case "build-db":
            module.build_db()

        case "start-crawler":
            module.start_crawler(crawler_config_path=args["crawler_config_path"])

        case "analyse":
            module.analyse()

        case "get":
            module.get()

        case _:
            return False
    return True


def create_parser() -> ArgumentParser:
    parser = ArgumentParser(description="process for data literacy class")

    parser = setup_parser(parser)

    return parser


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()
    args_dict = vars(args)
    if not execute(args_dict):
        parser.print_usage()


if __name__ == "__main__":
    main()
