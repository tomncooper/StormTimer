from sys import stdout
from logging import Logger, getLogger, DEBUG, INFO, Formatter, StreamHandler
from argparse import ArgumentParser


def create_parser() -> ArgumentParser:

    parser: ArgumentParser = ArgumentParser()
    parser.add_argument(
        "config", help="Path to the configuration file for the receiver"
    )
    parser.add_argument(
        "--debug",
        required=False,
        action="store_true",
        help="Flag indicating if debug level logging messages should be printed",
    )

    return parser


def setup_single_logging(debug: bool = False) -> Logger:

    top_log: Logger = getLogger()

    if debug:
        level = DEBUG
        fmt: str = (
            "{levelname} | {name} | "
            "function: {funcName} "
            "| line: {lineno} | {message}"
        )

        style: str = "{"
    else:
        level = INFO
        fmt = "{asctime} | {name} | {levelname} " "| {message}"
        style = "{"

    formatter: Formatter = Formatter(fmt=fmt, style=style)
    handler: StreamHandler = StreamHandler(stream=stdout)
    handler.setFormatter(formatter)

    top_log.setLevel(level)
    top_log.addHandler(handler)

    return top_log
