import logging

from argparse import ArgumentParser
from typing import Dict, Union


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


def setup_logging(debug: bool = False) -> logging.Logger:

    top_log: logging.Logger = logging.getLogger("stormtimer")

    if debug:
        level = logging.DEBUG
        fmt: str = (
            "{levelname} | {name} | "
            "function: {funcName} "
            "| line: {lineno} | {message}"
        )

        style: str = "{"
    else:
        level = logging.INFO
        fmt = "{asctime} | {name} | {levelname} " "| {message}"
        style = "{"

    formatter: logging.Formatter = logging.Formatter(fmt=fmt, style=style)
    handler: logging.StreamHandler = logging.StreamHandler()
    handler.setFormatter(formatter)

    top_log.setLevel(level)
    top_log.addHandler(handler)

    return top_log
