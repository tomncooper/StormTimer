import logging

from typing import List
from configparser import ConfigParser
from argparse import ArgumentParser, Namespace

from confluent_kafka import (
    Consumer,
    Message,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
)

from common import create_parser, setup_logging

LOG: logging.Logger = logging.getLogger("stormtimer.receiver")


def run(kafka_consumer: Consumer):

    while True:
        msg: Message = kafka_consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            LOG.error("Kafka Consumer error: %s", msg.error())
            continue
        else:

            ts_type, ts_value = msg.timestamp()

            if ts_type == TIMESTAMP_NOT_AVAILABLE:
                LOG.error("No time stamp available")
                continue
            elif ts_type != TIMESTAMP_LOG_APPEND_TIME:
                LOG.warning("Time stamp is not log append time")

            payload = msg.value().decode("utf-8")

            LOG.debug("Received message: %s ts_value: %s", payload, str(ts_value))


if __name__ == "__main__":

    PARSER: ArgumentParser = create_parser()
    ARGS: Namespace = PARSER.parse_args()

    ST_LOG: logging.Logger = setup_logging(ARGS.debug)

    CONFIG: ConfigParser = ConfigParser()
    CONFIG.read(ARGS.config)
    if not CONFIG:
        err_msg: str = f"Could not open config file: {ARGS.config}"
        LOG.error(err_msg)
        raise FileNotFoundError(err_msg)

    KAF_CON: Consumer = Consumer(
        {
            "bootstrap.servers": CONFIG["kafka"]["server"],
            "group.id": CONFIG["consumer"]["group"],
        },
        logger=ST_LOG,
    )

    topic_list: List[str] = [CONFIG["consumer"]["topic"]]

    KAF_CON.subscribe(topic_list)

    try:
        LOG.info("Processing messages from topics: %s", str(topic_list))
        run(KAF_CON)
    except KeyboardInterrupt:
        LOG.info("Keyboard interrupt signal receive. Closing connection")
        KAF_CON.close()
    except SystemExit:
        LOG.info("System exit signal received. Closing connection")
        KAF_CON.close()
