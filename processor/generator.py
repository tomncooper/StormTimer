import time
import uuid
import logging

from typing import Iterator
from argparse import ArgumentParser, Namespace
from configparser import ConfigParser

from confluent_kafka import Producer

from common import create_parser, setup_logging

LOG: logging.Logger = logging.getLogger("stormtimer.generator")


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        LOG.error("Message delivery failed: {}".format(err))
    else:
        LOG.debug("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def run(kafka_producer: Producer, topic: str, delay: float):

    while True:
        msg_id: uuid.UUID = uuid.uuid4()
        kafka_producer.poll(1)
        kafka_producer.produce(
            topic, str(msg_id).encode("utf-8"), callback=delivery_report
        )
        time.sleep(delay)


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

    KAF_PROD: Producer = Producer(
        {"bootstrap.servers": CONFIG["kafka"]["server"]}, logger=ST_LOG
    )

    TOPIC: str = CONFIG["producer"]["topic"]

    try:
        LOG.info("Sending messages to topic: %s", TOPIC)
        run(KAF_PROD, TOPIC, 1.0)
    except KeyboardInterrupt:
        print("\nFlushing Producer...")
        KAF_PROD.flush()
