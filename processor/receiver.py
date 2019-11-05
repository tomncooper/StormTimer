import logging
import json
import uuid

import datetime as dt

from typing import List, Dict, Union
from configparser import ConfigParser
from argparse import ArgumentParser, Namespace

from confluent_kafka import (
    Consumer,
    Message,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    KafkaError,
)

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError

from common import create_parser, setup_single_logging

LOG: logging.Logger = logging.getLogger("stormtimer.receiver")

METRIC = Dict[str, Union[str, Dict[str, Union[str, int, float]]]]


def process_payload(payload: str, kafka_ts_value: int) -> List[METRIC]:

    path_message = json.loads(payload)

    kafka_diff: int = kafka_ts_value - path_message["originTimestamp"]
    storm_ms_ms: float = path_message["stormMilliLatencyMs"]

    LOG.debug(
        "Received message: %s at time: %d with time delta %d ms",
        payload,
        kafka_ts_value,
        kafka_diff,
    )

    path: List[str] = path_message["path"]
    spout_comp: str
    spout_task: str
    spout_comp, spout_task = path[0].split(":")
    sink_comp: str
    sink_task: str
    sink_comp, sink_task = path[-1].split(":")

    path_str: str = ">".join(path)

    kafka_metric: METRIC = {
        "measurement": "measured-kafka-latency",
        "tags": {"spout_component": spout_comp, "spout_task": int(spout_task)},
        "fields": {"value": kafka_diff, "path": path_str},
    }

    e2e_metric: METRIC = {
        "measurement": "measured-end2end-latency",
        "tags": {
            "spout_component": spout_comp,
            "spout_task": int(spout_task),
            "sink_component": sink_comp,
            "sink_task": int(sink_task),
        },
        "fields": {"ms_latency_ms": storm_ms_ms, "path": path_str},
    }

    return [kafka_metric, e2e_metric]


def run(kafka_consumer: Consumer, influx_client: InfluxDBClient) -> None:

    time_limit: dt.timedelta = dt.timedelta(minutes=1)
    last_download: dt.datetime = dt.datetime.now()

    while True:

        time_since_last_download: dt.timedelta = dt.datetime.now() - last_download

        if time_since_last_download >= time_limit:
            delay_err: str = (
                f"It has been {time_since_last_download} since the last download from "
                f"the broker"
            )
            LOG.warning(delay_err)
            raise RuntimeError(delay_err)

        try:
            msgs: List[Message] = kafka_consumer.consume(num_messages=100)
        except KafkaError as kafka_err:
            LOG.error(
                "Attempting to consume from Kafka broker resulted in Kafka error: %s",
                str(kafka_err),
            )
            continue
        except Exception as read_err:
            LOG.error(
                "Consuming from Kafka broker resulted in error (%s): %s",
                str(type(read_err)),
                str(read_err),
            )
            continue
        else:
            last_download = dt.datetime.now()

        if not msgs:
            LOG.debug("Returned list from kafka consumer was empty")
            continue
        else:
            LOG.info("Fetched %d messages from Kafka broker", len(msgs))

        for msg in msgs:
            if msg.error():
                LOG.error("Kafka message error code: %s", msg.error().str())
            else:
                kafka_ts_type: int
                kafka_ts_value: int
                kafka_ts_type, kafka_ts_value = msg.timestamp()

                if kafka_ts_type == TIMESTAMP_NOT_AVAILABLE:
                    LOG.error("No time stamp available")
                    continue
                elif kafka_ts_type != TIMESTAMP_LOG_APPEND_TIME:
                    LOG.error("Time stamp is not log append time")
                    continue

                payload = msg.value().decode("utf-8")

                try:
                    metrics: List[METRIC] = process_payload(payload, kafka_ts_value)
                except Exception as proc_error:
                    LOG.error("Error processing message payload: %s", str(proc_error))
                else:
                    try:
                        influx_client.write_points(metrics)
                    except InfluxDBServerError as ifdb:
                        LOG.error(
                            "Received error from InfluxDB whist writing results: %s",
                            ifdb,
                        )
                    except Exception as err:
                        LOG.error(
                            f"Failed to send metrics to InfluxDB due to error: %s", err
                        )
                    else:
                        LOG.debug("Metrics sent to influx: %s", metrics)


def create_kafka_consumer(config, logger):

    kafka_consumer: Consumer = Consumer(
        {
            "bootstrap.servers": config["kafka"]["server"],
            "group.id": "stormtimer.receiver",
            "client.id": "receiver",
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",
        },
        logger=logger,
    )
    topic_list: List[str] = [config["consumer"]["topic"]]
    kafka_consumer.subscribe(topic_list)

    return kafka_consumer


if __name__ == "__main__":

    PARSER: ArgumentParser = create_parser()
    ARGS: Namespace = PARSER.parse_args()

    ST_LOG: logging.Logger = setup_single_logging(ARGS.debug)

    CONFIG: ConfigParser = ConfigParser()
    CONFIG.read(ARGS.config)
    if not CONFIG:
        err_msg: str = f"Could not open config file: {ARGS.config}"
        LOG.error(err_msg)
        raise FileNotFoundError(err_msg)

    INFLUX_CLIENT: InfluxDBClient = InfluxDBClient(
        host=CONFIG["influx"]["server"],
        port=8086,
        username=CONFIG["influx"]["user"],
        password=CONFIG["influx"]["password"],
        database=CONFIG["influx"]["database"],
    )

    KAF_CON: Consumer = create_kafka_consumer(CONFIG, ST_LOG)

    try:

        while True:
            try:
                LOG.info("Processing messages from broker")
                run(KAF_CON, INFLUX_CLIENT)
            except RuntimeError:
                LOG.error("Restarting kafka consumer")
                continue

    except KeyboardInterrupt:
        LOG.info("Keyboard interrupt signal receive. Closing connection")
        KAF_CON.close()
        INFLUX_CLIENT.close()
    except SystemExit:
        LOG.info("System exit signal received. Closing connection")
        KAF_CON.close()
        INFLUX_CLIENT.close()
