import logging
import json
import uuid

from typing import List, Dict, Union
from configparser import ConfigParser
from argparse import ArgumentParser, Namespace

from confluent_kafka import (
    Consumer,
    Message,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
)

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError

from common import create_parser, setup_single_logging

LOG: logging.Logger = logging.getLogger("stormtimer.receiver")

METRIC = Dict[str, Union[str, Dict[str, Union[str, int, float]]]]


def process_payload(payload: str, kafka_ts_value: int) -> List[METRIC]:

    path_message = json.loads(payload)

    kafka_diff: int = kafka_ts_value - path_message["originTimestamp"]
    storm_ns_ms: float = path_message["stormNanoLatencyMs"]
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
        "fields": {
            "ns_latency_ms": storm_ns_ms,
            "ms_latency_ms": storm_ms_ms,
            "path": path_str,
        },
    }

    return [kafka_metric, e2e_metric]


def run(kafka_consumer: Consumer, influx_client: InfluxDBClient) -> None:

    while True:
        msg: Message = kafka_consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            LOG.error("Kafka Consumer error: %s", msg.error())
            continue
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

            metrics: List[METRIC] = process_payload(payload, kafka_ts_value)

            try:
                influx_client.write_points(metrics)
            except InfluxDBServerError as ifdb:
                LOG.error(
                    "Received error from InfluxDB whist writing results: %s", ifdb
                )
            except Exception as err:
                LOG.error(f"Failed to send metrics to InfluxDB due to error: %s", err)
            else:
                LOG.debug("Metrics sent to influx: %s", metrics)


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

    KAF_CON: Consumer = Consumer(
        {
            "bootstrap.servers": CONFIG["kafka"]["server"],
            "group.id": str(uuid.uuid4()),
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",
        },
        logger=ST_LOG,
    )

    INFLUX_CLIENT: InfluxDBClient = InfluxDBClient(
        host=CONFIG["influx"]["server"],
        port=8086,
        username=CONFIG["influx"]["user"],
        password=CONFIG["influx"]["password"],
        database=CONFIG["influx"]["database"],
    )

    topic_list: List[str] = [CONFIG["consumer"]["topic"]]

    KAF_CON.subscribe(topic_list)

    try:
        LOG.info("Processing messages from topics: %s", str(topic_list))
        run(KAF_CON, INFLUX_CLIENT)
    except KeyboardInterrupt:
        LOG.info("Keyboard interrupt signal receive. Closing connection")
        KAF_CON.close()
        INFLUX_CLIENT.close()
    except SystemExit:
        LOG.info("System exit signal received. Closing connection")
        KAF_CON.close()
        INFLUX_CLIENT.close()
