import time
import uuid
import logging
import signal

from logging.handlers import QueueHandler, QueueListener
from sys import stdout
from typing import Optional, List, Tuple
from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from multiprocessing import Process, Event, Queue
from multiprocessing.synchronize import Event as MP_Event
from copy import deepcopy

from confluent_kafka import Producer

from common import create_parser

LOG: logging.Logger = logging.getLogger("stormtimer.generator")


class MessageGenerator(Process):
    def __init__(
        self,
        proc_name: str,
        kafka_server: str,
        topic: str,
        emission_delay: Optional[float] = None,
    ):

        Process.__init__(self, name=proc_name)

        self.server: str = kafka_server
        self.topic: str = topic
        self.name: str = proc_name
        self.emission_delay: Optional[float] = emission_delay

        self.exit: MP_Event = Event()

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            LOG.error(
                "Message delivery from Generator %s failed with error: %s",
                self.name,
                str(err),
            )
        else:
            LOG.debug(
                "Message from Generator %s delivered to %s [%d]",
                self.name,
                msg.topic(),
                msg.partition(),
            )

    def run(self):

        # get this child process to ignore the interrupt signals as these will be handled
        # by the main process
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        producer: Producer = Producer(
            {"bootstrap.servers": self.server, "client.id": self.name}
        )

        while not self.exit.is_set():
            msg_id: uuid.UUID = uuid.uuid4()
            producer.poll(1)
            producer.produce(
                self.topic, str(msg_id).encode("utf-8"), callback=self.delivery_report
            )
            if self.emission_delay:
                time.sleep(self.emission_delay)

        producer.flush()


def setup_multi_logging(
    queue: Queue, debug: bool = False
) -> Tuple[logging.Logger, logging.Formatter]:

    top_log: logging.Logger = logging.getLogger("stormtimer.generator")

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

    queue_handler: QueueHandler = QueueHandler(queue)
    queue_handler.setFormatter(formatter)

    top_log.addHandler(queue_handler)
    top_log.setLevel(level)

    return top_log, deepcopy(formatter)


if __name__ == "__main__":

    PARSER: ArgumentParser = create_parser()
    PARSER.add_argument("-p", "--processes", type=int, required=False, default=1)
    PARSER.add_argument(
        "-ed",
        "--emission_delay",
        type=float,
        required=False,
        default=1.0,
        help="The time (in seconds) between sending messages",
    )
    ARGS: Namespace = PARSER.parse_args()

    # Get the module level logger and set it to send all logging messages to a
    # multiprocess queue.
    QUEUE: Queue = Queue()
    ST_LOG: logging.Logger
    ST_FORMATTER: logging.Formatter
    ST_LOG, ST_FORMATTER = setup_multi_logging(QUEUE, ARGS.debug)
    # Attache a listener to the queue with a stream handler to print out the messages on
    # standard out
    ST_HANDLER: logging.StreamHandler = logging.StreamHandler(stream=stdout)
    ST_HANDLER.setFormatter(ST_FORMATTER)
    ST_LISTENER: QueueListener = QueueListener(QUEUE, ST_HANDLER)
    # Start the listeners background thread
    ST_LISTENER.start()

    CONFIG: ConfigParser = ConfigParser()
    CONFIG.read(ARGS.config)
    if not CONFIG:
        err_msg: str = f"Could not open config file: {ARGS.config}"
        LOG.error(err_msg)
        raise FileNotFoundError(err_msg)

    TOPIC: str = CONFIG["producer"]["topic"]
    LOG.info("Sending messages to topic: %s", TOPIC)

    PROCESSES: List[MessageGenerator] = []
    for i in range(ARGS.processes):
        generator: MessageGenerator = MessageGenerator(
            f"Gen_{i}", CONFIG["kafka"]["server"], TOPIC, ARGS.emission_delay
        )
        LOG.info("Starting Generator: %d", i)
        generator.start()
        PROCESSES.append(generator)

    LOG.info("All generators started")

    try:
        for process in PROCESSES:
            process.join()
    except KeyboardInterrupt:
        for i, process in enumerate(PROCESSES):
            LOG.info("Stopping process %d", i)
            process.exit.set()
    finally:
        ST_LISTENER.stop()
        QUEUE.close()
