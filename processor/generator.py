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


class MessageGenerator(Process):
    def __init__(
        self,
        proc_name: str,
        kafka_server: str,
        topic: str,
        log_queue: Queue,
        emission_delay: Optional[float] = None,
        debug: bool = False,
    ):

        Process.__init__(self, name=proc_name)

        self.server: str = kafka_server
        self.topic: str = topic
        self.name: str = proc_name
        self.emission_delay: Optional[float] = emission_delay
        self.setup_logging(log_queue, debug)

        self.exit: MP_Event = Event()

    def setup_logging(self, queue: Queue, debug: bool):

        self.log: logging.Logger = logging.getLogger(
            f"stormtimer.generator.{self.name}"
        )

        if debug:
            level = logging.DEBUG
        else:
            level = logging.INFO

        self.queue_handler: QueueHandler = QueueHandler(queue)
        self.log.handlers = []
        self.log.addHandler(self.queue_handler)
        self.log.setLevel(level)

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            self.log.error(
                "Message delivery from Generator %s failed with error: %s",
                self.name,
                str(err),
            )
        else:
            self.log.debug(
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
            {
                "bootstrap.servers": self.server,
                "client.id": self.name,
                "acks": 0,
                "retries": 0,
            },
            logger=self.log,
        )

        producer.poll(1)

        while not self.exit.is_set():
            msg_id: uuid.UUID = uuid.uuid4()
            try:
                producer.produce(self.topic, str(msg_id).encode("utf-8"))
            except Exception as err:
                self.log.error("Message sending failed with error: %s", str(err))
            else:
                if self.emission_delay:
                    time.sleep(self.emission_delay)

        self.log.info(
            "Generator: %s received stop signal, flushing remaining messages", self.name
        )
        remaining: int = producer.flush(10)
        self.log.info(
            "Generator: %s stopped flushing with %d messages remaining in buffer",
            self.name,
            remaining,
        )


def setup_multi_logging(
    queue: Queue, debug: bool = False
) -> Tuple[logging.Logger, QueueListener]:

    top_log: logging.Logger = logging.getLogger()

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

    queue_handler: QueueHandler = QueueHandler(queue)
    top_log.addHandler(queue_handler)
    top_log.setLevel(level)

    formatter: logging.Formatter = logging.Formatter(fmt=fmt, style=style)
    console: logging.StreamHandler = logging.StreamHandler(stream=stdout)
    console.setFormatter(formatter)

    listener: QueueListener = QueueListener(queue, console)

    return top_log, listener


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
    ST_LISTENER: QueueListener
    ST_LOG, ST_LISTENER = setup_multi_logging(QUEUE, ARGS.debug)
    ST_LISTENER.start()

    CONFIG: ConfigParser = ConfigParser()
    CONFIG.read(ARGS.config)
    if not CONFIG:
        err_msg: str = f"Could not open config file: {ARGS.config}"
        ST_LOG.error(err_msg)
        raise FileNotFoundError(err_msg)

    TOPIC: str = CONFIG["producer"]["topic"]
    ST_LOG.info("Sending messages to topic: %s", TOPIC)

    PROCESSES: List[MessageGenerator] = []
    for i in range(ARGS.processes):
        generator: MessageGenerator = MessageGenerator(
            f"Gen_{i}",
            CONFIG["kafka"]["server"],
            TOPIC,
            QUEUE,
            ARGS.emission_delay,
            ARGS.debug,
        )
        ST_LOG.info("Starting Generator: %d", i)
        generator.start()
        PROCESSES.append(generator)

    ST_LOG.info("All generators started")

    try:
        for process in PROCESSES:
            process.join()
    except KeyboardInterrupt:
        for i, process in enumerate(PROCESSES):
            ST_LOG.info("Stopping process %d", i)
            process.exit.set()
    finally:
        ST_LISTENER.stop()
        QUEUE.close()
