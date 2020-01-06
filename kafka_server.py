"""Kafka producer driver for producing data"""
import logging
import logging.config
from configparser import ConfigParser
from pathlib import Path

import logsetup
import producer_server

PARENT_DIR: Path = Path(__file__).parents[0]

logger: logging.Logger = logging.getLogger(__name__)

config: ConfigParser = ConfigParser()
config.read(PARENT_DIR / "config" / "config.ini")


def run_kafka_server(
    input_file: str, topic_name: str, bootstrap_server: str, client_id: str
) -> producer_server.ProducerServer:
    """Wrapper function for producer server"""
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic_=topic_name,
        bootstrap_servers=bootstrap_server,
        client_id=client_id,
    )

    return producer


def main() -> None:
    """Entry point for Kafka producer driver"""
    topic_name: str = config.get("kafka", "topic_name")
    bootstrap_server: str = config.get("kafka", "bootstrap_server")
    input_file: str = config.get("producer", "input_file")
    client_id: str = config.get("producer", "client_id")

    producer: producer_server.ProducerServer = run_kafka_server(
        input_file=input_file,
        topic_name=topic_name,
        bootstrap_server=bootstrap_server,
        client_id=client_id,
    )

    try:
        logger.debug(f"generating data from input file to topic {topic_name}")
        producer.generate_data()
    except KeyboardInterrupt:
        logger.debug("interrupted by user, stopping production")


if __name__ == "__main__":
    main()
