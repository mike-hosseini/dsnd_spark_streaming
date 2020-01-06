import logging
import logging.config
from configparser import ConfigParser
from pathlib import Path

import logsetup
from consumer import KafkaConsumer
from topic_check import topic_exists

PARENT_DIR: Path = Path(__file__).parents[0]

logger: logging.Logger = logging.getLogger(__name__)

logging.getLogger("confluent_kafka").setLevel(logging.NOTSET)

config: ConfigParser = ConfigParser()
config.read(PARENT_DIR / "config.ini")


def main() -> None:
    topic_name: str = config.get("kafka", "topic_name")
    bootstrap_server: str = config.get("kafka", "bootstrap_server")
    client_id: str = config.get("consumer", "client_id")

    if not topic_exists(topic_name):
        logger.fatal(f"ensure topic {topic_name} exists")
        exit(1)

    consumer = KafkaConsumer(
        topic_name=topic_name, bootstrap_server=bootstrap_server, client_id=client_id
    )
    try:
        logger.debug(f"consuming messages from topic {topic_name}")
        consumer.consume()
    except KeyboardInterrupt:
        consumer.close()
        logger.debug("interrupted by user, stopping consumption")


if __name__ == "__main__":
    main()
