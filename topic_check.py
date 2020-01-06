from configparser import ConfigParser
from pathlib import Path

from confluent_kafka.admin import AdminClient

PARENT_DIR: Path = Path(__file__).parents[0]

config: ConfigParser = ConfigParser()
config.read(PARENT_DIR / "config" / "config.ini")


def topic_exists(topic: str) -> bool:
    """Checks if the given topic exists in Kafka"""
    bootstrap_server: str = config.get("kafka", "bootstrap_server")
    client = AdminClient({"bootstrap.servers": bootstrap_server})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
