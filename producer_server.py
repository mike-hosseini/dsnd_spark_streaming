"""Kafka producer client for ingesting JSON event data"""
import json
import logging
import time

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class ProducerServer(KafkaProducer):
    def __init__(self, input_file: str, topic: str, **kwargs) -> None:
        """Creates producer object for synchronous use"""
        super().__init__(
            value_serializer=lambda v: json.dumps(v).encode("utf-8"), **kwargs
        )
        self.input_file = input_file
        self.topic = topic

    def generate_data(self) -> None:
        """Generate data using input JSON data"""
        with open(file=self.input_file) as f:
            json_lines = json.loads(f.read())
            logger.debug(f"processing {len(json_lines)} events")
            for line in json_lines:
                self.send(self.topic, line)
                time.sleep(1)
