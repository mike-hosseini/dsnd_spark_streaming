"""Kafka consumer client for ingesting Kafka event data"""
import logging
import time
from typing import Any, List

import confluent_kafka
from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(
        self,
        topic_name: str,
        bootstrap_server: str,
        client_id: str,
        offset_earliest: bool = True,
        sleep_secs: float = 1.0,
        consume_timeout: float = 0.1,
    ) -> None:
        """Creates consumer object for synchronous use"""
        self.topic_name: str = topic_name
        self.bootstrap_server = bootstrap_server
        self.client_id = client_id
        self.offset_earliest = offset_earliest
        self.sleep_secs: float = sleep_secs
        self.consume_timeout: float = consume_timeout

        self.broker_properties: List[str, Any] = {
            "bootstrap.servers": self.bootstrap_server,
            "group.id": self.topic_name,
            "client.id": self.client_id,
            "default.topic.config": {"auto.offset.reset": "earliest"},
        }

        self.consumer: Consumer = Consumer(self.broker_properties)
        self.consumer.subscribe([self.topic_name], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            if self.offset_earliest is True:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

    def consume(self) -> None:
        """Consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            time.sleep(self.sleep_secs)

    def message_handler(self, message: confluent_kafka.Message) -> None:
        """Given a kafka message, extract data"""
        logger.debug("messeage retrieved: {0}".format(message.value()))

    def _consume(self) -> int:
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        msg = self.consumer.poll(timeout=self.consume_timeout)

        if msg is None:
            return 0
        elif not msg.error():
            self.message_handler(msg)
            return 1
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logger.debug(
                "end of partition reached {0}/{1}".format(msg.topic(), msg.partition())
            )
            return 0
        else:
            logger.error("error occured: {0}".format(msg.error().str()))
            return 0

        return 0

    def close(self) -> None:
        """Cleans up open Kafka consumer"""
        logger.debug("gracefully shutting down consumer")
        self.consumer.close()
