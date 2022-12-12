"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "client.id": "com.udacity.opt.producer.client.id",
            "schema.registry.url": "http://localhost:8081",
        }

        self.admin_client = AdminClient(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "client_id",
            }
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in self.existing_topics:
            self.create_topic()
            self.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        new_topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
        )

        self.admin_client.create_topics([new_topic])

        logger.info(
            f"Created new topic: {self.topic_name} (partitions: {self.num_partitions}, replication factor: {self.num_replicas}"
        )

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.existing_topics.clear()
        if self.producer:
            self.producer.close()

        logger.info("Producer cleaned up and successfully closed.")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
