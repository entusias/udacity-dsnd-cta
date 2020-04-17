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

    TOPIC_BASE_NAME = "org.chicago.cta"

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

        # TODO: (done) Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry! --> no such attribute, defined in AvroProducer init, s.o.
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092"
        }

        self.avro_producer_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: (done) Configure the AvroProducer
        self.producer = AvroProducer(
            self.avro_producer_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def topic_exists(self,  client: AdminClient):
        """Returns true if a topic with the same name exists on the cluster"""
        topic_list = client.list_topics(timeout=5).topics.values()
        exists = self.topic_name in set(t.topic for t in iter(topic_list))
        return exists

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # TODO: (done) Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        client = AdminClient(self.broker_properties)

        if self.topic_exists(client):
            logger.info(f"Topic {self.topic_name} already exists in Kafka cluster. Skipping.")
            return
        else:
            futures = client.create_topics([
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    # TODO add configs
                    # config = {    # f.e.
                    #              "cleanup.policy": "delete",
                    #              "compression.type": "lz4",
                    #              "delete.retention.ms": "2000",
                    #              "file.delete.delay.ms": "2000",
                    #          },
                )
            ])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic {self.topic_name} created.")
                except Exception as e:
                    logger.exception(f"failed to create topic {self.topic_name}: {e}.")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # TODO: (done) Write cleanup code for the Producer here
        if self.producer is None:
            logger.info("No Producer instantiated, doing nothing.")
        else:
            logger.info("Cleaning up producer.")
            self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
