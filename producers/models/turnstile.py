"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    # TODO: (done) Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        self.station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

        # TODO: (done) Complete the below by deciding on a topic name, number of partitions, and number of replicas
        topic_name = f"{self.TOPIC_BASE_NAME}.station.turnstile.v1"  # create single turnstile topic is enough according to Akash A: https://knowledge.udacity.com/questions/69131
        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,  # TODO check for better parameters
            num_replicas=1
        )


    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        # TODO: (done) Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.debug(f"Producing turnstile message for {num_entries} passengers leaving the station at {timestamp}")

        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station_name,
                        "line": self.station.color.name
                    }
                )
            except Exception as e:
                logger.fatal(f"Error while producing sturnstile message. {e}")
                raise e
