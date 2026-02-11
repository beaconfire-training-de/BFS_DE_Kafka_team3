"""
Kafka Project 2: Changing Data Capture (CDC)

Admin client for managing Kafka topics.
Creates the CDC topic used for streaming employee changes between databases.
"""

from confluent_kafka.admin import AdminClient, NewTopic


# Kafka topic for CDC events
CDC_TOPIC_NAME = "employee_cdc"


class CDCAdminClient(AdminClient):
    """
    AdminClient that manages Kafka topics for the CDC pipeline.
    """
    def __init__(self, host="localhost", port="29092"):
        config = {'bootstrap.servers': f'{host}:{port}'}
        super().__init__(config)

    def topic_exists(self, topic):
        """Check if a topic already exists in the Kafka cluster."""
        metadata = self.list_topics()
        for t in iter(metadata.topics.values()):
            if t.topic == topic:
                return True
        return False

    def create_topic(self, topic, num_partitions=3):
        """Create a new Kafka topic with the specified number of partitions."""
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        result_dict = self.create_topics([new_topic])
        for topic_name, future in result_dict.items():
            try:
                future.result()
                print(f"Topic '{topic_name}' created with {num_partitions} partitions.")
            except Exception as e:
                print(f"Failed to create topic '{topic_name}': {e}")

    def delete_topic(self, topics):
        """Delete one or more Kafka topics."""
        fs = self.delete_topics(topics, operation_timeout=5)
        for topic_name, f in fs.items():
            try:
                f.result()
                print(f"Topic '{topic_name}' deleted.")
            except Exception as e:
                print(f"Failed to delete topic '{topic_name}': {e}")


if __name__ == '__main__':
    client = CDCAdminClient()
    num_partitions = 3

    if client.topic_exists(CDC_TOPIC_NAME):
        print(f"Topic '{CDC_TOPIC_NAME}' already exists. Deleting...")
        client.delete_topic([CDC_TOPIC_NAME])
    else:
        print(f"Creating topic '{CDC_TOPIC_NAME}'...")
        client.create_topic(CDC_TOPIC_NAME, num_partitions)
