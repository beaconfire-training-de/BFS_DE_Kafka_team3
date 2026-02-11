"""
Kafka Project 2: Changing Data Capture (CDC)

Producer that continuously scans the emp_cdc table in the source database (db1)
and sends new CDC records to the Kafka topic. It tracks the last consumed offset
(cdc_id) to avoid re-scanning already processed records.

Flow: Source DB (emp_cdc table) -> Producer -> Kafka Topic
"""

import json
import time
import psycopg2

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from employee import CDCRecord
from admin import CDC_TOPIC_NAME


class CDCProducer(Producer):
    """
    Kafka producer that reads from the emp_cdc table and publishes
    CDC events to the Kafka topic.
    """
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producer_config = {
            'bootstrap.servers': f"{self.host}:{self.port}",
            'acks': 'all'
        }
        super().__init__(producer_config)


class CDCDataHandler:
    """
    Handles reading CDC records from the source database's emp_cdc table.
    Keeps track of the last processed cdc_id (offset) to only fetch new changes.
    """
    def __init__(self, db_host="localhost", db_port="5434",
                 db_name="postgres", db_user="postgres", db_password="postgres"):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.last_offset = 0  # Track the last processed cdc_id

    def get_connection(self):
        """Create and return a new database connection."""
        return psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )

    def get_new_cdc_records(self):
        """
        Fetch all CDC records with cdc_id greater than the last processed offset.
        This avoids full table scans on every poll cycle.
        """
        records = []
        try:
            conn = self.get_connection()
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                "SELECT cdc_id, emp_id, first_name, last_name, dob, city, salary, action "
                "FROM emp_cdc WHERE cdc_id > %s ORDER BY cdc_id ASC",
                (self.last_offset,)
            )

            rows = cur.fetchall()
            for row in rows:
                record = CDCRecord(
                    cdc_id=row[0],
                    emp_id=row[1],
                    first_name=row[2],
                    last_name=row[3],
                    dob=str(row[4]) if row[4] else '',
                    city=row[5],
                    salary=row[6],
                    action=row[7]
                )
                records.append(record)

                # Update the last offset to the latest cdc_id processed
                if record.cdc_id > self.last_offset:
                    self.last_offset = record.cdc_id

            cur.close()
            conn.close()

        except Exception as e:
            print(f"Error fetching CDC records: {e}")

        return records


def delivery_report(err, msg):
    """Callback for Kafka message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = CDCProducer()
    data_handler = CDCDataHandler()

    print("CDC Producer started. Polling emp_cdc table for changes...")
    print(f"Publishing to Kafka topic: '{CDC_TOPIC_NAME}'")
    print("-" * 60)

    try:
        while True:
            # Fetch new CDC records since last offset
            new_records = data_handler.get_new_cdc_records()

            if new_records:
                print(f"Found {len(new_records)} new CDC record(s). Sending to Kafka...")
                for record in new_records:
                    # Use emp_id as the key for partitioning
                    producer.produce(
                        CDC_TOPIC_NAME,
                        key=encoder(str(record.emp_id)),
                        value=encoder(record.to_json()),
                        callback=delivery_report
                    )
                    producer.poll(0)

                producer.flush()
                print(f"All records sent. Last offset: {data_handler.last_offset}")
            else:
                pass  # No new records, continue polling

            # Poll interval: check for new CDC records every 0.5 seconds
            # This ensures changes are reflected in < 1 second
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nCDC Producer stopped.")
    finally:
        producer.flush()
