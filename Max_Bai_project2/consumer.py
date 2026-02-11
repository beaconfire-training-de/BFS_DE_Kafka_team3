"""
Kafka Project 2: Changing Data Capture (CDC)

Consumer that reads CDC events from the Kafka topic and applies them
to the destination database (db2). Handles INSERT, UPDATE, and DELETE
actions to keep the destination employees table in sync with the source.

Flow: Kafka Topic -> Consumer -> Destination DB (employees table)
"""

import json
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import CDCRecord
from admin import CDC_TOPIC_NAME


class CDCConsumer(Consumer):
    """
    Kafka consumer that subscribes to the CDC topic and processes
    change events to synchronize the destination database.
    """
    def __init__(self, host="localhost", port="29092", group_id="cdc_consumer_group"):
        self.conf = {
            'bootstrap.servers': f'{host}:{port}',
            'group.id': group_id,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest'
        }
        super().__init__(self.conf)
        self.keep_running = True
        self.group_id = group_id

    def consume_cdc(self, topics, processing_func):
        """
        Subscribe to the CDC topic and continuously process messages.
        Each message is passed to the processing_func for handling.
        """
        try:
            self.subscribe(topics)
            print(f"Subscribed to topic(s): {topics}")
            print(f"Consumer group: {self.group_id}")
            print("-" * 60)

            while self.keep_running:
                msg = self.poll(timeout=1.0)

                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue
                else:
                    processing_func(msg)

        except KeyboardInterrupt:
            print("\nConsumer interrupted by user.")
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            self.close()
            print("Consumer closed.")


class CDCProcessor:
    """
    Processes CDC messages and applies changes to the destination database (db2).
    Handles INSERT, UPDATE, and DELETE operations.
    """
    def __init__(self, db_host="localhost", db_port="5435",
                 db_name="postgres", db_user="postgres", db_password="postgres"):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    def get_connection(self):
        """Create and return a new database connection to the destination DB."""
        return psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )

    def process_message(self, msg):
        """
        Process a single CDC message from Kafka.
        Parses the CDCRecord and applies the corresponding action
        (INSERT, UPDATE, DELETE) to the destination database.
        """
        try:
            # Deserialize the message value
            value = msg.value().decode('utf-8')
            cdc_record = CDCRecord.from_json(value)

            print(f"Processing CDC record: action={cdc_record.action}, "
                  f"emp_id={cdc_record.emp_id}, name={cdc_record.first_name} {cdc_record.last_name}")

            conn = self.get_connection()
            conn.autocommit = True
            cur = conn.cursor()

            if cdc_record.action == 'INSERT':
                self._handle_insert(cur, cdc_record)
            elif cdc_record.action == 'UPDATE':
                self._handle_update(cur, cdc_record)
            elif cdc_record.action == 'DELETE':
                self._handle_delete(cur, cdc_record)
            else:
                print(f"Unknown action: {cdc_record.action}")

            cur.close()
            conn.close()

        except Exception as e:
            print(f"Error processing message: {e}")

    def _handle_insert(self, cur, record):
        """Insert a new employee record into the destination database."""
        try:
            cur.execute(
                """
                INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    dob = EXCLUDED.dob,
                    city = EXCLUDED.city,
                    salary = EXCLUDED.salary
                """,
                (record.emp_id, record.first_name, record.last_name,
                 record.dob if record.dob else None, record.city, record.salary)
            )
            print(f"  -> INSERT applied for emp_id={record.emp_id}")
        except Exception as e:
            print(f"  -> Error on INSERT for emp_id={record.emp_id}: {e}")

    def _handle_update(self, cur, record):
        """Update an existing employee record in the destination database."""
        try:
            cur.execute(
                """
                UPDATE employees
                SET first_name = %s, last_name = %s, dob = %s, city = %s, salary = %s
                WHERE emp_id = %s
                """,
                (record.first_name, record.last_name,
                 record.dob if record.dob else None, record.city, record.salary,
                 record.emp_id)
            )
            if cur.rowcount == 0:
                # If the row doesn't exist yet, insert it (handles out-of-order messages)
                self._handle_insert(cur, record)
                print(f"  -> UPDATE: emp_id={record.emp_id} not found, inserted instead")
            else:
                print(f"  -> UPDATE applied for emp_id={record.emp_id}")
        except Exception as e:
            print(f"  -> Error on UPDATE for emp_id={record.emp_id}: {e}")

    def _handle_delete(self, cur, record):
        """Delete an employee record from the destination database."""
        try:
            cur.execute(
                "DELETE FROM employees WHERE emp_id = %s",
                (record.emp_id,)
            )
            if cur.rowcount > 0:
                print(f"  -> DELETE applied for emp_id={record.emp_id}")
            else:
                print(f"  -> DELETE: emp_id={record.emp_id} not found (already deleted)")
        except Exception as e:
            print(f"  -> Error on DELETE for emp_id={record.emp_id}: {e}")


if __name__ == '__main__':
    processor = CDCProcessor()
    consumer = CDCConsumer(group_id='cdc_consumer_group')

    print("CDC Consumer started. Waiting for CDC events...")
    consumer.consume_cdc([CDC_TOPIC_NAME], processor.process_message)
