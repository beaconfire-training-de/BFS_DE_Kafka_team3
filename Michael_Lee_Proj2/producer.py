"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import json
import time
import psycopg2
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

employee_topic_name = "bf_employee_cdc"

class cdcProducer(Producer):
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {
            'bootstrap.servers': f"{self.host}:{self.port}",
            'acks': 'all'
        }
        super().__init__(producerConfig)
        self.running = True
        # Track the last ID to avoid full table scans
        self.last_processed_cdc_id = 0

    def fetch_cdc(self):
        records = []
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres"
            )
            cur = conn.cursor()

            # Fetch only records greater than our last checkpoint
            query = """
                    SELECT emp_id, first_name, last_name, dob, city, salary, action, cdc_id
                    FROM emp_cdc
                    WHERE cdc_id > %s
                    ORDER BY cdc_id ASC \
                    """
            cur.execute(query, (self.last_processed_cdc_id,))
            rows = cur.fetchall()

            for row in rows:
                # Construct dictionary for JSON serialization
                records.append({
                    "emp_id": row[0],
                    "first_name": row[1],
                    "last_name": row[2],
                    "dob": str(row[3]),  # Date objects must be strings for JSON
                    "city": row[4],
                    "salary": row[5],
                    "action": row[6]
                })
                # Update checkpoint
                self.last_processed_cdc_id = row[7]

            cur.close()
            conn.close()
        except Exception as err:
            print(f"Database error: {err}")

        return records


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == '__main__':
    producer = cdcProducer()
    print("Producer started. Polling for CDC changes...")

    try:
        while producer.running:
            # 1. Fetch new records from Postgres
            new_records = producer.fetch_cdc()

            if new_records:
                for record in new_records:
                    # 2. Produce to Kafka
                    producer.produce(
                        topic=employee_topic_name,
                        key=str(record['emp_id']),
                        value=json.dumps(record),
                        callback=delivery_report
                    )
                # 3. Ensure messages are sent
                producer.flush()

            # 4. Wait briefly before next poll to save CPU/DB resources
            time.sleep(5)

    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.running = False