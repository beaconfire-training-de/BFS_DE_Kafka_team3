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
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from employee import Employee
from producer import employee_topic_name

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': False,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                #implement your logic here

                 # Poll for messages with 1 second timeout
                msg = self.poll(timeout=1.0)
                if msg is None:
                    # No message received
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        print(f'Reached end of partition {msg.partition()}')
                    else:
                        # Real error
                        raise KafkaException(msg.error())
                else:
                    # Process the message
                    print(f"Received message from partition {msg.partition()} at offset {msg.offset()}")
                    try:
                        processing_func(msg)
                        
                        # commit only after successful processing
                        self.commit(msg)
                        print(f"successfully Committed offset {msg.offset()}")
                    except Exception as e:
                        print(f"Error processing message, NOT committing: {e}")
                        # Don't commit - message will be reprocessed on restart

        except KeyboardInterrupt:
            print("\nConsumer interrupted by user")
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            self.close()
            print("consumer closed")

def update_dst(msg):
    e = Employee(**(json.loads(msg.value())))
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port = '5433', # change this port number to align with the docker compose file
            password="postgres")
        conn.autocommit = False
        cur = conn.cursor()
        #your logic goes here
        # Apply changes based on action type
        if e.action == 'INSERT':
            # Insert new employee into destination
            insert_query = """
                INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO NOTHING
            """
            cur.execute(insert_query, (e.emp_id, e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_salary))
            print(f"✓ Inserted emp_id={e.emp_id}")
            
        elif e.action == 'UPDATE':
            # Update existing employee in destination
            update_query = """
                UPDATE employees
                SET first_name=%s, last_name=%s, dob=%s, city=%s, salary=%s
                WHERE emp_id=%s
            """
            cur.execute(update_query, (e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_salary, e.emp_id))
            print(f"✓ Updated emp_id={e.emp_id}")
            
        elif e.action == 'DELETE':
            # Delete employee from destination
            delete_query = """
                DELETE FROM employees WHERE emp_id=%s
            """
            cur.execute(delete_query, (e.emp_id,))
            print(f"✓ Deleted emp_id={e.emp_id}")
            
        else:
            print(f"Unknown action: {e.action}")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as err:
        if conn:
            conn.rollback()
        raise #re-raise to prevent kafka offset commit

if __name__ == '__main__':
    consumer = cdcConsumer(group_id='cdc_consumer_group_1') 
    consumer.consume([employee_topic_name], update_dst)