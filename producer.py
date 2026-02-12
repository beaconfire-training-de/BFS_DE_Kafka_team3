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

import csv
import json
import os
from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
from pyspark.sql import SparkSession
import pandas as pd
from confluent_kafka.serialization import StringSerializer
import psycopg2
import time
from employee import Employee

employee_topic_name = "bf_employee_cdc"
OFFSET_FILE = "producer_offset.txt"
class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True
        self.last_processed_id = self.load_offset()

    def load_offset(self):
        """Load the last processed action_id from file"""
        try:
            with open(OFFSET_FILE, 'r') as f:
                offset = int(f.read().strip())
                print(f"Loaded offset: {offset}")
                return offset
        except FileNotFoundError:
            print("No offset file found, starting from 0")
            return 0
        except Exception as e:
            print(f"Error loading offset: {e}, starting from 0")
            return 0
    
    def save_offset(self, action_id):
        """Save the last processed action_id to file"""
        try:
            with open(OFFSET_FILE, 'w') as f:
                f.write(str(action_id))
            self.last_processed_id = action_id
        except Exception as e:
            print(f"Error saving offset: {e}")

    def fetch_cdc(self,):
        """fetch new CDC records since last offset"""
        records = []
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic should go here
             # Query only new records after last_processed_id (avoids full table scan!)
            query = """
                SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
                FROM emp_cdc
                WHERE action_id > %s
                ORDER BY action_id
            """
            cur.execute(query, (self.last_processed_id,))
            
            rows = cur.fetchall()
            
            for row in rows:
                records.append(row)            

            cur.close()
            conn.close()
        except Exception as err:
            # pass
            print(f"Error fetching CDC records: {err}")
        return records # if you need to return sth, modify here
     
    def delivery_report(self, err, msg):
        """Callback for message delivery confirmation"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()
    
    print("CDC Producer started.")
    
    # ========== SNAPSHOT PHASE ==========
    if producer.last_processed_id == 0:
        print("Starting SNAPSHOT processing...")
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            
            # Read ALL existing employees
            cur.execute("SELECT emp_id, first_name, last_name, dob, city, salary FROM employees")
            rows = cur.fetchall()
            
            print(f"Found {len(rows)} existing employees to snapshot")
            
            for row in rows:
                # Create a synthetic INSERT CDC record
                snapshot_record = {
                    'action_id': 0,  # Special marker for snapshot
                    'emp_id': row[0],
                    'emp_FN': row[1],
                    'emp_LN': row[2],
                    'emp_dob': str(row[3]),
                    'emp_city': row[4],
                    'emp_salary': row[5],
                    'action': 'INSERT'
                }
                
                employee_json = json.dumps(snapshot_record)
                
                producer.produce(
                    topic=employee_topic_name,
                    key=encoder(str(row[0])),
                    value=encoder(employee_json),
                    callback=producer.delivery_report
                )
            
            producer.flush()
            print("Snapshot completed!")
            
            cur.close()
            conn.close()
            
        except Exception as e:
            print(f"Error during snapshot: {e}")
    
    # ========== STREAM PROCESSING PHASE ==========
    print("Starting STREAM processing...")
    
    try:
        while producer.running:
            # Fetch new CDC records
            cdc_records = producer.fetch_cdc()
            
            if cdc_records:
                print(f"Found {len(cdc_records)} new CDC record(s)")
                
                for record in cdc_records:
                    employee = Employee.from_line(record)
                    employee_json = employee.to_json()
                    
                    producer.produce(
                        topic=employee_topic_name,
                        key=encoder(str(employee.emp_id)),
                        value=encoder(employee_json),
                        callback=producer.delivery_report
                    )
                    
                    producer.flush()
                    producer.save_offset(employee.action_id)
                    
                    print(f"Sent: action_id={employee.action_id}, emp_id={employee.emp_id}, action={employee.action}")
            else:
                time.sleep(1)
            
            producer.poll(0)
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        print("Producer stopped.")