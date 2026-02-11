import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
from producer import employee_topic_name


class cdcConsumer(Consumer):
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = 'cdc-sync-group'):
        self.conf = {
            'bootstrap.servers': f'{host}:{port}',
            'group.id': group_id,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest'
        }
        super().__init__(self.conf)
        self.keep_running = True

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            print(f"Started consuming from {topics}...")
            while self.keep_running:
                # Poll Kafka for new messages
                msg = self.poll(timeout=1.0)
                if msg is None: continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error()); break

                # Execute the database sync logic
                processing_func(msg)
        finally:
            self.close()


def update_dst(msg):
    print("--- Message Received! ---")  # Debug line
    data = json.loads(msg.value().decode('utf-8'))
    print(f"Attempting to sync: {data}")  # Debug line
    # Parse the JSON payload from Kafka
    data = json.loads(msg.value().decode('utf-8'))

    try:
        # Note the port 5433 to target db_dst per your docker-compose.yml
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port='5433',
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()

        action = data.get('action')
        emp_id = data.get('emp_id')

        if action == 'INSERT':
            cur.execute("""
                        INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                        VALUES (%s, %s, %s, %s, %s, %s);
                        """, (emp_id, data['first_name'], data['last_name'], data['dob'], data['city'], data['salary']))

        elif action == 'UPDATE':
            cur.execute("""
                        UPDATE employees
                        SET first_name=%s,
                            last_name=%s,
                            dob=%s,
                            city=%s,
                            salary=%s
                        WHERE emp_id = %s;
                        """, (data['first_name'], data['last_name'], data['dob'], data['city'], data['salary'], emp_id))

        elif action == 'DELETE':
            cur.execute("DELETE FROM employees WHERE emp_id=%s;", (emp_id,))

        print(f"Successfully synced {action} for emp_id: {emp_id}")
        cur.close()
        conn.close()
    except Exception as err:
        print(f"Sync error: {err}")


if __name__ == '__main__':
    # Using a unique group_id to track progress
    consumer = cdcConsumer(group_id='employee-sync-v4')
    consumer.consume([employee_topic_name], update_dst)