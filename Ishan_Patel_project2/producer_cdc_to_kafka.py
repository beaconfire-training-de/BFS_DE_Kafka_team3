import json
import os
import time
from dotenv import load_dotenv
import psycopg
from confluent_kafka import Producer

load_dotenv()

STATE_FILE = "producer_last_cdc_id.txt"

def load_last_id() -> int:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip() or "0")
    except FileNotFoundError:
        return 0

def save_last_id(x: int) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(x))

def db1_conn():
    return psycopg.connect(
        host=os.getenv("DB1_HOST", "localhost"),
        port=int(os.getenv("DB1_PORT", "5432")),
        dbname=os.getenv("DB1_NAME", "postgres"),
        user=os.getenv("DB1_USER", "postgres"),
        password=os.getenv("DB1_PASSWORD", "postgres"),
    )

def main():
    topic = os.getenv("KAFKA_TOPIC", "bf_employee_cdc")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    poll_sleep = float(os.getenv("PRODUCER_POLL_SECONDS", "0.2"))

    p = Producer({"bootstrap.servers": bootstrap})
    last_id = load_last_id()

    conn = db1_conn()
    conn.autocommit = True

    print(f"✅ Producer running. topic={topic} bootstrap={bootstrap} last_cdc_id={last_id}")

    try:
        while True:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cdc_id, emp_id, first_name, last_name, dob, city, salary, action
                    FROM emp_cdc
                    WHERE cdc_id > %s
                    ORDER BY cdc_id ASC
                    LIMIT 500
                    """,
                    (last_id,),
                )
                rows = cur.fetchall()

            if not rows:
                time.sleep(poll_sleep)
                continue

            for r in rows:
                cdc_id, emp_id, first_name, last_name, dob, city, salary, action = r
                msg = {
                    "cdc_id": int(cdc_id),
                    "emp_id": int(emp_id) if emp_id is not None else None,
                    "first_name": first_name,
                    "last_name": last_name,
                    "dob": dob.isoformat() if dob else None,
                    "city": city,
                    "salary": int(salary) if salary is not None else None,
                    "action": action,
                }

                p.produce(
                    topic=topic,
                    key=str(msg["emp_id"]).encode("utf-8"),
                    value=json.dumps(msg).encode("utf-8"),
                )
                p.poll(0)

                last_id = int(cdc_id)
                save_last_id(last_id)

            p.flush()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
