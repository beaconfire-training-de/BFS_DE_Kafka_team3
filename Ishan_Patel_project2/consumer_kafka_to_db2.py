import json
import os
from dotenv import load_dotenv
import psycopg
from confluent_kafka import Consumer

load_dotenv()

def db2_conn():
    return psycopg.connect(
        host=os.getenv("DB2_HOST", "localhost"),
        port=int(os.getenv("DB2_PORT", "5433")),
        dbname=os.getenv("DB2_NAME", "postgres"),
        user=os.getenv("DB2_USER", "postgres"),
        password=os.getenv("DB2_PASSWORD", "postgres"),
    )

def apply(cur, e):
    action = (e.get("action") or "").upper()
    emp_id = e.get("emp_id")

    if emp_id is None:
        return

    if action == "INSERT":
        cur.execute(
            """
            INSERT INTO dest_employees(emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (emp_id) DO UPDATE
            SET first_name=EXCLUDED.first_name,
                last_name=EXCLUDED.last_name,
                dob=EXCLUDED.dob,
                city=EXCLUDED.city,
                salary=EXCLUDED.salary
            """,
            (emp_id, e.get("first_name"), e.get("last_name"), e.get("dob"), e.get("city"), e.get("salary")),
        )
    elif action == "UPDATE":
        cur.execute(
            """
            UPDATE dest_employees
            SET first_name=%s, last_name=%s, dob=%s, city=%s, salary=%s
            WHERE emp_id=%s
            """,
            (e.get("first_name"), e.get("last_name"), e.get("dob"), e.get("city"), e.get("salary"), emp_id),
        )
    elif action == "DELETE":
        cur.execute("DELETE FROM dest_employees WHERE emp_id=%s", (emp_id,))

def main():
    topic = os.getenv("KAFKA_TOPIC", "bf_employee_cdc")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    group = os.getenv("KAFKA_GROUP", "employee_sync_group")

    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit only after DB commit
    })
    c.subscribe([topic])

    conn = db2_conn()
    conn.autocommit = False

    print(f"✅ Consumer running. topic={topic} bootstrap={bootstrap} group={group}")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"❌ Kafka error: {msg.error()}")
                continue

            try:
                e = json.loads(msg.value().decode("utf-8"))
                with conn.cursor() as cur:
                    apply(cur, e)
                conn.commit()
                c.commit(message=msg)
            except Exception as ex:
                conn.rollback()
                print(f"❌ Failed message -> rollback. {ex}")
    finally:
        conn.close()
        c.close()

if __name__ == "__main__":
    main()
