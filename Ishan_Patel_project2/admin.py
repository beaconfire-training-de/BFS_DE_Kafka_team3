import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

def connect(host, port, db, user, pwd):
    return psycopg.connect(host=host, port=int(port), dbname=db, user=user, password=pwd)

def run_sql(conn, path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def main():
    db1 = connect(
        os.getenv("DB1_HOST", "localhost"),
        os.getenv("DB1_PORT", "5432"),
        os.getenv("DB1_NAME", "postgres"),
        os.getenv("DB1_USER", "postgres"),
        os.getenv("DB1_PASSWORD", "postgres"),
    )
    db2 = connect(
        os.getenv("DB2_HOST", "localhost"),
        os.getenv("DB2_PORT", "5433"),
        os.getenv("DB2_NAME", "postgres"),
        os.getenv("DB2_USER", "postgres"),
        os.getenv("DB2_PASSWORD", "postgres"),
    )

    try:
        run_sql(db1, "db1_source_setup.sql")
        run_sql(db2, "db2_destination_setup.sql")
        print("✅ DB1 + DB2 setup completed.")
    finally:
        db1.close()
        db2.close()

if __name__ == "__main__":
    main()
