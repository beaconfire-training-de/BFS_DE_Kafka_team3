"""
Kafka Project 2: Changing Data Capture (CDC)

Test script to demonstrate CDC functionality by performing INSERT, UPDATE,
and DELETE operations on the source database's employees table.
The SQL triggers will automatically capture these changes into the emp_cdc table,
and the producer/consumer pipeline will replicate them to the destination database.

Usage:
    1. Start Docker containers: docker-compose up -d
    2. Run db_setup.py to create tables and triggers
    3. Run admin.py to create the Kafka topic
    4. Start producer.py in one terminal
    5. Start consumer.py in another terminal
    6. Run this script to perform test operations
"""

import psycopg2
import time


def get_source_connection():
    """Connect to the source database (db1) on port 5434."""
    return psycopg2.connect(
        host="localhost",
        port="5434",
        database="postgres",
        user="postgres",
        password="postgres"
    )


def get_destination_connection():
    """Connect to the destination database (db2) on port 5435."""
    return psycopg2.connect(
        host="localhost",
        port="5435",
        database="postgres",
        user="postgres",
        password="postgres"
    )


def show_table(conn, label):
    """Display all records in the employees table."""
    cur = conn.cursor()
    cur.execute("SELECT emp_id, first_name, last_name, dob, city, salary FROM employees ORDER BY emp_id")
    rows = cur.fetchall()
    print(f"\n{'='*60}")
    print(f"  {label} - employees table ({len(rows)} records)")
    print(f"{'='*60}")
    if rows:
        print(f"  {'emp_id':<8} {'first_name':<15} {'last_name':<15} {'dob':<12} {'city':<15} {'salary':<10}")
        print(f"  {'-'*8} {'-'*15} {'-'*15} {'-'*12} {'-'*15} {'-'*10}")
        for row in rows:
            print(f"  {row[0]:<8} {row[1]:<15} {row[2]:<15} {str(row[3]):<12} {row[4]:<15} {row[5]:<10}")
    else:
        print("  (empty)")
    cur.close()


def show_cdc_table(conn):
    """Display all records in the emp_cdc table."""
    cur = conn.cursor()
    cur.execute("SELECT cdc_id, emp_id, first_name, last_name, action FROM emp_cdc ORDER BY cdc_id")
    rows = cur.fetchall()
    print(f"\n{'='*60}")
    print(f"  Source DB - emp_cdc table ({len(rows)} records)")
    print(f"{'='*60}")
    if rows:
        print(f"  {'cdc_id':<8} {'emp_id':<8} {'first_name':<15} {'last_name':<15} {'action':<10}")
        print(f"  {'-'*8} {'-'*8} {'-'*15} {'-'*15} {'-'*10}")
        for row in rows:
            print(f"  {row[0]:<8} {row[1]:<8} {row[2]:<15} {row[3]:<15} {row[4]:<10}")
    else:
        print("  (empty)")
    cur.close()


def test_insert(conn):
    """Test INSERT operations on the source database."""
    print("\n" + "=" * 60)
    print("  TEST: Inserting 3 employees into source database...")
    print("=" * 60)

    conn.autocommit = True
    cur = conn.cursor()

    employees = [
        ("John", "Doe", "1990-05-15", "New York", 75000),
        ("Jane", "Smith", "1985-08-22", "Los Angeles", 82000),
        ("Bob", "Johnson", "1992-03-10", "Chicago", 68000),
    ]

    for emp in employees:
        cur.execute(
            "INSERT INTO employees (first_name, last_name, dob, city, salary) "
            "VALUES (%s, %s, %s, %s, %s)",
            emp
        )
        print(f"  Inserted: {emp[0]} {emp[1]}")

    cur.close()


def test_update(conn):
    """Test UPDATE operation on the source database."""
    print("\n" + "=" * 60)
    print("  TEST: Updating John Doe's salary to 85000...")
    print("=" * 60)

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        "UPDATE employees SET salary = 85000 WHERE first_name = 'John' AND last_name = 'Doe'"
    )
    print(f"  Updated {cur.rowcount} row(s)")

    cur.close()


def test_delete(conn):
    """Test DELETE operation on the source database."""
    print("\n" + "=" * 60)
    print("  TEST: Deleting Bob Johnson from source database...")
    print("=" * 60)

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM employees WHERE first_name = 'Bob' AND last_name = 'Johnson'"
    )
    print(f"  Deleted {cur.rowcount} row(s)")

    cur.close()


if __name__ == '__main__':
    print("=" * 60)
    print("  Kafka Project 2: CDC Test Script")
    print("=" * 60)
    print("\nMake sure producer.py and consumer.py are running in separate terminals!\n")

    source_conn = get_source_connection()
    dest_conn = get_destination_connection()

    # Step 1: Insert employees
    test_insert(source_conn)
    print("\nWaiting 3 seconds for CDC pipeline to process...")
    time.sleep(3)

    # Show state after inserts
    show_table(source_conn, "Source DB (db1)")
    show_cdc_table(source_conn)
    show_table(dest_conn, "Destination DB (db2)")

    # Step 2: Update an employee
    test_update(source_conn)
    print("\nWaiting 3 seconds for CDC pipeline to process...")
    time.sleep(3)

    # Show state after update
    show_table(source_conn, "Source DB (db1)")
    show_cdc_table(source_conn)
    show_table(dest_conn, "Destination DB (db2)")

    # Step 3: Delete an employee
    test_delete(source_conn)
    print("\nWaiting 3 seconds for CDC pipeline to process...")
    time.sleep(3)

    # Show final state
    show_table(source_conn, "Source DB (db1)")
    show_cdc_table(source_conn)
    show_table(dest_conn, "Destination DB (db2)")

    source_conn.close()
    dest_conn.close()

    print("\n" + "=" * 60)
    print("  Test complete! Compare Source DB and Destination DB above.")
    print("=" * 60)
