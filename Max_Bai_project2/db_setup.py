"""
Kafka Project 2: Changing Data Capture (CDC)

Database setup script that:
1. Creates the employees table and emp_cdc table in the source database (db1)
2. Creates the PSQL trigger function and trigger on the employees table in db1
3. Creates the employees table in the destination database (db2)
"""

import psycopg2


# ============================================================
# SQL Statements for Source Database (db1)
# ============================================================

# The main employees table in the source database
CREATE_EMPLOYEES_TABLE = """
CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);
"""

# The CDC (Change Data Capture) table that logs all changes
# cdc_id is an auto-incrementing ID used by the producer to track offset
CREATE_CDC_TABLE = """
CREATE TABLE IF NOT EXISTS emp_cdc (
    cdc_id SERIAL PRIMARY KEY,
    emp_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(100)
);
"""

# PSQL trigger function: captures INSERT, UPDATE, DELETE on employees table
# and inserts a corresponding row into emp_cdc with the action type
CREATE_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION employee_cdc_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

# Create the trigger on the employees table for INSERT, UPDATE, DELETE
CREATE_TRIGGER = """
DROP TRIGGER IF EXISTS employee_cdc_trigger ON employees;
CREATE TRIGGER employee_cdc_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW
EXECUTE FUNCTION employee_cdc_trigger_func();
"""


def setup_source_db(host="localhost", port="5434", database="postgres",
                    user="postgres", password="postgres"):
    """
    Sets up the source database (db1) with:
    - employees table
    - emp_cdc table (CDC log)
    - Trigger function and trigger on employees table
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        conn.autocommit = True
        cur = conn.cursor()

        print("Setting up source database (db1)...")

        # Create the employees table
        cur.execute(CREATE_EMPLOYEES_TABLE)
        print("  - Created employees table")

        # Create the CDC table
        cur.execute(CREATE_CDC_TABLE)
        print("  - Created emp_cdc table")

        # Create the trigger function
        cur.execute(CREATE_TRIGGER_FUNCTION)
        print("  - Created trigger function: employee_cdc_trigger_func()")

        # Create the trigger
        cur.execute(CREATE_TRIGGER)
        print("  - Created trigger: employee_cdc_trigger on employees table")

        cur.close()
        conn.close()
        print("Source database setup complete.\n")

    except Exception as e:
        print(f"Error setting up source database: {e}")
        raise


def setup_destination_db(host="localhost", port="5435", database="postgres",
                         user="postgres", password="postgres"):
    """
    Sets up the destination database (db2) with:
    - employees table (same schema as source, to be kept in sync)
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        conn.autocommit = True
        cur = conn.cursor()

        print("Setting up destination database (db2)...")

        # Create the employees table (same schema as source)
        cur.execute(CREATE_EMPLOYEES_TABLE)
        print("  - Created employees table")

        cur.close()
        conn.close()
        print("Destination database setup complete.\n")

    except Exception as e:
        print(f"Error setting up destination database: {e}")
        raise


if __name__ == '__main__':
    setup_source_db()
    setup_destination_db()
    print("All database setup completed successfully!")
