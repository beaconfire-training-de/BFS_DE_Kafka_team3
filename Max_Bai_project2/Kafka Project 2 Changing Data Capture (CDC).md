# Kafka Project 2: Changing Data Capture (CDC)

This project implements a **Change Data Capture (CDC)** pipeline using **PostgreSQL triggers**, **Apache Kafka**, and **Python** to maintain real-time synchronization between two separate databases. Any INSERT, UPDATE, or DELETE operation on the source database's `employees` table is automatically captured and replicated to the destination database in under 1 second.

## Architecture Overview

```
Source DB (db1:5433)                    Kafka                     Destination DB (db2:5434)
┌──────────────────┐              ┌──────────────┐              ┌──────────────────┐
│  employees table │──trigger──>  │              │              │  employees table │
│                  │              │  CDC Topic   │              │  (synced copy)   │
│  emp_cdc table   │──producer──> │              │──consumer──> │                  │
└──────────────────┘              └──────────────┘              └──────────────────┘
```

The data flow works as follows:

1. A SQL trigger on the `employees` table in the source database captures every INSERT, UPDATE, and DELETE operation and writes a record into the `emp_cdc` table (the CDC log).
2. The **producer** continuously polls the `emp_cdc` table for new records (tracking the last processed offset) and publishes them to a Kafka topic.
3. The **consumer** subscribes to the Kafka topic and applies each change (INSERT, UPDATE, DELETE) to the `employees` table in the destination database.

## File Descriptions

| File | Purpose |
|---|---|
| `docker-compose.yml` | Defines the infrastructure: Zookeeper, Kafka, Source DB (port 5433), Destination DB (port 5434) |
| `employee.py` | Data model classes: `Employee` (matches employees table) and `CDCRecord` (matches emp_cdc table with action field) |
| `db_setup.py` | Creates tables, trigger function, and trigger in both databases |
| `admin.py` | Kafka admin client to create/delete the `employee_cdc` topic |
| `producer.py` | Polls `emp_cdc` table in source DB and publishes CDC events to Kafka |
| `consumer.py` | Consumes CDC events from Kafka and applies changes to destination DB |
| `test_cdc.py` | Test script that performs INSERT, UPDATE, DELETE operations and shows sync results |

## Database Schema

### employees table (both databases)

```sql
CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);
```

### emp_cdc table (source database only)

```sql
CREATE TABLE emp_cdc (
    cdc_id SERIAL PRIMARY KEY,
    emp_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(100)   -- 'INSERT', 'UPDATE', or 'DELETE'
);
```

### SQL Trigger Function

```sql
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
```

### SQL Trigger

```sql
CREATE TRIGGER employee_cdc_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW
EXECUTE FUNCTION employee_cdc_trigger_func();
```

## How to Run

### Step 1: Start Docker Containers

```bash
docker-compose up -d
```

This starts Zookeeper, Kafka, and two PostgreSQL databases (source on port 5433, destination on port 5434).

### Step 2: Set Up Databases

```bash
python db_setup.py
```

This creates the `employees` table and `emp_cdc` table in the source database, sets up the trigger function and trigger, and creates the `employees` table in the destination database.

### Step 3: Create Kafka Topic

```bash
python admin.py
```

This creates the `employee_cdc` Kafka topic with 3 partitions.

### Step 4: Start the Producer (Terminal 1)

```bash
python producer.py
```

The producer will continuously poll the `emp_cdc` table every 0.5 seconds for new CDC records and send them to Kafka.

### Step 5: Start the Consumer (Terminal 2)

```bash
python consumer.py
```

The consumer will subscribe to the `employee_cdc` topic and apply changes to the destination database.

### Step 6: Test the Pipeline (Terminal 3)

```bash
python test_cdc.py
```

This script performs test INSERT, UPDATE, and DELETE operations on the source database and shows the synchronized state of both databases.

### Manual Testing via psql

You can also connect directly to the source database and make changes:

```bash
# Connect to source database
psql -h localhost -p 5433 -U postgres -d postgres

# Insert a new employee
INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Alice', 'Williams', '1995-07-20', 'Boston', 90000);

# Update an employee
UPDATE employees SET salary = 95000 WHERE first_name = 'Alice';

# Delete an employee
DELETE FROM employees WHERE first_name = 'Alice';

# Check the CDC log
SELECT * FROM emp_cdc;
```

Then verify the changes appeared in the destination database:

```bash
# Connect to destination database
psql -h localhost -p 5434 -U postgres -d postgres

# Check the employees table
SELECT * FROM employees;
```

## Key Design Decisions

The **SQL trigger approach** was chosen over the polling approach because triggers capture all three types of operations (INSERT, UPDATE, DELETE), whereas simple table polling can only detect new inserts. The trigger function writes every change to the `emp_cdc` table with an action column indicating the type of operation, which the producer then reads and forwards through Kafka.

The producer uses **offset tracking** (`last_offset` based on `cdc_id`) to avoid full table scans on every poll cycle. It only queries for records with `cdc_id > last_offset`, making the process efficient and resumable.

The consumer handles **idempotent operations** using `ON CONFLICT` for inserts and graceful handling of missing rows for updates and deletes, ensuring the pipeline is resilient to duplicate or out-of-order messages.

## Requirements

- Docker and Docker Compose
- Python 3.x
- `confluent-kafka` (`pip install confluent-kafka`)
- `psycopg2-binary` (`pip install psycopg2-binary`)
