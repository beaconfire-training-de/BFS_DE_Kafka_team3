# Kafka Project 2: Changing Data Capture (CDC)

## Overview
- Create a data tunnel to maintain sync between 2 tables in two different databases
- Stream processing responsible for:
  - Snapshot processing: Reading all data from db1 => send asynchronously to db2
  - Stream processing: Once snapshot synchronized, stream processing keeps looking for new changes
- Insert/Delete/Update => any changes on Emp_A should be reflected in Emp_B < 1 sec
- Architecture: Source DB -> Streaming Replication (Kafka) -> Destination DB

## Approach: SQL Triggering (Postgres Triggers)
- Use Postgres triggers to call an SQL function on every row insert/update/delete
- The SQL function inserts affected rows + action type into a new CDC table
- Producer scans the CDC table (not the original table) and sends to Kafka
- Producer tracks last offset/row consumed to avoid full scans
- Consumer reads from Kafka and applies changes to destination DB based on action

## Steps
1. Modify Docker compose to have TWO databases, both port-forwarded to different ports
2. Use same schema for employee table in both databases:
   - CREATE TABLE employees(emp_id SERIAL, first_name VARCHAR(100), last_name VARCHAR(100), dob DATE, city VARCHAR(100), salary INT);
3. Add PSQL functions and triggers on employee_A table to insert rows into CDC table:
   - CREATE TABLE emp_cdc(emp_id SERIAL, first_name VARCHAR(100), last_name VARCHAR(100), dob DATE, city VARCHAR(100), salary INT, action VARCHAR(100));
4. Modify producer and consumer code:
   - Producer: scan emp_cdc table, send records to Kafka topic
   - Consumer: consume data and update employee_B table based on action
