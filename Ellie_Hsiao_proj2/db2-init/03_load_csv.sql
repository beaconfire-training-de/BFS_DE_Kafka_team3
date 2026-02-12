-- Create staging table
CREATE TABLE employees_stage (
    emp_id TEXT,
    first_name TEXT,
    last_name TEXT,
    dob TEXT,
    city TEXT,
    salary TEXT
);

-- Load raw CSV into staging
COPY employees_stage
FROM '/docker-entrypoint-initdb.d/data/employees.csv'
DELIMITER ','
CSV HEADER;

-- Insert into real table with proper type conversion
INSERT INTO employees(emp_id, first_name, last_name, dob, city, salary)
SELECT 
    emp_id::INT,
    first_name,
    last_name,
    TO_DATE(dob, 'MM/DD/YYYY'),
    city,
    salary::INT
FROM employees_stage;
