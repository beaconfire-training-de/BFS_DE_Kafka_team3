-- Create employees table
CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);


-- Create CDC table
CREATE TABLE IF NOT EXISTS emp_cdc (
    cdc_id SERIAL PRIMARY KEY,
    emp_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(10),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);