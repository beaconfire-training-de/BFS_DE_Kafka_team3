-- DB2 (DESTINATION)
-- Table that should mirror DB1 changes

CREATE TABLE IF NOT EXISTS dest_employees (
  emp_id INT PRIMARY KEY,
  first_name VARCHAR(100),
  last_name  VARCHAR(100),
  dob DATE,
  city VARCHAR(100),
  salary INT
);
