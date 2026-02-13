-- Create employees table
CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

-- Create CDC table
CREATE TABLE emp_cdc (
    action_id SERIAL PRIMARY KEY,
    emp_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(20)
);

-- Add index for efficient querying
CREATE INDEX idx_emp_cdc_action_id ON emp_cdc(action_id);


-- Create the trigger function
CREATE OR REPLACE FUNCTION log_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- For INSERT operations
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
        RETURN NEW;
    
    -- For UPDATE operations
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
        RETURN NEW;
    
    -- For DELETE operations
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create the triggers
CREATE TRIGGER employee_insert_trigger
AFTER INSERT ON employees
FOR EACH ROW
EXECUTE FUNCTION log_employee_changes();

CREATE TRIGGER employee_update_trigger
AFTER UPDATE ON employees
FOR EACH ROW
EXECUTE FUNCTION log_employee_changes();

CREATE TRIGGER employee_delete_trigger
AFTER DELETE ON employees
FOR EACH ROW
EXECUTE FUNCTION log_employee_changes();


SELECT * FROM employees;
-- Insert a new employee into the SOURCE database
INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Alice', 'Johnson', '1990-05-15', 'New York', 75000);


-- Update employee in SOURCE database
UPDATE employees 
SET salary = 85000, city = 'Boston'
WHERE first_name = 'Alice' AND last_name = 'Johnson';


-- Delete employee from SOURCE database
DELETE FROM employees 
WHERE first_name = 'Alice' AND last_name = 'Johnson';


-- Insert multiple employees
INSERT INTO employees (first_name, last_name, dob, city, salary) VALUES
('Bob', 'Smith', '1985-08-20', 'Chicago', 65000),
('Carol', 'Davis', '1992-03-10', 'Seattle', 70000),
('David', 'Wilson', '1988-11-25', 'Miami', 68000);

-- Update Bob's salary
UPDATE employees SET salary = 75000 WHERE first_name = 'Bob';

-- Delete Carol
DELETE FROM employees WHERE first_name = 'Carol';








