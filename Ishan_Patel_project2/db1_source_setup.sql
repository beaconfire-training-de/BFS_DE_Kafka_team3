

CREATE TABLE IF NOT EXISTS src_employees (
  emp_id SERIAL PRIMARY KEY,
  first_name VARCHAR(100),
  last_name  VARCHAR(100),
  dob DATE,
  city VARCHAR(100),
  salary INT
);

CREATE TABLE IF NOT EXISTS emp_cdc (
  cdc_id BIGSERIAL PRIMARY KEY,
  emp_id INT,
  first_name VARCHAR(100),
  last_name  VARCHAR(100),
  dob DATE,
  city VARCHAR(100),
  salary INT,
  action VARCHAR(10) NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION fn_log_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action)
    VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
    RETURN OLD;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_src_emp_insert ON src_employees;
DROP TRIGGER IF EXISTS trg_src_emp_update ON src_employees;
DROP TRIGGER IF EXISTS trg_src_emp_delete ON src_employees;

CREATE TRIGGER trg_src_emp_insert
AFTER INSERT ON src_employees
FOR EACH ROW EXECUTE FUNCTION fn_log_employee_changes();

CREATE TRIGGER trg_src_emp_update
AFTER UPDATE ON src_employees
FOR EACH ROW EXECUTE FUNCTION fn_log_employee_changes();

CREATE TRIGGER trg_src_emp_delete
AFTER DELETE ON src_employees
FOR EACH ROW EXECUTE FUNCTION fn_log_employee_changes();
