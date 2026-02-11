
CREATE TABLE IF NOT exists employees( emp_id SERIAL, first_name VARCHAR(100), last_name VARCHAR(100), dob DATE, city
VARCHAR(100), salary INT );

CREATE TABLE IF NOT exists emp_cdc( emp_id SERIAL, first_name VARCHAR(100), last_name VARCHAR(100), dob DATE, city
VARCHAR(100), salary INT, action VARCHAR(100) );

ALTER TABLE emp_cdc ADD COLUMN cdc_id SERIAL PRIMARY KEY;


CREATE TABLE IF NOT exists employees( emp_id SERIAL, first_name VARCHAR(100), last_name VARCHAR(100), dob DATE, city
VARCHAR(100), salary INT );

CREATE TABLE IF NOT exists emp_cdc( emp_id SERIAL, first_name VARCHAR(100), last_name VARCHAR(100), dob DATE, city
VARCHAR(100), salary INT, action VARCHAR(100) );

ALTER TABLE emp_cdc ADD COLUMN cdc_id SERIAL PRIMARY KEY;

CREATE OR REPLACE FUNCTION fn_capture_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
        RETURN NEW;

    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
        RETURN NEW;

    ELSIF (TG_OP = 'DELETE') THEN
        -- Capturing OLD record data because NEW is null on DELETE
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_employee_cdc
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW
EXECUTE FUNCTION fn_capture_employee_changes();