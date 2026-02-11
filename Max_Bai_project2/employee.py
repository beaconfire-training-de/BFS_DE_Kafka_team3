"""
Kafka Project 2: Changing Data Capture (CDC)

Employee model class for CDC-based synchronization between two databases.
This model matches the employees table schema and includes the CDC action field.
"""

import json


class Employee:
    """
    Represents an employee record matching the employees table schema:
    emp_id SERIAL, first_name VARCHAR(100), last_name VARCHAR(100),
    dob DATE, city VARCHAR(100), salary INT
    """
    def __init__(self, emp_id: int = 0, first_name: str = '', last_name: str = '',
                 dob: str = '', city: str = '', salary: int = 0):
        self.emp_id = emp_id
        self.first_name = first_name
        self.last_name = last_name
        self.dob = dob
        self.city = city
        self.salary = salary

    def to_json(self):
        """Converts the Employee object into a JSON string for Kafka messaging."""
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(json_str):
        """Creates an Employee object from a JSON string."""
        data = json.loads(json_str)
        return Employee(**data)

    def __repr__(self):
        return (f"Employee(emp_id={self.emp_id}, first_name='{self.first_name}', "
                f"last_name='{self.last_name}', dob='{self.dob}', "
                f"city='{self.city}', salary={self.salary})")


class CDCRecord:
    """
    Represents a Change Data Capture record from the emp_cdc table.
    Contains the employee data plus the action (INSERT, UPDATE, DELETE).
    """
    def __init__(self, cdc_id: int = 0, emp_id: int = 0, first_name: str = '',
                 last_name: str = '', dob: str = '', city: str = '',
                 salary: int = 0, action: str = ''):
        self.cdc_id = cdc_id
        self.emp_id = emp_id
        self.first_name = first_name
        self.last_name = last_name
        self.dob = dob
        self.city = city
        self.salary = salary
        self.action = action

    def to_json(self):
        """Converts the CDCRecord object into a JSON string for Kafka messaging."""
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(json_str):
        """Creates a CDCRecord object from a JSON string."""
        data = json.loads(json_str)
        return CDCRecord(**data)

    def __repr__(self):
        return (f"CDCRecord(cdc_id={self.cdc_id}, emp_id={self.emp_id}, "
                f"first_name='{self.first_name}', last_name='{self.last_name}', "
                f"dob='{self.dob}', city='{self.city}', salary={self.salary}, "
                f"action='{self.action}')")
