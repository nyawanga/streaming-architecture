"""MODULE TO HELP WITH POSTGRES DATABASE OPERATIONS"""

import os
import sys
from typing import List, Any
from faker import Faker

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")

from app.lib.pg_connect import PostgreSQLDatabase

fake = Faker()


class DataGenerator:
    """CLASS TO GENERATE FAKE DATA FOR POSTGRES DATABASE"""

    def generate_customers(self, n) -> List[Any]:
        """METHOD TO GENERATE FAKE DATA FOR CUSTOMER TABLE"""
        customers = [
            (
                fake.last_name()[:50],
                fake.street_address()[:50],
                fake.street_address()[:50],
                fake.date_of_birth().isoformat()[:50],
                str(fake.random_int(min=18, max=99))[:50],
                f"{fake.random_int(min=1, max=50)} miles"[:50],
                fake.uuid4()[:50],
                fake.uuid4()[:50],
                fake.date_this_decade().isoformat()[:50],
                fake.email()[:50],
                fake.word()[:50],
                fake.job()[:50],
                fake.word()[:50],
                fake.first_name()[:50],
                fake.job()[:50],
                fake.random_element(["M", "F"])[:50],
                fake.random_element(["Y", "N"])[:50],
                fake.random_element(["S", "M"])[:50],
                fake.first_name()[:50],
                "Formal"[:50],
                str(fake.random_int(min=0, max=5))[:50],
                str(fake.random_int(min=0, max=3))[:50],
                fake.phone_number()[:50],
                fake.word()[:50],
                fake.job()[:50],
                fake.suffix()[:50],
                fake.prefix()[:50],
                str(fake.random_int(min=0, max=3))[:50],
                str(fake.random_int(min=20000, max=200000))[:50],
            )
            for _ in range(n)
        ]
        return customers

    def generate_sales_territories(self, n) -> List[Any]:
        """METHOD TO GENERATE FAKE DATA FOR SALES TERRITORY TABLE"""
        territories = [
            (fake.country()[:50], fake.state()[:50], fake.city()[:50]) for _ in range(n)
        ]
        return territories

    def generate_employees(self, n) -> List[Any]:
        """METHOD TO GENERATE FAKE DATA FOR EMPLOYEE TABLE"""
        employees = [(fake.name()[:50], fake.state()[:50]) for _ in range(n)]
        return employees

    def generate_sales(
        self, n, max_id, customer_ids, employee_ids, territory_ids
    ) -> List[Any]:
        """METHOD TO GENERATE FAKE DATA FOR SALES TABLE"""
        sales = []
        if max_id is None:
            max_id = 0
        end = max_id + n
        for sales_id in range(max_id + 1, end, 1):
            sales.append(
                (
                    sales_id,
                    # str(fake.random_element(customer_ids)),
                    str(fake.random_element(employee_ids)),
                    str(fake.random_element(territory_ids)),
                    str(fake.random_number(digits=3))[:50],
                    fake.date_this_decade().isoformat()[:50],
                    fake.date_this_decade().isoformat()[:50],
                    str(fake.random_number(digits=5))[:50],
                    str(fake.random_number(digits=2))[:50],
                    fake.date_this_decade().isoformat()[:50],
                    str(fake.random_int(min=1, max=100))[:50],
                    str(fake.random_number(digits=2))[:50],
                    str(fake.random_number(digits=2))[:50],
                    str(fake.random_number(digits=3))[:50],
                    str(fake.random_int(min=1, max=100))[:50],
                    str(fake.random_int(min=1000, max=9999))[:50],
                    str(fake.random_number(digits=2))[:50],
                    fake.date_this_decade().isoformat()[:50],
                    str(fake.random_number(digits=2))[:50],
                    str(fake.random_number(digits=5))[:50],
                    str(fake.random_number(digits=2))[:50],
                    str(fake.random_number(digits=2))[:50],
                    str(fake.random_element(employee_ids)),
                )
            )
        return sales


def get_table_columns(connector: PostgreSQLDatabase, table_name: str, schema: str):
    """METHOD TO GET TABLE COLUMNS"""
    result = connector.query(
        f"""
        SELECT *
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name   = '{table_name}';
        """
    )
    return [row[3] for row in result]
