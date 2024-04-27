"""MODULE FOR INSERTING DATA INTO POSTGRES DATABASE"""

import os
import sys
from dotenv import load_dotenv

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")

from app.postgres.pg_lib import DataGenerator, get_table_columns
from app.lib.pg_connect import PostgreSQLDatabase

load_dotenv()

# Initialize Faker to generate fake data
data_generator = DataGenerator()

connector = PostgreSQLDatabase(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    database=os.getenv("POSTGRES_DB"),
)


def main():
    """MAIN FUNCTION FOR INSERTING DATA INTO POSTGRES DATABASE"""
    # Generate and insert data
    customer_data = data_generator.generate_customers(100)
    customer_columns = [
        col
        for col in get_table_columns(connector, "customer", "franco")
        if col != "customer_id"
    ]
    connector.insert_bulk("franco.customer", customer_data, customer_columns)

    territory_data = data_generator.generate_sales_territories(10)
    territory_columns = [
        col
        for col in get_table_columns(connector, "sales_territory", "franco")
        if col != "sales_territory_id"
    ]
    connector.insert_bulk("franco.sales_territory", territory_data, territory_columns)

    employee_data = data_generator.generate_employees(15)
    employee_columns = [
        col
        for col in get_table_columns(connector, "employee", "franco")
        if col != "employee_id"
    ]
    connector.insert_bulk("franco.employee", employee_data, employee_columns)

    # Fetch IDs to ensure foreign key constraints are met
    result = connector.query("SELECT customer_id FROM franco.customer")
    customer_ids = [row[0] for row in result]

    result = connector.query("SELECT sales_territory_id FROM franco.sales_territory")
    territory_ids = [row[0] for row in result]

    result = connector.query("SELECT employee_id FROM franco.employee")
    employee_ids = [row[0] for row in result]

    result = connector.query("SELECT MAX(sales_id) FROM franco.sales")
    max_sales_id = [row[0] for row in result][0]

    print(f"max sales_id : {max_sales_id}")

    sales_data = data_generator.generate_sales(
        500, max_sales_id, customer_ids, employee_ids, territory_ids
    )
    sales_columns = get_table_columns(connector, "sales", "franco")
    connector.insert_bulk("franco.sales", sales_data, sales_columns)


if __name__ == "__main__":
    main()
