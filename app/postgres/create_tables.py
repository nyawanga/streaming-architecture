"""MODULE TO CREATE TABLES IN POSTGRES DATABASE"""

import os
import sys
from typing import List
from dotenv import load_dotenv

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")

from app.lib.pg_connect import PostgreSQLDatabase
from app.lib.file_handler import load_file

load_dotenv()


def generate_sql_from_yaml(yaml_file: str) -> List[str]:
    """METHOD TO GENERATE SQL COMMANDS FROM YAML FILE"""
    config = load_file(yaml_file)
    # print(config)
    sql_commands = []
    for table_name, table_config in config["tables"].items():
        table_name = table_name.lower()  # Ensure table name is lowercase
        columns = table_config["columns"]
        column_definitions = ", ".join(
            [f"{col.lower()} {props}" for col, props in columns.items()]
        )
        primary_key = f"PRIMARY KEY ({table_config['primary_key'].lower()})"  # Ensure primary key is lowercase

        create_statement = f"CREATE TABLE IF NOT EXISTS franco.{table_name} ({column_definitions}, {primary_key});"
        sql_commands.append(create_statement)

    return sql_commands


def main():
    """main module function"""
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    connector = PostgreSQLDatabase(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    )
    yaml_file = f"{curr_dir}/tables_config.yml"
    sql_commands = generate_sql_from_yaml(yaml_file)
    connector.query("CREATE SCHEMA IF NOT EXISTS franco AUTHORIZATION franco;")
    connector.query(
        "SELECT * FROM information_schema.tables WHERE table_schema='franco';"
    )
    connector.query("SELECT schema_name FROM information_schema.schemata;")
    for command in sql_commands:
        connector.query(command)


if __name__ == "__main__":
    main()
