"""MODULE TO LOAD CLICKHOUSE TABLE CREATION"""

import os
import sys

# from typing import List
from contextlib import contextmanager
from dotenv import load_dotenv

from clickhouse_driver import Client

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")

from app.lib.file_handler import load_file


class ClickHouseDB:
    """Class to create tables in ClickHouse database."""

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @contextmanager
    def get_client(self):
        """methoid to get clickhouse client"""
        try:
            client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print("Connected to ClickHouse.")
            yield client
        except Exception as err:
            print("Failed to connect to ClickHouse:", err)
            yield None
        finally:
            if client:
                client.disconnect()
                print("ClickHouse connection closed.")

    def query(self, query):
        """Execute a query and return the result."""
        with self.get_client() as client:
            if client is None:
                return
            result = client.execute(query)
            return result

    def table_exists(self, table_name):
        """Check if a table exists in the database."""

        result = self.query(f"EXISTS TABLE {table_name}")
        return result[0][0] == 1

    def create_table(self, table_name, table_schema):
        """Create a table if it does not exist."""
        if not self.table_exists(table_name):
            self.query(table_schema)
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")
