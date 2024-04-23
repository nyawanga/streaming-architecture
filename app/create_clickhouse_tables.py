"""MODULE TO LOAD CLICKHOUSE TABLE CREATION"""

import os
import yaml
from dotenv import load_dotenv
from clickhouse_driver import Client


class ClickHouseDB:
    """Class to create tables in ClickHouse database."""

    def __init__(self, host, port, user, password, database):
        self.client = Client(
            host=host, port=port, user=user, password=password, database=database
        )

    def table_exists(self, table_name):
        """Check if a table exists in the database."""
        result = self.client.execute(f"EXISTS TABLE {table_name}")
        return result[0][0] == 1

    def create_table(self, table_name, table_schema):
        """Create a table if it does not exist."""
        if not self.table_exists(table_name):
            self.client.execute(table_schema)
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")

    def close(self):
        self.client.disconnect()


def main():
    # Load environment variables from .env file
    load_dotenv()

    # Retrieve environment variables
    host = os.getenv("CLICKHOUSE_HOST")
    port = os.getenv("CLICKHOUSE_PORT")
    user = os.getenv("CLICKHOUSE_USER")
    password = os.getenv("CLICKHOUSE_PASSWORD")
    database = os.getenv("CLICKHOUSE_DB")

    # Initialize database connection
    db = ClickHouseDB(host, port, user, password, database)

    # Load table schemas from a YAML file (assumes 'config.yaml' is present and formatted correctly)
    with open(
        "./app/clickhouse_config.yml",
        mode="r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    try:
        for table_name, table_info in config["tables"].items():
            db.create_table(table_name, table_info["schema"])
    finally:
        db.close()


if __name__ == "__main__":
    main()
