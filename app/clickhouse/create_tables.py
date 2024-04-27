"""MODULE TO LOAD CLICKHOUSE TABLE CREATION"""

import os
import sys
from typing import List
from dotenv import load_dotenv

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")

from app.lib.file_handler import load_file
from app.clickhouse.clickhouse_lib import ClickHouseDB


def main():
    """MAIN FUNCTION FOR CREATING TABLES IN CLICKHOUSE DATABASE"""
    # Load environment variables from .env file
    load_dotenv()

    # Initialize database connection
    connector = ClickHouseDB(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=os.getenv("CLICKHOUSE_PORT"),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        database=os.getenv("CLICKHOUSE_DB"),
    )

    # Load table schemas from a YAML file (assumes 'config.yaml' is present and formatted correctly)
    curr_dir: str = os.path.dirname(os.path.realpath(__file__))
    file_path: str = f"{curr_dir}/clickhouse_config.yml"
    config: dict = load_file(file_path)
    connector.query("CREATE DATABASE IF NOT EXISTS franco")

    try:
        for table_name, table_info in config["tables"].items():
            connector.create_table(table_name, table_info["schema"])
    except Exception as e:
        print(f"Error creating table: {e}")


if __name__ == "__main__":
    main()
