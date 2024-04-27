"""MODULE TO CREATE CONNECTORS"""

import os
import sys
import json
from typing import List
from dotenv import load_dotenv

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")

from app.lib.file_handler import load_file
from app.connectors.connector_lib import (
    inject_credentials,
    get_connetors,
    delete_connectors,
    create_connector,
)

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")


URL = "http://localhost:8082/connectors"

curr_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(curr_dir, "..", ".."))
config_prefix = "infra/data/kafka-connect/connector_configs"
config_path = os.path.join(parent_dir, config_prefix)
conns = get_connetors(URL)
delete_connectors(URL, conns)
print(conns)

src_conns = ["sales", "customer", "employee", "sales_territory"]
for conn in src_conns:
    src_config = f"{config_prefix}/source/{conn}.json"
    source_config = load_file(f"{src_config}")
    source_config = inject_credentials(
        "source",
        source_config,
        "postgres",
        POSTGRES_DB,
        POSTGRES_USER,
        POSTGRES_PASSWORD,
    )
    create_connector(URL, source_config)
    sink_config = f"{config_prefix}/sink/{conn}.json"
    sink_config = load_file(f"{sink_config}")
    sink_config = inject_credentials(
        "sink",
        sink_config,
        "clickhouse-server",
        CLICKHOUSE_DB,
        CLICKHOUSE_USER,
        CLICKHOUSE_PASSWORD,
    )
    create_connector(URL, sink_config)

conns = get_connetors()
# print(conns)
