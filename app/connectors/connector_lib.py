"""LIBRARY OF HELPERS USED TO CREATE CONNECTORS"""

import os
import sys
import json
from typing import Union, Dict, Any, List
import requests
from dotenv import load_dotenv

parent_dir = os.path.join(os.getcwd())
sys.path.append(f"{parent_dir}")

from app.lib.file_handler import load_file


def inject_sink(
    config: Dict[str, Union[str, int]],
    host: str,
    database: str,
    user: str,
    password: str,
) -> Dict[str, Union[str, int]]:
    """function to inject credentials into the sink config"""
    config["config"].update(
        {
            "connection.url": f"jdbc:clickhouse://{host}:8123/{database}",
            "connection.user": f"{user}",
            "connection.password": f"{password}",
        }
    )
    return config


def inject_source(
    config: Dict[str, Union[str, int]],
    host: str,
    database: str,
    user: str,
    password: str,
) -> Dict[str, Union[str, int]]:
    """function to inject credentials into the source config"""
    config["config"].update(
        {
            "database.hostname": f"{host}",
            "database.port": "5432",
            "database.user": f"{user}",
            "database.password": f"{password}",
            "database.dbname": f"{database}",
            "database.server.name": f"{host}",
        }
    )
    return config


def inject_credentials(
    target: str,
    config: Dict[str, Union[str, int]],
    host: str,
    database: str,
    user: str,
    password: str,
) -> Dict[str, Union[str, int]]:
    """method that decides what is to be adjusted source/sink"""
    injector = {"source": inject_source, "sink": inject_sink}
    return injector[target](config, host, database, user, password)


def make_request(
    url: str,
    method: str = "GET",
    params: Union[Dict[str, Any], None] = None,
    data: Any = None,
    headers: Union[Dict[str, Any], None] = None,
) -> Union[requests.Response, None]:
    """
    Send an HTTP request.

    Args:
        url (str): The URL to which the request is sent.
        method (str): The HTTP method, e.g., 'GET' or 'POST'.
        params (dict, optional): Dictionary of URL parameters for GET requests.
        data (dict, optional): Dictionary of form data to send for POST requests.
        headers (dict, optional): Dictionary of HTTP headers.

    Returns:
        requests.Response: The response from the server.
    """
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params, headers=headers, timeout=61)
        elif method.upper() == "POST":
            response = requests.post(url, data=data, headers=headers, timeout=61)
        elif method.upper() == "DELETE":
            response = requests.delete(url, data=data, headers=headers, timeout=61)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        return response
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def get_connetors(url: str) -> List[str]:
    """METHOD TO GET CONNECTORS"""
    url = f"{url}/"
    response = make_request(
        url, method="GET", params=None, headers={"Accept": "application/json"}
    )
    connectors = []
    if response:
        connectors = response.json()
    # print(connectors)
    return connectors


def delete_connectors(url: str, connectors: List[str]) -> None:
    """METHOD TO DELETE CONNECTORS"""
    if not connectors:
        return
    for connector_name in connectors:
        print(f"Deleting connector: {connector_name}")
        response = make_request(f"{url}/{connector_name}", method="DELETE")
        if response:
            print(f"Connector {connector_name} deleted successfully")
        else:
            print(f"Failed to delete connector {connector_name}")


def create_connector(url, config_data: dict) -> None:
    """METHOD TO CREATE CONNECTOR"""

    try:
        response = make_request(
            f"{url}/",
            method="POST",
            data=json.dumps(config_data, indent=4),
            headers={"Content-Type": "application/json"},
        )
        print(response.status_code)
        print(response.json())
        if response.status_code not in [200, 201]:
            raise RuntimeError()

    except RuntimeError as err:
        print(f"Failed to create connector {config_data.get('name')}")
        print(err)
        return
    except Exception as err:
        print(f"Failed to create connector {config_data.get('name')}")
        print(err)
        return
