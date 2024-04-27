"""MODULE TO LOAD JSON, YAML FILES"""
import json
from typing import Union, Dict, Any

import yaml

def load_yaml(file_path: str) -> Union[Dict[str, Any], None]:
    """
    Loads a YAML configuration file.
    Args:
        file_path (str): The path to the YAML file to be loaded.
    Returns:
        dict: The contents of the YAML file as a dictionary.
    """
    try:
        with open(file_path, mode='r', encoding="utf-8") as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        print("The file was not found.")
    except yaml.YAMLError as exc:
        print("Error while parsing YAML file:", exc)

def load_json(file_path) -> Union[Dict[str, Any], None]:
    """
    Loads a JSON configuration file.
    Args:
        file_path (str): The path to the JSON file to be loaded.
    Returns:
        dict: The contents of the JSON file as a dictionary.
    """
    try:
        with open(file_path, mode='r', encoding="utf-8") as file:
            config = json.load(file)
            return config
    except FileNotFoundError:
        print("The file was not found.")
    except json.JSONDecodeError as exc:
        print("Error while parsing JSON file:", exc)


def load_file(file_path: str) -> Union[Dict[str, Any], None]:
    """methosd to load json, yaml files"""
    if not file_path:
        raise ValueError("File path is required.")
    ext = file_path.split(".")[-1].strip()
    loader = {
        "json": load_json,
        "yml": load_yaml,
        "yaml": load_yaml
    }
    loaded_data = loader[ext](file_path)
    return loaded_data
