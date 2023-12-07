import json
from typing import Any, Dict
import yaml


def load_yaml(path: str):
    with open(path, "r") as file:
        content = yaml.safe_load(file)
    return content

def write_json(file_path: str, content: Dict[Any, Any]):
    with open(file_path, "w") as file:
        json.dump(content)

    return
