import json
from typing import Any, Dict
import yaml

from project.utils.decorator import create_path


def load_yaml(path: str, encoding: str = "utf-8"):
    with open(path, "r", encoding=encoding) as file:
        content = yaml.safe_load(file)
    return content


@create_path
def write_json(file_path: str, content: Dict[Any, Any], encoding: str = "utf-8"):
    with open(file_path, "w", encoding=encoding) as file:
        json.dump(content, file)

    return


def load_json(path) -> Dict[str, Any]:
    with open(path, "r") as file:
        content = json.load(file)

    return content
