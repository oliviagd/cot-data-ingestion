import re
import yaml
from jinja2 import Template


def load_yaml(config_path, config_variables):
    """
    Load yaml file with jinja templated arguments
    """
    with open(file=config_path, mode="r") as file:
        template_content = file.read()

    config = Template(template_content).render(**config_variables)
    config = yaml.safe_load(config)
    return config


def to_snake_case(bad_string: str) -> str:
    """
    Converts a string to snake case,
    e.g. my_snake_case_string
    Non-alphanumeric symbols will be replaced with an underscore
    """
    clean_str = "".join([char if char.isalnum() else "_" for char in bad_string])
    clean_str = re.sub(r"_{2,}", "_", clean_str)
    clean_str = clean_str.strip("_")
    return clean_str.lower()
