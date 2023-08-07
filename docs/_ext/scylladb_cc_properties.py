import os
import re
import yaml
from sphinx.application import Sphinx

CONFIG_FILE_PATH = '../db/config.cc'
DESTINATION_PATH = '_data/db_config.yaml'

def create_yaml_file(destination, data):
    current_data = None
    
    try:
        with open(destination, 'r') as file:
            current_data = yaml.safe_load(file)
    except FileNotFoundError:
        pass

    if current_data != data:
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        with open(destination, 'w') as file:
            yaml.dump(data, file)

def parse_db_properties(config_file):
    with open(config_file, 'r') as file:
        content = file.read()

    pattern = r',\s*(\w+)\(this,\s*"(\w+)",\s*(?:liveness::(\w+),\s*)?value_status::(\w+),\s*([^,]+),\s*"([^"]+)"\)'
    matches = re.findall(pattern, content)

    properties = []
    for match in matches:
        property_data = {
            "name": match[1].strip(),
            "value_status": match[3].strip(), 
            "default": match[4].strip(),
            "liveness": 'True' if match[2] else 'False',
            "description": match[5].strip().replace('\n', '<br />')
        }
        properties.append(property_data)

    return properties

def generate_cc_docs(app: Sphinx):
    dest_path = os.path.join(app.builder.srcdir, DESTINATION_PATH)
    parsed_properties = parse_db_properties(CONFIG_FILE_PATH)
    create_yaml_file(dest_path, parsed_properties)


def setup(app: Sphinx):
    app.connect("builder-inited", generate_cc_docs)
