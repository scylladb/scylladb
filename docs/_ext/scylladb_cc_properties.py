import os
import re
import yaml
from sphinx.application import Sphinx
from sphinxcontrib.datatemplates.directive import DataTemplateYAML
from docutils.parsers.rst import directives

CONFIG_FILE_PATH = '../db/config.cc'
CONFIG_HEADER_FILE_PATH = '../db/config.hh'
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

def parse_db_properties(config_file, config_header_file):

    properties_dict = {}

    # Parse config file
    with open(config_file, 'r') as file:
        config_content = file.read()

    config_pattern = r',\s*(\w+)\(this,\s*"(\w+)",\s*(?:liveness::(\w+),\s*)?value_status::(\w+),\s*([^,]+),\s*"([^"]+)"\)'
    config_matches = re.findall(config_pattern, config_content)

    for match in config_matches:
        property_data = {
            "name": match[1].strip(),
            "value_status": match[3].strip(), 
            "default": match[4].strip(),
            "liveness": 'True' if match[2] else 'False',
            "description": match[5].strip().replace('\n', '<br />')
        }
        properties_dict[match[1].strip()] = property_data

    # Parse header file
    with open(config_header_file, 'r') as file:
        config_header_content = file.read()

    config_header_pattern = r'\s*named_value<(\w+)> (\w+);'
    config_header_matches = re.findall(config_header_pattern, config_header_content)

    for match in config_header_matches:
        if match[1] in properties_dict: 
            properties_dict[match[1]]['type'] = match[0].strip()

    return list(properties_dict.values())


def generate_cc_docs(app: Sphinx):
    dest_path = os.path.join(app.builder.srcdir, DESTINATION_PATH)
    parsed_properties = parse_db_properties(CONFIG_FILE_PATH, CONFIG_HEADER_FILE_PATH)
    create_yaml_file(dest_path, parsed_properties)


class DBConfigTemplateDirective(DataTemplateYAML):
    
    option_spec = DataTemplateYAML.option_spec.copy()
    option_spec["value_status"] = directives.unchanged_required

    def _make_context(self, data, config, env):
        context = super()._make_context(data, config, env)
        context['value_status'] = self.options.get('value_status')
        return context

def setup(app: Sphinx):
    app.connect("builder-inited", generate_cc_docs)
    app.add_directive('scylladb_config_template', DBConfigTemplateDirective)
