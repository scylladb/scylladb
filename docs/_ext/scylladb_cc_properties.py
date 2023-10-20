import os
import re
import yaml
from sphinx.application import Sphinx
from sphinxcontrib.datatemplates.directive import DataTemplateYAML
from docutils.parsers.rst import directives

CONFIG_FILE_PATH = "../db/config.cc"
CONFIG_HEADER_FILE_PATH = "../db/config.hh"
DESTINATION_PATH = "_data/db_config.yaml"


class DBConfigParser:

    """
    Regex pattern for parsing group comments.
    """
    GROUP_REGEX_PATTERN = (
        r'/\*\*\s*'
        r'\*+\s*@Group\s+(.*?)\s*'
        r'(?:\*+\s*@GroupDescription\s+(.*?)\s*)?'
        r'\*/'
    )

    """
    Regex pattern for parsing the configuration properties.
    """
    CONFIG_CC_REGEX_PATTERN = (
        r',\s*(\w+)\(this,'  # 0. Property name
        r'\s*"([^"]*)",\s*'  # 1. Property key
        r'(?:\s*"([^"]+)",)?'  # 2. Property alias (optional)
        r'\s*(?:liveness::(\w+),\s*)?'  # 3. Liveness (optional)
        r'(?:value_status::)?(\w+),\s*'  # 4. Value status
        r'(\{[^{}]*\}|"[^"]*"|[^,]+)?,'  # 5. Default value
        r'\s*"(.*?)"'  # 6. Description text, divided in multiple lines.
        r'(?:,\s*(.+?))?'  # 7. Available values (optional)
        r'\s*\)'
    )

    """
    Regex pattern for parsing named values in the configuration header file:
    """
    CONFIG_H_REGEX_PATTERN = r"\s*named_value<([\w:<>,\s]+)> (\w+);"

    """
    Regex pattern for parsing comments.
    """
    COMMENT_PATTERN = r"/\*.*?\*/|//.*?$"

    def __init__(self, config_file_path, config_header_file_path, destination_path):
        self.config_file_path = config_file_path
        self.config_header_file_path = config_header_file_path
        self.destination_path = destination_path

    def _create_yaml_file(self, destination, data):
        current_data = None

        try:
            with open(destination, "r") as file:
                current_data = yaml.safe_load(file)
        except FileNotFoundError:
            pass

        if current_data != data:
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            with open(destination, "w") as file:
                yaml.dump(data, file)

    @staticmethod
    def _clean_description(description):
        return (
            description.replace("\\n", "")
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace("\n", "<br>")
            .replace("\\t", "- ")
            .replace('"', "")
        )
    
    def _clean_comments(self, content):
        return re.sub(self.COMMENT_PATTERN, "", content, flags=re.DOTALL | re.MULTILINE)

    def _parse_group(self, group_match, config_group_content):
        group_name = group_match.group(1).strip()
        group_description = self._clean_description(group_match.group(2).strip()) if group_match.group(2) else ""

        current_group = {
            "name": group_name,
            "description": group_description,
            "properties": [],
            "value_status_count": {
                'Used': 0,
                'Unused': 0,
                'Invalid': 0
            },
        }

        cleaned_properties_content = self._clean_comments(config_group_content)
        properties = self._parse_properties(cleaned_properties_content)
        current_group['properties'] = properties

        for prop in properties:
            value_status = prop['value_status']
            if value_status in current_group['value_status_count']:
                current_group['value_status_count'][value_status] += 1
        return current_group


    def _parse_properties(self, content):
        properties = []
        config_matches = re.findall(self.CONFIG_CC_REGEX_PATTERN, content, re.DOTALL)

        for match in config_matches:
            property_data = {
                "name": match[1].strip(),
                "value_status": match[4].strip(),
                "default": match[5].strip(),
                "liveness": "True" if match[3] else "False",
                "description": self._clean_description(match[6].strip()),
            }
            properties.append(property_data)

        return properties

    def _add_type_information(self, groups):
        with open(self.config_header_file_path, "r") as file:
            config_header_content = file.read()

        config_header_matches = re.findall(self.CONFIG_H_REGEX_PATTERN, config_header_content)

        for match in config_header_matches:
            property_key = match[1].strip()
            for group in groups:
                for property_data in group["properties"]:
                    if property_data["name"] == property_key:
                        property_data["type"] = match[0].strip()

    def _parse_db_properties(self):
        groups = []

        with open(self.config_file_path, "r", encoding='utf-8') as file:
            config_content = file.read()

        group_matches = list(re.finditer(self.GROUP_REGEX_PATTERN, config_content, re.DOTALL))
        group_matches.append(None)  # Add a sentinel to process the last group

        for i in range(len(group_matches) - 1):
            group_match = group_matches[i]
            next_group_match = group_matches[i + 1]

            end_pos = next_group_match.start() if next_group_match else len(config_content)
            config_group_content = config_content[group_match.end():end_pos]
            
            current_group = self._parse_group(group_match, config_group_content)
            groups.append(current_group)

        self._add_type_information(groups)

        return groups

    def run(self, app: Sphinx):
        dest_path = os.path.join(app.builder.srcdir, self.destination_path)
        parsed_properties = self._parse_db_properties()
        self._create_yaml_file(dest_path, parsed_properties)


class DBConfigTemplateDirective(DataTemplateYAML):

    option_spec = DataTemplateYAML.option_spec.copy()
    option_spec["value_status"] = directives.unchanged_required

    def _make_context(self, data, config, env):
        context = super()._make_context(data, config, env)
        context["value_status"] = self.options.get("value_status")
        return context


def setup(app: Sphinx):
    db_parser = DBConfigParser(
        CONFIG_FILE_PATH, CONFIG_HEADER_FILE_PATH, DESTINATION_PATH
    )
    app.connect("builder-inited", db_parser.run)
    app.add_directive("scylladb_config_template", DBConfigTemplateDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }