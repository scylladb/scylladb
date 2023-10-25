import os
import re
import yaml
from typing import Any, Dict, List

import jinja2

from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.directives import ObjectDescription
from sphinx.util import logging, status_iterator, ws_re
from sphinx.util.docfields import Field
from sphinx.util.docutils import switch_source_input, SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles
from sphinx.jinja2glue import BuiltinTemplateLoader
from docutils import nodes
from docutils.parsers.rst import directives
from docutils.statemachine import StringList


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

    all_properties = {}

    def __init__(self, config_file_path, config_header_file_path):
        self.config_file_path = config_file_path
        self.config_header_file_path = config_header_file_path

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
            name = match[1].strip()
            property_data = {
                "name": name,
                "value_status": match[4].strip(),
                "default": match[5].strip(),
                "liveness": "True" if match[3] else "False",
                "description": self._clean_description(match[6].strip()),
            }
            properties.append(property_data)
            DBConfigParser.all_properties[name] = property_data

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

    def parse(self):
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


class ConfigOptionList(SphinxDirective):
    has_content = False
    required_arguments = 2
    optional_arguments = 0
    final_argument_whitespace = True
    option_spec = {
        'template': directives.path,
        'value_status': directives.unchanged_required,
    }

    @property
    def env(self):
        document = self.state.document
        return document.settings.env

    def _resolve_src_path(self, path: str) -> str:
        rel_filename, filename = self.env.relfn2path(path)
        self.env.note_dependency(filename)
        return filename

    def _render(self, context: Dict[str, Any]) -> str:
        builder = self.env.app.builder
        template = self.options.get('template')
        if template is None:
            self.error(f'Option "template" not specified!')
        return builder.templates.render(template, context)

    def _make_context(self) -> Dict[str, Any]:
        header = self._resolve_src_path(self.arguments[0])
        source = self._resolve_src_path(self.arguments[1])
        db_parser = DBConfigParser(source, header)
        value_status = self.options.get("value_status")
        return dict(data=db_parser.parse(),
                    value_status=value_status)

    def run(self) -> List[nodes.Node]:
        rendered = self._render(self._make_context())
        contents = StringList(rendered.splitlines())
        node = nodes.section()
        node.document = self.state.document
        nested_parse_with_titles(self.state, contents, node)
        return node.children


def setup(app: Sphinx) -> Dict[str, Any]:
    app.add_directive('scylladb_config_list', ConfigOptionList)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
