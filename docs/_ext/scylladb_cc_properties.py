import re
from typing import Any, Dict, List

from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.directives import ObjectDescription
from sphinx.util import logging, ws_re
from sphinx.util.docfields import Field
from sphinx.util.docutils import switch_source_input, SphinxDirective
from sphinx.util.nodes import make_id, nested_parse_with_titles
from docutils import nodes
from docutils.parsers.rst import directives
from docutils.statemachine import StringList

from utils import get_templates_or_fallback, maybe_add_filters

logger = logging.getLogger(__name__)

# A C++ braced initializer, such as { { "a", "b" }, { "c", "d" } }, with up
# to four levels of nested braces. String literals in it may contain braces
# and commas.
def _braced_pattern(depth):
    item = r'[^{}"]|"(?:[^"\\]|\\.)*"'
    pattern = r'\{(?:' + item + r')*\}'
    for _ in range(depth - 1):
        pattern = r'\{(?:' + item + '|' + pattern + r')*\}'
    return pattern

BRACED = _braced_pattern(4)


def _tokenize_braced(text):
    """Splits a C++ braced initializer into braces, commas, string literals
    and other atoms. Returns None if it contains anything else, such as an
    expression or a nested type name."""
    tokens = []
    pos = 0
    token_re = re.compile(r'\s*(?:([{},])|("(?:[^"\\]|\\.)*")|(-?[0-9][0-9.]*|true|false))')
    while pos < len(text):
        if text[pos:].strip() == '':
            break
        m = token_re.match(text, pos)
        if not m:
            return None
        if m.group(1):
            tokens.append(m.group(1))
        elif m.group(2):
            tokens.append(('str', re.sub(r'\\(.)', r'\1', m.group(2)[1:-1])))
        else:
            tokens.append(('atom', m.group(3)))
        pos = m.end()
    return tokens


def _parse_braced(tokens):
    """Parses the tokens of a braced initializer into nested lists of
    ('str', s) and ('atom', a) leaves. Returns None if they are malformed."""
    def parse(i):
        if tokens[i] != '{':
            return tokens[i], i + 1
        elems = []
        i += 1
        while tokens[i] != '}':
            elem, i = parse(i)
            elems.append(elem)
            if tokens[i] == ',':
                i += 1
            elif tokens[i] != '}':
                raise ValueError
        return elems, i + 1
    try:
        value, end = parse(0)
    except (IndexError, ValueError):
        return None
    return value if end == len(tokens) else None


def _yaml_scalar(leaf):
    kind, value = leaf
    if kind == 'atom' or re.fullmatch(r'[A-Za-z0-9_./-]+', value):
        return value
    return "'" + value.replace("'", "''") + "'"


def _is_map(value):
    return isinstance(value, list) and all(
        isinstance(e, list) and len(e) == 2 and isinstance(e[0], tuple) and e[0][0] == 'str' for e in value)


def _yaml_value(value, cpp_type):
    """Renders a parsed braced initializer as a YAML (flow style) value, using
    the option's C++ type to tell whether braces initialize a list, a map, or
    are the arguments of a constructor of some other class."""
    if isinstance(value, tuple):
        return _yaml_scalar(value)
    cpp_type = (cpp_type or '').strip()
    if cpp_type.startswith('std::vector<') or cpp_type.endswith('_list'):
        elem_type = cpp_type[len('std::vector<'):-1] if cpp_type.startswith('std::vector<') else None
        return '[' + ', '.join(_yaml_value(e, elem_type) for e in value) + ']'
    if 'map' in cpp_type or (not cpp_type and _is_map(value)):
        if not _is_map(value):
            raise ValueError
        return '{' + ', '.join(_yaml_scalar(k) + ': ' + _yaml_value(v, None) for k, v in value) + '}'
    if cpp_type and len(value) == 1:
        # The braces hold the single argument of the class's constructor.
        return _yaml_value(value[0], None)
    if not cpp_type:
        return '[' + ', '.join(_yaml_value(e, None) for e in value) + ']'
    raise ValueError


def readable_default(default, cpp_type):
    """If a default value is a C++ braced initializer of string and number
    literals, returns it as the YAML value which a user would write in
    scylla.yaml. Otherwise, returns the default unchanged."""
    if not default.startswith('{'):
        return default
    tokens = _tokenize_braced(default)
    value = _parse_braced(tokens) if tokens else None
    if not value:
        return default
    try:
        return _yaml_value(value, cpp_type)
    except ValueError:
        return default


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
        r'(' + BRACED + r'|"[^"]*"|[^,]+)?,'  # 5. Default value
        r'\s*"(.*?)"'  # 6. Description text, divided in multiple lines.
        r'(?:,\s*(.+?))?'  # 7. Available values (optional)
        r'\s*\)'
    )

    """
    Regex pattern for parsing named values in the configuration header file:
    """
    CONFIG_H_REGEX_PATTERN = r"\s*(?:named_value|log_legacy_value)<([\w:<>,\s]+)> (\w+);"

    """
    Regex pattern for parsing comments.
    """
    COMMENT_PATTERN = r"/\*.*?\*/|//.*?$"

    all_properties = {}

    def __init__(self, config_file_path, config_header_file_path):
        self.config_file_path = config_file_path
        self.config_header_file_path = config_header_file_path

    def _clean_comments(self, content):
        return re.sub(self.COMMENT_PATTERN, "", content, flags=re.DOTALL | re.MULTILINE)

    def _parse_group(self, group_match, config_group_content):
        group_name = group_match.group(1).strip()
        group_description = group_match.group(2).strip() if group_match.group(2) else ""

        current_group = {
            "name": group_name,
            "description": group_description,
            "properties": [],
            "value_status_count": {
                'Used': 0,
                'Unused': 0,
                'Invalid': 0,
                'Deprecated': 0,
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
            liveness_value = match[3].strip() if match[3] else ""
            property_data = {
                "name": name,
                "value_status": match[4].strip(),
                "default": match[5].strip(),
                "liveness": "True" if liveness_value == "LiveUpdate" else "False",
                "description": match[6].strip(),
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

        for group in groups:
            for property_data in group["properties"]:
                property_data["default"] = readable_default(property_data["default"], property_data.get("type"))

        return groups

    @classmethod
    def get(cls, name: str):
        return DBConfigParser.all_properties[name]



class ConfigOption(ObjectDescription):
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False

    # TODO: instead of overriding transform_content(), render option properties
    #       as a field list.
    doc_field_types = [
        Field('type',
              label='Type',
              has_arg=False,
              names=('type',)),
        Field('default',
              label='Default value',
              has_arg=False,
              names=('default',)),
        Field('liveness',
              label='Liveness',
              has_arg=False,
              names=('liveness',)),
    ]

    def handle_signature(self,
                         sig: str,
                         signode: addnodes.desc_signature) -> str:
        signode.clear()
        signode += addnodes.desc_name(sig, sig)
        # normalize whitespace like XRefRole does
        return ws_re.sub(' ', sig)

    @property
    def env(self):
        document = self.state.document
        return document.settings.env

    def before_content(self) -> None:
        maybe_add_filters(self.env.app.builder)

    def _render(self, name) -> str:
        item = DBConfigParser.get(name)
        if item is None:
            raise self.error(f'Option "{name}" not found!')
        builder = self.env.app.builder
        template = self.config.scylladb_cc_properties_option_tmpl
        return get_templates_or_fallback(builder).render(template, item)

    def transform_content(self,
                          contentnode: addnodes.desc_content) -> None:
        name = self.arguments[0]
        # the source is always None here
        _, lineno = self.get_source_info()
        source = f'scylla_config:{lineno}:<{name}>'
        fields = StringList(self._render(name).splitlines(),
                            source=source, parent_offset=lineno)
        with switch_source_input(self.state, fields):
            self.state.nested_parse(fields, 0, contentnode)

    def add_target_and_index(self,
                             name: str,
                             sig: str,
                             signode: addnodes.desc_signature) -> None:
        node_id = make_id(self.env, self.state.document, self.objtype, name)
        signode['ids'].append(node_id)
        self.state.document.note_explicit_target(signode)
        entry = f'{name}; configuration option'
        self.indexnode['entries'].append(('pair', entry, node_id, '', None))
        std = self.env.get_domain('std')
        std.note_object(self.objtype, name, node_id, location=signode)


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
        return get_templates_or_fallback(builder).render(template, context)

    def _make_context(self) -> Dict[str, Any]:
        header = self._resolve_src_path(self.arguments[0])
        source = self._resolve_src_path(self.arguments[1])
        db_parser = DBConfigParser(source, header)
        value_status = self.options.get("value_status")
        return dict(data=db_parser.parse(),
                    value_status=value_status)

    def run(self) -> List[nodes.Node]:
        maybe_add_filters(self.env.app.builder)
        rendered = self._render(self._make_context())
        contents = StringList(rendered.splitlines())
        node = nodes.section()
        node.document = self.state.document
        nested_parse_with_titles(self.state, contents, node)
        return node.children


def setup(app: Sphinx) -> Dict[str, Any]:
    app.add_config_value(
        'scylladb_cc_properties_option_tmpl',
        default='db_option.tmpl',
        rebuild='html',
        types=[str])

    app.add_object_type(
        'confgroup',
        'confgroup',
        objname='configuration group',
        indextemplate='pair: %s; configuration group',
        doc_field_types=[
            Field('example',
                  label='Example',
                  has_arg=False)
        ])
    app.add_object_type(
        'confval',
        'confval',
        objname='configuration option')
    app.add_directive_to_domain('std', 'confval', ConfigOption, override=True)
    app.add_directive('scylladb_config_list', ConfigOptionList)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
