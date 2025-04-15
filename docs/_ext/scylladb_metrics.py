import os
import sys
import json
from sphinx import addnodes
from sphinx.directives import ObjectDescription
from sphinx.util.docfields import Field
from sphinx.util.docutils import switch_source_input
from sphinx.util.nodes import make_id
from sphinx.util import logging, ws_re
from docutils.parsers.rst import Directive, directives
from docutils.statemachine import StringList
from sphinxcontrib.datatemplates.directive import DataTemplateJSON
from utils import maybe_add_filters

sys.path.insert(0, os.path.abspath("../../scripts"))
import scripts.get_description as metrics

LOGGER = logging.getLogger(__name__)


class MetricsProcessor:

    MARKER = "::description"

    def _create_output_directory(self, app, metrics_directory):
        output_directory = os.path.join(app.builder.srcdir, metrics_directory)
        os.makedirs(output_directory, exist_ok=True)
        return output_directory

    def _process_single_file(self, file_path, destination_path, metrics_config_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        if self.MARKER in content and not os.path.exists(destination_path):
            try:
                metrics_file = metrics.get_metrics_from_file(file_path, "scylla", metrics.get_metrics_information(metrics_config_path))
                with open(destination_path, 'w+', encoding='utf-8') as f:
                    json.dump(metrics_file, f, indent=4)
            except SystemExit:
                LOGGER.info(f'Skipping file: {file_path}')
            except Exception as error:
                LOGGER.info(error)

    def _process_metrics_files(self, repo_dir, output_directory, metrics_config_path):
        for root, _, files in os.walk(repo_dir):
            for file in files:
                if file.endswith(".cc"):
                    file_path = os.path.join(root, file)
                    file_name = os.path.splitext(file)[0] + ".json"
                    destination_path = os.path.join(output_directory, file_name)
                    self._process_single_file(file_path, destination_path, metrics_config_path)

    def run(self, app, exception=None):
        repo_dir = os.path.abspath(os.path.join(app.srcdir, ".."))
        metrics_config_path = os.path.join(repo_dir, app.config.scylladb_metrics_config_path)
        output_directory = self._create_output_directory(app, app.config.scylladb_metrics_directory)

        self._process_metrics_files(repo_dir, output_directory, metrics_config_path)


class MetricsTemplateDirective(DataTemplateJSON):
    option_spec = DataTemplateJSON.option_spec.copy()
    option_spec["title"] = lambda x: x

    def _make_context(self, data, config, env):
        context = super()._make_context(data, config, env)
        context["title"] = self.options.get("title")
        return context

    def run(self):
        return super().run()


class MetricsOption(ObjectDescription):
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {
        'type': directives.unchanged,
        'component': directives.unchanged,
        'key': directives.unchanged,
        'source': directives.unchanged,
    }

    doc_field_types = [
        Field('type', label='Type', has_arg=False, names=('type',)),
        Field('component', label='Component', has_arg=False, names=('component',)),
        Field('key', label='Key', has_arg=False, names=('key',)),
        Field('source', label='Source', has_arg=False, names=('source',)),
    ]

    def handle_signature(self, sig: str, signode: addnodes.desc_signature):
        signode.clear()
        signode += addnodes.desc_name(sig, sig)
        return ws_re.sub(' ', sig)

    @property
    def env(self):
        return self.state.document.settings.env

    def _render(self, name, option_type, component, key, source):
        item = {'name': name, 'type': option_type, 'component': component, 'key': key, 'source': source }
        template = self.config.scylladb_metrics_option_template
        return self.env.app.builder.templates.render(template, item)

    def transform_content(self, contentnode: addnodes.desc_content) -> None:
        name = self.arguments[0]
        option_type = self.options.get('type', '')
        component = self.options.get('component', '')
        key = self.options.get('key', '')
        source_file = self.options.get('source', '')
        _, lineno = self.get_source_info()
        source = f'scylladb_metrics:{lineno}:<{name}>'
        fields = StringList(self._render(name, option_type, component, key, source_file).splitlines(), source=source, parent_offset=lineno)
        with switch_source_input(self.state, fields):
            self.state.nested_parse(fields, 0, contentnode)

    def add_target_and_index(self, name: str, sig: str, signode: addnodes.desc_signature) -> None:
        node_id = make_id(self.env, self.state.document, self.objtype, name)
        signode['ids'].append(node_id)
        self.state.document.note_explicit_target(signode)
        entry = f'{name}; metrics option'
        self.indexnode['entries'].append(('pair', entry, node_id, '', None))
        self.env.get_domain('std').note_object(self.objtype, name, node_id, location=signode)

class MetricsDirective(Directive):
    TEMPLATE = 'metrics.tmpl'
    required_arguments = 0
    optional_arguments = 1
    option_spec = {'template': directives.path}
    has_content = True

    def _process_file(self, file, relative_path_from_current_rst):
        data_directive = MetricsTemplateDirective(
            name=self.name,
            arguments=[os.path.join(relative_path_from_current_rst, file)],
            options=self.options,
            content=self.content,
            lineno=self.lineno,
            content_offset=self.content_offset,
            block_text=self.block_text,
            state=self.state,
            state_machine=self.state_machine,
        )
        data_directive.options["template"] = self.options.get('template', self.TEMPLATE)
        data_directive.options["title"] = file.replace('_', ' ').replace('.json','').capitalize()
        return data_directive.run()

    def _get_relative_path(self, output_directory, app, docname):
        current_rst_path = os.path.join(app.builder.srcdir, docname + ".rst")
        return os.path.relpath(output_directory, os.path.dirname(current_rst_path))


    def run(self):
        maybe_add_filters(self.state.document.settings.env.app.builder)
        app = self.state.document.settings.env.app
        docname = self.state.document.settings.env.docname
        metrics_directory = os.path.join(app.builder.srcdir, app.config.scylladb_metrics_directory)
        output = []
        try:
            relative_path_from_current_rst = self._get_relative_path(metrics_directory, app, docname)
            files = os.listdir(metrics_directory)
            for _, file in enumerate(files):
                output.extend(self._process_file(file, relative_path_from_current_rst))
        except Exception as error:
            LOGGER.info(error)
        return output

def setup(app):
    app.add_config_value("scylladb_metrics_directory", default="_data/metrics", rebuild="html")
    app.add_config_value("scylladb_metrics_config_path", default='scripts/metrics-config.yml', rebuild="html")
    app.add_config_value('scylladb_metrics_option_template', default='metrics_option.tmpl', rebuild='html', types=[str])
    app.connect("builder-inited", MetricsProcessor().run)
    app.add_object_type(
        'metrics_option',
        'metrics_option',
        objname='metrics option')
    app.add_directive_to_domain('std', 'metrics_option', MetricsOption, override=True)
    app.add_directive("metrics_option", MetricsOption)
    app.add_directive("scylladb_metrics", MetricsDirective)

   
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }

