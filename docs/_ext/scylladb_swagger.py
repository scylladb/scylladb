import json
import os
from os.path import relpath
from pathlib import Path
import shutil

from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.parsers.rst.directives import unchanged
from sphinx.util import logging, relative_uri

from utils import get_templates

LOGGER = logging.getLogger(__name__)

class SwaggerIncDirective(Directive):
    required_arguments = 0
    option_spec = {
        'template': unchanged
    }

    def run(self):
        template_file = self.options.get('template', self.state.document.settings.env.config.scylladb_swagger_inc_template)
        app = self.state.document.settings.env.app
        env = self.state.document.settings.env
        app = env.app
        templates = get_templates(app.builder)
        if templates is None:
            return []
        template_env = templates.environment
        try:
            template = template_env.get_template(template_file)
        except Exception as e:
            LOGGER.error(f"Template file '{template_file}' not found: {e}")
        rendered_html = template.render()
        raw_node = nodes.raw('', rendered_html, format='html')

        return [raw_node]

class SwaggerDirective(Directive):
    required_arguments = 0
    option_spec = {
        'spec': unchanged,
        'template': unchanged
    }

    def run(self):
        env = self.state.document.settings.env
        config = self.state.document.settings.env.config
        app = self.state.document.settings.env.app
        templates = get_templates(app.builder)
        if templates is None:
            return []
        template_env = templates.environment

        spec_file = self.options.get('spec')
        spec_file_path = "_static/" + spec_file
        template_file = self.options.get('template', config.scylladb_swagger_template)

        try:
            template = template_env.get_template(template_file)
        except Exception as e:
            LOGGER.error(f"Template file '{template_file}' not found: {e}")

        swagger_file_name = os.path.splitext(os.path.basename(spec_file))[0]
        rendered_html = template.render(swagger_file_path= spec_file_path, swagger_file_name=swagger_file_name)
        raw_node = nodes.raw('', rendered_html, format='html')

        return [raw_node]


class SwaggerProcessor():

    def run(self, app, exception=None):
        if get_templates(app.builder) is None:
            # Non-HTML builder: nothing references these static assets.
            return
        config = app.config
        folder_name = 'api-doc'
        source_dir = source_dir = os.path.join(app.srcdir, config.scylladb_swagger_origin_api)
        source_folder = os.path.join(source_dir, folder_name)
        dest_dir = os.path.join(app.builder.outdir, '_static/api')
        dest_folder = os.path.join(dest_dir, folder_name)

        if os.path.exists(source_folder):
            if os.path.exists(dest_folder):
                shutil.rmtree(dest_folder)
            shutil.copytree(source_folder, dest_folder)
            # Swagger 2.0 APIs are fragments assembled by the server at runtime.
            # Publish standalone specifications for the shared Swagger UI template.
            for definitions_file in Path(source_folder).glob('*.def.json'):
                name = definitions_file.name.removesuffix('.def.json')
                spec = _assemble_spec(Path(source_folder), name)
                (Path(dest_folder) / f'{name}.json').write_text(json.dumps(spec, indent=2) + '\n', encoding='utf-8')
            LOGGER.info(f"{folder_name} directory copied.")
        else:
            LOGGER.error(f"{source_folder} does not exists.")


def _read_fragment(path: Path) -> dict:
    return json.loads('{' + path.read_text(encoding='utf-8').lstrip().removeprefix(',') + '}')


def _assemble_spec(api_dir: Path, name: str) -> dict:
    spec = json.loads((api_dir / 'swagger20_header.json').read_text(encoding='utf-8') + '}}')
    spec['paths'] = _read_fragment(api_dir / f'{name}.json')
    spec['definitions'] = _read_fragment(api_dir / f'{name}.def.json')
    # Error responses refer to Seastar's HTTP exception schema.
    spec['definitions']['ErrorModel'] = {
        'type': 'object',
        'properties': {'message': {'type': 'string'}, 'code': {'type': 'integer'}},
    }
    spec['host'] = '127.0.0.1:10000'
    return spec


def custom_pathto(app, docname, typ=None, anchor=None):
    current_doc = app.env.docname
    current_version = os.environ.get('SPHINX_MULTIVERSION_NAME', '')
    flag = os.environ.get('FLAG', 'manual')

    if current_version:
        prefix = "/manual/" if flag == 'manual' else "/"
        return f"{prefix}{current_version}/{docname}"
    return relative_uri(app.builder.get_target_uri(current_doc), docname) + (('#' + anchor) if anchor else '')

def setup(app):
    app.add_config_value(
        "scylladb_swagger_origin_api",
        default="../api",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_swagger_template",
        default="swagger.tmpl",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_swagger_inc_template",
        default="swagger_inc.tmpl",
        rebuild="html",
    )
    def add_template_globals(app):
        templates = get_templates(app.builder)
        if templates is None:
            return
        templates.environment.globals.update(
            custom_pathto=lambda docname, typ=None, anchor=None: custom_pathto(app, docname, typ, anchor)
        )

    app.connect("builder-inited",  SwaggerProcessor().run)
    app.connect('builder-inited', add_template_globals)

    app.add_directive("scylladb_swagger_inc", SwaggerIncDirective)
    app.add_directive("scylladb_swagger", SwaggerDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
