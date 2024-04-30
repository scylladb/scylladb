import os
from os.path import relpath
import shutil

from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.parsers.rst.directives import unchanged
from sphinx.util import logging, relative_uri

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
        template_env = app.builder.templates.environment        
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
        template_env = app.builder.templates.environment

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
        config = app.config
        folder_name = 'api-doc'
        source_dir = source_dir = os.path.join(app.srcdir, config.scylladb_swagger_origin_api)
        source_folder = os.path.join(source_dir, folder_name)
        dest_dir = os.path.join(app.builder.outdir, '_static/api')
        dest_folder = os.path.join(dest_dir, folder_name)
        
        if os.path.exists(dest_folder):
            LOGGER.info(f"{folder_name} directory already exists in {dest_folder}. Skipping copy.")
        elif os.path.exists(source_folder):
            shutil.copytree(source_folder, dest_folder)
            LOGGER.info(f"{folder_name} directory copied.")
        else:
            LOGGER.error(f"{source_folder} does not exists.")


def custom_pathto(app, docname, typ=None, anchor=None):
    current_doc = app.env.docname
    current_version =  os.environ.get('SPHINX_MULTIVERSION_NAME', '')
    if current_version:
        return "/" + current_version + "/" + docname
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
    app.connect("builder-inited",  SwaggerProcessor().run)
    app.connect('builder-inited', lambda app: app.builder.templates.environment.globals.update(
        custom_pathto=lambda docname, typ=None, anchor=None: custom_pathto(app, docname, typ, anchor)
    ))

    app.add_directive("scylladb_swagger_inc", SwaggerIncDirective)
    app.add_directive("scylladb_swagger", SwaggerDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
