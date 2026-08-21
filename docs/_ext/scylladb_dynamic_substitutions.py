import os
from sphinx.application import Sphinx
from sphinx.util import logging

LOGGER = logging.getLogger(__name__)

class DynamicSubstitutions:

    def _get_current_version(self, app):
        current_version = os.environ.get('SPHINX_MULTIVERSION_NAME', '')
        stable_version = app.config.smv_latest_version
        prefix = 'branch-'
        version = current_version

        if current_version.startswith(prefix):
            version = current_version
        elif not stable_version.startswith(prefix):
            LOGGER.error("Invalid stable_version format in conf.py. It should start with 'branch-'")
        else:
            version = stable_version

        return version.replace(prefix, '')

    def run(self, app):
        current_version = self._get_current_version(app)
        
        if not hasattr(app.config, 'rst_prolog') or app.config.rst_prolog is None:
            app.config.rst_prolog = ""
        elif not app.config.rst_prolog.endswith('\n'):
            app.config.rst_prolog += '\n'

        app.config.rst_prolog += f"""
.. |CURRENT_VERSION| replace:: {current_version}
.. |UBUNTU_SCYLLADB_LIST| replace:: scylla-{current_version}.list
.. |CENTOS_SCYLLADB_REPO| replace:: scylla-{current_version}.repo
"""

def setup(app: Sphinx):
    app.connect("builder-inited",  DynamicSubstitutions().run)
   
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }