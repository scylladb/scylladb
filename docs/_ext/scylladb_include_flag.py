import os
from sphinx.directives.other import Include
from sphinx.util import logging
from docutils.parsers.rst import directives

LOGGER = logging.getLogger(__name__)

class IncludeFlagDirective(Include):
    option_spec = Include.option_spec.copy()
    option_spec['base_path'] = directives.unchanged

    def run(self):
        env = self.state.document.settings.env
        base_path = self.options.get('base_path', '_common')
        file_path = self.arguments[0]

        if env.app.tags.has('enterprise'):
            enterprise_path = os.path.join(base_path + "_enterprise", file_path)
            _, enterprise_abs_path = env.relfn2path(enterprise_path)
            if os.path.exists(enterprise_abs_path):
                self.arguments[0] = enterprise_path
            else:
                LOGGER.info(f"Enterprise content not found: Skipping inclusion of {file_path}")
                return []
        else:
            self.arguments[0] = os.path.join(base_path, file_path)
        return super().run()

def setup(app):
    app.add_directive('scylladb_include_flag', IncludeFlagDirective, override=True)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
