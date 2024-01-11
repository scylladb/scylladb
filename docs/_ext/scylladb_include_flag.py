from sphinx.directives.other import Include
from docutils.parsers.rst import directives

class IncludeFlagDirective(Include):
    option_spec = Include.option_spec.copy()
    option_spec['base_path'] = directives.unchanged

    def run(self):
        env = self.state.document.settings.env
        base_path = self.options.get('base_path', '_common')

        if env.app.tags.has('enterprise'):
            self.arguments[0] = base_path + "_enterprise/" + self.arguments[0]
        else:
            self.arguments[0] = base_path + "/" + self.arguments[0]
        return super().run()

def setup(app):
    app.add_directive('scylladb_include_flag', IncludeFlagDirective, override=True)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
