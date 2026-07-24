import os


def readable_desc(description: str) -> str:
    """
    This function is deprecated and maintained only for backward compatibility 
    with previous versions. Use ``readable_desc_rst``instead.
    """
    return (
        description.replace("\\n", "")
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace("\n", "<br>")
        .replace("\\t", "- ")
        .replace('"', "")
    )


def readable_desc_rst(description):
    indent = ' ' * 3
    lines = description.split('\n')
    cleaned_lines = []

    for line in lines:

        cleaned_line = line.replace('\\n', '\n')

        cleaned_line = cleaned_line.replace('\\t', '\n' + indent * 2)

        if line.endswith('"'):
            cleaned_line = cleaned_line[:-1] + ' '

        cleaned_line = cleaned_line.lstrip()
        cleaned_line = cleaned_line.replace('"', '')
        cleaned_line = cleaned_line.replace('`', '\\`')

        if cleaned_line != '':
            cleaned_line = indent + cleaned_line
            cleaned_lines.append(cleaned_line)

    return ''.join(cleaned_lines)


def get_templates(builder):
    """Return the builder's template bridge, or ``None`` if it has none.
    """
    return getattr(builder, 'templates', None)


class _FallbackTemplates:
    """Stand-in template bridge that loads ``templates_path`` directly."""

    def __init__(self, builder):
        from jinja2 import Environment, FileSystemLoader

        search_path = [os.path.join(builder.srcdir, path)
                       for path in builder.config.templates_path]
        self.environment = Environment(loader=FileSystemLoader(search_path))

    def render(self, template, context):
        return self.environment.get_template(template).render(context)


def get_templates_or_fallback(builder):
    """Like get_templates, but never None: use it when the output is content."""
    templates = get_templates(builder)
    if templates is not None:
        return templates
    if not hasattr(builder, '_scylladb_fallback_templates'):
        builder._scylladb_fallback_templates = _FallbackTemplates(builder)
    return builder._scylladb_fallback_templates


def maybe_add_filters(builder):
    env = get_templates_or_fallback(builder).environment
    if 'readable_desc' not in env.filters:
        env.filters['readable_desc'] = readable_desc

    if 'readable_desc_rst' not in env.filters:
        env.filters['readable_desc_rst'] = readable_desc_rst
