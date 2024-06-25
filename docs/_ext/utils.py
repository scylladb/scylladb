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

        if line.endswith('"'):
            cleaned_line = cleaned_line[:-1] + ' '

        cleaned_line = cleaned_line.lstrip()
        cleaned_line = cleaned_line.replace('"', '')
        
        if cleaned_line != '':
            cleaned_line = indent + cleaned_line
            cleaned_lines.append(cleaned_line)
    
    return ''.join(cleaned_lines)


def readable_default(value, type_):
    # default values of options are extracted from source file, but the
    # representation in source code of a certain value does not necessarily
    # identical to its representation in scylla.yaml, sometimes the default
    # value is not even a literal. strictly speaking, it is an expression,
    # so a function call is also valid. we have to translate them on a
    # case-by-case basis
    if type_ == 'tri_mode_restriction':
        # see db/config.cc
        #   db::tri_mode_restriction_t::map()
        *_, v = value.rsplit('::', 1)
        return v.lower()
    if type_ == 'seastar::log_level':
        # see seastar/src/util/log.cc
        #   log_level_names
        *_, v = value.rsplit('::', 1)
        return v
    return value


def maybe_add_filters(builder):
    env = builder.templates.environment
    if 'readable_desc' not in env.filters:
        env.filters['readable_desc'] = readable_desc

    if 'readable_desc_rst' not in env.filters:
        env.filters['readable_desc_rst'] = readable_desc_rst

    if 'readable_default' not in env.filters:
        env.filters['readable_default'] = readable_default
