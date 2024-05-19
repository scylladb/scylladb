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


def maybe_add_filters(builder):
    env = builder.templates.environment
    if 'readable_desc' not in env.filters:
        env.filters['readable_desc'] = readable_desc

    if 'readable_desc_rst' not in env.filters:
        env.filters['readable_desc_rst'] = readable_desc_rst
