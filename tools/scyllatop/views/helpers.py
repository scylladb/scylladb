def _safeFormat(value):
    try:
        return '{value:.1f}'.format(value=float(value))
    except ValueError:
        return '{}'.format(value)


def formatValues(status):
    values = []
    if len(status) == 1:
        value = list(status.values())[0]
        return _safeFormat(value)
    for key, value in status.items():
        values.append('{key}: {value}'.format(key=key, value=_safeFormat(value)))

    return ' '.join(values)
