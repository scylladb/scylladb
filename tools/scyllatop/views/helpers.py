def formatValues(status):
    values = []
    if len(status) == 1:
        value = status.values()[0]
        return '{value:.1f}'.format(value=float(value))
    for key, value in status.iteritems():
        values.append('{key}: {value:.1f}'.format(key=key, value=float(value)))

    return ' '.join(values)
