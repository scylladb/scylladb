class Table(object):
    def __init__(self, columns, separator=' '):
        self._columns = columns
        self._separator = separator
        self._rows = []
        self._widths = [0] * len(columns)

    def add(self, * columns):
        assert len(columns) == len(self._columns), 'wrong number of columns'
        self._rows.append(columns)
        for i, column in enumerate(columns):
            self._widths[i] = max(self._widths[i], len(column))

    def rows(self):
        for row in self._rows:
            formatted = []
            for i, column in enumerate(row):
                JUSTIFIERS = {'r': column.rjust,
                              'l': column.ljust,
                              'c': column.center}
                justifier = JUSTIFIERS[self._columns[i]]
                justified = justifier(self._widths[i])
                formatted.append(justified)

            yield self._separator.join(formatted)
