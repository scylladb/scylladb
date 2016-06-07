import groups
import table
import base
import helpers


class Aggregate(base.Base):
    def update(self, liveData):
        self.clearScreen()
        self.writeStatusLine(liveData.measurements)
        metricGroups = groups.Groups(liveData.measurements)
        visible = metricGroups.all()
        tableForm = self._prepareTable(visible)
        for row in tableForm.rows():
            self.writeLine(row)

        self.refresh()

    def _prepareTable(self, groups):
        result = table.Table('lr')
        for group in groups:
            formatted = 'avg[{0}] tot[{1}]'.format(
                helpers.formatValues(group.aggregate(self._mean)),
                helpers.formatValues(group.aggregate(self._sum)))
            result.add(self._label(group), formatted)
        return result

    def _mean(self, values):
        valid = self._valid(values)
        if len(valid) == 0:
            return 'not available'
        return sum(x for x in valid) / len(valid)

    def _sum(self, values):
        valid = self._valid(values)
        return sum(x for x in valid)

    def _valid(self, values):
        floats = [self._float(value) for value in values]
        valid = filter(lambda x: x is not None, floats)
        return valid

    def _float(self, value):
        try:
            return float(value)
        except ValueError:
            return None

    def _label(self, group):
        label = '{label}({size})'.format(label=group.label, size=group.size)
        return label
