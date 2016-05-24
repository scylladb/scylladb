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
        mean = lambda vector: sum(float(x) for x in vector) / len(vector)
        _sum = lambda vector: sum(float(x) for x in vector)
        result = table.Table('lr')
        for group in groups:
            formatted = 'avg[{0}] tot[{1}]'.format(
                helpers.formatValues(group.aggregate(mean)),
                helpers.formatValues(group.aggregate(_sum)))
            result.add(self._label(group), formatted)
        return result

    def _label(self, group):
        label = '{label}({size})'.format(label=group.label, size=group.size)
        return label
