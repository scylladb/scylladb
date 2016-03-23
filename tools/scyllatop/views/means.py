import groups
import table
import base
import helpers


class Means(base.Base):
    def update(self, liveData):
        self.clearScreen()
        self.writeStatusLine(liveData.measurements)
        metricGroups = groups.Groups(liveData.measurements)
        visible = metricGroups.all()[:self.availableLines()]
        tableForm = self._prepareTable(visible)
        for index, row in enumerate(tableForm.rows()):
            self.writeLine(row, index + 1)

        self.refresh()

    def _prepareTable(self, groups):
        result = table.Table('lr')
        for group in groups:
            formatted = helpers.formatValues(group.means)
            result.add(self._label(group), formatted)
        return result

    def _label(self, group):
        label = '{label}({size})'.format(label=group.label, size=group.size)
        return label
