import base
import helpers
import table


class Simple(base.Base):
    def update(self, liveData):
        self.clearScreen()
        self.writeStatusLine(liveData.measurements)
        visible = liveData.measurements[:self.availableLines()]
        tableForm = self._prepareTable(visible)
        for index, row in enumerate(tableForm.rows()):
            line = row[: self.dimensions()['columns']]
            self.writeLine(line, index + 1)
        self.refresh()

    def _prepareTable(self, measurements):
        result = table.Table('lr')
        for metric in measurements:
            result.add(metric.name, helpers.formatValues(metric.status))
        return result
