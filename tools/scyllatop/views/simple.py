import base
import helpers
import table


class Simple(base.Base):
    def update(self, liveData):
        self.clearScreen()
        self.writeStatusLine(liveData.measurements)
        tableForm = self._prepareTable(liveData.measurements)
        for row in tableForm.rows():
            self.writeLine(row)
        self.refresh()

    def _prepareTable(self, measurements):
        result = table.Table('lr')
        for metric in measurements:
            result.add(metric.symbol, helpers.formatValues(metric.status))
        return result
