import base
import helpers
import sys


class Stdout(base.Base):
    def update(self, liveData):
        for metric in liveData.measurements:
            print('{} {}'.format(metric.symbol, helpers.formatValues(metric.status)))
        sys.stdout.flush()
