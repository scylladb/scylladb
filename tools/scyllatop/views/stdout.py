from . import base
from . import helpers
import sys
import logging


class Stdout(base.Base):
    def update(self, liveData):
        logging.debug('stdout: {} measurements'.format(len(liveData.measurements)))
        for metric in liveData.measurements:
            print('{} {}'.format(metric.symbol, helpers.formatValues(metric.status)))
        sys.stdout.flush()
