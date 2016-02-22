import logging
import re


class Metric(object):
    _METRIC_SYMBOL_HOST_PATTERN = re.compile('^[^/]+/')
    _METRIC_INFO_PATTERN = re.compile('^(?P<key>[^=]+)=(?P<value>.*)$')
    _METRIC_DISCOVER_PATTERN = re.compile('[^ ]+ (?P<metric>.+)$')

    def __init__(self, symbol, collectd):
        self._symbol = symbol
        self._collectd = collectd
        self._status = {}

    @property
    def name(self):
        hostStripped = self._METRIC_SYMBOL_HOST_PATTERN.sub('', self._symbol)
        return hostStripped

    @property
    def symbol(self):
        return self._symbol

    @property
    def status(self):
        return self._status

    def update(self):
        response = self._collectd.query('GETVAL "{metric}"'.format(metric=self._symbol))
        for line in response:
            match = self._METRIC_INFO_PATTERN.search(line)
            key = match.groupdict()['key']
            value = match.groupdict()['value']
            self._status[key] = value

    @classmethod
    def discover(cls, collectd):
        results = []
        logging.info('discovering metrics...')
        response = collectd.query('LISTVAL')
        for line in response:
            logging.debug('LISTVAL result: {0}'.format(line))
            match = cls._METRIC_DISCOVER_PATTERN.search(line)
            metric = match.groupdict()['metric']
            results.append(Metric(metric, collectd))

        logging.info('found {0} metrics'.format(len(results)))
        return results
