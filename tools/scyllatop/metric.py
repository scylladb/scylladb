import logging
import parseexception


class Metric(object):
    def __init__(self, symbol, metric_source, hlp):
        self._symbol = symbol
        self._metric_source = metric_source
        self._status = {}
        self._help_line = hlp

    @property
    def symbol(self):
        return self._symbol

    @property
    def help(self):
        return self._help_line

    @property
    def status(self):
        return self._status

    def update(self):
        response = self._metric_source.query_val(self._symbol)
        if response is None:
            self.markAbsent()
            return
        for line in response:
            match = self._metric_source._METRIC_INFO_PATTERN.search(line)
            if match is None:
                raise parseexception.ParseException('could not parse metric pattern from line: {0}'.format(line))
            key = match.groupdict()['key']
            value = match.groupdict()['value']
            self._status[key] = value
            logging.debug('update {}: {}'.format(self.symbol, line.strip()))

    def markAbsent(self):
        for key in self._status.keys():
            self._status[key] = 'not available'

    @classmethod
    def discover(cls, metric_source):
        results = []
        logging.info('discovering metrics...')
        response = metric_source.query_list()
        for line in response:
            match = metric_source._METRIC_DISCOVER_PATTERN.search(line)
            if match:
                metric = match.groupdict()['metric']
                logging.debug('discover list result: {0}'.format(metric))
                hlp = ""
                results.append(Metric(metric, metric_source, hlp))

        logging.info('found {0} metrics'.format(len(results)))
        return results

    @classmethod
    def discover_with_help(cls, metric_source):
        results = []
        logging.info('discovering metrics...')
        response = metric_source.query_list()
        for line in response:
            logging.debug('list result: {0}'.format(line))
            match = metric_source._METRIC_DISCOVER_PATTERN_WITH_HELP.search(line)
            if match:
                metric = match.groupdict()['metric']
                hlp = match.groupdict()['help']
                results.append(Metric(metric, metric_source, hlp))

        logging.info('found {0} metrics'.format(len(results)))
        return results

    def __repr__(self):
        return '{0}:{1}'.format(self.symbol, self.status)
