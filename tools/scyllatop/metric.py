import logging
import parseexception


class Metric(object):
    def __init__(self, symbol, metric_source, hlp):
        self._symbol = symbol
        self._metric_source = metric_source
        self._status = {}
        self._help_line = hlp
        self._expiration = None
        self._absent = False

    @property
    def symbol(self):
        return self._symbol

    @property
    def help(self):
        return self._help_line

    @property
    def status(self):
        return self._status

    @property
    def is_absent(self):
        return self._absent

    @property
    def expiration(self):
        return self._expiration

    def update_info(self, line):
        match = self._metric_source._METRIC_INFO_PATTERN.search(line)
        if match is None:
            raise parseexception.ParseException('could not parse metric pattern from line: {0}'.format(line))
        key = match.groupdict()['key']
        value = match.groupdict()['value']
        self._status[key] = value

    def update(self):
        response = self._metric_source.query_val(self._symbol)
        if response is None:
            self.markAbsent()
            return
        for line in response:
            self.update_info(line)
            logging.debug('update {}: {}'.format(self.symbol, line.strip()))

    def markAbsent(self, expiration=None):
        for key in list(self._status.keys()):
            self._status[key] = 'not available'
        self._absent = True
        self._expiration = expiration

    def add_to_results(self, results):
        if not isinstance(results, dict):
            raise Exception("results must be a dictionary")
        results[self._symbol] = self
    @classmethod
    def _discover(cls, metric_source, with_help = False):
        results = {}
        logging.info('discovering metrics{}...'.format(" with help" if with_help else ""))
        response = metric_source.query_list()
        for line in response:
            if with_help:
                pattern = metric_source._METRIC_DISCOVER_PATTERN_WITH_HELP
            else:
                pattern = metric_source._METRIC_DISCOVER_PATTERN
            match = pattern.search(line)
            if match:
                metric = match.groupdict()['metric']
                hlp = match.groupdict()['help'] if with_help else ""
                m = Metric(metric, metric_source, hlp)
                m.update_info(line)
                m.add_to_results(results)
                logging.debug('discover: {}'.format(m))

        logging.info('found {} metrics'.format(len(results)))
        return results

    @classmethod
    def discover(cls, metric_source):
        return cls._discover(metric_source)

    @classmethod
    def discover_with_help(cls, metric_source):
        return cls._discover(metric_source, with_help=True)

    def __repr__(self):
        return '{0}:{1}'.format(self.symbol, self.status)
