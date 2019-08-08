import logging
import parseexception
import fnmatch
import time
import metric
import defaults


class LiveData(object):
    def __init__(self, metricPatterns, interval, metric_source):
        logging.info('will query metric_source {} every {} seconds'.format(metric_source, interval))
        self._startedAt = time.time()
        self._measurements = []
        self._interval = interval
        self._metric_source = metric_source
        self._initializeMetrics(metricPatterns)
        self._views = []
        self._stop = False

    def addView(self, view):
        logging.debug('addView {}'.format(view))
        self._views.append(view)

    @property
    def measurements(self):
        return self._measurements

    def _initializeMetrics(self, metricPatterns):
        if len(metricPatterns) > 0:
            self._setupUserSpecifiedMetrics(metricPatterns)
        else:
            self._setupUserSpecifiedMetrics(defaults.DEFAULT_METRIC_PATTERNS)
        logging.debug('_initializeMetrics: {} measurements'.format(len(self._measurements)))

    def _setupUserSpecifiedMetrics(self, metricPatterns):
        availableSymbols = [m.symbol for m in metric.Metric.discover(self._metric_source)]
        symbols = [symbol for symbol in availableSymbols if self._matches(symbol, metricPatterns)]
        for symbol in symbols:
                logging.info('adding {0}'.format(symbol))
                self._measurements.append(metric.Metric(symbol, self._metric_source, ""))

    def _matches(self, symbol, metricPatterns):
        for pattern in metricPatterns:
            match = fnmatch.fnmatch(symbol, pattern)
            if match:
                return True
        return False

    def go(self, mainLoop):
        while not self._stop:
            for metric_obj in self._measurements:
                self._update(metric_obj)
            logging.debug('go: updated {} measurements'.format(len(self._measurements)))

            for view in self._views:
                logging.debug('go: updating view {}'.format(view))
                view.update(self)
            logging.debug('go: sleeping for {} seconds'.format(self._interval))
            time.sleep(self._interval)
            logging.debug('go: drawing screen...')
            mainLoop.draw_screen()

    def _update(self, metric_obj):
        try:
            metric_obj.update()
        except parseexception.ParseException:
            logging.exception('exception while updating metric {0}'.format(metric_obj))

    def stop(self):
        self._stop = True
