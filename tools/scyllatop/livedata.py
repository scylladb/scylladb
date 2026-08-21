import logging
import parseexception
import fnmatch
import time
import metric
import defaults


class LiveData(object):
    def __init__(self, metricPatterns, interval, metric_source, ttl=None):
        logging.info('will query metric_source {} every {} seconds'.format(metric_source, interval))
        if ttl:
            logging.info('absent data will be kept for {} seconds'.format(ttl))
        self._startedAt = time.time()
        self._results = {}
        self._interval = interval
        self._ttl = ttl
        self._metric_source = metric_source
        if metricPatterns and len(metricPatterns) > 0:
            self._metricPatterns = metricPatterns
        else:
            self._metricPatterns = defaults.DEFAULT_METRIC_PATTERNS
        self._results = self._discoverMetrics()
        self._views = []
        self._stop = False

    def addView(self, view):
        logging.debug('addView {}'.format(view))
        self._views.append(view)

    @property
    def results(self):
        return self._results

    @property
    def measurements(self):
        return self._results.values()

    def _discoverMetrics(self):
        results = metric.Metric.discover(self._metric_source)
        logging.debug('_discoverMetrics: {} results discovered'.format(len(results)))
        for symbol in list(results):
            if not self._matches(symbol, self._metricPatterns):
                results.pop(symbol)
        logging.debug('_initializeMetrics: {} results matched'.format(len(results)))
        return results

    def _matches(self, symbol, metricPatterns):
        for pattern in metricPatterns:
            match = fnmatch.fnmatch(symbol, pattern)
            if match:
                return True
        return False

    def go(self, mainLoop):
        num_updated = 0
        num_absent = 0
        num_expired = 0
        num_added = 0
        now = time.time()
        while not self._stop:
            new_results = self._discoverMetrics();
            expiration = time.time() + self._ttl if self._ttl else None
            for metric in self._results:
                if not metric in new_results:
                    metric_obj = self._results[metric]
                    if not metric_obj.is_absent:
                        metric_obj.markAbsent(expiration)
                        num_absent += 1
                    elif metric_obj.expiration and now >= metric_obj.expiration:
                        self._results.pop(metric)
                        num_expired += 1
                    else:
                        num_absent += 1
            num_updated = len(self._results) - num_absent
            num_added = len(new_results) - num_updated
            self._results.update(new_results)
            logging.debug('go: updated {} measurements, added {}, {} marked absent, {} expired'.format(num_updated, num_added, num_absent, num_expired))

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
