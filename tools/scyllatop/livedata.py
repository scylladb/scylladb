import logging
import time
import metric


class LiveData(object):
    def __init__(self, metrics, interval, collectd):
        logging.info('will query collectd every {0} seconds'.format(interval))
        self._startedAt = time.time()
        self._measurements = []
        self._interval = interval
        self._collectd = collectd
        self._initializeMetrics(metrics)
        self._views = []
        self._stop = False

    def addView(self, view):
        self._views.append(view)

    @property
    def measurements(self):
        return self._measurements

    def _initializeMetrics(self, metrics):
        if len(metrics) > 0:
            for metricName in metrics:
                self._measurements.append(metric.Metric(metricName, self._collectd))
            return

        self._measurements = metric.Metric.discover(self._collectd)

    def go(self):
        while not self._stop:
            for metric in self._measurements:
                metric.update()

            for view in self._views:
                view.update(self)
            time.sleep(self._interval)

    def stop(self):
        self._stop = True
