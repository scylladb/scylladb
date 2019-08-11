import metric
import os
import random
import collectd
import logging


class FakeCollectd(object):
    def __init__(self, socketName):
        pass

    def query(self, command):
        pass


Metric = metric.Metric
if 'MARK_ABSENT_PROBABILITY' in os.environ:
    MARK_ABSENT_PROBABILITY = float(os.environ['MARK_ABSENT_PROBABILITY'])
else:
    MARK_ABSENT_PROBABILITY = 0


class FakeMetric(Metric):
    def update(self):
        self._status['value'] = random.randint(0, 100000)
        if random.random() > 1 - MARK_ABSENT_PROBABILITY:
            self.markAbsent()
        logging.info('{} {}'.format(self.symbol, self.status))

    @classmethod
    def discover(self, unused):
        results = {}

        def _add_metric(metric, results):
            m = FakeMetric(metric, None)
            m.add_to_results(results)

        for cpu in range(4):
            _add_metric('localhost/cpu-{}/cache'.format(cpu))
            _add_metric('localhost/cpu-{}/storage_proxy'.format(cpu))
            _add_metric('localhost/cpu-{}/transport'.format(cpu))

        for x in range(100):
            _add_metric('localhost/fake_{}/{}'.format(x, random.choice('abcdefghijklmnopqrstuvwxyz')))
        return results


def fake():
    metric.Metric = FakeMetric
    collectd.Collectd = FakeCollectd
