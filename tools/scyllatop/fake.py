import metric
import random
import collectd
import logging


class FakeCollectd(object):
    def __init__(self, socketName):
        pass

    def query(self, command):
        pass

Metric = metric.Metric


class FakeMetric(Metric):
    def update(self):
        self._status['value'] = random.randint(0, 100000)
        logging.info('{} {}'.format(self.symbol, self.status))

    @classmethod
    def discover(self, unused):
        results = []
        for cpu in range(4):
            results += [
                FakeMetric('localhost/cpu-{}/cache'.format(cpu), None),
                FakeMetric('localhost/cpu-{}/storage_proxy'.format(cpu), None),
                FakeMetric('localhost/cpu-{}/transport'.format(cpu), None)]

        for x in range(100):
            results.append(FakeMetric('localhost/fake_{}/{}'.format(x, random.choice('abcdefghijklmnopqrstuvwxyz')), None))
        return results


def fake():
    metric.Metric = FakeMetric
    collectd.Collectd = FakeCollectd
