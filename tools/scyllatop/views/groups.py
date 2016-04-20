import re
import mergeable


class Group(object):
    _HEAD_PATTERN = re.compile('^([^-]+)-\d+/')

    def __init__(self, label):
        self._label = label
        self._metrics = []

    def add(self, metric):
        self._metrics.append(metric)

    @property
    def metrics(self):
        return self._metrics

    def aggregate(self, mergeMethod):
        merger = mergeable.Mergeable(mergeMethod)
        for metric in self._metrics:
            merger.add(metric.status)

        return merger.merged()

    @property
    def label(self):
        return self._label

    @classmethod
    def extractLabel(cls, metric):
        return cls._HEAD_PATTERN.sub(r'\1-*/', metric.symbol)

    @property
    def size(self):
        return len(self._metrics)


class Groups(object):
    def __init__(self, measurements):
        self._groups = {}
        self._load(measurements)

    def _load(self, measurements):
        for metric in measurements:
            label = Group.extractLabel(metric)
            self._groups.setdefault(label, Group(label))
            self._groups[label].add(metric)

    def all(self):
        return sorted(self._groups.values(), key=lambda group: group.label)
