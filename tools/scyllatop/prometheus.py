#! /usr/bin/python

import urllib2
import re


class Prometheus(object):
    _FIRST_LINE_PATTERN = re.compile('^(?P<lines>\d+)')
    _METRIC_INFO_PATTERN = re.compile('^(?P<key>.+) (?P<value>[^ ]+)[ ]*$')
    _METRIC_DISCOVER_PATTERN = re.compile('^(?P<metric>[^#].+) (?P<value>[^ ]+)[ ]*$')
    _METRIC_DISCOVER_PATTERN_WITH_HELP = re.compile('^# HELP (?P<metric>[^ ]+)(?P<help>.*)$')

    def __init__(self, host):
        self._host = host

    def read_metrics(self):
        return urllib2.urlopen(self._host).readlines()

    def get_metrics(self):
        return self.read_metrics()

    def query_val(self, val):
        return [l for l in self.get_metrics() if (not l.startswith('#')) and (val == "" or re.match(val, l))]

    def query_list(self):
        return self.get_metrics()
