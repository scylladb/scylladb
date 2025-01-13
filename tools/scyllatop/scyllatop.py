#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
import threading
import pprint
import logging
import prometheus
import metric
import fake
import livedata
import views.simple
import views.aggregate
import userinput
import dumptostdout
import urwid


def shell():
    try:
        import IPython
        IPython.embed()
    except ImportError:
        logging.error('shell mode requires IPython to be installed')


def fancyUserInterface(metricPatterns, interval, metric_source, ttl):
    aggregateView = views.aggregate.Aggregate()
    simpleView = views.simple.Simple()
    userInput = userinput.UserInput()
    loop = urwid.MainLoop(aggregateView.widget(), unhandled_input=userInput)
    userInput.setLoop(loop)
    userInput.setMap(M=aggregateView, S=simpleView)
    try:
        liveData = livedata.LiveData(metricPatterns, interval, metric_source, ttl)
    except Exception as inst:
        print("scyllatop failed connecting to Scylla With an error: {error}".format(error=inst))
        sys.exit(1)
    liveData.addView(simpleView)
    liveData.addView(aggregateView)
    liveDataThread = threading.Thread(target=lambda: liveData.go(loop))
    liveDataThread.daemon = True
    liveDataThread.start()
    try:
        loop.run()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    description = '\n'.join(['A top-like tool for scylladb prometheus metrics.',
                             'Keyboard shortcuts: S - simple view, M - aggregate over multiple cores, Q -quits',
                             '',
                             'By default it would work with the Prometheus API and does not require configuration.',
                             'before you can use this, use the --print-config option to give you a configuration example',
                             'enjoy!'])
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-v',
        '--verbosity',
        help='python log level, e.g. DEBUG, INFO or ERROR',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='ERROR')
    parser.add_argument(dest='metricPattern', nargs='*', default=[], help='metrics to query, separated by spaces. You can use shell globs (e.g. *cpu*nice*) here to efficiently specify metrics')
    parser.add_argument('-i', '--interval', help="time resolution in seconds, default: 1", type=float, default=1)
    parser.add_argument('-p', '--prometheus-address', default='http://localhost:9180/metrics', help="The prometheus end-point")
    parser.add_argument('-l', '--list', action='store_true',
                        help="print out a list of all metrics exposed and exit")
    parser.add_argument('-L', '--logfile', default='scyllatop.log',
                        help="specify path for log file")
    parser.add_argument('-S', '--shell', action='store_true', help="uses IPython to enter a debug shell, usefull for development")
    parser.add_argument('-F', '--fake', action='store_true', help="fake metric updates - this is for developers only")
    parser.add_argument('-n', '--iterations', type=int, default=None, help="Exit after a given number of iterations. This is only relevant if output is redirected")
    parser.add_argument('-b', '--batch', action='store_true', help="batch mode - dump metrics to stdout instead of using an interactive user session")
    parser.add_argument('-t', '--ttl', type=int, default=60, help="Keep absent metrics for ttl seconds (default=60)")
    arguments = parser.parse_args()
    stream_log = logging.StreamHandler()
    stream_log.setLevel(logging.ERROR)
    stream_log.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    try:
        file_log = logging.FileHandler(filename=arguments.logfile)
    except Exception as inst:
        print("scyllatop failed opening log file: '{file}' With an error: {error}".format(file=arguments.logfile, error=inst))
        sys.exit(1)
    file_log.setLevel(getattr(logging, arguments.verbosity))
    file_log.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))

    logging.getLogger().addHandler(stream_log)
    logging.getLogger().addHandler(file_log)
    logging.getLogger().setLevel(logging.DEBUG)

    if arguments.fake:
        fake.fake()

    metric_source = prometheus.Prometheus(arguments.prometheus_address)
    if arguments.shell:
        shell()
        quit()
    if arguments.list:
        pprint.pprint([m.symbol + m.help for m in metric.Metric.discover_with_help(metric_source).values()])
        quit()

    logging.debug('arguments={} isatty={}'.format(arguments, sys.stdout.isatty()))
    try:
        if not sys.stdout.isatty() or arguments.batch:
            dumptostdout.dumpToStdout(arguments.metricPattern, arguments.interval, metric_source, arguments.iterations, arguments.ttl)
        else:
            fancyUserInterface(arguments.metricPattern, arguments.interval, metric_source, arguments.ttl)
    except KeyboardInterrupt:
        pass
