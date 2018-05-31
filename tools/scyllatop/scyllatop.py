#!/usr/bin/env python
import argparse
import sys
import threading
import pprint
import logging
import collectd
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


def fancyUserInterface(metricPatterns, interval, metric_source):
    aggregateView = views.aggregate.Aggregate()
    simpleView = views.simple.Simple()
    userInput = userinput.UserInput()
    loop = urwid.MainLoop(aggregateView.widget(), unhandled_input=userInput)
    userInput.setLoop(loop)
    userInput.setMap(M=aggregateView, S=simpleView)
    liveData = livedata.LiveData(metricPatterns, interval, metric_source)
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
    description = '\n'.join(['A top-like tool for scylladb collectd/prometheus metrics.',
                             'Keyboard shortcuts: S - simple view, M - aggregate over multiple cores, Q -quits',
                             '',
                             'By default it would work with the Prometheus API and does not require configuration.',
                             'For collectd, you need to configure the unix-sock plugin for collectd'
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
    parser.add_argument('-s', '--socket', default='/var/run/collectd-unixsock', help="unixsock plugin to connect to, default: /var/run/collectd-unixsock")
    parser.add_argument('-p', '--prometheus-address', default='http://localhost:9180/metrics', help="The prometheus end-point")
    parser.add_argument('--print-config', action='store_true',
                        help="print out a configuration to put in your collectd.conf (you can use -s here to define the socket path)")
    parser.add_argument('-l', '--list', action='store_true',
                        help="print out a list of all metrics exposed by collectd and exit")
    parser.add_argument('-c', '--collectd', action='store_true',
                        help="Use collectd instead of Prometheus to connect to scylla")
    parser.add_argument('-L', '--logfile', default='scyllatop.log',
                        help="specify path for log file")
    parser.add_argument('-S', '--shell', action='store_true', help="uses IPython to enter a debug shell, usefull for development")
    parser.add_argument('-F', '--fake', action='store_true', help="fake metric updates - this is for developers only")
    parser.add_argument('-n', '--iterations', type=int, default=None, help="Exit after a given number of iterations. This is only relevant if output is redirected")
    parser.add_argument('-b', '--batch', action='store_true', help="batch mode - dump metrics to stdout instead of using an interactive user session")
    arguments = parser.parse_args()
    stream_log = logging.StreamHandler()
    stream_log.setLevel(logging.ERROR)
    stream_log.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

    file_log = logging.FileHandler(filename=arguments.logfile)
    file_log.setLevel(getattr(logging, arguments.verbosity))
    file_log.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))

    logging.getLogger().addHandler(stream_log)
    logging.getLogger().addHandler(file_log)
    logging.getLogger().setLevel(logging.DEBUG)

    if arguments.print_config:
        print(collectd.COLLECTD_EXAMPLE_CONFIGURATION.format(socket=arguments.socket))
        quit()

    if arguments.fake:
        fake.fake()
    if arguments.collectd:
        metric_source = collectd.Collectd(arguments.socket)
    else:
        metric_source = prometheus.Prometheus(arguments.prometheus_address)
    if arguments.shell:
        shell()
        quit()
    if arguments.list:
        pprint.pprint([m.symbol + m.help for m in metric.Metric.discover_with_help(metric_source)])
        quit()

    try:
        if not sys.stdout.isatty() or arguments.batch:
            dumptostdout.dumpToStdout(arguments.metricPattern, arguments.interval, metric_source, arguments.iterations)
        else:
            fancyUserInterface(arguments.metricPattern, arguments.interval, metric_source)
    except KeyboardInterrupt:
        pass
