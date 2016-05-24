#!/usr/bin/env python
import argparse
import threading
import pprint
import logging
import collectd
import metric
import fake
import livedata
import views.simple
import views.aggregate
import userinput
import signal
import urwid


def halt(* args):
    quit()

signal.signal(signal.SIGINT, halt)


def shell():
    try:
        import IPython
        IPython.embed()
    except ImportError:
        logging.error('shell mode requires IPython to be installed')


def main(metricPatterns, interval, collectd):
    aggregateView = views.aggregate.Aggregate()
    simpleView = views.simple.Simple()
    userInput = userinput.UserInput()
    loop = urwid.MainLoop(aggregateView.widget(), unhandled_input=userInput)
    userInput.setLoop(loop)
    userInput.setMap(M=aggregateView, S=simpleView)
    liveData = livedata.LiveData(metricPatterns, interval, collectd)
    liveData.addView(simpleView)
    liveData.addView(aggregateView)
    liveDataThread = threading.Thread(target=lambda: liveData.go(loop))
    liveDataThread.daemon = True
    liveDataThread.start()
    loop.run()

if __name__ == '__main__':
    description = '\n'.join(['A top-like tool for scylladb collectd metrics.',
                             'Keyborad shortcuts: S - simple view, M - aggregate over multiple cores, Q -quits',
                             '',
                             'You need to configure the unix-sock plugin for collectd'
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
    parser.add_argument('--print-config', action='store_true',
                        help="print out a configuration to put in your collectd.conf (you can use -s here to define the socket path)")
    parser.add_argument('-l', '--list', action='store_true',
                        help="print out a list of all metrics exposed by collectd and exit")
    parser.add_argument('-L', '--logfile', default='scyllatop.log',
                        help="specify path for log file")
    parser.add_argument('-S', '--shell', action='store_true', help="uses IPython to enter a debug shell, usefull for development")
    parser.add_argument('-F', '--fake', action='store_true', help="fake metric updates - this is for developers only")
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
    collectd = collectd.Collectd(arguments.socket)
    if arguments.shell:
        shell()
        quit()
    if arguments.list:
        pprint.pprint([m.symbol for m in metric.Metric.discover(collectd)])
        quit()

    main(arguments.metricPattern, arguments.interval, collectd)
