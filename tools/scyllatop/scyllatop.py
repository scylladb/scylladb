#!/usr/bin/env python
import argparse
import curses
import pprint
import logging
import collectd
import metric
import livedata
import views.simple
import views.means
import userinput
import signal


def halt(* args):
    quit()

signal.signal(signal.SIGINT, halt)


def main(screen, metricPatterns, interval, collectd):
    curses.curs_set(0)
    liveData = livedata.LiveData(metricPatterns, interval, collectd)
    simpleView = views.simple.Simple(screen)
    meansView = views.means.Means(screen)
    liveData.addView(simpleView)
    liveData.addView(meansView)
    meansView.onTop()
    userinput.UserInput(liveData, screen, simpleView, meansView)
    liveData.go()

if __name__ == '__main__':
    description = '\n'.join(['A top-like tool for scylladb collectd metrics.',
                            'Keyborad shortcuts: S - simple view, M - avergages over multiple cores, Q -quits',
                            '',
                            'You need to configure the unix-sock plugin for collectd'
                            'before you can use this, use the -c option to give you a configuration example',
                            'enjoy!'])
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-v',
        '--verbosity',
        help='python log level, e.g. DEBUG, INFO or ERROR',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='ERROR')
    parser.add_argument(dest='metricPattern', nargs='*', default=[], help='metrics to query, separated by spaces. You can use regular expressions here to efficiently specify metrics')
    parser.add_argument('-i', '--interval', help="time resolution in seconds, default: 1", type=float, default=1)
    parser.add_argument('-s', '--socket', default='/var/run/collectd-unixsock', help="unixsock plugin to connect to, default: /var/run/collectd-unixsock")
    parser.add_argument('--print-config', action='store_true',
                        help="print out a configuration to put in your collectd.conf (you can use -s here to define the socket path)")
    parser.add_argument('-l', '--list', action='store_true',
                        help="print out a list of all metrics exposed by collectd and exit")
    arguments = parser.parse_args()
    logging.basicConfig(filename='scyllatop.log',
                        level=getattr(logging, arguments.verbosity),
                        format='%(asctime)s %(levelname)s: %(message)s')
    if arguments.print_config:
        print(collectd.COLLECTD_EXAMPLE_CONFIGURATION.format(socket=arguments.socket))
        quit()
    collectd = collectd.Collectd(arguments.socket)
    if arguments.list:
        pprint.pprint([m.symbol for m in metric.Metric.discover(collectd)])
        quit()

    curses.wrapper(main, arguments.metricPattern, arguments.interval, collectd)
