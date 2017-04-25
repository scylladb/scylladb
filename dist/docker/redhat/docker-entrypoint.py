#!/usr/bin/env python3
import logging
import signal
import subprocess
import sys

import commandlineparser
import scyllasetup

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

try:
    arguments = commandlineparser.parse()
    setup = scyllasetup.ScyllaSetup(arguments)
    setup.developerMode()
    setup.cpuSet()
    setup.io()
    setup.cqlshrc()
    setup.arguments()
    p = subprocess.Popen(["usr/bin/supervisord", "-c", "/etc/supervisord.conf"])


    def signal_handler(sig, frame):
        p.terminate()


    [signal.signal(sig, signal_handler) for sig in [signal.SIGINT, signal.SIGTERM]]

    p.wait()
except:
    logging.exception('failed!')
