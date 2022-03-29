#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import signal
import subprocess
import scyllasetup
import logging
import commandlineparser

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

supervisord = None

def signal_handler(signum, frame):
    supervisord.send_signal(signum)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

try:
    arguments, extra_arguments = commandlineparser.parse()
    setup = scyllasetup.ScyllaSetup(arguments, extra_arguments=extra_arguments)
    setup.developerMode()
    setup.cpuSet()
    setup.io()
    setup.cqlshrc()
    setup.arguments()
    setup.set_housekeeping()
    # supervisord mode
    if os.path.exists("/usr/bin/supervisord"):
        supervisord = subprocess.Popen(["/usr/bin/supervisord", "-c",  "/etc/supervisord.conf"])
        supervisord.wait()
    # systemd mode
    else:
        subprocess.run(["/usr/bin/systemctl", "start", "scylla-server.service"])
except Exception:
    logging.exception('failed!')
