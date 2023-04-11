#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import signal
import subprocess
import scyllasetup
import logging
import commandlineparser
import minimalcontainer

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
    setup.arguments()
    if not minimalcontainer.is_minimal_container():
        setup.cqlshrc()
        setup.set_housekeeping()
        supervisord = subprocess.Popen(["/usr/bin/supervisord", "-c",  "/etc/supervisord.conf"])
        supervisord.wait()
    else:
        minimalcontainer.exec_scylla(arguments)
except Exception:
    logging.exception('failed!')
