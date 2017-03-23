#!/usr/bin/env python3
import os
import sys
import scyllasetup
import logging
import commandlineparser

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

try:
    arguments = commandlineparser.parse()
    setup = scyllasetup.ScyllaSetup(arguments)
    setup.developerMode()
    setup.cpuSet()
    setup.io()
    setup.scyllaYAML()
    setup.cqlshrc()
    setup.arguments()
    os.system("/usr/bin/supervisord -c /etc/supervisord.conf")
except:
    logging.exception('failed!')
