#!/usr/bin/env python3
import os
import scyllasetup
import logging
import commandlineparser

logging.basicConfig(filename="/var/log/scylla/docker-entrypoint.log", level=logging.DEBUG, format="%(message)s")

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
