#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import scyllasetup
import logging
import commandlineparser

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

try:
    arguments, extra_arguments = commandlineparser.parse()
    setup = scyllasetup.ScyllaSetup(arguments, extra_arguments=extra_arguments)
    setup.developerMode()
    setup.cpuSet()
    setup.io()
    setup.coredumpSetup()
    setup.cqlshrc()
    setup.write_rackdc_properties()
    setup.arguments()
    # Replace ourselves with scylla, so it receives signals directly
    # and its exit status becomes the container's exit status.
    scylla_server = "/opt/scylladb/supervisor/scylla-server.sh"
    sys.stdout.flush()
    os.execv(scylla_server, [scylla_server])
except Exception:
    logging.exception('failed!')
