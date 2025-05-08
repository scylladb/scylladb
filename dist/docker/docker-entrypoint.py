#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import signal
import subprocess
import scyllasetup
import logging
import commandlineparser
import time
sys.path.append('/opt/scylladb/scripts')
from scylla_util import sysconfig_parser

def extract_args_from_conf(arguments):
    scylla_server_conf = sysconfig_parser('/etc/sysconfig/scylla-server')
    scylla_args = scylla_server_conf.get('SCYLLA_ARGS')
    extracted_args = scylla_args.split()
    if arguments.developerMode == '0' and arguments.io_setup == '1':
        io_conf = sysconfig_parser('/etc/scylla.d/io.conf')
        seastar_io = io_conf.get('SEASTAR_IO')
        extracted_args += seastar_io.split()
    if arguments.developerMode == '1':
        dev_mode_conf = sysconfig_parser('/etc/scylla.d/dev-mode.conf')
        dev_mode = dev_mode_conf.get('DEV_MODE')
        extracted_args += dev_mode.split()
    if arguments.cpuset:
        cpuset_conf = sysconfig_parser('/etc/scylla.d/cpuset.conf')
        cpuset = cpuset_conf.get('CPUSET')
        extracted_args += cpuset.split()
    return extracted_args

def generate_env():
    env = os.environ.copy()
    env['SCYLLA_HOME'] = '/var/lib/scylla'
    env['SCYLLA_CONF'] = '/etc/scylla'
    return env

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

scylla = None

def signal_handler(signum, frame):
    scylla.send_signal(signum)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

try:
    cmdline = ['/usr/bin/scylla']
    arguments, extra_arguments = commandlineparser.parse()
    setup = scyllasetup.ScyllaSetup(arguments, extra_arguments=extra_arguments)
    setup.developerMode()
    setup.cpuSet()
    setup.io()
    setup.cqlshrc()
    setup.write_rackdc_properties()
    cmdline += setup.arguments()
    setup.set_housekeeping()
    cmdline += extract_args_from_conf(arguments)
    env = generate_env()
    logging.info('running: ({})'.format(cmdline))
    scylla = subprocess.Popen(cmdline, env=env)
    logging.info('running: ({})'.format(['/scylla-housekeeping-service.sh']))
    housekeeping = subprocess.Popen(['/scylla-housekeeping-service.sh'])
    exit_code = scylla.wait()
    if exit_code:
        logger.error(f'scylla process terminated with exit code: {exit_code}')
    housekeeping.kill()
    sys.exit(exit_code)
except Exception:
    logging.exception('failed!')
