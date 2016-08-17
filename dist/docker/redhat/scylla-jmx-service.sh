#!/bin/bash

source /etc/sysconfig/scylla-jmx

exec /usr/lib/scylla/jmx/scylla-jmx -l /usr/lib/scylla/jmx
