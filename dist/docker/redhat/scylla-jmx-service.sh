#!/bin/bash

source /etc/sysconfig/scylla-jmx

exec /opt/scylladb/scripts/jmx/scylla-jmx -l /opt/scylladb/scripts/jmx
