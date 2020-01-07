#!/bin/bash

source /etc/sysconfig/scylla-jmx

exec /opt/scylladb/jmx/scylla-jmx -l /opt/scylladb/jmx
