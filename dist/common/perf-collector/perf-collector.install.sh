#!/bin/bash
set -e
mkdir -p /var/log/scylla-perf/old
cp dist/common/perf-collector/scylla-perf-collector.service /usr/lib/systemd/system/
cp dist/common/perf-collector/scylla-perf-collector.logrotate /etc/logrotate.d/scylla-perf-collector
systemctl daemon-reload
systemctl enable scylla-perf-collector.service
systemctl start scylla-perf-collector.service
