#!/bin/bash
sleep 5
/usr/lib/scylla/scylla-housekeeping --uuid-file /var/lib/scylla-housekeeping/housekeeping.uuid --repo-files '/etc/yum.repos.d/scylla*.repo'  -q version --mode cr || true
while true; do
    sleep 1d
    /usr/lib/scylla/scylla-housekeeping --uuid-file /var/lib/scylla-housekeeping/housekeeping.uuid --repo-files '/etc/yum.repos.d/scylla*.repo' -q version --mode cd || true
done

