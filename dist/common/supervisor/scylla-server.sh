#!/bin/bash

. $(dirname "$0")/scylla_util.sh

. "$sysconfdir"/scylla-server

for f in "$etcdir"/scylla.d/*.conf; do
    . "$f"
done

if is_privileged; then
    # Override pipe-based core_pattern that may not work inside a container
    # (e.g. Ubuntu host's apport).  File-based patterns resolve inside the
    # container's mount namespace, so coredumps land in the right place.
    # Derive workdir from scylla.yaml, matching the Python entrypoint logic.
    _workdir=$(python3 -c "import yaml; cfg=yaml.safe_load(open('/etc/scylla/scylla.yaml')); print(cfg.get('workdir') or '/var/lib/scylla')" 2>/dev/null || echo "/var/lib/scylla")
    _coredump_dir="${_workdir}/coredump"
    core_pattern=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || true)
    if [[ "$core_pattern" == "|"* ]]; then
        if ! mkdir -p "$_coredump_dir" 2>/dev/null; then
            echo "WARNING: could not create coredump directory $_coredump_dir" >&2
        elif echo "${_coredump_dir}/core.%e.%p.%t" > /proc/sys/kernel/core_pattern 2>/dev/null; then
            echo "kernel.core_pattern overridden to file-based pattern: ${_coredump_dir}/core.%e.%p.%t" >&2
        else
            echo "WARNING: pipe-based core_pattern detected but could not override. Coredumps may be lost." >&2
        fi
    fi
    "$scriptsdir"/scylla_prepare
fi
execsudo /usr/bin/env SCYLLA_HOME=$SCYLLA_HOME SCYLLA_CONF=$SCYLLA_CONF "$bindir"/scylla $SCYLLA_ARGS $SEASTAR_IO $DEV_MODE $CPUSET $SCYLLA_DOCKER_ARGS
