#!/bin/bash

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

# Install capabilities.conf when AmbientCapabilities supported
. /etc/os-release

# the command below will still work in systems without systemctl (like docker) and across all
# versions of systemd. We will set the version to 0 if systemctl is not found and then be able
# to use that in tests.
SYSTEMD_VER=$(( systemctl --version 2>/dev/null || echo 0 0) | head -n1 | awk '{print $2}')
RHEL=$(echo $ID_LIKE | grep -oi rhel)
SYSTEMD_REL=0
if [ "$RHEL" ]; then
    SYSTEMD_REL=`rpm -q systemd --qf %{release}|sed -n "s/\([0-9]*\).*/\1/p"`
fi

AMB_SUPPORT=`grep -c ^CapAmb: /proc/self/status`

# all other distributions supported by Scylla are systemd enabled, only older debian/ubuntu need these checks.
if [[ "$ID" = "debian" && "$VERSION_ID" = "8" ]] || [[ "$ID" = "ubuntu" && "$VERSION_ID" = "14.04" ]]; then
    echo "kernel.core_pattern=|/opt/scylladb/scripts/scylla_save_coredump %e %t %p" > /etc/sysctl.d/99-scylla-coredump.conf
    echo "scylla ALL=(ALL) NOPASSWD: /opt/scylladb/scripts/scylla_prepare,/opt/scylladb/scripts/scylla_stop,/opt/scylladb/scripts/scylla_io_setup,/opt/scylladb/scripts/scylla-ami/scylla_ami_setup" > /etc/sudoers.d/scylla
else
    # AmbientCapabilities supported from v229 but it backported to v219-33 on RHEL7
    if [ $SYSTEMD_VER -ge 229 ] || [ $SYSTEMD_VER -eq 219 ] && [ $SYSTEMD_REL -ge 33 ]; then
        if [ $AMB_SUPPORT -eq 1 ]; then
            mkdir -p /etc/systemd/system/scylla-server.service.d/
            cat << EOS > /etc/systemd/system/scylla-server.service.d/capabilities.conf
[Service]
AmbientCapabilities=CAP_SYS_NICE
EOS
        fi
    fi

    systemctl --system daemon-reload >/dev/null || true
fi
