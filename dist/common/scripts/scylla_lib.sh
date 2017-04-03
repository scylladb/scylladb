#
#  Copyright (C) 2016 ScyllaDB

is_debian_variant() {
    [ -f /etc/debian_version ]
}

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}

is_systemd() {
    grep -q '^systemd$' /proc/1/comm
}

is_ec2() {
    [ -f /sys/hypervisor/uuid ] && [ "$(head -c 3 /sys/hypervisor/uuid)" = "ec2" ]
}

. /etc/os-release
if is_debian_variant; then
    SYSCONFIG=/etc/default
else
    SYSCONFIG=/etc/sysconfig
fi
. $SYSCONFIG/scylla-server

for i in /etc/scylla.d/*.conf; do
    if [ "$i" = "/etc/scylla.d/*.conf" ]; then
        break
    fi
    . "$i"
done
