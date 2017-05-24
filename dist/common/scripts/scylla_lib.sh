#
#  Copyright (C) 2016 ScyllaDB

is_debian_variant() {
    [ -f /etc/debian_version ]
}

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}

is_gentoo_variant() {
    [ -f /etc/gentoo-release ]
}

is_systemd() {
    grep -q '^systemd$' /proc/1/comm
}

is_ec2() {
    [ -f /sys/hypervisor/uuid ] && [ "$(head -c 3 /sys/hypervisor/uuid)" = "ec2" ]
}

is_selinux_enabled() {
    STATUS=`getenforce`
    if [ "$STATUS" = "Disabled" ]; then
        return 0
    else
        return 1
    fi
}

ec2_is_supported_instance_type() {
    TYPE=`curl -s http://169.254.169.254/latest/meta-data/instance-type|cut -d . -f 1`
    case $TYPE in
           "m3"|"c3"|"i2"|"i3") echo 1;;
            *) echo 0;;
    esac
}

. /etc/os-release
if is_debian_variant || is_gentoo_variant; then
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
