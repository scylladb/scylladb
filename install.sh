#!/bin/bash
#
# Copyright (C) 2018-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

set -e

if [ -z "$BASH_VERSION" ]; then
    echo "Unsupported shell, please run this script on bash."
    exit 1
fi

print_usage() {
    cat <<EOF
Usage: install.sh [options]

Options:
  --root /path/to/root     alternative install root (default /)
  --prefix /prefix         directory prefix (default /usr)
  --python3 /opt/python3   path of the python3 interpreter relative to install root (default /opt/scylladb/python3/bin/python3)
  --housekeeping           enable housekeeping service
  --nonroot                install Scylla without required root privilege
  --sysconfdir /etc/sysconfig   specify sysconfig directory name
  --packaging               use install.sh for packaging
  --upgrade                 upgrade existing scylla installation (don't overwrite config files)
  --supervisor             enable supervisor to manage scylla processes
  --supervisor-log-to-stdout logging to stdout on supervisor
  --without-systemd         skip installing systemd units
  --help                   this helpful message
EOF
    exit 1
}

# Some words about pathnames in this script.
#
# A pathname has three components: "$root/$prefix/$rest".
#
# $root is used to point at the entire installed hierarchy, so you can perform
# an install to a temporary directory without modifying your system, with the intent
# that the files are copied later to the real /. So, if "$root"="/tmp/xyz", you'd get
# a standard hierarchy under /tmp/xyz: /tmp/xyz/etc, /tmp/xyz/var, and 
# /tmp/xyz/opt/scylladb/bin/scylla. This is used by rpmbuild --root to create a filesystem
# image to package.
#
# When this script creates a file, it must use "$root" to refer to the file. When this
# script inserts a file name into a file, it must not use "$root", because in the installed
# system "$root" is stripped out. Example:
#
#    echo "This file's name is /a/b/c. > "$root/a/b/c"
#
# The second component is "$prefix". It is used by non-root install to place files into
# a directory of the user's choice (typically somewhere under their home directory). In theory
# all files should be always under "$prefix", but in practice /etc files are not under "$prefix"
# for standard installs (we use /etc not /usr/etc) and are under "$prefix" for non-root installs.
# Another exception is files that go under /opt/scylladb in a standard install go under "$prefix"
# for a non-root install.
#
# The last component is the rest of the file name, which doesn't matter for this script and
# isn't changed by it.

root=/
housekeeping=false
nonroot=false
packaging=false
upgrade=false
supervisor=false
supervisor_log_to_stdout=false
without_systemd=false
skip_systemd_check=false

while [ $# -gt 0 ]; do
    case "$1" in
        "--root")
            root="$(realpath "$2")"
            shift 2
            ;;
        "--prefix")
            prefix="$2"
            shift 2
            ;;
        "--housekeeping")
            housekeeping=true
            shift 1
            ;;
        "--python3")
            python3="$2"
            shift 2
            ;;
        "--nonroot")
            nonroot=true
            shift 1
            ;;
        "--sysconfdir")
            sysconfdir="$2"
            shift 2
            ;;
        "--packaging")
            packaging=true
            skip_systemd_check=true
            shift 1
            ;;
        "--upgrade")
            upgrade=true
            shift 1
            ;;
        "--supervisor")
            supervisor=true
            skip_systemd_check=true
            shift 1
            ;;
        "--supervisor-log-to-stdout")
            supervisor_log_to_stdout=true
            shift 1
            ;;
        "--without-systemd")
            without_systemd=true
            skip_systemd_check=true
            shift 1
            ;;
        "--help")
            shift 1
	    print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done

patchelf() {
    # patchelf comes from the build system, so it needs the build system's ld.so and
    # shared libraries. We can't use patchelf on patchelf itself, so invoke it via
    # ld.so.
    LD_LIBRARY_PATH="$PWD/libreloc" libreloc/ld.so libexec/patchelf "$@"
}

remove_rpath() {
    local file="$1"
    local rpath
    # $file might not be an elf image
    if rpath=$(patchelf --print-rpath "$file" 2>/dev/null); then
      if [ -n "$rpath" ]; then
        echo "remove rpath from $file"
        patchelf --remove-rpath "$file"
      fi
    fi
}

adjust_bin() {
    local bin="$1"
    # We could add --set-rpath too, but then debugedit (called by rpmbuild) barfs
    # on the result. So use LD_LIBRARY_PATH in the thunk, below.
    patchelf \
	--set-interpreter "$prefix/libreloc/ld.so" \
	"$root/$prefix/libexec/$bin"
    cat > "$root/$prefix/bin/$bin" <<EOF
#!/bin/bash -e
[[ -z "\$LD_PRELOAD" ]] || { echo "\$0: not compatible with LD_PRELOAD" >&2; exit 110; }
export GNUTLS_SYSTEM_PRIORITY_FILE="\${GNUTLS_SYSTEM_PRIORITY_FILE-$prefix/libreloc/gnutls.config}"
export LD_LIBRARY_PATH="$prefix/libreloc"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}suppressions=$prefix/libexec/ubsan-suppressions.supp"
exec -a "\$0" "$prefix/libexec/$bin" "\$@"
EOF
    chmod 755 "$root/$prefix/bin/$bin"
}

relocate_python3() {
    local script="$2"
    local scriptname="$(basename "$script")"
    local installdir="$1"
    local install="$installdir/$scriptname"
    local relocateddir="$installdir/libexec"
    local pythoncmd=$(realpath -ms --relative-to "$installdir" "$rpython3")
    local pythonpath="$(dirname "$pythoncmd")"

    if [ ! -x "$script" ]; then
        install -m755 "$script" "$install"
        return
    fi
    install -d -m755 "$relocateddir"
    install -m755 "$script" "$relocateddir"
    cat > "$install"<<EOF
#!/usr/bin/env bash
[[ -z "\$LD_PRELOAD" ]] || { echo "\$0: not compatible with LD_PRELOAD" >&2; exit 110; }
x="\$(readlink -f "\$0")"
b="\$(basename "\$x")"
d="\$(dirname "\$x")"
CENTOS_SSL_CERT_FILE="/etc/pki/tls/cert.pem"
if [ -f "\${CENTOS_SSL_CERT_FILE}" ]; then
  c=\${CENTOS_SSL_CERT_FILE}
fi
DEBIAN_SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt"
if [ -f "\${DEBIAN_SSL_CERT_FILE}" ]; then
  c=\${DEBIAN_SSL_CERT_FILE}
fi
PYTHONPATH="\${d}:\${d}/libexec:\$PYTHONPATH" PATH="\${d}/../bin:\${d}/$pythonpath:\${PATH}" SSL_CERT_FILE="\${c}" exec -a "\$0" "\${d}/libexec/\${b}" "\$@"
EOF
    chmod 755 "$install"
}

install() {
    command install -Z "$@"
}

installconfig() {
    local perm="$1"
    local src="$2"
    local dest="$3"
    local bname=$(basename "$src")

    # do not overwrite config file when upgrade mode
    if $upgrade && [ -e "$dest/$bname" ]; then
        local oldsum=$(md5sum "$dest/$bname" | cut -f 1 -d " ")
        local newsum=$(md5sum "$src" | cut -f 1 -d " ")
        # if old one and new one are same, we can skip installing
        if [ "$oldsum" != "$newsum" ]; then
            install "-m$perm" "$src" -T "$dest/$bname.new"
        fi
    else
        install "-m$perm" "$src" -Dt "$dest"
    fi
}

check_usermode_support() {
    user=$(systemctl --help|grep -e '--user')
    [ -n "$user" ]
}

. /etc/os-release

is_debian_variant() {
    [ "$ID_LIKE" = "debian" -o "$ID" = "debian" ]
}

is_alpine() {
    [ "$ID" = "alpine" ]
}

supervisor_dir() {
    local etcdir="$1"
    if is_debian_variant; then
        echo "$etcdir"/supervisor/conf.d
    elif is_alpine; then
        echo "$etcdir"/supervisor.d
    else
        echo "$etcdir"/supervisord.d
    fi
}

supervisor_conf() {
    local etcdir="$1"
    local service="$2"
    if is_debian_variant; then
        echo `supervisor_dir "$etcdir"`/"$service".conf
    else
        echo `supervisor_dir "$etcdir"`/"$service".ini
    fi
}

if ! $skip_systemd_check && [ ! -d /run/systemd/system/ ]; then
    echo "systemd is not detected, unsupported distribution."
    exit 1
fi

# change directory to the package's root directory
cd "$(dirname "$0")"

product="$(cat ./SCYLLA-PRODUCT-FILE)"

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi

rprefix=$(realpath -m "$root/$prefix")

if [ -f "/etc/os-release" ]; then
    . /etc/os-release
fi

if [ -z "$sysconfdir" ]; then
    sysconfdir=/etc/sysconfig
    if ! $nonroot; then
        if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
            sysconfdir=/etc/default
        fi
    fi
fi

if [ -z "$python3" ]; then
    python3=$prefix/python3/bin/python3
fi
rpython3=$(realpath -m "$root/$python3")
if ! $nonroot; then
    retc=$(realpath -m "$root/etc")
    rsysconfdir=$(realpath -m "$root/$sysconfdir")
    rusr=$(realpath -m "$root/usr")
    rsystemd=$(realpath -m "$rusr/lib/systemd/system")
    rdoc="$rprefix/share/doc"
    rdata=$(realpath -m "$root/var/lib/scylla")
    rhkdata=$(realpath -m "$root/var/lib/scylla-housekeeping")
else
    retc="$rprefix/etc"
    rsysconfdir="$rprefix/$sysconfdir"
    rsystemd="$HOME/.config/systemd/user"
    rdoc="$rprefix/share/doc"
    rdata="$rprefix"
fi

# scylla-conf
install -d -m755 "$retc"/scylla
install -d -m755 "$retc"/scylla.d

scylla_yaml_dir=$(mktemp -d)
scylla_yaml=$scylla_yaml_dir/scylla.yaml
grep -v api_ui_dir conf/scylla.yaml | grep -v api_doc_dir > $scylla_yaml
echo "api_ui_dir: /opt/scylladb/swagger-ui/dist/" >> $scylla_yaml
echo "api_doc_dir: /opt/scylladb/api/api-doc/" >> $scylla_yaml
installconfig 644 $scylla_yaml "$retc"/scylla
rm -rf $scylla_yaml_dir

installconfig 644 conf/cassandra-rackdc.properties "$retc"/scylla
if $housekeeping; then
    installconfig 644 conf/housekeeping.cfg "$retc"/scylla.d
fi
# scylla-kernel-conf
if ! $nonroot; then
    install -m755 -d "$rusr/lib/sysctl.d"
    for file in dist/common/sysctl.d/*.conf; do
        installconfig 644 "$file" "$rusr"/lib/sysctl.d
    done
fi
install -d -m755 -d "$rprefix"/kernel_conf
install -m755 dist/common/kernel_conf/scylla_tune_sched -Dt "$rprefix"/kernel_conf
install -m755 dist/common/kernel_conf/post_install.sh "$rprefix"/kernel_conf
if ! $without_systemd; then
    install -m644 dist/common/systemd/scylla-tune-sched.service -Dt "$rsystemd"
    if ! $nonroot && [ "$prefix" != "/opt/scylladb" ]; then
        install -d -m755 "$retc"/systemd/system/scylla-tune-sched.service.d
        cat << EOS > "$retc"/systemd/system/scylla-tune-sched.service.d/execpath.conf
[Service]
ExecStart=
ExecStart=$prefix/kernel_conf/scylla_tune_sched
EOS
    fi
fi
relocate_python3 "$rprefix"/kernel_conf dist/common/kernel_conf/scylla_tune_sched
# scylla-node-exporter
if ! $without_systemd; then
    install -d -m755 "$rsystemd"
fi
install -d -m755 "$rsysconfdir"
install -d -m755 "$rprefix"/node_exporter
install -d -m755 "$rprefix"/node_exporter/licenses
install -m755 node_exporter/node_exporter "$rprefix"/node_exporter
install -m644 node_exporter/LICENSE "$rprefix"/node_exporter/licenses
install -m644 node_exporter/NOTICE "$rprefix"/node_exporter/licenses
if ! $without_systemd; then
    install -m644 dist/common/systemd/scylla-node-exporter.service -Dt "$rsystemd"
fi
installconfig 644 dist/common/sysconfig/scylla-node-exporter "$rsysconfdir"
if ! $nonroot && ! $without_systemd; then
    install -d -m755 "$retc"/systemd/system/scylla-node-exporter.service.d
    install -m644 dist/common/systemd/scylla-node-exporter.service.d/dependencies.conf -Dt "$retc"/systemd/system/scylla-node-exporter.service.d
    if [ "$sysconfdir" != "/etc/sysconfig" ]; then
        cat << EOS > "$retc"/systemd/system/scylla-node-exporter.service.d/sysconfdir.conf
[Service]
EnvironmentFile=
EnvironmentFile=$sysconfdir/scylla-node-exporter
EOS
    fi
elif ! $without_systemd; then
    install -d -m755 "$rsystemd"/scylla-node-exporter.service.d
    cat << EOS > "$rsystemd"/scylla-node-exporter.service.d/nonroot.conf
[Service]
EnvironmentFile=
EnvironmentFile=$(realpath -m "$rsysconfdir/scylla-node-exporter")
ExecStart=
ExecStart=$rprefix/node_exporter/node_exporter $SCYLLA_NODE_EXPORTER_ARGS
User=
Group=
EOS

fi

# scylla-server
install -m755 -d "$rprefix"
install -m755 -d "$retc/scylla.d"
installconfig 644 dist/common/sysconfig/scylla-housekeeping "$rsysconfdir"
installconfig 644 dist/common/sysconfig/scylla-server "$rsysconfdir"
for file in dist/common/scylla.d/*.conf; do
    installconfig 644 "$file" "$retc"/scylla.d
done

install -d -m755 "$retc"/scylla "$rprefix/bin" "$rprefix/libexec" "$rprefix/libreloc" "$rprefix/scripts" "$rprefix/bin"
if ! $without_systemd; then
    install -m644 dist/common/systemd/scylla-fstrim.service -Dt "$rsystemd"
    install -m644 dist/common/systemd/scylla-housekeeping-daily.service -Dt "$rsystemd"
    install -m644 dist/common/systemd/scylla-housekeeping-restart.service -Dt "$rsystemd"
    install -m644 dist/common/systemd/scylla-server.service -Dt "$rsystemd"
    install -m644 dist/common/systemd/*.slice -Dt "$rsystemd"
    install -m644 dist/common/systemd/*.timer -Dt "$rsystemd"
fi
install -m755 seastar/scripts/seastar-cpu-map.sh -Dt "$rprefix"/scripts
install -m755 seastar/dpdk/usertools/dpdk-devbind.py -Dt "$rprefix"/scripts
install -m755 libreloc/* -Dt "$rprefix/libreloc"
for lib in libreloc/*; do
    remove_rpath "$rprefix/$lib"
done
# some files in libexec are symlinks, which "install" dereferences
# use cp -P for the symlinks instead.
install -m755 libexec/* -Dt "$rprefix/libexec"
for bin in libexec/*; do
    remove_rpath "$rprefix/$bin"
    adjust_bin "${bin#libexec/}"
done
install -m644 ubsan-suppressions.supp -Dt "$rprefix/libexec"

install -d -m755 "$rdoc"/scylla
install -m644 README.md -Dt "$rdoc"/scylla/
install -m644 NOTICE.txt -Dt "$rdoc"/scylla/
install -m644 ORIGIN -Dt "$rdoc"/scylla/
install -d -m755 -d "$rdoc"/scylla/licenses/
install -m644 licenses/* -Dt "$rdoc"/scylla/licenses/
install -m755 -d "$rdata"
install -m755 -d "$rdata"/data
install -m755 -d "$rdata"/commitlog
install -m755 -d "$rdata"/hints
install -m755 -d "$rdata"/view_hints
install -m755 -d "$rdata"/coredump
install -m755 -d "$rprefix"/swagger-ui
cp -r swagger-ui/dist "$rprefix"/swagger-ui
install -d -m755 -d "$rprefix"/api
cp -r api/api-doc "$rprefix"/api
install -d -m755 -d "$rprefix"/scyllatop
cp -r tools/scyllatop/* "$rprefix"/scyllatop
install -d -m755 -d "$rprefix"/scripts
cp -r dist/common/scripts/* "$rprefix"/scripts
chmod 755 "$rprefix"/scripts/*
ln -srf "$rprefix/scyllatop/scyllatop.py" "$rprefix/bin/scyllatop"
if $supervisor; then
    install -d -m755 "$rprefix"/supervisor
    install -m755 dist/common/supervisor/* -Dt "$rprefix"/supervisor
fi

# scylla tools
install -d -m755 "$retc"/bash_completion.d
install -m644 dist/common/nodetool-completion "$retc"/bash_completion.d
install -m755 bin/nodetool "$rprefix/bin"

SBINFILES=$(cd dist/common/scripts/; ls scylla_*setup node_health_check scylla_kernel_check)
SBINFILES+=" $(cd seastar/scripts; ls seastar-cpu-map.sh)"

cat << EOS > "$rprefix"/scripts/scylla_product.py
PRODUCT="$product"
EOS

if ! $nonroot && ! $without_systemd; then
    install -d -m755 "$retc"/systemd/system/scylla-server.service.d
    install -m644 dist/common/systemd/scylla-server.service.d/dependencies.conf -Dt "$retc"/systemd/system/scylla-server.service.d
    if [ "$sysconfdir" != "/etc/sysconfig" ]; then
        cat << EOS > "$retc"/systemd/system/scylla-server.service.d/sysconfdir.conf
[Service]
EnvironmentFile=
EnvironmentFile=$sysconfdir/scylla-server
EnvironmentFile=/etc/scylla.d/*.conf
EOS
        for i in daily restart; do
            install -d -m755 "$retc"/systemd/system/scylla-housekeeping-$i.service.d
            cat << EOS > "$retc"/systemd/system/scylla-housekeeping-$i.service.d/sysconfdir.conf
[Service]
EnvironmentFile=
EnvironmentFile=$sysconfdir/scylla-housekeeping
EOS
        done
    fi
elif ! $without_systemd; then
    install -d -m755 "$rsystemd"/scylla-server.service.d
    if [ -d /var/log/journal ]; then
        cat << EOS > "$rsystemd"/scylla-server.service.d/nonroot.conf
[Service]
EnvironmentFile=
EnvironmentFile=$(realpath -m "$rsysconfdir/scylla-server")
EnvironmentFile=$retc/scylla.d/*.conf
ExecStartPre=
ExecStart=
ExecStart=$rprefix/bin/scylla \$SCYLLA_ARGS \$SEASTAR_IO \$DEV_MODE \$CPUSET
ExecStopPost=
User=
AmbientCapabilities=
EOS
    else
        cat << EOS > "$rsystemd"/scylla-server.service.d/nonroot.conf
[Service]
EnvironmentFile=
EnvironmentFile=$(realpath -m "$rsysconfdir/scylla-server")
EnvironmentFile=$retc/scylla.d/*.conf
ExecStartPre=
ExecStartPre=$rprefix/scripts/scylla_logrotate
ExecStart=
ExecStart=$rprefix/bin/scylla \$SCYLLA_ARGS \$SEASTAR_IO \$DEV_MODE \$CPUSET
ExecStopPost=
User=
AmbientCapabilities=
StandardOutput=
StandardOutput=file:$rprefix/scylla-server.log
StandardError=
StandardError=inherit
EOS
    fi
fi


if ! $nonroot; then
    if [ "$sysconfdir" != "/etc/sysconfig" ]; then
        cat << EOS > "$rprefix"/scripts/scylla_sysconfdir.py
SYSCONFDIR="$sysconfdir"
EOS
    fi
    install -m755 -d "$rusr/bin"
    install -m755 -d "$rhkdata"
    ln -srf "$rprefix/bin/scylla" "$rusr/bin/scylla"
    ln -srf "$rprefix/bin/iotune" "$rusr/bin/iotune"
    ln -srf "$rprefix/bin/scyllatop" "$rusr/bin/scyllatop"
    ln -srf "$rprefix/bin/nodetool" "$rusr/bin/nodetool"
    install -d "$rusr"/sbin
    for i in $SBINFILES; do
        ln -srf "$rprefix/scripts/$i" "$rusr/sbin/$i"
    done

    # we need keep /usr/lib/scylla directory to support upgrade/downgrade
    # without error, so we need to create symlink for each script on the
    # directory
    install -m755 -d "$rusr"/lib/scylla/scyllatop/views
    for i in $(find "$rprefix"/scripts/ -maxdepth 1 -type f); do
        ln -srf $i "$rusr"/lib/scylla/
    done
    for i in $(find "$rprefix"/scyllatop/ -maxdepth 1 -type f); do
        ln -srf $i "$rusr"/lib/scylla/scyllatop
    done
    for i in $(find "$rprefix"/scyllatop/views -maxdepth 1 -type f); do
        ln -srf $i "$rusr"/lib/scylla/scyllatop/views
    done
else
    install -m755 -d "$rdata"/saved_caches
    cat << EOS > "$rprefix"/scripts/scylla_sysconfdir.py
SYSCONFDIR="$sysconfdir"
EOS
    install -d "$rprefix"/sbin
    for i in $SBINFILES; do
        ln -srf "$rprefix/scripts/$i" "$rprefix/sbin/$i"
    done
fi



install -m755 scylla-gdb.py -Dt "$rprefix"/scripts/

PYSCRIPTS=$(find dist/common/scripts/ -maxdepth 1 -type f -exec grep -Pls '\A#!/usr/bin/env python3' {} +)
for i in $PYSCRIPTS; do
    relocate_python3 "$rprefix"/scripts "$i"
done
for i in seastar/scripts/perftune.py seastar/scripts/seastar-addr2line; do
    relocate_python3 "$rprefix"/scripts "$i"
done
relocate_python3 "$rprefix"/scyllatop tools/scyllatop/scyllatop.py
relocate_python3 "$rprefix"/scripts fix_system_distributed_tables.py

if $supervisor; then
    install -d -m755 `supervisor_dir $retc`
    for service in scylla-server scylla-jmx scylla-node-exporter; do
        if [ "$service" = "scylla-server" ]; then
            program="scylla"
        else
            program=$service
        fi
        cat << EOS > `supervisor_conf $retc $service`
[program:$program]
directory=$rprefix
command=/bin/bash -c './supervisor/$service.sh'
EOS
        if [ "$service" != "scylla-server" ]; then
            cat << EOS >> `supervisor_conf $retc $service`
user=scylla
EOS
        fi
        if $supervisor_log_to_stdout; then
            cat << EOS >> `supervisor_conf $retc $service`
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
EOS
        fi
    done
fi

if $nonroot; then
    sed -i -e "s#/var/lib/scylla#$rprefix#g" $rsysconfdir/scylla-server
    sed -i -e "s#/etc/scylla#$retc/scylla#g" $rsysconfdir/scylla-server
    sed -i -e "s#^SCYLLA_ARGS=\"#SCYLLA_ARGS=\"--workdir $rprefix #g" $rsysconfdir/scylla-server
    if [ ! -d /var/log/journal ] || $supervisor_log_to_stdout; then
        sed -i -e "s#--log-to-stdout 0#--log-to-stdout 1#g" $rsysconfdir/scylla-server
    fi
    # nonroot install is also 'offline install'
    touch $rprefix/SCYLLA-OFFLINE-FILE
    touch $rprefix/SCYLLA-NONROOT-FILE
    if ! $without_systemd_check && check_usermode_support; then
        systemctl --user daemon-reload
    fi
    echo "Scylla non-root install completed."
elif ! $packaging; then
    if $supervisor_log_to_stdout; then
        sed -i -e "s#--log-to-stdout 0#--log-to-stdout 1#g" $rsysconfdir/scylla-server
    fi
    # run install.sh without --packaging is 'offline install'
    touch $rprefix/SCYLLA-OFFLINE-FILE
    nousr=
    nogrp=
    getent passwd scylla || nousr=1
    getent group scylla || nogrp=1

    # this handles both case group is not exist || group already exists
    if [ $nousr ]; then
        useradd -r -d /var/lib/scylla -M scylla
    # only group is not exist, create it and add user to the group
    elif [ $nogrp ]; then
        groupadd -r scylla
        usermod -g scylla scylla
    fi
    chown -R scylla:scylla $rdata
    chown -R scylla:scylla $rhkdata

    for file in dist/common/sysctl.d/*.conf; do
        bn=$(basename "$file")
        # ignore error since some kernel may not have specified parameter
        sysctl -p "$rusr"/lib/sysctl.d/"$bn" || :
    done
    if ! $supervisor; then
        $rprefix/scripts/scylla_post_install.sh
        $rprefix/kernel_conf/post_install.sh
    fi
    echo "Scylla offline install completed."
fi
