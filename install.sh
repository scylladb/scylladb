#!/bin/bash
#
# Copyright (C) 2018 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

set -e

print_usage() {
    cat <<EOF
Usage: install.sh [options]

Options:
  --root /path/to/root     alternative install root (default /)
  --prefix /prefix         directory prefix (default /usr)
  --python3 /opt/python3   path of the python3 interpreter relative to install root (default /opt/scylladb/python3/bin/python3)
  --housekeeping           enable housekeeping service
  --nonroot                install Scylla without required root priviledge
  --pkg package            specify build package (server/conf/kernel-conf)
  --sysconfdir /etc/sysconfig   specify sysconfig directory name
  --help                   this helpful message
EOF
    exit 1
}

root=/
housekeeping=false
python3=/opt/scylladb/python3/bin/python3
sysconfdir=/etc/sysconfig
nonroot=false

while [ $# -gt 0 ]; do
    case "$1" in
        "--root")
            root="$2"
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
        "--pkg")
            pkg="$2"
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
        "--help")
            shift 1
	    print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done
if [ -n "$pkg" ] && [ "$pkg" != "server" -a "$pkg" != "conf" -a "$pkg" != "kernel-conf" ]; then
    print_usage
    exit 1
fi

patchelf() {
    # patchelf comes from the build system, so it needs the build system's ld.so and
    # shared libraries. We can't use patchelf on patchelf itself, so invoke it via
    # ld.so.
    LD_LIBRARY_PATH="$PWD/libreloc" libreloc/ld.so libexec/patchelf "$@"
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
export GNUTLS_SYSTEM_PRIORITY_FILE="\${GNUTLS_SYSTEM_PRIORITY_FILE-$prefix/libreloc/gnutls.config}"
export LD_LIBRARY_PATH="$prefix/libreloc"
exec -a "\$0" "$prefix/libexec/$bin" "\$@"
EOF
    chmod +x "$root/$prefix/bin/$bin"
}

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi

rprefix=$(realpath -m "$root/$prefix")
if [ -z "$python3" ]; then
    python3=$prefix/python3/bin/python3
fi
rpython3=$(realpath -m "$root/$python3")
if ! $nonroot; then
    retc="$root/etc"
    rsysconfdir="$root/$sysconfdir"
    rusr="$root/usr"
    rsystemd="$rusr/lib/systemd/system"
    rdoc="$rprefix/share/doc"
    rdata="$root/var/lib/scylla"
    rhkdata="$root/var/lib/scylla-housekeeping"
else
    retc="$rprefix/etc"
    rsysconfdir="$rprefix/$sysconfdir"
    rsystemd="$retc/systemd"
    rdoc="$rprefix/share/doc"
    rdata="$rprefix"
fi

if [ -z "$pkg" ] || [ "$pkg" = "conf" ]; then
    install -d -m755 "$retc"/scylla
    install -d -m755 "$retc"/scylla.d
    install -m644 conf/scylla.yaml -Dt "$retc"/scylla
    install -m644 conf/cassandra-rackdc.properties -Dt "$retc"/scylla
    if $housekeeping; then
        install -m644 conf/housekeeping.cfg -Dt "$retc"/scylla.d
    fi
fi
if [ -z "$pkg" ] || [ "$pkg" = "kernel-conf" ]; then
    if ! $nonroot; then
        install -m755 -d "$rusr/lib/sysctl.d"
        install -m644 dist/common/sysctl.d/*.conf -Dt "$rusr"/lib/sysctl.d
    fi
fi
if [ -z "$pkg" ] || [ "$pkg" = "server" ]; then
    install -m755 -d "$rsysconfdir"
    install -m755 -d "$retc/scylla.d"
    install -m644 dist/common/sysconfig/* -Dt "$rsysconfdir"
    install -m644 dist/common/limits.d/scylla.conf -Dt "$retc"/security/limits.d
    install -m644 dist/common/scylla.d/*.conf -Dt "$retc"/scylla.d

    install -d -m755 "$retc"/scylla "$rsystemd" "$rprefix/bin" "$rprefix/libexec" "$rprefix/libreloc" "$rprefix/scripts" "$rprefix/bin"
    install -m644 dist/common/systemd/*.service -Dt "$rsystemd"
    install -m644 dist/common/systemd/*.slice -Dt "$rsystemd"
    install -m644 dist/common/systemd/*.timer -Dt "$rsystemd"
    install -m755 seastar/scripts/seastar-cpu-map.sh -Dt "$rprefix"/scripts
    install -m755 seastar/dpdk/usertools/dpdk-devbind.py -Dt "$rprefix"/scripts
    install -m755 libreloc/* -Dt "$rprefix/libreloc"
    # some files in libexec are symlinks, which "install" dereferences
    # use cp -P for the symlinks instead.
    install -m755 libexec/* -Dt "$rprefix/libexec"
    for bin in libexec/*; do
	adjust_bin "${bin#libexec/}"
    done

    install -d -m755 "$rdoc"/scylla
    install -m644 README.md -Dt "$rdoc"/scylla/
    install -m644 README-DPDK.md -Dt "$rdoc"/scylla
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
    ln -srf "$rprefix/scyllatop/scyllatop.py" "$rprefix/bin/scyllatop"

    SBINFILES=$(cd dist/common/scripts/; ls scylla_*setup node_exporter_install node_health_check scylla_ec2_check scylla_kernel_check)
    if ! $nonroot; then
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
        install -m755 -d "$retc/security/limits.d"
        install -m755 -d "$rusr/bin"
        install -m755 -d "$rhkdata"
        install -m644 dist/common/limits.d/scylla.conf -Dt "$retc"/security/limits.d
        ln -srf "$rprefix/bin/scylla" "$rusr/bin/scylla"
        ln -srf "$rprefix/bin/iotune" "$rusr/bin/iotune"
        ln -srf "$rprefix/bin/scyllatop" "$rusr/bin/scyllatop"
        install -d "$rusr"/sbin
        for i in $SBINFILES; do
            ln -srf "$rprefix/scripts/$i" "$rusr/sbin/$i"
        done
    else
        install -m755 -d "$rdata"/saved_caches
        install -d -m755 "$retc"/systemd/system/scylla-server.service.d
        cat << EOS > "$retc"/systemd/system/scylla-server.service.d/nonroot.conf
[Service]
EnvironmentFile=
EnvironmentFile=$rsysconfdir/scylla-server
EnvironmentFile=$retc/scylla.d/*.conf
ExecStartPre=
ExecStart=
ExecStart=$rprefix/bin/scylla \$SCYLLA_ARGS \$SEASTAR_IO \$DEV_MODE \$CPUSET
ExecStopPost=
User=
EOS
         install -d -m755 "$retc"/systemd/system/node-exporter.service.d
         cat << EOS > "$retc"/systemd/system/node-exporter.service.d/nonroot.conf
[Service]
ExecStart=
ExecStart=$rprefix/bin/node_exporter  --collector.interrupts
User=
Group=
EOS
        install -d "$rprefix"/sbin
        for i in $SBINFILES; do
            ln -srf "$rprefix/scripts/$i" "$rprefix/sbin/$i"
        done
        if [ ! -d ~/.config/systemd/user/scylla-server.service.d ]; then
            mkdir -p ~/.config/systemd/user/scylla-server.service.d
        fi
        ln -srf $rsystemd/scylla-server.service ~/.config/systemd/user/
        ln -srf "$retc"/systemd/system/scylla-server.service.d/nonroot.conf ~/.config/systemd/user/scylla-server.service.d
        if [ ! -d ~/.config/systemd/user/node-exporter.service.d ]; then
            mkdir -p ~/.config/systemd/user/node-exporter.service.d
        fi
        ln -srf $rsystemd/node-exporter.service ~/.config/systemd/user/
        ln -srf "$retc"/systemd/system/node-exporter.service.d/nonroot.conf ~/.config/systemd/user/node-exporter.service.d
    fi

    install -m755 scylla-gdb.py -Dt "$rprefix"/scripts/

    PYSCRIPTS=$(find dist/common/scripts/ -maxdepth 1 -type f -exec grep -Pls '\A#!/usr/bin/env python3' {} +)
    for i in $PYSCRIPTS; do
        ./relocate_python_scripts.py \
                --installroot $rprefix/scripts/ --with-python3 "$rpython3" $i
    done
    ./relocate_python_scripts.py \
                --installroot $rprefix/scripts/ --with-python3 "$rpython3" \
                seastar/scripts/perftune.py seastar/scripts/seastar-addr2line seastar/scripts/perftune.py

    ./relocate_python_scripts.py \
                --installroot $rprefix/scyllatop/ --with-python3 "$rpython3" \
                tools/scyllatop/scyllatop.py
fi

if $nonroot; then
    sed -i -e "s#/var/lib/scylla#$rprefix#g" $retc/scylla/scylla.yaml
    sed -i -e "s/^# hints_directory/hints_directory/" $retc/scylla/scylla.yaml
    sed -i -e "s/^# view_hints_directory/view_hints_directory/" $retc/scylla/scylla.yaml
    sed -i -e "s/^# saved_caches_directory/saved_caches_directory/" $retc/scylla/scylla.yaml
    sed -i -e "s#/var/lib/scylla#$rprefix#g" $rsysconfdir/scylla-server
    sed -i -e "s#/etc/scylla#$retc/scylla#g" $rsysconfdir/scylla-server
    touch $rprefix/SCYLLA-NONROOT-FILE
    systemctl --user daemon-reload
    echo "Scylla non-root install completed."
fi
