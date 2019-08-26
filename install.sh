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
  --target centos          specify target distribution
  --disttype [redhat|debian] specify type of distribution (redhat or debian)
  --pkg package            specify build package (server/conf/kernel-conf)
  --help                   this helpful message
EOF
    exit 1
}

root=/
prefix=/opt/scylladb
housekeeping=false
target=centos
python3=/opt/scylladb/python3/bin/python3

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
        "--target")
            target="$2"
            shift 2
            ;;
        "--python3")
            python3="$2"
            shift 2
            ;;
        "--disttype")
            disttype="$2"
            shift 2
            ;;
        "--pkg")
            pkg="$2"
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

rprefix="$root/$prefix"
retc="$root/etc"
rusr="$root/usr"
rdoc="$rprefix/share/doc"

is_redhat=false
is_debian=false
MUSTACHE_DIST="\"$target\": true, \"target\": \"$target\""
if [ "$disttype" = "redhat" ]; then
    MUSTACHE_DIST="\"redhat\": true, $MUSTACHE_DIST"
    is_redhat=true
    sysconfdir=sysconfig
elif [ "$disttype" = "debian" ]; then
    MUSTACHE_DIST="\"debian\": true, $MUSTACHE_DIST"
    is_debian=true
    sysconfdir=default
else
    print_usage
    exit 1
fi

mkdir -p build
pystache dist/common/systemd/scylla-server.service.mustache "{ $MUSTACHE_DIST }" > build/scylla-server.service
pystache dist/common/systemd/scylla-housekeeping-daily.service.mustache "{ $MUSTACHE_DIST }" > build/scylla-housekeeping-daily.service
pystache dist/common/systemd/scylla-housekeeping-restart.service.mustache "{ $MUSTACHE_DIST }" > build/scylla-housekeeping-restart.service


if [ -z "$pkg" ] || [ "$pkg" = "conf" ]; then
    install -d -m755 "$retc"/scylla
    install -d -m755 "$retc"/scylla.d
    install -m644 conf/scylla.yaml -Dt "$retc"/scylla
    install -m644 conf/cassandra-rackdc.properties -Dt "$retc"/scylla
    # XXX: since housekeeping.cfg is mistakenly belongs to different package
    # in .rpm/.deb, we need this workaround to make package upgradable
    if $is_redhat && $housekeeping; then
        install -m644 conf/housekeeping.cfg -Dt "$retc"/scylla.d
    fi
fi
if [ -z "$pkg" ] || [ "$pkg" = "kernel-conf" ]; then
    install -m755 -d "$rusr/lib/sysctl.d"
    install -m644 dist/common/sysctl.d/*.conf -Dt "$rusr"/lib/sysctl.d
fi
if [ -z "$pkg" ] || [ "$pkg" = "server" ]; then
    install -m755 -d "$retc/$sysconfdir"
    install -m755 -d "$retc/security/limits.d"
    install -m755 -d "$retc/scylla.d"
    install -m644 dist/common/sysconfig/scylla-server -Dt "$retc"/$sysconfdir
    install -m644 dist/common/limits.d/scylla.conf -Dt "$retc"/security/limits.d
    install -m644 dist/common/scylla.d/*.conf -Dt "$retc"/scylla.d

    install -d -m755 "$retc"/scylla "$rusr/lib/systemd/system" "$rusr/bin" "$rprefix/bin" "$rprefix/libexec" "$rprefix/libreloc" "$rprefix/scripts"
    install -m644 build/*.service -Dt "$rusr"/lib/systemd/system
    install -m644 dist/common/systemd/*.service -Dt "$rusr"/lib/systemd/system
    install -m644 dist/common/systemd/*.slice -Dt "$rusr"/lib/systemd/system
    install -m644 dist/common/systemd/*.timer -Dt "$rusr"/lib/systemd/system
    install -m755 seastar/scripts/seastar-cpu-map.sh -Dt "$rprefix"/scripts
    install -m755 seastar/dpdk/usertools/dpdk-devbind.py -Dt "$rprefix"/scripts
    install -m755 libreloc/* -Dt "$rprefix/libreloc"
    # some files in libexec are symlinks, which "install" dereferences
    # use cp -P for the symlinks instead.
    install -m755 libexec/* -Dt "$rprefix/libexec"
    for bin in libexec/*; do
	adjust_bin "${bin#libexec/}"
    done
    ln -srf "$rprefix/bin/scylla" "$rusr/bin/scylla"
    ln -srf "$rprefix/bin/iotune" "$rusr/bin/iotune"

    # XXX: since housekeeping.cfg is mistakenly belongs to different package
    # in .rpm/.deb, we need this workaround to make package upgradable
    if $is_debian && $housekeeping; then
        install -m644 conf/housekeeping.cfg -Dt "$retc"/scylla.d
    fi
    install -d -m755 "$rdoc"/scylla
    install -m644 README.md -Dt "$rdoc"/scylla/
    install -m644 README-DPDK.md -Dt "$rdoc"/scylla
    install -m644 NOTICE.txt -Dt "$rdoc"/scylla/
    install -m644 ORIGIN -Dt "$rdoc"/scylla/
    install -d -m755 -d "$rdoc"/scylla/licenses/
    install -m644 licenses/* -Dt "$rdoc"/scylla/licenses/
    install -m755 -d "$root"/var/lib/scylla/
    install -m755 -d "$root"/var/lib/scylla/data
    install -m755 -d "$root"/var/lib/scylla/commitlog
    install -m755 -d "$root"/var/lib/scylla/hints
    install -m755 -d "$root"/var/lib/scylla/view_hints
    install -m755 -d "$root"/var/lib/scylla/coredump
    install -m755 -d "$root"/var/lib/scylla-housekeeping
    install -m755 -d "$rprefix"/swagger-ui
    cp -r swagger-ui/dist "$rprefix"/swagger-ui
    install -d -m755 -d "$rprefix"/api
    cp -r api/api-doc "$rprefix"/api
    install -d -m755 -d "$rprefix"/scyllatop
    cp -r tools/scyllatop/* "$rprefix"/scyllatop
    install -d -m755 -d "$rprefix"/scripts
    cp -r dist/common/scripts/* "$rprefix"/scripts
    ln -srf "$rprefix/scyllatop/scyllatop.py" "$rusr/bin/scyllatop"

    SBINFILES=$(cd dist/common/scripts/; ls scylla_*setup node_exporter_install node_health_check scylla_ec2_check scylla_kernel_check)
    install -d "$rusr"/sbin
    for i in $SBINFILES; do
        ln -srf "$rprefix/scripts/$i" "$rusr/sbin/$i"
    done

    install -m755 scylla-gdb.py -Dt "$rprefix"/scripts/

    PYSCRIPTS=$(find dist/common/scripts/ -maxdepth 1 -type f -exec grep -Pls '\A#!/usr/bin/env python3' {} +)
    for i in $PYSCRIPTS; do
        ./relocate_python_scripts.py \
                --installroot $rprefix/scripts/ --with-python3 "$root/$python3" $i
    done
    ./relocate_python_scripts.py \
                --installroot $rprefix/scripts/ --with-python3 "$root/$python3" \
                seastar/scripts/perftune.py seastar/scripts/seastar-addr2line seastar/scripts/perftune.py

    ./relocate_python_scripts.py \
                --installroot $rprefix/scyllatop/ --with-python3 "$root/$python3" \
                tools/scyllatop/scyllatop.py
fi
