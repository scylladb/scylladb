#!/usr/bin/bash
#
# Copyright (C) 2019 ScyllaDB
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
print_usage() {
    echo "${0} --reloc-pkg build/release/scylla-python3-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}

RELOC_PKG=
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            RELOC_PKG="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}


if [ ! -e SCYLLA-RELOCATABLE-FILE ]; then
    echo "do not directly execute build_rpm.sh, use reloc/build_rpm.sh instead."
    exit 1
fi

if [ -z "$RELOC_PKG" ]; then
    print_usage
    exit 1
fi
if [ ! -f "$RELOC_PKG" ]; then
    echo "$RELOC_PKG is not found."
    exit 1
fi

if [ ! -f /usr/bin/rpmbuild ]; then
    pkg_install rpm-build
fi
if [ ! -f /usr/bin/git ]; then
    pkg_install git
fi
if [ ! -f /usr/bin/pystache ]; then
    if is_redhat_variant; then
        sudo yum install -y python2-pystache || sudo yum install -y pystache
    elif is_debian_variant; then
        sudo apt-get install -y python2-pystache
    fi
fi

RELOC_PKG_BASENAME=$(basename "$RELOC_PKG")
SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)

RPMBUILD=$(readlink -f ../)
mkdir -p "$RPMBUILD"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

ln -fv "$RELOC_PKG" "$RPMBUILD"/SOURCES/
pystache dist/redhat/python3/python.spec.mustache "{ \"version\": \"${SCYLLA_VERSION}\", \"release\": \"${SCYLLA_RELEASE}\", \"reloc_pkg\": \"${RELOC_PKG_BASENAME}\", \"name\": \"scylla-python3\", \"target\": \"/opt/scylladb/python3\" }" > "$RPMBUILD"/SPECS/python.spec
rpmbuild --nodebuginfo -ba --define "_build_id_links none" --define "_topdir ${RPMBUILD}" --define "dist .el7" "$RPMBUILD"/SPECS/python.spec
