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
    echo "${0} --reloc-pkg build/release/scylla-python3.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --rpmbuild specify directory to use for building rpms"
    exit 1
}

RELOC_PKG=
RPMBUILD=
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            RELOC_PKG="$2"
            shift 2
            ;;
        "--rpmbuild")
            RPMBUILD="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ -z "$RELOC_PKG" ]; then
    print_usage
    exit 1
fi

if [ -z "$RPMBUILD" ]; then
    print_usage
    exit 1
fi

if [ ! -f "$RELOC_PKG" ]; then
    echo "${RELOC_PKG} not found."
    exit 1
fi
RELOC_PKG_BASENAME=$(basename "$RELOC_PKG")

RPMBUILD=$(readlink -f "$RPMBUILD")
SPEC=$(dirname $(readlink -f "$0"))

mkdir -p "$RPMBUILD"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
PYVER=$(python3 -V | cut -d' ' -f2)

ln -fv "$RELOC_PKG" "$RPMBUILD"/SOURCES/
pystache "$SPEC"/python.spec.mustache "{ \"version\": \"${PYVER}\", \"reloc_pkg\": \"${RELOC_PKG_BASENAME}\", \"name\": \"scylla-python3\", \"target\": \"/opt/scylladb/python3\" }" > "$RPMBUILD"/SPECS/python.spec
rpmbuild --nodebuginfo -ba --define "_build_id_links none" --define "_topdir ${RPMBUILD}" --define "dist .el7" "$RPMBUILD"/SPECS/python.spec
