#
# Copyright (C) 2020-present ScyllaDB
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

PROGRAM=$(basename $0)

print_usage() {
    echo "Usage: $PROGRAM [OPTION]..."
    echo ""
    echo "  --mode MODE             Specify the Scylla server build mode to install."
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        "--mode")
            MODE=$2
            shift 2
            ;;
        "--help")
            print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ -z "$MODE" ]; then
  print_usage
fi

SCYLLA_PRODUCT=$(cat build/SCYLLA-PRODUCT-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
SCYLLA_PYTHON_RELEASE=$(cat build/redhat/scylla-python3/SCYLLA-RELEASE-FILE)
SCYLLA_JMX_PRODUCT=$(cat scylla-jmx/build/SCYLLA-PRODUCT-FILE)
SCYLLA_JMX_RELEASE=$(cat scylla-jmx/build/SCYLLA-RELEASE-FILE)
SCYLLA_TOOLS_PRODUCT=$(cat scylla-tools/build/SCYLLA-PRODUCT-FILE)
SCYLLA_TOOLS_RELEASE=$(cat scylla-tools/build/SCYLLA-RELEASE-FILE)

SCYLLA_RPMS=(
    build/dist/$MODE/redhat/RPMS/x86_64/$SCYLLA_PRODUCT-*$SCYLLA_RELEASE*.rpm
    build/redhat/RPMS/x86_64/$SCYLLA_PRODUCT-python3-*$SCYLLA_PYTHON_RELEASE*.rpm
    scylla-jmx/build/redhat/RPMS/noarch/$SCYLLA_JMX_PRODUCT*$SCYLLA_JMX_RELEASE*.rpm
    scylla-tools/build/redhat/RPMS/noarch/$SCYLLA_TOOLS_PRODUCT*$SCYLLA_TOOLS_RELEASE*.rpm
)

source /etc/os-release
