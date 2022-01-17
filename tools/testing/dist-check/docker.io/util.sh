#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
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
