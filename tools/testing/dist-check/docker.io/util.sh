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
SCYLLA_PYTHON_RELEASE=$(cat tools/python3/build/SCYLLA-RELEASE-FILE)
SCYLLA_JMX_PRODUCT=$(cat tools/jmx/build/SCYLLA-PRODUCT-FILE)
SCYLLA_JMX_RELEASE=$(cat tools/jmx/build/SCYLLA-RELEASE-FILE)
SCYLLA_TOOLS_PRODUCT=$(cat tools/java/build/SCYLLA-PRODUCT-FILE)
SCYLLA_TOOLS_RELEASE=$(cat tools/java/build/SCYLLA-RELEASE-FILE)
SCYLLA_CQLSH_PRODUCT=$(cat tools/cqlsh/build/SCYLLA-PRODUCT-FILE)
SCYLLA_CQLSH_RELEASE=$(cat tools/cqlsh/build/SCYLLA-RELEASE-FILE)

case $MODE in
    debug)
        config=Debug
        ;;
    release)
        config=RelWithDebInfo
        ;;
    dev)
        config=Dev
        ;;
    sanitize)
        config=Sanitize
        ;;
    coverage)
        config=Coverage
        ;;
    *)
        echo "unknown mode: $MODE"
        exit 1
        ;;
esac

if [ ! -e build/dist/$MODE ]; then
  # fallback to cmake directory
  MODE=$config
fi

SCYLLA_RPMS=(
    build/dist/$MODE/redhat/RPMS/x86_64/$SCYLLA_PRODUCT-*$SCYLLA_RELEASE*.rpm
    tools/python3/build/redhat/RPMS/x86_64/$SCYLLA_PRODUCT-python3-*$SCYLLA_PYTHON_RELEASE*.rpm
    tools/jmx/build/redhat/RPMS/noarch/$SCYLLA_JMX_PRODUCT*$SCYLLA_JMX_RELEASE*.rpm
    tools/java/build/redhat/RPMS/noarch/$SCYLLA_TOOLS_PRODUCT*$SCYLLA_TOOLS_RELEASE*.rpm
    tools/cqlsh/build/redhat/RPMS/x86_64/$SCYLLA_CQLSH_PRODUCT-cqlsh-*$SCYLLA_CQLSH_RELEASE*.rpm
)

source /etc/os-release
