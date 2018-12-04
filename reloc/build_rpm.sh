#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --rebuild-dep --target centos7 --reloc-pkg build/release/scylla-package.tar.gz"
    echo "  --dist  create a public distribution rpm"
    echo "  --target target distribution in mock cfg name"
    echo "  --xtrace print command traces before executing command"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}
RELOC_PKG=$(readlink -f build/release/scylla-package.tar.gz)
OPTS=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
            OPTS="$OPTS $1"
            shift 1
            ;;
        "--target")
            OPTS="$OPTS $1 $2"
            shift 2
            ;;
        "--xtrace")
            set -o xtrace
            OPTS="$OPTS $1"
            shift 1
            ;;
        "--reloc-pkg")
            OPTS="$OPTS $1 $(readlink -f $2)"
            RELOC_PKG=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [[ ! $OPTS =~ --reloc-pkg ]]; then
    OPTS="$OPTS --reloc-pkg $RELOC_PKG"
fi
mkdir -p build/redhat/scylla-package
tar -C build/redhat/scylla-package -xpf $RELOC_PKG SCYLLA-*-FILE dist/redhat
cd build/redhat/scylla-package
exec ./dist/redhat/build_rpm.sh $OPTS
