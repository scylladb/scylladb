#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --reloc-pkg build/release/scylla-python3-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --builddir specify rpmbuild directory"
    exit 1
}
RELOC_PKG=build/release/scylla-python3-package.tar.gz
BUILDDIR=build/redhat
OPTS=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            OPTS="$OPTS $1 $(readlink -f $2)"
            RELOC_PKG=$2
            shift 2
            ;;
        "--builddir")
            BUILDDIR="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ ! -e $RELOC_PKG ]; then
    echo "$RELOC_PKG does not exist."
    echo "Run ./reloc/python3/build_reloc.sh first."
    exit 1
fi
RELOC_PKG=$(readlink -f $RELOC_PKG)
if [[ ! $OPTS =~ --reloc-pkg ]]; then
    OPTS="$OPTS --reloc-pkg $RELOC_PKG"
fi
mkdir -p $BUILDDIR/scylla-python3
tar -C $BUILDDIR -xpf $RELOC_PKG scylla-python3/SCYLLA-RELOCATABLE-FILE scylla-python3/SCYLLA-RELEASE-FILE scylla-python3/SCYLLA-VERSION-FILE scylla-python3/SCYLLA-PRODUCT-FILE scylla-python3/dist/redhat/python3
cd $BUILDDIR/scylla-python3
exec ./dist/redhat/python3/build_rpm.sh $OPTS
