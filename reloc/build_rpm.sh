#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --rebuild-dep --target centos7 --reloc-pkg build/release/scylla-package.tar.gz"
    echo "  --dist  create a public distribution rpm"
    echo "  --target target distribution in mock cfg name"
    echo "  --xtrace print command traces before executing command"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --builddir specify rpmbuild directory"
    exit 1
}
RELOC_PKG=build/release/scylla-package.tar.gz
BUILDDIR=build/redhat
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
    echo "Run ./reloc/build_reloc.sh first."
    exit 1
fi
RELOC_PKG=$(readlink -f $RELOC_PKG)
if [[ ! $OPTS =~ --reloc-pkg ]]; then
    OPTS="$OPTS --reloc-pkg $RELOC_PKG"
fi
mkdir -p $BUILDDIR/
tar -C $BUILDDIR/ -xpf $RELOC_PKG scylla/SCYLLA-RELOCATABLE-FILE scylla/SCYLLA-RELEASE-FILE scylla/SCYLLA-VERSION-FILE scylla/SCYLLA-PRODUCT-FILE scylla/dist/redhat
cd $BUILDDIR/scylla
echo "Running './dist/redhat/build_rpm.sh $OPTS' under $BUILDDIR/scylla directory"
exec ./dist/redhat/build_rpm.sh $OPTS
