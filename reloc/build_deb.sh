#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh -target <codename> --dist --rebuild-dep --reloc-pkg build/release/scylla-package.tar.gz"
    echo "  --dist  create a public distribution package"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}

RELOC_PKG=build/release/scylla-package.tar.gz
OPTS=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
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

if [ ! -e $RELOC_PKG ]; then
    echo "$RELOC_PKG does not exist."
    echo "Run ./reloc/build_reloc.sh first."
    exit 1
fi
RELOC_PKG=$(readlink -f $RELOC_PKG)
if [[ ! $OPTS =~ --reloc-pkg ]]; then
    OPTS="$OPTS --reloc-pkg $RELOC_PKG"
fi
rm -rf build/debian
mkdir -p build/debian/scylla-package
tar -C build/debian/scylla-package -xpf $RELOC_PKG
cd build/debian/scylla-package
exec ./scylla/dist/debian/build_deb.sh $OPTS
