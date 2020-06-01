#!/bin/bash -e

print_usage() {
    echo "build_deb.sh -target <codename> --dist --reloc-pkg build/release/scylla-package.tar.gz"
    echo "  --dist  create a public distribution package"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}

DIST="false"
RELOC_PKG=build/release/scylla-package.tar.gz
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
            DIST="true"
            shift 1
            ;;
        "--reloc-pkg")
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
mkdir -p build/debian/scylla-package
tar -C build/debian/scylla-package -xpf $RELOC_PKG
cd build/debian/scylla-package

PRODUCT=$(cat SCYLLA-PRODUCT-FILE)
SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)

ln -fv $RELOC_PKG ../$PRODUCT-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz

if $DIST; then
    export DEB_BUILD_OPTIONS="housekeeping"
fi
debuild -rfakeroot -us -uc
