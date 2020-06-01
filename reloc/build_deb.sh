#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh -target <codename> --dist --rebuild-dep --reloc-pkg build/release/scylla-package.tar.gz"
    echo "  --dist  create a public distribution package"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --builddir specify Debian package build path"
    exit 1
}

DIST="false"
RELOC_PKG=build/release/scylla-package.tar.gz
BUILDDIR=build/debian
OPTS=""
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
rm -rf $BUILDDIR
mkdir -p $BUILDDIR/scylla-package
tar -C $BUILDDIR/scylla-package -xpf $RELOC_PKG
cd $BUILDDIR/scylla-package

PRODUCT=$(cat scylla/SCYLLA-PRODUCT-FILE)
SCYLLA_VERSION=$(cat scylla/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat scylla/SCYLLA-RELEASE-FILE)

ln -fv $RELOC_PKG ../$PRODUCT-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz

if $DIST; then
    export DEB_BUILD_OPTIONS="housekeeping"
fi

mv scylla/debian debian
debuild -rfakeroot -us -uc
