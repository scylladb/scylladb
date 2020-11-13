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
    echo "$RELOC_PKG does not exist. Please build it with 'ninja' before running this script."
    exit 1
fi
RELOC_PKG=$(readlink -f $RELOC_PKG)
rm -rf $BUILDDIR
mkdir -p $BUILDDIR/scylla-package
tar -C $BUILDDIR/scylla-package -xpf $RELOC_PKG
cd $BUILDDIR/scylla-package

if $DIST; then
    export DEB_BUILD_OPTIONS="housekeeping"
fi

mv scylla/debian debian

PKG_NAME=$(dpkg-parsechangelog --show-field Source)
# XXX: Drop revision number from version string.
#      Since it always '1', this should be okay for now.
PKG_VERSION=$(dpkg-parsechangelog --show-field Version |sed -e 's/-1$//')
ln -fv $RELOC_PKG ../"$PKG_NAME"_"$PKG_VERSION".orig.tar.gz
debuild -rfakeroot -us -uc
