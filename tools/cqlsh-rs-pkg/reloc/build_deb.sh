#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh --reloc-pkg build/scylla-cqlsh-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --builddir specify Debian package build path"
    exit 1
}

RELOC_PKG=build/scylla-cqlsh-package.tar.gz
BUILDDIR=build/debian
while [ $# -gt 0 ]; do
    case "$1" in
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

RELOC_PKG=$(readlink -f $RELOC_PKG)
rm -rf "$BUILDDIR"/scylla-package "$BUILDDIR"/scylla-package.orig "$BUILDDIR"/debian
mkdir -p "$BUILDDIR"/scylla-package
tar -C "$BUILDDIR"/scylla-package -xpf $RELOC_PKG
cd "$BUILDDIR"/scylla-package

mv scylla-cqlsh/debian debian
PKG_NAME=$(dpkg-parsechangelog --show-field Source)
# XXX: Drop revision number from version string.
#      Since it always '1', this should be okay for now.
PKG_VERSION=$(dpkg-parsechangelog --show-field Version |sed -e 's/-1$//')
ln -fv $RELOC_PKG ../"$PKG_NAME"_"$PKG_VERSION".orig.tar.gz
debuild -rfakeroot -us -uc
