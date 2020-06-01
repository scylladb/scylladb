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
DIST=0
RELOC_PKG=build/release/scylla-package.tar.gz
BUILDDIR=build/redhat
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
            DIST=1
            shift 1
            ;;
        "--target") # This is obsolete, but I keep this in order not to break people's scripts.
            shift 2
            ;;
        "--xtrace")
            set -o xtrace
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
RPMBUILD=$(readlink -f $BUILDDIR)
mkdir -p $BUILDDIR/
tar -C $BUILDDIR/ -xpf $RELOC_PKG scylla/SCYLLA-RELOCATABLE-FILE scylla/SCYLLA-RELEASE-FILE scylla/SCYLLA-VERSION-FILE scylla/SCYLLA-PRODUCT-FILE scylla/dist/redhat
cd $BUILDDIR/scylla

RELOC_PKG_BASENAME=$(basename $RELOC_PKG)
SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)
PRODUCT=$(cat SCYLLA-PRODUCT-FILE)

mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

xz_thread_param=
if xz --help | grep -q thread; then
    # use as many threads as there are CPUs
    xz_thread_param="T$(nproc)"
fi

rpm_payload_opts=(--define "_binary_payload w2${xz_thread_param}.xzdio")

ln -fv $RELOC_PKG $RPMBUILD/SOURCES/

parameters=(
    -D"version $SCYLLA_VERSION"
    -D"release $SCYLLA_RELEASE"
    -D"housekeeping $DIST"
    -D"product $PRODUCT"
    -D"${PRODUCT/-/_} 1"
    -D"reloc_pkg $RELOC_PKG_BASENAME"
)

ln -fv dist/redhat/scylla.spec $RPMBUILD/SPECS/
rpmbuild "${parameters[@]}" -ba "${rpm_payload_opts[@]}" --define "_topdir $RPMBUILD" $RPMBUILD/SPECS/scylla.spec
