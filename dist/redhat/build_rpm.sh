#!/bin/bash -e

PRODUCT=scylla

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --rebuild-dep --jobs 2 --target epel-7-$(uname -m)"
    echo "  --jobs  specify number of jobs"
    echo "  --dist  create a public distribution rpm"
    echo "  --target target distribution in mock cfg name"
    echo "  --rebuild-dep  ignored (for compatibility with previous versions)"
    echo "  --xtrace print command traces before executing command"
    exit 1
}
JOBS=0
DIST=false
TARGET=
while [ $# -gt 0 ]; do
    case "$1" in
        "--jobs")
            JOBS=$2
            shift 2
            ;;
        "--dist")
            DIST=true
            shift 1
            ;;
        "--target")
            TARGET=$2
            shift 2
            ;;
        "--rebuild-dep")
            shift 1
            ;;
        "--xtrace")
            set -o xtrace
            shift 1
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}


if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla dir"
    exit 1
fi

if [ -z "$TARGET" ]; then
    if [ "$ID" = "centos" -o "$ID" = "rhel" ] && [ "$VERSION_ID" = "7" ]; then
        TARGET=epel-7-$(uname -m)
    elif [ "$ID" = "fedora" ]; then
        TARGET=$ID-$VERSION_ID-$(uname -m)
    else
        echo "Please specify target"
        exit 1
    fi
fi

if [ ! -f /usr/bin/mock ]; then
    pkg_install mock
fi
if [ ! -f /usr/bin/git ]; then
    pkg_install git
fi
if [ ! -f /usr/bin/wget ]; then
    pkg_install wget
fi
if [ ! -f /usr/bin/yum-builddep ]; then
    pkg_install yum-utils
fi
if [ ! -f /usr/bin/pystache ]; then
    if is_redhat_variant; then
        sudo yum install -y python2-pystache || sudo yum install -y pystache
    elif is_debian_variant; then
        sudo apt-get install -y python2-pystache
    fi
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
echo $VERSION >version
./scripts/git-archive-all --extra version --force-submodules --prefix $PRODUCT-$SCYLLA_VERSION build/$PRODUCT-$VERSION.tar
rm -f version

pystache dist/redhat/scylla.spec.mustache "{ \"version\": \"$SCYLLA_VERSION\", \"release\": \"$SCYLLA_RELEASE\", \"housekeeping\": $DIST, \"product\": \"$PRODUCT\", \"$PRODUCT\": true }" > build/scylla.spec

# mock generates files owned by root, fix this up
fix_ownership() {
    sudo chown "$(id -u):$(id -g)" -R "$@"
}

if [ $JOBS -gt 0 ]; then
    RPM_JOBS_OPTS=(--define="_smp_mflags -j$JOBS")
fi
sudo mock --rootdir=`pwd`/build/mock --buildsrpm --root=$TARGET --resultdir=`pwd`/build/srpms --spec=build/scylla.spec --sources=build/$PRODUCT-$VERSION.tar $SRPM_OPTS "${RPM_JOBS_OPTS[@]}"
fix_ownership build/srpms
if [[ "$TARGET" =~ ^epel-7- ]]; then
    TARGET=scylla-$TARGET
    RPM_OPTS="$RPM_OPTS --configdir=dist/redhat/mock"
fi
sudo mock --rootdir=`pwd`/build/mock --rebuild --root=$TARGET --resultdir=`pwd`/build/rpms $RPM_OPTS "${RPM_JOBS_OPTS[@]}" build/srpms/$PRODUCT-$VERSION*.src.rpm
fix_ownership build/rpms
