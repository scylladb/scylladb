#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --rebuild-dep --jobs 2 --target epel-7-x86_64"
    echo "  --jobs  specify number of jobs"
    echo "  --dist  create a public distribution rpm"
    echo "  --target target distribution in mock cfg name"
    echo "  --rebuild-dep  ignored (for compatibility with previous versions)"
    exit 1
}
JOBS=0
DIST=0
TARGET=
while [ $# -gt 0 ]; do
    case "$1" in
        "--jobs")
            JOBS=$2
            shift 2
            ;;
        "--dist")
            DIST=1
            shift 1
            ;;
        "--target")
            TARGET=$2
            shift 2
            ;;
        "--rebuild-dep")
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

if [ "$(arch)" != "x86_64" ]; then
    echo "Unsupported architecture: $(arch)"
    exit 1
fi
if [ -z "$TARGET" ]; then
    if [ "$ID" = "centos" -o "$ID" = "rhel" ] && [ "$VERSION_ID" = "7" ]; then
        TARGET=epel-7-x86_64
    elif [ "$ID" = "fedora" ]; then
        TARGET=$ID-$VERSION_ID-x86_64
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

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
echo $VERSION >version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-$SCYLLA_VERSION build/scylla-$VERSION.tar
rm -f version
cp dist/redhat/scylla.spec.in build/scylla.spec
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" build/scylla.spec
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" build/scylla.spec

if [ $DIST -gt 0 ]; then
  sed -i -e "s/@@HOUSEKEEPING_CONF@@/true/g" build/scylla.spec
else
  sed -i -e "s/@@HOUSEKEEPING_CONF@@/false/g" build/scylla.spec
fi


if [ $JOBS -gt 0 ]; then
    RPM_JOBS_OPTS=(--define="_smp_mflags -j$JOBS")
fi
sudo mock --buildsrpm --root=$TARGET --resultdir=`pwd`/build/srpms --spec=build/scylla.spec --sources=build/scylla-$VERSION.tar $SRPM_OPTS "${RPM_JOBS_OPTS[@]}"
if [ "$TARGET" = "epel-7-x86_64" ]; then
    TARGET=scylla-$TARGET
    RPM_OPTS="$RPM_OPTS --configdir=dist/redhat/mock"
fi
sudo mock --rebuild --root=$TARGET --resultdir=`pwd`/build/rpms $RPM_OPTS "${RPM_JOBS_OPTS[@]}" build/srpms/scylla-$VERSION*.src.rpm
