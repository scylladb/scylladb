#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh -target <codename> --dist --rebuild-dep --jobs 2"
    echo "  --target target distribution codename"
    echo "  --dist  create a public distribution package"
    echo "  --no-clean  don't rebuild pbuilder tgz"
    echo "  --jobs  specify number of jobs"
    exit 1
}
install_deps() {
    echo Y | sudo mk-build-deps
    DEB_FILE=`ls *-build-deps*.deb`
    sudo gdebi -n $DEB_FILE
    sudo rm -f $DEB_FILE
    sudo dpkg -P ${DEB_FILE%%_*.deb}
}

DIST="false"
TARGET=
NO_CLEAN=0
JOBS=0
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
            DIST="true"
            shift 1
            ;;
        "--target")
            TARGET=$2
            shift 2
            ;;
        "--no-clean")
            NO_CLEAN=1
            shift 1
            ;;
        "--jobs")
            JOBS=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
is_debian_variant() {
    [ -f /etc/debian_version ]
}


pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    elif is_debian_variant; then
        sudo apt-get install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}

if [ ! -e dist/debian/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi
if [ "$(arch)" != "x86_64" ]; then
    echo "Unsupported architecture: $(arch)"
    exit 1
fi

if [ -e debian ] || [ -e build/release ]; then
    sudo rm -rf debian build
    mkdir build
fi
if is_debian_variant; then
    sudo apt-get -y update
fi
# this hack is needed since some environment installs 'git-core' package, it's
# subset of the git command and doesn't works for our git-archive-all script.
if is_redhat_variant && [ ! -f /usr/libexec/git-core/git-submodule ]; then
    sudo yum install -y git
fi
if [ ! -f /usr/bin/git ]; then
    pkg_install git
fi
if [ ! -f /usr/bin/python ]; then
    pkg_install python
fi
if [ ! -f /usr/sbin/pbuilder ]; then
    pkg_install pbuilder
fi
if [ ! -f /usr/bin/dh_testdir ]; then
    pkg_install debhelper
fi
if [ ! -f /usr/bin/pystache ]; then
    if is_redhat_variant; then
        sudo yum install -y python2-pystache || sudo yum install -y pystache
    elif is_debian_variant; then
        sudo apt-get install -y python-pystache
    fi
fi

if [ -z "$TARGET" ]; then
    if is_debian_variant; then
        if [ ! -f /usr/bin/lsb_release ]; then
            pkg_install lsb-release
        fi
        TARGET=`lsb_release -c|awk '{print $2}'`
    else
        echo "Please specify target"
        exit 1
    fi
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-server ../scylla-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/debian/debian debian
cp dist/common/sysconfig/scylla-server debian/scylla-server.default
if [ "$TARGET" = "jessie" ] || [ "$TARGET" = "stretch" ]; then
    REVISION="1~$TARGET"
elif [ "$TARGET" = "trusty" ]; then
    cp dist/debian/scylla-server.cron.d debian/
    REVISION="0ubuntu1~$TARGET"
elif [ "$TARGET" = "xenial" ] || [ "$TARGET" = "bionic" ]; then
    REVISION="0ubuntu1~$TARGET"
else
   echo "Unknown distribution: $TARGET"
fi
MUSTACHE_DIST="\"debian\": true, \"$TARGET\": true"
pystache dist/debian/changelog.mustache "{ \"version\": \"$SCYLLA_VERSION\", \"release\": \"$SCYLLA_RELEASE\", \"revision\": \"$REVISION\", \"codename\": \"$TARGET\" }" > debian/changelog
pystache dist/debian/rules.mustache "{ $MUSTACHE_DIST }" > debian/rules
pystache dist/debian/control.mustache "{ $MUSTACHE_DIST }" > debian/control
pystache dist/debian/scylla-server.install.mustache "{ $MUSTACHE_DIST, \"dist\": $DIST }" > debian/scylla-server.install
pystache dist/debian/scylla-conf.preinst.mustache "{ \"version\": \"$SCYLLA_VERSION\" }" > debian/scylla-conf.preinst
chmod a+rx debian/rules

if [ "$TARGET" != "trusty" ]; then
    pystache dist/common/systemd/scylla-server.service.mustache "{ $MUSTACHE_DIST }" > debian/scylla-server.service
    pystache dist/common/systemd/scylla-housekeeping-daily.service.mustache "{ $MUSTACHE_DIST }" > debian/scylla-housekeeping-daily.service
    pystache dist/common/systemd/scylla-housekeeping-restart.service.mustache "{ $MUSTACHE_DIST }" > debian/scylla-housekeeping-restart.service
    cp dist/common/systemd/scylla-fstrim.service debian/scylla-server.scylla-fstrim.service
    cp dist/common/systemd/node-exporter.service debian/scylla-server.node-exporter.service
fi

if [ $NO_CLEAN -eq 0 ]; then
    sudo rm -fv /var/cache/pbuilder/scylla-server-$TARGET.tgz
    sudo DIST=$TARGET /usr/sbin/pbuilder clean --configfile ./dist/debian/pbuilderrc
    sudo DIST=$TARGET /usr/sbin/pbuilder create --configfile ./dist/debian/pbuilderrc --allow-untrusted
fi
if [ $JOBS -ne 0 ]; then
    DEB_BUILD_OPTIONS="parallel=$JOBS"
fi
sudo DIST=$TARGET /usr/sbin/pbuilder update --configfile ./dist/debian/pbuilderrc --allow-untrusted
if [ "$TARGET" = "trusty" ] || [ "$TARGET" = "xenial" ] || [ "$TARGET" = "yakkety" ] || [ "$TARGET" = "zesty" ] || [ "$TARGET" = "artful" ] || [ "$TARGET" = "bionic" ]; then
    sudo DIST=$TARGET /usr/sbin/pbuilder execute --configfile ./dist/debian/pbuilderrc --save-after-exec dist/debian/ubuntu_enable_ppa.sh
elif [ "$TARGET" = "jessie" ] || [ "$TARGET" = "stretch" ]; then
    sudo DIST=$TARGET /usr/sbin/pbuilder execute --configfile ./dist/debian/pbuilderrc --save-after-exec dist/debian/debian_install_gpgkey.sh
fi
sudo -E DIST=$TARGET DEB_BUILD_OPTIONS=$DEB_BUILD_OPTIONS pdebuild --configfile ./dist/debian/pbuilderrc --buildresult build/debs
