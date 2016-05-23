#!/bin/bash -e

print_usage() {
    echo "build_deb.sh --rebuild-dep"
    echo "  --rebuild-dep  rebuild dependency packages"
    exit 1
}
REBUILD=0
while [ $# -gt 0 ]; do
    case "$1" in
        "--rebuild-dep")
            REBUILD=1
            shift 1
            ;;
        *)
            print_usage
            ;;
    esac
done


if [ ! -e dist/ubuntu/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

if [ -e debian ] || [ -e build/release ]; then
    rm -rf debian build
    mkdir build
fi
sudo apt-get -y update
if [ ! -f /usr/bin/git ]; then
    sudo apt-get -y install git
fi
if [ ! -f /usr/bin/mk-build-deps ]; then
    sudo apt-get -y install devscripts
fi
if [ ! -f /usr/bin/equivs-build ]; then
    sudo apt-get -y install equivs
fi
if [ ! -f /usr/bin/add-apt-repository ]; then
    sudo apt-get -y install software-properties-common
fi
if [ ! -f /usr/bin/wget ]; then
    sudo apt-get -y install wget
fi

DISTRIBUTION=`lsb_release -i|awk '{print $3}'`
RELEASE=`lsb_release -r|awk '{print $2}'`
CODENAME=`lsb_release -c|awk '{print $2}'`
if [ `grep -c $RELEASE dist/ubuntu/supported_release` -lt 1 ]; then
    echo "Unsupported release: $RELEASE"
    echo "Pless any key to continue..."
    read input
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-server ../scylla-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/ubuntu/debian debian
cp dist/common/sysconfig/scylla-server debian/scylla-server.default
cp dist/ubuntu/changelog.in debian/changelog
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog
sed -i -e "s/@@CODENAME@@/$CODENAME/g" debian/changelog
cp dist/ubuntu/rules.in debian/rules
cp dist/ubuntu/control.in debian/control
if [ "$RELEASE" = "14.04" ]; then
    sed -i -e "s/@@DH_INSTALLINIT@@/--upstart-only/g" debian/rules
    sed -i -e "s/@@COMPILER@@/g++-5/g" debian/rules
    sed -i -e "s/@@BUILD_DEPENDS@@/g++-5/g" debian/control
else
    sed -i -e "s/@@DH_INSTALLINIT@@//g" debian/rules
    sed -i -e "s/@@COMPILER@@/g++/g" debian/rules
    sed -i -e "s/@@BUILD_DEPENDS@@/libsystemd-dev, g++/g" debian/control
fi

if [ "$DISTRIBUTION" = "Ubuntu" ]; then
    sed -i -e "s/@@DEPENDS@@/hugepages, /g" debian/control
else
    sed -i -e "s/@@DEPENDS@@//g" debian/control
fi

cp dist/common/systemd/scylla-server.service.in debian/scylla-server.service
sed -i -e "s#@@SYSCONFDIR@@#/etc/default#g" debian/scylla-server.service

if [ "$RELEASE" = "14.04" ] && [ $REBUILD -eq 0 ]; then
    if [ ! -f /etc/apt/sources.list.d/scylla-3rdparty-trusty.list ]; then
        cd /etc/apt/sources.list.d
        sudo wget https://s3.amazonaws.com/downloads.scylladb.com/deb/3rdparty/ubuntu/scylla-3rdparty-trusty.list
        cd -
    fi
    sudo apt-get -y update
    sudo apt-get -y --allow-unauthenticated install antlr3 antlr3-c++-dev libthrift-dev libthrift0 thrift-compiler
else
    ./dist/ubuntu/dep/build_dependency.sh
fi

if [ "$RELEASE" = "14.04" ]; then
    sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
    sudo apt-get -y update
elif [ "$DISTRIBUTION" = "Ubuntu" ]; then
    sudo apt-get -y install g++-5
else
    sudo apt-get install g++
fi

echo Y | sudo mk-build-deps -i -r
debuild -r fakeroot -us -uc
