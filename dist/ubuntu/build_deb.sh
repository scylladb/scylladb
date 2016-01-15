#!/bin/sh -e

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

RELEASE=`lsb_release -r|awk '{print $2}'`
CODENAME=`lsb_release -c|awk '{print $2}'`
if [ `grep -c $RELEASE dist/ubuntu/supported_release` -lt 1 ]; then
    echo "Unsupported release: $RELEASE"
    echo "Pless any key to continue..."
    read input
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-server ../scylla-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/ubuntu/debian debian
cp dist/common/sysconfig/scylla-server debian/scylla-server.default
cp dist/ubuntu/changelog.in debian/changelog
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog
sed -i -e "s/@@CODENAME@@/$CODENAME/g" debian/changelog

./dist/ubuntu/dep/build_dependency.sh

if [ "$RELEASE" != "15.10" ]; then
    sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
    sudo apt-get -y update
fi
sudo apt-get -y install g++-4.9
echo Y | sudo mk-build-deps -i -r

debuild -r fakeroot -us -uc
