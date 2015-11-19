#!/bin/sh -e

if [ ! -e dist/ubuntu/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

if [ -e debian ] || [ -e build/release ]; then
    rm -rf debian build
    mkdir build
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
if [ "$SCYLLA_VERSION" = "development" ]; then
	SCYLLA_VERSION=0development
fi
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-server ../scylla-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/ubuntu/debian debian
cp dist/common/sysconfig/scylla-server debian/scylla-server.default
cp dist/ubuntu/changelog.in debian/changelog
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog
sed -i -e "s/@@CODENAME@@/$CODENAME/g" debian/changelog

sudo apt-get -y update

./dist/ubuntu/dep/build_dependency.sh

DEP="libyaml-cpp-dev liblz4-dev libsnappy-dev libcrypto++-dev libjsoncpp-dev libaio-dev ragel ninja-build git liblz4-1 libaio1 hugepages software-properties-common"

if [ "$RELEASE" = "14.04" ]; then
    DEP="$DEP libboost1.55-dev libboost-program-options1.55.0 libboost-program-options1.55-dev libboost-system1.55.0 libboost-system1.55-dev libboost-thread1.55.0 libboost-thread1.55-dev libboost-test1.55.0 libboost-test1.55-dev libboost-filesystem1.55-dev libboost-filesystem1.55.0 libsnappy1"
else
    DEP="$DEP libboost-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev libboost-test-dev libboost-filesystem-dev libboost-filesystem-dev libsnappy1v5"
fi
if [ "$RELEASE" = "15.10" ]; then
    DEP="$DEP libjsoncpp0v5 libcrypto++9v5 libyaml-cpp0.5v5 antlr3"
else
    DEP="$DEP libjsoncpp0 libcrypto++9 libyaml-cpp0.5"
fi
sudo apt-get -y install $DEP
if [ "$RELEASE" != "15.10" ]; then
    sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
    sudo apt-get -y update
fi
sudo apt-get -y install g++-5

debuild -r fakeroot -us -uc
