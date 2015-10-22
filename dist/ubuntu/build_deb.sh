#!/bin/sh -e

if [ ! -e dist/ubuntu/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

./dist/ubuntu/dep/build_dependency.sh

sudo apt-get -y install libyaml-cpp-dev liblz4-dev libsnappy-dev libcrypto++-dev libboost1.55-dev libjsoncpp-dev libaio-dev ragel ninja-build git libyaml-cpp0.5 liblz4-1 libsnappy1 libcrypto++9 libboost-program-options1.55.0 libboost-program-options1.55-dev libboost-system1.55.0 libboost-system1.55-dev libboost-thread1.55.0 libboost-thread1.55-dev libboost-test1.55.0 libboost-test1.55-dev libjsoncpp0 libaio1 hugepages software-properties-common libboost-filesystem1.55-dev libboost-filesystem1.55.0
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get -y update
sudo apt-get -y install g++-4.9

debuild -r fakeroot --no-tgz-check -us -uc
