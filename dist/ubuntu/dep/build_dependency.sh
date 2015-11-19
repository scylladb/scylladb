#!/bin/sh -e

RELEASE=`lsb_release -r|awk '{print $2}'`
DEP="build-essential debhelper openjdk-7-jre-headless build-essential autoconf automake pkg-config libtool bison flex libevent-dev libglib2.0-dev libqt4-dev python-dev python-dbg php5-dev devscripts python-support xfslibs-dev"

if [ "$RELEASE" = "14.04" ]; then
    DEP="$DEP libboost1.55-dev libboost-test1.55-dev"
else
    DEP="$DEP libboost-dev libboost-test-dev"
fi
sudo apt-get -y install $DEP

if [ ! -f build/antlr3-tool_3.5.2-1_all.deb ]; then
    rm -rf build/antlr3-tool-3.5.2
    mkdir -p build/antlr3-tool-3.5.2
    cp -a dist/ubuntu/dep/antlr3-tool-3.5.2/* build/antlr3-tool-3.5.2
    cd build/antlr3-tool-3.5.2
    wget http://www.antlr3.org/download/antlr-3.5.2-complete-no-st3.jar
    debuild -r fakeroot --no-tgz-check -us -uc
    cd -
fi

if [ ! -f build/antlr3-c++-dev_3.5.2-1_all.deb ]; then
    rm -rf build/antlr3-c++-dev-3.5.2
    if [ ! -f build/3.5.2.tar.gz ]; then
        wget -O build/3.5.2.tar.gz https://github.com/antlr/antlr3/archive/3.5.2.tar.gz
    fi
    cd build
    tar xpf 3.5.2.tar.gz
    mv antlr3-3.5.2 antlr3-c++-dev-3.5.2
    cd -
    cp -a dist/ubuntu/dep/antlr3-c++-dev-3.5.2/debian build/antlr3-c++-dev-3.5.2
    cd build/antlr3-c++-dev-3.5.2
    debuild -r fakeroot --no-tgz-check -us -uc
    cd -
fi

if [ ! -f build/libthrift0_1.0.0-dev_amd64.deb ]; then
    rm -rf build/thrift-0.9.1
    if [ ! -f build/thrift-0.9.1.tar.gz ]; then
        wget -O build/thrift-0.9.1.tar.gz http://archive.apache.org/dist/thrift/0.9.1/thrift-0.9.1.tar.gz
    fi
    cd build
    tar xpf thrift-0.9.1.tar.gz
    cd thrift-0.9.1
    patch -p0 < ../../dist/ubuntu/dep/thrift.diff
    debuild -r fakeroot --no-tgz-check -us -uc
    cd ../..
fi

sudo dpkg -i build/*.deb
