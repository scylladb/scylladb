#!/bin/sh -e

RELEASE=`lsb_release -r|awk '{print $2}'`

sudo apt-get install -y gdebi-core
if [ "$RELEASE" = "14.04" ]; then
    if [ ! -f build/antlr3_3.5.2-1_all.deb ]; then
        rm -rf build/antlr3-3.5.2
        mkdir -p build/antlr3-3.5.2
        cp -a dist/ubuntu/dep/antlr3-3.5.2/* build/antlr3-3.5.2
        cd build/antlr3-3.5.2
        wget http://www.antlr3.org/download/antlr-3.5.2-complete-no-st3.jar
        echo Y | sudo mk-build-deps -i -r
        debuild -r fakeroot --no-tgz-check -us -uc
        cd -
    fi
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
    echo Y | sudo mk-build-deps -i -r
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
    echo Y | sudo mk-build-deps -i -r
    debuild -r fakeroot --no-tgz-check -us -uc
    cd ../..
fi

if [ "$RELEASE" = "14.04" ]; then
    sudo gdebi -n build/antlr3_*.deb
fi
sudo gdebi -n build/antlr3-c++-dev_*.deb
sudo gdebi -n build/libthrift0_*.deb
sudo gdebi -n build/libthrift-dev_*.deb
sudo gdebi -n build/thrift-compiler_*.deb
