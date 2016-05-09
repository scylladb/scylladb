#!/bin/bash -e

DISTRIBUTION=`lsb_release -i|awk '{print $3}'`
RELEASE=`lsb_release -r|awk '{print $2}'`

sudo apt-get install -y gdebi-core
if [ "$RELEASE" = "14.04" ] || [ "$DISTRIBUTION" = "Debian" ]; then
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
    if [ ! -f build/scylla-env_1.0-0ubuntu1_all.deb ]; then
        rm -rf build/scylla-env-1.0
        cp -a dist/common/dep/scylla-env-1.0 build/
        cd build/scylla-env-1.0
        debuild -r fakeroot --no-tgz-check -us -uc
        cd -
    fi
    if [ ! -f build/scylla-gdb_7.11-0ubuntu1_amd64.deb ]; then
        rm -rf build/gdb-7.11
        if [ ! -f build/gdb_7.11-0ubuntu1.dsc ]; then
            wget -O build/gdb_7.11-0ubuntu1.dsc http://archive.ubuntu.com/ubuntu/pool/main/g/gdb/gdb_7.11-0ubuntu1.dsc
        fi
        if [ ! -f build/gdb_7.11.orig.tar.xz ]; then
            wget -O build/gdb_7.11.orig.tar.xz http://archive.ubuntu.com/ubuntu/pool/main/g/gdb/gdb_7.11.orig.tar.xz
        fi
        if [ ! -f build/gdb_7.11-0ubuntu1.debian.tar.xz ]; then
            wget -O build/gdb_7.11-0ubuntu1.debian.tar.xz http://archive.ubuntu.com/ubuntu/pool/main/g/gdb/gdb_7.11-0ubuntu1.debian.tar.xz
        fi
        cd build
        dpkg-source -x gdb_7.11-0ubuntu1.dsc
        mv gdb_7.11.orig.tar.xz scylla-gdb_7.11.orig.tar.xz
        cd -
        cd build/gdb-7.11
        patch -p0 < ../../dist/ubuntu/dep/gdb.diff
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
    if [ "$RELEASE" = "16.04" ]; then
        sed -i "s/, python-support//" debian/control
        sed -i "s/dh_pysupport//" debian/rules
    fi
    echo Y | sudo mk-build-deps -i -r
    debuild -r fakeroot --no-tgz-check -us -uc
    cd ../..
fi

if [ "$RELEASE" = "14.04" ]; then
    sudo gdebi -n build/antlr3_*.deb
    sudo gdebi -n build/thrift-compiler_*.deb
elif [ "$DISTRIBUTION" = "Debian" ]; then
    sudo gdebi -n build/antlr3_*.deb
fi
sudo gdebi -n build/antlr3-c++-dev_*.deb
sudo gdebi -n build/libthrift0_*.deb
sudo gdebi -n build/libthrift-dev_*.deb
