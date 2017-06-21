#!/bin/bash -e

. /etc/os-release
install_deps() {
    echo Y | sudo mk-build-deps
    DEB_FILE=`ls *-build-deps*.deb`
    sudo gdebi -n $DEB_FILE
    sudo rm -f $DEB_FILE
    sudo dpkg -P ${DEB_FILE%%_*.deb}
}

CODENAME=`lsb_release -c|awk '{print $2}'`

# workaround fix for #2444
if [ "$CODENAME" = "jessie" ]; then
    if [ ! -e /etc/apt/sources.list.d/jessie-backports.list ]; then
        sudo sh -c 'echo deb "http://httpredir.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list'
    fi
    sudo apt-get -y update
    sudo apt-get install -t jessie-backports -y texlive
fi

if [ ! -f /usr/bin/gdebi ]; then
    sudo apt-get install -y gdebi-core
fi
if [ ! -f /usr/bin/mk-build-deps ]; then
    sudo apt-get install -y devscripts
fi
if [ ! -f /usr/bin/equivs ]; then
    sudo apt-get install -y equivs
fi

if [ "$CODENAME" = "trusty" ] || [ "$CODENAME" = "jessie" ]; then
    if [ ! -f build/antlr3_*.deb ]; then
        rm -rf build/antlr3-3.5.2
        mkdir -p build/antlr3-3.5.2
        cp -a dist/debian/dep/antlr3-3.5.2/* build/antlr3-3.5.2
        cd build/antlr3-3.5.2
        wget -nv http://www.antlr3.org/download/antlr-3.5.2-complete-no-st3.jar
        install_deps
        debuild -r fakeroot --no-tgz-check -us -uc
        cd -
    fi
    if [ ! -f build/scylla-env_*.deb ]; then
        rm -rf build/scylla-env-1.0
        cp -a dist/common/dep/scylla-env-1.0 build/
        cd build/scylla-env-1.0
        debuild -r fakeroot --no-tgz-check -us -uc
        cd -
    fi
    if [ ! -f build/scylla-gdb_*.deb ]; then
        rm -rf build/gdb-7.11
        if [ ! -f build/gdb_7.11-0ubuntu1.dsc ]; then
            wget -nv -O build/gdb_7.11-0ubuntu1.dsc http://archive.ubuntu.com/ubuntu/pool/main/g/gdb/gdb_7.11-0ubuntu1.dsc
        fi
        if [ ! -f build/gdb_7.11.orig.tar.xz ]; then
            wget -nv -O build/gdb_7.11.orig.tar.xz http://archive.ubuntu.com/ubuntu/pool/main/g/gdb/gdb_7.11.orig.tar.xz
        fi
        if [ ! -f build/gdb_7.11-0ubuntu1.debian.tar.xz ]; then
            wget -nv -O build/gdb_7.11-0ubuntu1.debian.tar.xz http://archive.ubuntu.com/ubuntu/pool/main/g/gdb/gdb_7.11-0ubuntu1.debian.tar.xz
        fi
        cd build
        dpkg-source -x gdb_7.11-0ubuntu1.dsc
        mv gdb_7.11.orig.tar.xz scylla-gdb_7.11.orig.tar.xz
        cd -
        cd build/gdb-7.11
        patch -p0 < ../../dist/debian/dep/gdb.diff
        install_deps
        debuild -r fakeroot --no-tgz-check -us -uc
        cd -
    fi
fi

if [ ! -f build/antlr3-c++-dev_*.deb ]; then
    rm -rf build/antlr3-c++-dev-3.5.2
    if [ ! -f build/3.5.2.tar.gz ]; then
        wget -nv -O build/3.5.2.tar.gz https://github.com/antlr/antlr3/archive/3.5.2.tar.gz
    fi
    cd build
    tar xpf 3.5.2.tar.gz
    mv antlr3-3.5.2 antlr3-c++-dev-3.5.2
    cd -
    cp -a dist/debian/dep/antlr3-c++-dev-3.5.2/debian build/antlr3-c++-dev-3.5.2
    cd build/antlr3-c++-dev-3.5.2
    install_deps
    debuild -r fakeroot --no-tgz-check -us -uc
    cd -
fi

if [ ! -f build/libthrift0_*.deb ]; then
    rm -rf build/thrift-0.10.0
    if [ ! -f build/thrift-0.10.0.tar.gz ]; then
        wget -nv -O build/thrift-0.10.0.tar.gz http://archive.apache.org/dist/thrift/0.10.0/thrift-0.10.0.tar.gz
    fi
    cd build
    tar xpf thrift-0.10.0.tar.gz
    cd thrift-0.10.0
    patch -p0 < ../../dist/debian/dep/thrift.diff
    install_deps
    debuild -r fakeroot --no-tgz-check -us -uc
    cd ../..
fi

if [ "$CODENAME" = "jessie" ]; then
    if [ ! -f build/gcc-5_*.deb ]; then
        cd build
        wget https://launchpad.net/debian/+archive/primary/+files/gcc-5_5.4.1-5.dsc
        wget https://launchpad.net/debian/+archive/primary/+files/gcc-5_5.4.1.orig.tar.gz
        wget https://launchpad.net/debian/+archive/primary/+files/gcc-5_5.4.1-5.diff.gz
        dpkg-source -x gcc-5_5.4.1-5.dsc
        cd gcc-5-5.4.1
        # resolve build time dependencies manually, since mk-build-deps doesn't works for gcc package
        sudo apt-get install -y g++-multilib libc6-dev-i386 lib32gcc1 libc6-dev-x32 libx32gcc1 libc6-dbg m4 libtool autoconf2.64 autogen gawk zlib1g-dev systemtap-sdt-dev gperf bison flex gdb texinfo locales sharutils libantlr-java libffi-dev gnat-4.9 libisl-dev libmpc-dev libmpfr-dev libgmp-dev dejagnu realpath chrpath quilt doxygen graphviz ghostscript texlive-latex-base xsltproc libxml2-utils docbook-xsl-ns
        patch -p0 < ../../dist/debian/dep/debian-gcc-5-jessie.diff
        ./debian/rules control
        debuild -r fakeroot -us -uc
        cd ../..
    fi
fi

rm -rf /var/tmp/pbuilder
mkdir /var/tmp/pbuilder
cp -v build/*.deb /var/tmp/pbuilder/
