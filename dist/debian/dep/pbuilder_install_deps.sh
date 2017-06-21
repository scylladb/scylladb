#!/bin/bash -e

sudo apt update
if [ ! -f /usr/bin/gdebi ]; then
    sudo apt install -y gdebi-core
fi
if [ ! -f /usr/bin/lsb_release ]; then
    sudo apt install -y lsb-release
fi

CODENAME=`lsb_release -c|awk '{print $2}'`

sudo gdebi -n /var/tmp/pbuilder/antlr3-c++-dev_*.deb
sudo gdebi -n /var/tmp/pbuilder/libthrift0_*.deb
sudo gdebi -n /var/tmp/pbuilder/libthrift-dev_*.deb
if [ "$CODENAME" = "trusty" ] || [ "$CODENAME" = "jessie" ]; then
    sudo gdebi -n /var/tmp/pbuilder/antlr3_*.deb
fi
if [ "$CODENAME" = "jessie" ]; then
    sudo gdebi -n /var/tmp/pbuilder/gcc-5-base_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libatomic1_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libcilkrts5_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libgcc1_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libgomp1_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libitm1_*.deb
    sudo gdebi -n /var/tmp/pbuilder/liblsan0_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libstdc++6_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libtsan0_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libubsan0_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libasan2_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libcc1-0_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libmpx0_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libgcc-5-dev_*.deb
    sudo gdebi -n /var/tmp/pbuilder/libstdc++-5-dev_*.deb
    sudo gdebi -n /var/tmp/pbuilder/cpp-5_*.deb
    sudo gdebi -n /var/tmp/pbuilder/gcc-5_*.deb
    sudo gdebi -n /var/tmp/pbuilder/g++-5_*.deb
fi
