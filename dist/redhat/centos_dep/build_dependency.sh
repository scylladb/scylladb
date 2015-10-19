#!/bin/sh -e
export RPMBUILD=`pwd`/build/rpmbuild

do_install()
{
    pkg=$1
    sudo yum install -y $RPMBUILD/RPMS/*/$pkg 2> build/err || if [ "`cat build/err`" != "Error: Nothing to do" ]; then cat build/err; exit 1;fi
    echo Install $name done
}

sudo yum install -y wget yum-utils rpm-build rpmdevtools gcc gcc-c++ make patch
mkdir -p build/srpms
cd build/srpms

if [ ! -f binutils-2.25-5.fc22.src.rpm ]; then
    wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/b/binutils-2.25-5.fc22.src.rpm
fi

if [ ! -f isl-0.14-3.fc22.src.rpm ]; then
    wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/i/isl-0.14-3.fc22.src.rpm
fi

if [ ! -f gcc-5.1.1-4.fc22.src.rpm ]; then
    wget http://download.fedoraproject.org/pub/fedora/linux/updates/22/SRPMS/g/gcc-5.1.1-4.fc22.src.rpm
fi

if [ ! -f boost-1.57.0-6.fc22.src.rpm ]; then
    wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/b/boost-1.57.0-6.fc22.src.rpm
fi

if [ ! -f ninja-build-1.5.3-2.fc22.src.rpm ]; then
    wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/n/ninja-build-1.5.3-2.fc22.src.rpm
fi

if [ ! -f ragel-6.8-3.fc22.src.rpm ]; then
   wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/r/ragel-6.8-3.fc22.src.rpm
fi

if [ ! -f re2c-0.13.5-9.fc22.src.rpm ]; then
   wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/r/re2c-0.13.5-9.fc22.src.rpm
fi

cd -

sudo yum install -y epel-release
sudo yum install -y cryptopp cryptopp-devel jsoncpp jsoncpp-devel lz4 lz4-devel yaml-cpp yaml-cpp-devel thrift thrift-devel scons gtest gtest-devel python34
sudo ln -sf /usr/bin/python3.4 /usr/bin/python3

sudo yum install -y python-devel libicu-devel openmpi-devel mpich-devel libstdc++-devel bzip2-devel zlib-devel
sudo yum install -y flex bison dejagnu zlib-static glibc-static sharutils bc libstdc++-static gmp-devel texinfo texinfo-tex systemtap-sdt-devel mpfr-devel libmpc-devel elfutils-devel elfutils-libelf-devel glibc-devel.x86_64 glibc-devel.i686 gcc-gnat libgnat doxygen graphviz dblatex texlive-collection-latex docbook5-style-xsl python-sphinx cmake
sudo yum install -y gcc-objc
sudo yum install -y asciidoc

if [ ! -f $RPMBUILD/RPMS/noarch/scylla-env-1.0-1.el7.centos.noarch.rpm ]; then
    cd dist/redhat/centos_dep
    tar cpf $RPMBUILD/SOURCES/scylla-env-1.0.tar scylla-env-1.0
    cd -
    rpmbuild --define "_topdir $RPMBUILD" --ba dist/redhat/centos_dep/scylla-env.spec
fi
do_install scylla-env-1.0-1.el7.centos.noarch.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/scylla-binutils-2.25-5.el7.centos.x86_64.rpm ]; then
    rpm --define "_topdir $RPMBUILD" -ivh build/srpms/binutils-2.25-5.fc22.src.rpm
    patch $RPMBUILD/SPECS/binutils.spec < dist/redhat/centos_dep/binutils.diff
    rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/binutils.spec
fi
do_install scylla-binutils-2.25-5.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/scylla-isl-0.14-3.el7.centos.x86_64.rpm ]; then
    rpm --define "_topdir $RPMBUILD" -ivh build/srpms/isl-0.14-3.fc22.src.rpm
    patch $RPMBUILD/SPECS/isl.spec < dist/redhat/centos_dep/isl.diff
    rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/isl.spec
fi
do_install scylla-isl-0.14-3.el7.centos.x86_64.rpm
do_install scylla-isl-devel-0.14-3.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/gcc-5.1.1-4.el7.centos.x86_64.rpm ]; then
    rpmbuild --define "_topdir $RPMBUILD" --define "fedora 21" --rebuild build/srpms/gcc-5.1.1-4.fc22.src.rpm
fi
do_install *5.1.1-4*

if [ ! -f $RPMBUILD/RPMS/x86_64/boost-1.57.0-6.el7.centos.x86_64.rpm ]; then
    rpmbuild --define "_topdir $RPMBUILD" --without python3 --rebuild build/srpms/boost-1.57.0-6.fc22.src.rpm
fi
do_install boost*

if [ ! -f $RPMBUILD/RPMS/x86_64/re2c-0.13.5-9.el7.centos.x86_64.rpm ]; then
    rpmbuild --define "_topdir $RPMBUILD" --rebuild build/srpms/re2c-0.13.5-9.fc22.src.rpm
fi
do_install re2c-0.13.5-9.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/ninja-build-1.5.3-2.el7.centos.x86_64.rpm ]; then
   rpm --define "_topdir $RPMBUILD" -ivh build/srpms/ninja-build-1.5.3-2.fc22.src.rpm
   patch $RPMBUILD/SPECS/ninja-build.spec < dist/redhat/centos_dep/ninja-build.diff
   rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/ninja-build.spec
fi
do_install ninja-build-1.5.3-2.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/ragel-6.8-3.el7.centos.x86_64.rpm ]; then
    rpm --define "_topdir $RPMBUILD" -ivh build/srpms/ragel-6.8-3.fc22.src.rpm
    patch $RPMBUILD/SPECS/ragel.spec < dist/redhat/centos_dep/ragel.diff
    rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/ragel.spec
fi
do_install ragel-6.8-3.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/noarch/antlr3-tool-3.5.2-1.el7.centos.noarch.rpm ]; then
   mkdir build/antlr3-tool-3.5.2
   cp dist/redhat/centos_dep/antlr3 build/antlr3-tool-3.5.2
   cd build/antlr3-tool-3.5.2
   wget http://www.antlr3.org/download/antlr-3.5.2-complete-no-st3.jar
   cd -
   cd build
   tar cJpf $RPMBUILD/SOURCES/antlr3-tool-3.5.2.tar.xz antlr3-tool-3.5.2
   cd -
   rpmbuild --define "_topdir $RPMBUILD" -ba dist/redhat/centos_dep/antlr3-tool.spec
fi
do_install antlr3-tool-3.5.2-1.el7.centos.noarch.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/antlr3-C++-devel-3.5.2-1.el7.centos.x86_64.rpm ];then
   wget -O build/3.5.2.tar.gz https://github.com/antlr/antlr3/archive/3.5.2.tar.gz
   mv build/3.5.2.tar.gz $RPMBUILD/SOURCES
   rpmbuild --define "_topdir $RPMBUILD" -ba dist/redhat/centos_dep/antlr3-C++-devel.spec
fi
do_install antlr3-C++-devel-3.5.2-1.el7.centos.x86_64.rpm
