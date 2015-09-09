#!/bin/sh -e

SCYLLA_VER=0.00
RPMBUILD=build/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla dir"
    exit 1
fi
if [ ! -f /usr/bin/mock ]; then
    sudo yum -y install mock
fi
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
./scripts/git-archive-all --force-submodules --prefix scylla-server-$SCYLLA_VER $RPMBUILD/SOURCES/scylla-server-$SCYLLA_VER.tar
rpmbuild -bs --define "_topdir $RPMBUILD" -ba dist/redhat/scylla-server.spec
mock rebuild --resultdir=`pwd`/build/rpms $RPMBUILD/SRPMS/scylla-server-$SCYLLA_VER*.src.rpm
