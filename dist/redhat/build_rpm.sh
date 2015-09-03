#!/bin/sh -e

SCYLLA_VER=0.00
RPMBUILD=~/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla dir"
    exit 1
fi
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
./scripts/git-archive-all --force-submodules --prefix scylla-server-$SCYLLA_VER $RPMBUILD/SOURCES/scylla-server-$SCYLLA_VER.tar
cp dist/redhat/scylla-server.spec $RPMBUILD/SPECS
rpmbuild -bs --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/scylla-server.spec
cd $RPMBUILD/SRPMS
mock rebuild scylla-server-$SCYLLA_VER*.src.rpm
