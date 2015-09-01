#!/bin/sh -e

SCYLLA_VER=0.00
RPMBUILD=~/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla dir"
    exit 1
fi
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
sudo yum install -y yum-utils git rpm-build rpmdevtools
./scripts/git-archive-all --prefix scylla-server-$SCYLLA_VER $RPMBUILD/SOURCES/scylla-server-$SCYLLA_VER.tar
cp dist/redhat/scylla-server.spec $RPMBUILD/SPECS
sudo yum-builddep -y $RPMBUILD/SPECS/scylla-server.spec
rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/scylla-server.spec
