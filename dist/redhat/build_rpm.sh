#!/bin/sh -e

SCYLLA_VER=0.00
RPMBUILD=~/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla dir"
    exit 1
fi
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
sudo yum install -y yum-utils git rpm-build rpmdevtools
cp -a ./ /var/tmp/scylla-server-$SCYLLA_VER
cd /var/tmp
tar --exclude-vcs --exclude-vcs-ignores --exclude="scylla-server-$SCYLLA_VER/build" --exclude="scylla-server-$SCYLLA_VER/seastar/build" -cpf $RPMBUILD/SOURCES/scylla-server-$SCYLLA_VER.tar scylla-server-$SCYLLA_VER
cp scylla-server-$SCYLLA_VER/dist/redhat/scylla-server.spec $RPMBUILD/SPECS
rm -rf scylla-server-$SCYLLA_VER
cd -
sudo yum-builddep -y $RPMBUILD/SPECS/scylla-server.spec
rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/scylla-server.spec
