#!/bin/sh -e

RPMBUILD=build/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla dir"
    exit 1
fi
if [ ! -f /usr/bin/mock ]; then
    sudo yum -y install mock
fi
VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
echo $VERSION >version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-server-$VERSION $RPMBUILD/SOURCES/scylla-server-$VERSION.tar
rm -f version
cp dist/redhat/scylla-server.spec.in $RPMBUILD/SPECS/scylla-server.spec
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" $RPMBUILD/SPECS/scylla-server.spec
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" $RPMBUILD/SPECS/scylla-server.spec
rpmbuild -bs --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/scylla-server.spec
mock rebuild --resultdir=`pwd`/build/rpms $RPMBUILD/SRPMS/scylla-server-$VERSION*.src.rpm
