#!/bin/sh -e

if [ ! -e dist/ami/build_ami.sh ]; then
    echo "run build_ami.sh in top of scylla dir"
    exit 1
fi

cd dist/ami

if [ ! -f variables.json ]; then
    echo "create variables.json before start building AMI"
    exit 1
fi

if [ ! -f files/scylla-server.rpm ] || [ ! -f files/scylla-server-debuginfo.rpm ]; then
    cd ../../
    dist/redhat/build_rpm.sh
    RPM=`ls build/rpms/scylla-server-*.x86_64.rpm|grep -v debuginfo`
    cp $RPM dist/ami/files/scylla-server.rpm
    cp build/rpms/scylla-server-debuginfo-*.x86_64.rpm dist/ami/files/scylla-server-debuginfo.rpm
    cd -
fi

if [ ! -f files/scylla-jmx.rpm ]; then
    echo "copy files/scylla-jmx.rpm before building AMI"
    exit 1
fi

if [ ! -d packer ]; then
    wget https://dl.bintray.com/mitchellh/packer/packer_0.8.6_linux_amd64.zip
    mkdir packer
    cd packer
    unzip -x ../packer_0.8.6_linux_amd64.zip
    cd -
fi

packer/packer build -var-file=variables.json scylla.json
