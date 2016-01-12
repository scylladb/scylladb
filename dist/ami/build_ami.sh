#!/bin/sh -e

if [ ! -e dist/ami/build_ami.sh ]; then
    echo "run build_ami.sh in top of scylla dir"
    exit 1
fi

print_usage() {
    echo "build_ami.sh -l"
    echo "  -l  deploy locally built rpms"
    exit 1
}
LOCALRPM=0
while getopts lh OPT; do
    case "$OPT" in
        "l")
            LOCALRPM=1
            ;;
        "h")
            print_usage
            ;;
    esac
done

cd dist/ami

if [ ! -f variables.json ]; then
    echo "create variables.json before start building AMI"
    exit 1
fi

if [ ! -d packer ]; then
    wget https://dl.bintray.com/mitchellh/packer/packer_0.8.6_linux_amd64.zip
    mkdir packer
    cd packer
    unzip -x ../packer_0.8.6_linux_amd64.zip
    cd -
fi

if [ $LOCALRPM = 0 ]; then
    echo "sudo sh -x -e /home/centos/scylla_install_pkg; sudo sh -x -e /usr/lib/scylla/scylla_setup -a" > scylla_deploy.sh
else
    echo "sudo sh -x -e /home/centos/scylla_install_pkg -l /home/centos; sudo sh -x -e /usr/lib/scylla/scylla_setup -a" > scylla_deploy.sh

fi

chmod a+rx scylla_deploy.sh
packer/packer build -var-file=variables.json scylla.json
