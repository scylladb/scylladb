#!/bin/sh -e

if [ ! -e dist/ami/build_ami.sh ]; then
    echo "run build_ami.sh in top of scylla dir"
    exit 1
fi

print_usage() {
    echo "build_ami.sh --localrpm --unstable"
    echo "  --localrpm  deploy locally built rpms"
    echo "  --unstable  use unstable branch"
    exit 1
}
LOCALRPM=0
while [ $# -gt 0 ]; do
    case "$1" in
        "--localrpm")
            LOCALRPM=1
            INSTALL_ARGS="$INSTALL_ARGS --localrpm"
            shift 1
            ;;
        "--unstable")
            INSTALL_ARGS="$INSTALL_ARGS --unstable"
            shift 1
            ;;
        *)
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
    wget https://releases.hashicorp.com/packer/0.8.6/packer_0.8.6_linux_amd64.zip
    mkdir packer
    cd packer
    unzip -x ../packer_0.8.6_linux_amd64.zip
    cd -
fi

echo "sudo sh -x -e /home/centos/scylla_install_ami $INSTALL_ARGS" >> scylla_deploy.sh
chmod a+rx scylla_deploy.sh
packer/packer build -var-file=variables.json scylla.json
