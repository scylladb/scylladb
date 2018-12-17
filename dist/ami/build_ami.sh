#!/bin/bash -e

if [ ! -e dist/ami/build_ami.sh ]; then
    echo "run build_ami.sh in top of scylla dir"
    exit 1
fi

print_usage() {
    echo "build_ami.sh --localrpm --repo [URL] --target [distribution]"
    echo "  --localrpm  deploy locally built rpms"
    echo "  --repo  repository for both install and update, specify .repo/.list file URL"
    echo "  --repo-for-install  repository for install, specify .repo/.list file URL"
    echo "  --repo-for-update  repository for update, specify .repo/.list file URL"
    exit 1
}
LOCALRPM=0
while [ $# -gt 0 ]; do
    case "$1" in
        "--localrpm")
            LOCALRPM=1
            shift 1
            ;;
        "--repo")
            INSTALL_ARGS="$INSTALL_ARGS --repo $2"
            shift 2
            ;;
        "--repo-for-install")
            INSTALL_ARGS="$INSTALL_ARGS --repo-for-install $2"
            shift 2
            ;;
        "--repo-for-update")
            INSTALL_ARGS="$INSTALL_ARGS --repo-for-update $2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
is_debian_variant() {
    [ -f /etc/debian_version ]
}

pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    elif is_debian_variant; then
        sudo apt-get install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}

AMI=ami-ae7bfdb8
REGION=us-east-1
SSH_USERNAME=centos

if [ $LOCALRPM -eq 1 ]; then
    sudo rm -rf build/*
    REPO=`./scripts/scylla_current_repo --target centos`
    INSTALL_ARGS="$INSTALL_ARGS --localrpm --repo $REPO"
    if [ ! -f /usr/bin/git ]; then
        pkg_install git
    fi

    if [ ! -f dist/ami/files/scylla.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-kernel-conf.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-conf.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-server.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-debuginfo.x86_64.rpm ]; then
        dist/redhat/build_rpm.sh --dist --target epel-7-x86_64
        cp build/rpms/scylla-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla.x86_64.rpm
        cp build/rpms/scylla-kernel-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-kernel-conf.x86_64.rpm
        cp build/rpms/scylla-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-conf.x86_64.rpm
        cp build/rpms/scylla-server-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-server.x86_64.rpm
        cp build/rpms/scylla-debuginfo-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-debuginfo.x86_64.rpm
    fi
    if [ ! -f dist/ami/files/scylla-jmx.noarch.rpm ]; then
        cd build
        git clone -b branch-3.0 --depth 1 https://github.com/scylladb/scylla-jmx.git
        cd scylla-jmx
        dist/redhat/build_rpm.sh --target epel-7-x86_64
        cd ../..
        cp build/scylla-jmx/build/rpms/scylla-jmx-`cat build/scylla-jmx/build/SCYLLA-VERSION-FILE`-`cat build/scylla-jmx/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-jmx.noarch.rpm
    fi
    if [ ! -f dist/ami/files/scylla-tools.noarch.rpm ] || [ ! -f dist/ami/files/scylla-tools-core.noarch.rpm ]; then
        cd build
        git clone --depth 1 https://github.com/scylladb/scylla-tools-java.git
        cd scylla-tools-java
        dist/redhat/build_rpm.sh --target epel-7-x86_64
        cd ../..
        cp build/scylla-tools-java/build/rpms/scylla-tools-`cat build/scylla-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/scylla-tools-java/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-tools.noarch.rpm
        cp build/scylla-tools-java/build/rpms/scylla-tools-core-`cat build/scylla-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/scylla-tools-java/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-tools-core.noarch.rpm
    fi
    if [ ! -f dist/ami/files/scylla-ami.noarch.rpm ]; then
        cd build
        git clone --depth 1 https://github.com/scylladb/scylla-ami.git
        cd scylla-ami
        dist/redhat/build_rpm.sh --target epel-7-x86_64
        cd ../..
        cp build/scylla-ami/build/rpms/scylla-ami-`cat build/scylla-ami/build/SCYLLA-VERSION-FILE`-`cat build/scylla-ami/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-ami.noarch.rpm
    fi
fi

cd dist/ami

if [ ! -f variables.json ]; then
    echo "create variables.json before start building AMI"
    echo "see wiki page: https://github.com/scylladb/scylla/wiki/Building-CentOS-AMI"
    exit 1
fi

if [ ! -d packer ]; then
    EXPECTED="ed697ace39f8bb7bf6ccd78e21b2075f53c0f23cdfb5276c380a053a7b906853  packer_1.0.0_linux_amd64.zip"
    wget -nv https://releases.hashicorp.com/packer/1.0.0/packer_1.0.0_linux_amd64.zip -O packer_1.0.0_linux_amd64.zip
    CSUM=`sha256sum packer_1.0.0_linux_amd64.zip`
    if [ "$CSUM" != "$EXPECTED" ]; then
        echo "Error while downloading packer. Checksum doesn't match! ($CSUM)"
        exit 1
    fi
    mkdir packer
    cd packer
    unzip -x ../packer_1.0.0_linux_amd64.zip
    cd -
fi

packer/packer build -var-file=variables.json -var install_args="$INSTALL_ARGS" -var region="$REGION" -var source_ami="$AMI" -var ssh_username="$SSH_USERNAME" scylla.json
