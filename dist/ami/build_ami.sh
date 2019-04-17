#!/bin/bash -e

PRODUCT=$(cat SCYLLA-PRODUCT-FILE)

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
    REPO=`./scripts/scylla_current_repo --target centos`
    INSTALL_ARGS="$INSTALL_ARGS --localrpm --repo $REPO"
    if [ ! -f /usr/bin/git ]; then
        pkg_install git
    fi

    if [ ! -f dist/ami/files/$PRODUCT.x86_64.rpm ] || [ ! -f dist/ami/files/$PRODUCT-kernel-conf.x86_64.rpm ] || [ ! -f dist/ami/files/$PRODUCT-conf.x86_64.rpm ] || [ ! -f dist/ami/files/$PRODUCT-server.x86_64.rpm ] || [ ! -f dist/ami/files/$PRODUCT-debuginfo.x86_64.rpm ]; then
        reloc/build_reloc.sh
        reloc/build_rpm.sh --dist --target centos7
        cp build/redhat/RPMS/x86_64/$PRODUCT-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/$PRODUCT.x86_64.rpm
        cp build/redhat/RPMS/x86_64/$PRODUCT-kernel-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/$PRODUCT-kernel-conf.x86_64.rpm
        cp build/redhat/RPMS/x86_64/$PRODUCT-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/$PRODUCT-conf.x86_64.rpm
        cp build/redhat/RPMS/x86_64/$PRODUCT-server-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/$PRODUCT-server.x86_64.rpm
        cp build/redhat/RPMS/x86_64/$PRODUCT-debuginfo-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/$PRODUCT-debuginfo.x86_64.rpm
    fi
    if [ ! -f dist/ami/files/$PRODUCT-jmx.noarch.rpm ]; then
        cd build
        if [ ! -f $PRODUCT-jmx/reloc/build_reloc.sh ]; then
            # directory exists but file is missing, so need to try clone again
            rm -rf $PRODUCT-jmx
            git clone --depth 1 https://github.com/scylladb/$PRODUCT-jmx.git
        else
            git pull
        fi
        cd $PRODUCT-jmx
        reloc/build_reloc.sh
        reloc/build_rpm.sh
        cd ../..
        cp build/$PRODUCT-jmx/build/redhat/RPMS/noarch/$PRODUCT-jmx-`cat build/$PRODUCT-jmx/build/SCYLLA-VERSION-FILE`-`cat build/$PRODUCT-jmx/build/SCYLLA-RELEASE-FILE`.noarch.rpm dist/ami/files/$PRODUCT-jmx.noarch.rpm
    fi
    if [ ! -f dist/ami/files/$PRODUCT-tools.noarch.rpm ] || [ ! -f dist/ami/files/$PRODUCT-tools-core.noarch.rpm ]; then
        cd build
        if [ ! -f $PRODUCT-tools-java/reloc/build_reloc.sh ]; then
            # directory exists but file is missing, so need to try clone again
            rm -rf $PRODUCT-tools-java
            git clone --depth 1 https://github.com/scylladb/$PRODUCT-tools-java.git
        else
            git pull
        fi
        cd $PRODUCT-tools-java
        reloc/build_reloc.sh
        reloc/build_rpm.sh
        cd ../..
        cp build/$PRODUCT-tools-java/build/redhat/RPMS/noarch/$PRODUCT-tools-`cat build/$PRODUCT-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/$PRODUCT-tools-java/build/SCYLLA-RELEASE-FILE`.noarch.rpm dist/ami/files/$PRODUCT-tools.noarch.rpm
        cp build/$PRODUCT-tools-java/build/redhat/RPMS/noarch/$PRODUCT-tools-core-`cat build/$PRODUCT-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/$PRODUCT-tools-java/build/SCYLLA-RELEASE-FILE`.noarch.rpm dist/ami/files/$PRODUCT-tools-core.noarch.rpm
    fi
    if [ ! -f dist/ami/files/$PRODUCT-ami.noarch.rpm ]; then
        cd build
        if [ ! -f $PRODUCT-ami/dist/redhat/build_rpm.sh ]; then
            # directory exists but file is missing, so need to try clone again
            rm -rf $PRODUCT-ami
            git clone --depth 1 https://github.com/scylladb/$PRODUCT-ami.git
        else
            git pull
        fi
        cd $PRODUCT-ami
        dist/redhat/build_rpm.sh --target centos7
        cd ../..
        cp build/$PRODUCT-ami/build/RPMS/noarch/$PRODUCT-ami-`cat build/$PRODUCT-ami/build/SCYLLA-VERSION-FILE`-`cat build/$PRODUCT-ami/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/$PRODUCT-ami.noarch.rpm
    fi
fi

cd dist/ami

if [ ! -f variables.json ]; then
    echo "create variables.json before start building AMI"
    echo "see wiki page: https://github.com/scylladb/scylla/wiki/Building-CentOS-AMI"
    exit 1
fi

if [ ! -d packer ]; then
    EXPECTED="5e51808299135fee7a2e664b09f401b5712b5ef18bd4bad5bc50f4dcd8b149a1  packer_1.3.2_linux_amd64.zip"
    wget -nv https://releases.hashicorp.com/packer/1.3.2/packer_1.3.2_linux_amd64.zip -O packer_1.3.2_linux_amd64.zip
    CSUM=`sha256sum packer_1.3.2_linux_amd64.zip`
    if [ "$CSUM" != "$EXPECTED" ]; then
        echo "Error while downloading packer. Checksum doesn't match! ($CSUM)"
        exit 1
    fi
    mkdir packer
    cd packer
    unzip -x ../packer_1.3.2_linux_amd64.zip
    cd -
fi

env PACKER_LOG=1 PACKER_LOG_PATH=../../build/ami.log packer/packer build -var-file=variables.json -var install_args="$INSTALL_ARGS" -var region="$REGION" -var source_ami="$AMI" -var ssh_username="$SSH_USERNAME" scylla.json
