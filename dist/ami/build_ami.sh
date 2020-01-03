#!/bin/bash -e

./SCYLLA-VERSION-GEN
PRODUCT=$(cat build/SCYLLA-PRODUCT-FILE)

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
REPO_FOR_INSTALL=
while [ $# -gt 0 ]; do
    case "$1" in
        "--localrpm")
            LOCALRPM=1
            shift 1
            ;;
        "--repo")
            REPO_FOR_INSTALL=$2
            INSTALL_ARGS="$INSTALL_ARGS --repo $2"
            shift 2
            ;;
        "--repo-for-install")
            REPO_FOR_INSTALL=$2
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

    rpm_names=($PRODUCT $PRODUCT-kernel-conf $PRODUCT-conf $PRODUCT-server $PRODUCT-debuginfo)
    version_string=`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`
    rpms_path=build/redhat/RPMS/x86_64
    rpm_missing=""
    for i in ${rpm_names[@]}; do
        if [ ! -f dist/ami/files/$i.x86_64.rpm ]; then
            if [ -f $rpms_path/$i-$version_string.*.x86_64.rpm ]; then
                cp $rpms_path/$i-$version_string.*.x86_64.rpm dist/ami/files/$i.x86_64.rpm
            else
                rpm_missing=yes
            fi
        fi
    done
    if [ -n "$rpm_missing" ]; then
        echo "copy $PRODUCT rpm to following place and run build_ami.sh again:"
        for i in ${rpm_names[@]}; do
            echo "  dist/ami/files/$i.x86_64.rpm"
        done
        exit 1
    fi
    if [ ! -f dist/ami/files/$PRODUCT-jmx.noarch.rpm ]; then
        echo "copy $PRODUCT-jmx rpm to following place and run build_ami.sh again:"
        echo "  dist/ami/files/$PRODUCT-jmx.noarch.rpm"
        exit 1
    fi
    if [ ! -f dist/ami/files/$PRODUCT-tools.noarch.rpm ] || [ ! -f dist/ami/files/$PRODUCT-tools-core.noarch.rpm ]; then
        echo "copy $PRODUCT-tools rpm to following place and run build_ami.sh again:"
        echo "  dist/ami/files/$PRODUCT-tools.noarch.rpm"
        echo "  dist/ami/files/$PRODUCT-tools-core.noarch.rpm"
        exit 1
    fi
    if [ ! -f dist/ami/files/$PRODUCT-ami.noarch.rpm ]; then
        echo "copy $PRODUCT-ami rpm to following place and run build_ami.sh again:"
        echo "  dist/ami/files/$PRODUCT-ami.noarch.rpm"
        exit 1
    fi
    if [ ! -f dist/ami/files/$PRODUCT-python3.x86_64.rpm ]; then
        echo "copy $PRODUCT-python3 rpm to following place and run build_ami.sh again:"
        echo "  dist/ami/files/$PRODUCT-python3.x86_64.rpm"
        exit 1
    fi

    SCYLLA_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} dist/ami/files/$PRODUCT.x86_64.rpm || true)
    SCYLLA_AMI_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} dist/ami/files/$PRODUCT-ami.noarch.rpm || true)
    SCYLLA_JMX_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} dist/ami/files/$PRODUCT-jmx.noarch.rpm || true)
    SCYLLA_TOOLS_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} dist/ami/files/$PRODUCT-tools.noarch.rpm || true)
    SCYLLA_PYTHON3_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} dist/ami/files/$PRODUCT-python3.x86_64.rpm || true)
else
    if [ -z "$REPO_FOR_INSTALL" ]; then
        print_usage
        exit 1
    fi
    if [ ! -f /usr/bin/yumdownloader ]; then
        if is_redhat_variant; then
            sudo yum install /usr/bin/yumdownloader
        else
            sudo apt-get install yum-utils
        fi
    fi
    if [ ! -f /usr/bin/curl ]; then
        pkg_install curl
    fi
    TMPREPO=$(mktemp -u -p /etc/yum.repos.d/ --suffix .repo)
    sudo curl -o $TMPREPO $REPO_FOR_INSTALL
    rm -rf build/ami_packages
    mkdir -p build/ami_packages
    yumdownloader --downloaddir build/ami_packages/ $PRODUCT $PRODUCT-kernel-conf $PRODUCT-conf $PRODUCT-server $PRODUCT-debuginfo $PRODUCT-ami $PRODUCT-jmx $PRODUCT-tools-core $PRODUCT-tools $PRODUCT-python3
    sudo rm -f $TMPREPO
    SCYLLA_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} build/ami_packages/$PRODUCT-[0-9]*.rpm || true)
    SCYLLA_AMI_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} build/ami_packages/$PRODUCT-ami-*.rpm || true)
    SCYLLA_JMX_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} build/ami_packages/$PRODUCT-jmx-*.rpm || true)
    SCYLLA_TOOLS_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} build/ami_packages/$PRODUCT-tools-[0-9]*.rpm || true)
    SCYLLA_PYTHON3_VERSION=$(rpm -q --qf %{VERSION}-%{RELEASE} build/ami_packages/$PRODUCT-python3-*.rpm || true)
fi

SCYLLA_AMI_DESCRIPTION="scylla-$SCYLLA_VERSION scylla-ami-$SCYLLA_AMI_VERSION scylla-jmx-$SCYLLA_JMX_VERSION scylla-tools-$SCYLLA_TOOLS_VERSION scylla-python3-$SCYLLA_PYTHON3_VERSION"
cd dist/ami

if [ ! -f variables.json ]; then
    echo "create variables.json before start building AMI"
    echo "see wiki page: https://github.com/scylladb/scylla/wiki/Building-CentOS-AMI"
    exit 1
fi

if [ ! -d packer ]; then
    EXPECTED="3305ede8886bc3fd83ec0640fb87418cc2a702b2cb1567b48c8cb9315e80047d"
    wget -nv https://releases.hashicorp.com/packer/1.5.1/packer_1.5.1_linux_amd64.zip -O packer_1.5.1_linux_amd64.zip
    CSUM=$(sha256sum packer_1.5.1_linux_amd64.zip|cut -d ' ' -f 1)
    if [ "$CSUM" != "$EXPECTED" ]; then
        echo "Error while downloading packer. Checksum doesn't match! ($CSUM)"
        exit 1
    fi
    mkdir packer
    cd packer
    unzip -x ../packer_1.5.1_linux_amd64.zip
    cd -
fi

env PACKER_LOG=1 PACKER_LOG_PATH=../../build/ami.log packer/packer build -var-file=variables.json -var install_args="$INSTALL_ARGS" -var region="$REGION" -var source_ami="$AMI" -var ssh_username="$SSH_USERNAME" -var scylla_version="$SCYLLA_VERSION" -var scylla_ami_version="$SCYLLA_AMI_VERSION" -var scylla_jmx_version="$SCYLLA_JMX_VERSION" -var scylla_tools_version="$SCYLLA_TOOLS_VERSION" -var scylla_python3_version="$SCYLLA_PYTHON3_VERSION" -var scylla_ami_description="${SCYLLA_AMI_DESCRIPTION:0:255}" scylla.json
