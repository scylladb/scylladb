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
        reloc/build_reloc.sh
        reloc/build_rpm.sh --dist --target centos7
        for i in ${rpm_names[@]}; do
            cp $rpms_path/$i-$version_string.*.x86_64.rpm dist/ami/files/$i.x86_64.rpm
        done
    fi
    branch_arg=${BRANCH:-$(git rev-parse --abbrev-ref HEAD || echo -n)}
    if [ -n "$branch_arg" ]; then
        if [[ "$b" == '(HEAD detached at'* ]]; then
            branch_arg=$(echo "$branch_arg" | sed -e 's/^(HEAD detached at.*\///' -e 's/)$//')
        fi
        branch_arg="-b $branch_arg"
    fi
    if [ ! -f dist/ami/files/$PRODUCT-jmx.noarch.rpm ]; then
        cd build
        if [ ! -f $PRODUCT-jmx/reloc/build_reloc.sh ]; then
            # directory exists but file is missing, so need to try clone again
            rm -rf $PRODUCT-jmx
            git clone $branch_arg --depth 1 git@github.com:scylladb/$PRODUCT-jmx.git
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
            git clone $branch_arg --depth 1 git@github.com:scylladb/$PRODUCT-tools-java.git
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
            git clone $branch_arg --depth 1 git@github.com:scylladb/$PRODUCT-ami.git
        else
            git pull
        fi
        cd $PRODUCT-ami
        dist/redhat/build_rpm.sh --target centos7
        cd ../..
        cp build/$PRODUCT-ami/build/RPMS/noarch/$PRODUCT-ami-`cat build/$PRODUCT-ami/build/SCYLLA-VERSION-FILE`-`cat build/$PRODUCT-ami/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/$PRODUCT-ami.noarch.rpm
    fi
    if [ ! -f dist/ami/files/$PRODUCT-python3.x86_64.rpm ]; then
        reloc/python3/build_reloc.sh
        reloc/python3/build_rpm.sh
        cp build/redhat/RPMS/x86_64/$PRODUCT-python3*.x86_64.rpm dist/ami/files/$PRODUCT-python3.x86_64.rpm
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

env PACKER_LOG=1 PACKER_LOG_PATH=../../build/ami.log packer/packer build -var-file=variables.json -var install_args="$INSTALL_ARGS" -var region="$REGION" -var source_ami="$AMI" -var ssh_username="$SSH_USERNAME" -var scylla_version="$SCYLLA_VERSION" -var scylla_ami_version="$SCYLLA_AMI_VERSION" -var scylla_jmx_version="$SCYLLA_JMX_VERSION" -var scylla_tools_version="$SCYLLA_TOOLS_VERSION" -var scylla_python3_version="$SCYLLA_PYTHON3_VERSION" -var scylla_ami_description="${SCYLLA_AMI_DESCRIPTION:0:255}" scylla.json
