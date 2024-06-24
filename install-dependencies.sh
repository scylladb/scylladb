#!/bin/bash -e
#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# os-release may be missing in container environment by default.
if [ -f "/etc/os-release" ]; then
    . /etc/os-release
elif [ -f "/etc/arch-release" ]; then
    export ID=arch
else
    echo "/etc/os-release missing."
    exit 1
fi

debian_base_packages=(
    clang
    gdb
    cargo
    wabt
    liblua5.3-dev
    python3-aiohttp
    python3-pyparsing
    python3-colorama
    python3-tabulate
    libsnappy-dev
    libjsoncpp-dev
    rapidjson-dev
    scylla-antlr35-c++-dev
    git
    pigz
    libunistring-dev
    libzstd-dev
    libdeflate-dev
    librapidxml-dev
    libcrypto++-dev
    libxxhash-dev
)

fedora_packages=(
    clang
    compiler-rt
    libasan
    libubsan
    gdb
    lua-devel
    yaml-cpp-devel
    antlr3-tool
    antlr3-C++-devel
    jsoncpp-devel
    rapidjson-devel
    snappy-devel
    libdeflate-devel
    systemd-devel
    cryptopp-devel
    git
    python
    sudo
    patchelf
    python3
    python3-aiohttp
    python3-pip
    python3-magic
    python3-colorama
    python3-tabulate
    python3-boto3
    python3-pytest
    python3-pytest-asyncio
    python3-redis
    python3-unidiff
    python3-humanfriendly
    python3-jinja2
    dnf-utils
    pigz
    net-tools
    tar
    gzip
    gawk
    util-linux
    ethtool
    hwloc
    glibc-langpack-en
    xxhash-devel
    makeself
    libzstd-static libzstd-devel
    rpm-build
    devscripts
    debhelper
    fakeroot
    file
    dpkg-dev
    curl
    rust
    cargo
    rapidxml-devel
    rust-std-static-wasm32-wasi
    wabt
    binaryen
    lcov
)

# lld is not available on s390x, see
# https://src.fedoraproject.org/rpms/lld/c/aa6e69df60747496f8f22121ae8cc605c9d3498a?branch=rawhide
if [ "$(uname -m)" != "s390x" ]; then
    fedora_packages+=(lld)
fi

fedora_python3_packages=(
    python3-pyyaml
    python3-urwid
    python3-pyparsing
    python3-requests
    python3-setuptools
    python3-psutil
    python3-distro
    python3-click
    python3-six
    python3-pyudev
)

# an associative array from packages to constrains
declare -A pip_packages=(
    [scylla-driver]=
    [geomet]="<0.3,>=0.1"
    [traceback-with-variables]=
    [scylla-api-client]=
    [treelib]=
    [allure-pytest]=
)

pip_symlinks=(
    scylla-api-client
)

centos_packages=(
    gdb
    yaml-cpp-devel
    scylla-antlr35-tool
    scylla-antlr35-C++-devel
    jsoncpp-devel snappy-devel
    rapidjson-devel
    scylla-boost163-static
    scylla-python34-pyparsing20
    systemd-devel
    pigz
)

# 1) glibc 2.30-3 has sys/sdt.h (systemtap include)
#    some old containers may contain glibc older,
#    so enforce update on that one.
# 2) if problems with signatures, ensure having fresh
#    archlinux-keyring: pacman -Sy archlinux-keyring && pacman -Syyu
# 3) aur installations require having sudo and being
#    a sudoer. makepkg does not work otherwise.
#
# aur: antlr3, antlr3-cpp-headers-git
arch_packages=(
    gdb
    base-devel
    filesystem
    git
    glibc
    jsoncpp
    lua
    python-pyparsing
    python3
    rapidjson
    snappy
)

go_arch() {
    declare -A local GO_ARCH=(
        ["x86_64"]=amd64
        ["aarch64"]=arm64
        ["s390x"]=s390x
    )
    echo ${GO_ARCH["$(arch)"]}
}

NODE_EXPORTER_VERSION=1.7.0
declare -A NODE_EXPORTER_CHECKSUM=(
    ["x86_64"]=a550cd5c05f760b7934a2d0afad66d2e92e681482f5f57a917465b1fba3b02a6
    ["aarch64"]=e386c7b53bc130eaf5e74da28efc6b444857b77df8070537be52678aefd34d96
    ["s390x"]=aeda68884918f10b135b76bbcd4977cb7a1bb3c4c98a8551f8d2183bafdd9264
)
NODE_EXPORTER_DIR=/opt/scylladb/dependencies

node_exporter_filename() {
    echo "node_exporter-$NODE_EXPORTER_VERSION.linux-$(go_arch).tar.gz"
}

node_exporter_fullpath() {
    echo "$NODE_EXPORTER_DIR/$(node_exporter_filename)"
}

node_exporter_checksum() {
    sha256sum "$(node_exporter_fullpath)" | while read -r sum _; do [[ "$sum" == "${NODE_EXPORTER_CHECKSUM["$(arch)"]}" ]]; done
}

node_exporter_url() {
    echo "https://github.com/prometheus/node_exporter/releases/download/v$NODE_EXPORTER_VERSION/$(node_exporter_filename)"
}

MINIO_BINARIES_DIR=/usr/local/bin

minio_server_url() {
    echo "https://dl.minio.io/server/minio/release/linux-$(go_arch)/minio"
}

minio_client_url() {
    echo "https://dl.min.io/client/mc/release/linux-$(go_arch)/mc"
}

minio_download_jobs() {
    cfile=$(mktemp)
    echo $(curl -s "$(minio_server_url).sha256sum" | cut -f1 -d' ') "${MINIO_BINARIES_DIR}/minio" > $cfile
    echo $(curl -s "$(minio_client_url).sha256sum" | cut -f1 -d' ') "${MINIO_BINARIES_DIR}/mc" >> $cfile
    sha256sum -c $cfile | grep -F FAILED | sed \
        -e 's/:.*$//g' \
        -e "s#${MINIO_BINARIES_DIR}/minio#$(minio_server_url) -o ${MINIO_BINARIES_DIR}/minio#" \
        -e "s#${MINIO_BINARIES_DIR}/mc#$(minio_client_url) -o ${MINIO_BINARIES_DIR}/mc#"
    rm -f ${cfile}
}

print_usage() {
    echo "Usage: install-dependencies.sh [OPTION]..."
    echo ""
    echo "  --print-python3-runtime-packages Print required python3 packages for Scylla"
    echo "  --print-pip-runtime-packages Print required pip packages for Scylla"
    echo "  --print-pip-symlinks Print list of pip provided commands which need to install to /usr/bin"
    echo "  --print-node-exporter-filename Print node_exporter filename"
    exit 1
}

PRINT_PYTHON3=false
PRINT_PIP=false
PRINT_PIP_SYMLINK=false
PRINT_NODE_EXPORTER=false
while [ $# -gt 0 ]; do
    case "$1" in
        "--print-python3-runtime-packages")
            PRINT_PYTHON3=true
            shift 1
            ;;
        "--print-pip-runtime-packages")
            PRINT_PIP=true
            shift 1
            ;;
        "--print-pip-symlinks")
            PRINT_PIP_SYMLINK=true
            shift 1
            ;;
        "--print-node-exporter-filename")
            PRINT_NODE_EXPORTER=true
            shift 1
            ;;
         *)
            print_usage
            ;;
    esac
done

if $PRINT_PYTHON3; then
    if [ "$ID" != "fedora" ]; then
        echo "Unsupported Distribution: $ID"
        exit 1
    fi
    echo "${fedora_python3_packages[@]}"
    exit 0
fi

if $PRINT_PIP; then
    echo "${!pip_packages[@]}"
    exit 0
fi

if $PRINT_PIP_SYMLINK; then
    echo "${pip_symlinks[@]}"
    exit 0
fi

if $PRINT_NODE_EXPORTER; then
    node_exporter_fullpath
    exit 0
fi

umask 0022

./seastar/install-dependencies.sh
./tools/jmx/install-dependencies.sh
./tools/java/install-dependencies.sh

if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
    apt-get -y install "${debian_base_packages[@]}"
    if [ "$VERSION_ID" = "8" ]; then
        apt-get -y install libsystemd-dev scylla-antlr35 libyaml-cpp-dev
    elif [ "$VERSION_ID" = "14.04" ]; then
        apt-get -y install scylla-antlr35 libyaml-cpp-dev
    elif [ "$VERSION_ID" = "9" ]; then
        apt-get -y install libsystemd-dev antlr3 scylla-libyaml-cpp05-dev
    else
        apt-get -y install libsystemd-dev antlr3 libyaml-cpp-dev
    fi
    echo -e "Configure example:\n\t./configure.py --enable-dpdk --mode=release --static-boost --static-yaml-cpp --compiler=/opt/scylladb/bin/g++-7 --cflags=\"-I/opt/scylladb/include -L/opt/scylladb/lib/x86-linux-gnu/\" --ldflags=\"-Wl,-rpath=/opt/scylladb/lib\""
elif [ "$ID" = "fedora" ]; then
    if rpm -q --quiet yum-utils; then
        echo
        echo "This script will install dnf-utils package, witch will conflict with currently installed package: yum-utils"
        echo "Please remove the package and try to run this script again."
        exit 1
    fi
    dnf install -y "${fedora_packages[@]}" "${fedora_python3_packages[@]}"
    PIP_DEFAULT_ARGS="--only-binary=:all: -v"
    pip_constrained_packages=""
    for package in ${!pip_packages[@]}
    do
        pip_constrained_packages="${pip_constrained_packages} ${package}${pip_packages[$package]}"
    done
    pip3 install "$PIP_DEFAULT_ARGS" $pip_constrained_packages

    if [ -f "$(node_exporter_fullpath)" ] && node_exporter_checksum; then
        echo "$(node_exporter_filename) already exists, skipping download"
    else
        mkdir -p "$NODE_EXPORTER_DIR"
        curl -fSL -o "$(node_exporter_fullpath)" "$(node_exporter_url)"
        if ! node_exporter_checksum; then
            echo "$(node_exporter_filename) download failed"
            exit 1
        fi
    fi
elif [ "$ID" = "centos" ]; then
    dnf install -y "${centos_packages[@]}"
    echo -e "Configure example:\n\tpython3.4 ./configure.py --enable-dpdk --mode=release --static-boost --compiler=/opt/scylladb/bin/g++-7.3 --python python3.4 --ldflag=-Wl,-rpath=/opt/scylladb/lib64 --cflags=-I/opt/scylladb/include --with-antlr3=/opt/scylladb/bin/antlr3"
elif [ "$ID" == "arch" ]; then
    # main
    if [ "$EUID" -eq "0" ]; then
        pacman -Sy --needed --noconfirm "${arch_packages[@]}"
    else
        echo "scylla: You now ran $0 as non-root. Run it again as root to execute the pacman part of the installation." 1>&2
    fi

    # aur
    if [ ! -x /usr/bin/antlr3 ]; then
        echo "Installing aur/antlr3..."
        if (( EUID == 0 )); then
            echo "You now ran $0 as root. This can only update dependencies with pacman. Please run again it as non-root to complete the AUR part of the installation." 1>&2
            exit 1
        fi
        TEMP=$(mktemp -d)
        pushd "$TEMP" > /dev/null || exit 1
        git clone --depth 1 https://aur.archlinux.org/antlr3.git
        cd antlr3 || exit 1
        makepkg -si
        popd > /dev/null || exit 1
    fi
    if [ ! -f /usr/include/antlr3.hpp ]; then
        echo "Installing aur/antlr3-cpp-headers-git..."
        if (( EUID == 0 )); then
            echo "You now ran $0 as root. This can only update dependencies with pacman. Please run again it as non-root to complete the AUR part of the installation." 1>&2
            exit 1
        fi
        TEMP=$(mktemp -d)
        pushd "$TEMP" > /dev/null || exit 1
        git clone --depth 1 https://aur.archlinux.org/antlr3-cpp-headers-git.git
        cd antlr3-cpp-headers-git || exit 1
        makepkg -si
        popd > /dev/null || exit 1
    fi
    echo -e "Configure example:\n\t./configure.py\n\tninja release"
fi

cargo --config net.git-fetch-with-cli=true install cxxbridge-cmd --root /usr/local

CURL_ARGS=$(minio_download_jobs)
if [ ! -z "${CURL_ARGS}" ]; then
    curl -fSL --remove-on-error --parallel --parallel-immediate ${CURL_ARGS}
    chmod +x "${MINIO_BINARIES_DIR}/minio"
    chmod +x "${MINIO_BINARIES_DIR}/mc"
else
    echo "Minio server and client are up-to-date, skipping download"
fi
