#!/bin/bash
#
# A platform agnostic Scylla server installation script.
#
# To install Scylla, run the following command:
#
# curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/scylladb/scylla/master/scripts/scylla-get.sh | sudo bash
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

set -euo pipefail

check_arch() {
  ARCH=$(uname -m)

  if [[ "$ARCH" != "x86_64" ]]; then
    echo "Architecture $ARCH is not supported by this installer."
    exit 1
  fi
}

check_os() {
  OS=$(uname -s)

  case "$OS" in
    Linux)   ;;
    *)        echo "Operating system $OS is not supported by this installer." && exit 1 ;;
  esac
}

main() {
  require_cmd curl

  SCYLLA_VERSION_RAW=$(curl -s https://repositories.scylladb.com/scylla/check_version?system=scylla)

  SCYLLA_VERSION=$(echo $SCYLLA_VERSION_RAW | sed -e "s/.*version\":\"\(.*\)\".*/\1/g")

  SCYLLA_RELEASE=$(echo $SCYLLA_VERSION | sed -e "s/\([[:digit:]]\+.[[:digit:]]\+\).*/\1/g")

  while [ $# -gt 0 ]; do
    case "$1" in
      "-h" | "--help")
        usage
	exit 1
        ;;
      "--scylla-version")
        SCYLLA_VERSION="$2"
        shift 2
        ;;
      *)
        usage
	exit 1
        ;;
    esac
  done

  check_arch

  check_os

  # os-release may be missing in container environment by default.
  if [ -f "/etc/os-release" ]; then
      . /etc/os-release
  elif [ -f "/etc/arch-release" ]; then
      export ID=arch
  else
      echo "/etc/os-release missing."
      exit 1
  fi

  case "$ID" in
    "centos") centos_install ;;
    "debian") debian_install ;;
    "fedora") fedora_install ;;
    "ol") ol_install ;;
    "rhel") rhel_install ;;
    "ubuntu") ubuntu_install ;;
    *) echo "Operating system '$ID' is not supported by this installer." && exit 1
  esac

  echo "Scylla installation done!"
}

usage() {
  cat 1>&2 <<EOF
scylla-get
The platform agnostic installer for Scylla.

USAGE:
    scylla-get [FLAGS] [OPTIONS]

FLAGS:
    -h, --help              Prints help information

OPTIONS:
	--scylla-version <version>                 Scylla version to install (default: $SCYLLA_VERSION)
EOF
}

require_cmd() {
  CMD=$1
  if ! command -v "$CMD" &> /dev/null
  then
    echo "Please make sure '$CMD' is installed on this machine."
    exit
  fi
}

install_rpm() {
  SCYLLA_RPM_URL="http://downloads.scylladb.com/rpm/centos/scylla-$SCYLLA_RELEASE.repo"
  curl -s -L -o /etc/yum.repos.d/scylla.repo "$SCYLLA_RPM_URL"
  yum install --assumeyes --quiet "scylla-$SCYLLA_VERSION"
}

check_centos_version() {
  case "$VERSION_ID" in
    "7"|"8")
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

centos_install() {
  if check_centos_version = 0; then
    echo "CentOS $VERSION_ID is not supported by this installer."
    exit 1
  fi
  echo "Installing Scylla version $SCYLLA_VERSION for CentOS ..."
  yum install --assumeyes --quiet epel-release
  install_rpm
}

fedora_install() {
  echo "Installing Scylla version $SCYLLA_VERSION for Fedora ..."
  install_rpm
}

check_rhel_version() {
  case "$VERSION_ID" in
    "8.2")
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

rhel_install() {
  if check_rhel_version = 0; then
    echo "Red Hat Enterprise Linux $VERSION_ID is not supported by this installer."
    exit 1
  fi
  echo "Installing Scylla version $SCYLLA_VERSION for Red Hat Enterprise Linux ..."
  yum install --assumeyes --quiet https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
  install_rpm
}

check_ol_version() {
  case "$VERSION_ID" in
    "8.2")
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

ol_install() {
  if check_ol_version = 0; then
    echo "Oracle Linux $VERSION_ID is not supported by this installer."
    exit 1
  fi
  echo "Installing Scylla version $SCYLLA_VERSION for Oracle Linux ..."
  yum install --assumeyes --quiet https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
  install_rpm
}

check_debian_version() {
  case "$VERSION_ID" in
    "9"|"10")
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

debian_install() {
  if check_debian_version = 0; then
    echo "Debian $VERSION_ID is not supported by this installer."
    exit 1
  fi
  echo "Installing Scylla version $SCYLLA_VERSION for Debian ..."
  export DEBIAN_FRONTEND=noninteractive
  apt update && apt-get install -qq apt-transport-https curl gnupg2 dirmngr
  apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 5e08fbd8b5d6ec9c
  SCYLLA_DEBIAN_URL="http://downloads.scylladb.com/deb/debian/scylla-$SCYLLA_RELEASE-$VERSION_CODENAME.list"
  curl -s -L -o /etc/apt/sources.list.d/scylla.list "$SCYLLA_DEBIAN_URL"
  apt update && apt-get install -qq "scylla=$SCYLLA_VERSION*"
}

check_ubuntu_version() {
  case "$VERSION_ID" in
    "16.04"|"18.04"|"20.04")
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

ubuntu_install() {
  if check_ubuntu_version = 0; then
    echo "Ubuntu $VERSION_ID is not supported by this installer."
    exit 1
  fi
  if [ "$VERSION_ID" = "16.04" ]; then
    apt update && apt-get install -qq curl gnupg2 apt-transport-https
  else
    apt update && apt-get install -qq curl gnupg2
  fi
  echo "Installing Scylla version $SCYLLA_VERSION for Ubuntu ..."
  export DEBIAN_FRONTEND=noninteractive
  apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 5e08fbd8b5d6ec9c
  SCYLLA_UBUNTU_URL="http://downloads.scylladb.com/deb/ubuntu/scylla-$SCYLLA_RELEASE-$VERSION_CODENAME.list"
  curl -s -L -o /etc/apt/sources.list.d/scylla.list "$SCYLLA_UBUNTU_URL"
  apt update && apt-get install -qq "scylla=$SCYLLA_VERSION*"
}

main "$@"
