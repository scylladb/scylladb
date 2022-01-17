#!/bin/bash -e

#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

source "$(dirname $0)/util.sh"

echo "Installing Scylla ($MODE) packages on $PRETTY_NAME..."

rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
yum install -y -q deltarpm
yum update -y -q
yum install -y "${SCYLLA_RPMS[@]}"
