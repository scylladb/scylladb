#!/bin/bash
#
# Copyright (C) 2019-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Entry point for the RPM %post, the Debian postinst and the offline installer.
# The logic lives in the Python script next to this one.

# Nothing to configure without systemd running - notably in the container image
# build, which installs the packages without booting systemd.
if [ ! -d /run/systemd/system ]; then
    exit 0
fi

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# Never fail the package transaction: the Debian postinst runs under 'set -e'.
"$SCRIPT_DIR"/scylla_post_install.py "$@" || \
    echo "scylla_post_install: warning: post-install configuration failed" >&2

exit 0
