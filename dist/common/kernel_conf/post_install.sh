#!/bin/bash
#
# Copyright (C) 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

systemctl --system daemon-reload >/dev/null || true
systemctl --system enable --now scylla-tune-sched.service || true
