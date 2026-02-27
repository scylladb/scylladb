#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

# migrated from old suite.yaml when tasks tests was in separate folder topology_custom_tasks
extra_scylla_cmdline_options = [ '--task-ttl-in-seconds', '10000000', ]
