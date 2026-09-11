#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest

# As in the parent suite: one cluster per module of
# cluster.initial_size (default 1) at --smp 2, unchanged throughout.
pytestmark = pytest.mark.max_running_shards(2)
