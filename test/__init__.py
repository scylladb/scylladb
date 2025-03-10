#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import os
from pathlib import Path

TEST_RUNNER = os.environ.get("SCYLLA_TEST_RUNNER", "pytest")
TOP_SRC_DIR = Path(__file__).parent.parent # ScyllaDB's source code root directory
