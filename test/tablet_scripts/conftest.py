#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import sys

from test import TOP_SRC_DIR


# Put scripts/ on the path before the test modules import tablets.<module>.
SCRIPTS_DIR = str(TOP_SRC_DIR / "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)
