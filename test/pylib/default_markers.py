#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# What CI's PR stage leaves out: tier2 tests run in Next and Nightly only (an
# xfail test is tier2 too, see runner.py's modify_pytest_item()), tier3 tests
# in Nightly only, and non_gating tests in no automatic job.  A run that
# gives no marker expression leaves them out too, so test.py, pytest and the
# suites' run scripts check by default what CI does.  No imports: the run
# scripts load this file before anything else of the tree.
DEFAULT_EXCLUDED_MARKERS = ("non_gating", "tier2", "tier3")
DEFAULT_MARKER_EXPRESSION = " and ".join(f"not {marker}" for marker in DEFAULT_EXCLUDED_MARKERS)
