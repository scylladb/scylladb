# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# This file is automatically imported before importing porting.py, and
# causes pytest to rewrite (i.e., improve) assert calls in utility
# functions in porting.py.
import pytest
pytest.register_assert_rewrite("cassandra_tests.porting")
