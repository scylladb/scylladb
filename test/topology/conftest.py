#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import pathlib
import sys

# Add test.pylib to the search path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))


