#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use
from pytest import Config
from test.topology.conftest import *

def pytest_configure(config: Config):
    config.option.auth_username = 'cassandra'
    config.option.auth_password = 'cassandra'