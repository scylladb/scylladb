#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use
from pytest import Config

def pytest_configure(config: Config):
    config.option.auth_username = 'cassandra'
    config.option.auth_password = 'cassandra'