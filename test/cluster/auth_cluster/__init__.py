#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

# migrated from old suite.yaml when tasks tests was in separate folder auth_cluster
extra_scylla_config_options = {'authenticator':'PasswordAuthenticator',
                                 'authorizer':'CassandraAuthorizer'}