# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""The SSL category talks to the encrypted port instead of the default one."""

import pytest

from test import TEST_DIR
from test.conftest import dynamic_scope


@pytest.fixture(scope=dynamic_scope())
def port(request) -> int:
    """The encrypted port, configured as native_transport_port_ssl."""
    return int(request.config.getoption("--ssl-port"))


@pytest.fixture(scope="session")
def ssl_ca_path() -> str:
    """CA certificate of the test Scylla, which ScyllaServer installs as conf/scylla.crt."""
    return str(TEST_DIR / "pylib" / "resources" / "scylla.crt")
