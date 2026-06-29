#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest import Parser


# Use cache to execute this function once per pytest session.
@cache
def add_host_option(parser: Parser) -> None:
    parser.addoption("--host", default="localhost",
                     help="a DB server host to connect to")


# Use cache to execute this function once per pytest session.
@cache
def add_cql_connection_options(parser: Parser) -> None:
    """Add pytest options for a CQL connection."""

    cql_options = parser.getgroup("CQL connection options")
    cql_options.addoption("--port", default="9042",
                          help="CQL port to connect to")
    cql_options.addoption("--ssl", action="store_true",
                          help="Connect to CQL via an encrypted TLSv1.2 connection", default=False)
    cql_options.addoption("--auth_username",
                          help="username for authentication", default=None)
    cql_options.addoption("--auth_password",
                          help="password for authentication", default=None)


# Use cache to execute this function once per pytest session.
@cache
def add_s3_options(parser: Parser) -> None:
    """Options for tests which use S3 server (i.e., cluster/object_store and cqlpy/test_tools.py)"""

    s3_options = parser.getgroup("S3 server settings")
    s3_options.addoption('--s3-server-address', default=None)
    s3_options.addoption('--s3-server-port', type=int, default=None)
    s3_options.addoption('--aws-access-key', default=None)
    s3_options.addoption('--aws-secret-key', default=None)
    s3_options.addoption('--aws-region', default=None)
    s3_options.addoption('--s3-server-bucket', default=None)
