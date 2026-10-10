#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import subprocess
import sys

import pytest

from test import asan_options, path_to, ubsan_options


@pytest.fixture(scope="module")
def scylla_path(build_mode):
    return path_to(build_mode, "scylla")


@pytest.fixture(scope="module")
def scylla_types(scylla_path):
    """Run `scylla types <action> <args...>` and return the completed process.

    Unless check=False is passed, the command is expected to succeed.
    """
    def invoker(action, *args, check=True):
        cmd = [scylla_path, "types", action, *args]
        env = {'UBSAN_OPTIONS': ubsan_options(),
               'ASAN_OPTIONS': asan_options()}
        res = subprocess.run(cmd, capture_output=True, text=True, env=env)
        sys.stdout.write(res.stdout)
        sys.stderr.write(res.stderr)
        if check:
            assert res.returncode == 0, f"{cmd} failed with exit code {res.returncode}"
        return res

    return invoker


@pytest.fixture(scope="module")
def scylla_types_fails_with(scylla_types):
    """Run `scylla types <action> <args...>`, expecting it to fail with the given error.

    The error is looked for in the stderr of the command.
    """
    def invoker(action, *args, error):
        res = scylla_types(action, *args, check=False)
        assert res.returncode != 0
        assert error in res.stderr, f"expected error {error!r} not found in stderr"
        return res

    return invoker


@pytest.fixture(scope="module")
def schema_file(tmp_path_factory):
    """A schema file, to be used with --schema-file."""
    path = tmp_path_factory.mktemp("schema") / "schema.cql"
    path.write_text("""CREATE TABLE ks.tbl (
    pk1 int,
    pk2 text,
    ck1 timeuuid,
    ck2 int,
    v map<int, text>,
    PRIMARY KEY ((pk1, pk2), ck1, ck2)
) WITH CLUSTERING ORDER BY (ck1 DESC, ck2 ASC);
""")
    return str(path)
