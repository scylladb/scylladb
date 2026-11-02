# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for loading sstables which were copied into a table's "upload"
# directory, with the REST API request which "nodetool refresh" uses. These
# tests are scylla_only, because they use Scylla's REST API and write files
# in Scylla's data directory.
#############################################################################

import os
import re
import shutil

import pytest
import requests

from test.pylib.skip_types import skip_env

from . import nodetool
from .test_snapshot import table_dir
from .util import new_test_table

# Test that loading sstables from the upload directory fails, with an error
# message which explains why, when the directory isn't writable, when the
# sstables in it aren't readable, and when it has a symbolic link.
# Translated from scylla-dtest's nodetool_additional_test.py::TestNodetool::test_nodetool_refresh_with_wrong_upload_modes.
def test_refresh_bad_upload_directory(cql, test_keyspace, scylla_data_dir, scylla_only):
    if os.geteuid() == 0:
        skip_env('File permissions do not apply to root')
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES (1, 'asdf')")
        nodetool.flush(cql, table)
        # Copy the table's sstables into its upload directory
        directory = table_dir(cql, scylla_data_dir, table)
        upload_dir = os.path.join(directory, 'upload')
        for f in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, f)):
                shutil.copy(os.path.join(directory, f), upload_dir)
        ks, cf = table.split('.')
        def refresh_error():
            response = requests.post(f'{nodetool.rest_api_url(cql)}/storage_service/sstables/{ks}', params={'cf': cf})
            assert not response.ok
            return response.json()['message']
        # Check loading from an upload directory which isn't writable
        os.chmod(upload_dir, 0o555)
        try:
            assert re.search(r'Directory cannot be accessed .* write', refresh_error())
        finally:
            os.chmod(upload_dir, 0o755)
        # Check loading sstables which aren't readable
        files = [os.path.join(upload_dir, f) for f in os.listdir(upload_dir)]
        for f in files:
            os.chmod(f, 0o044)
        try:
            assert re.search(r'File cannot be accessed for read|open failed: Permission denied', refresh_error())
        finally:
            for f in files:
                os.chmod(f, 0o644)
        # Check loading from an upload directory which has a symbolic link
        symlink = os.path.join(upload_dir, 'test_symlink')
        os.symlink('broken', symlink)
        try:
            assert 'Must be either a regular file or a directory' in refresh_error()
        finally:
            os.remove(symlink)
