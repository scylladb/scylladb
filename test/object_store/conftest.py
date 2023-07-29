#!/usr/bin/python3

import os
import sys
import logging
import pytest
import shutil
import tempfile
from dataclasses import dataclass
from contextlib import contextmanager

# use minio_server
sys.path.insert(1, sys.path[0] + '/../..')
from test.pylib.minio_server import MinioServer
from test.pylib.host_registry import HostRegistry
from test.pylib.cql_repl import conftest

hosts = HostRegistry()


def pytest_addoption(parser):
    conftest.pytest_addoption(parser)
    # reserved for tests with real S3
    s3_options = parser.getgroup("s3-server", description="S3 Server settings")
    s3_options.addoption('--s3-server-address')
    s3_options.addoption('--s3-server-port', type=int)
    s3_options.addoption('--s3-server-bucket')


@dataclass
class S3_Server:
    address: str
    port: int
    bucket_name: str

    async def start(self):
        pass

    async def stop(self):
        pass


@pytest.fixture(scope="function")
def ssl(request):
    yield request.config.getoption('--ssl')


def _remove_all_but(tempdir, to_preserve):
    orig_fn = os.path.join(tempdir, to_preserve)
    # orig_fn does not exist
    if not os.path.exists(orig_fn):
        # it's fine if tempdir does not exist
        shutil.rmtree(tempdir=True)
        return

    with tempfile.TemporaryDirectory() as backup_tempdir:
        backup_fn = os.path.join(backup_tempdir, to_preserve)
        shutil.move(orig_fn, backup_fn)
        shutil.rmtree(tempdir)
        os.mkdir(tempdir)
        shutil.move(backup_fn, orig_fn)


@pytest.fixture(scope="function")
def test_tempdir(tmpdir):
    tempdir = tmpdir.strpath
    try:
        yield tempdir
    finally:
        _remove_all_but(tempdir, 'log')


@pytest.fixture(scope="function")
async def s3_server(pytestconfig, tmpdir):
    server = None
    s3_server_address = pytestconfig.getoption('--s3-server-address')
    s3_server_port = pytestconfig.getoption('--s3-server-port')
    s3_server_bucket = pytestconfig.getoption('--s3-server-bucket')

    default_address = os.environ.get('S3_SERVER_ADDRESS_FOR_TEST')
    default_port = os.environ.get('S3_SERVER_PORT_FOR_TEST')
    default_bucket = os.environ.get('S3_PUBLIC_BUCKET_FOR_TEST')

    if s3_server_address:
        server = S3_Server(s3_server_address,
                           s3_server_port,
                           s3_server_bucket)
    elif default_address:
        server = S3_Server(default_address,
                           int(default_port),
                           default_bucket)
    else:
        tempdir = tmpdir.strpath
        server = MinioServer(tempdir,
                             hosts,
                             logging.getLogger('minio'))
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
