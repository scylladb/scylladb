#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Reusable object-storage helpers for S3 and GCS test fixtures.

Classes, factory functions, and shared pytest fixtures used by
test/cluster/object_store/ and test/cqlpy/ tests.
"""

import logging
import os
import urllib.parse
import urllib.request
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

import boto3
import pytest
import requests

from test.pylib.host_registry import HostRegistry
from test.pylib.s3mock_server import S3MockServer, create_bucket, create_conf


@dataclass
class StoredObject:
    """An object of a test bucket, as a listing shows it."""
    key: str
    size: int
    etag: str


type StorageKind = Literal["s3", "gs"]
type Storage = S3Server | GSFront
type StorageFactory = Callable[[StorageKind], Awaitable[Storage]]


def format_tuples(tuples=None, **kwargs):
    '''format a dict to structured values (tuples) in CQL'''
    if tuples is None:
        tuples = {}
    tuples.update(kwargs)
    body = ', '.join(f"'{key}': '{value}'" for key, value in tuples.items())
    return f'{{ {body} }}'


def keyspace_options(object_storage, rf=1):
    storage_opts = format_tuples(type=f'{object_storage.type}', endpoint=object_storage.address, bucket=object_storage.bucket_name)
    return f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND STORAGE = {storage_opts}"


def _make_bucket_name(test_name: str) -> str:
    """Generate a valid S3 bucket name from a pytest test name.

    S3 bucket naming rules: lowercase, digits, hyphens only; 3-63 chars;
    must start/end with a letter or digit.
    """
    # Lowercase, replace non-alphanumeric runs with a single hyphen, strip leading/trailing hyphens
    name = re.sub(r'[^a-z0-9]+', '-', test_name.lower()).strip('-')
    # Add a short unique suffix to avoid collisions (e.g. parametrized tests with same prefix)
    suffix = uuid.uuid4().hex[:8]
    # Truncate so total length (name + '-' + suffix) <= 63
    max_prefix = 63 - len(suffix) - 1
    name = name[:max_prefix].rstrip('-')
    return f"{name}-{suffix}"


class S3Server:
    def __init__(self, tempdir: str, address: str, port: int, acc_key: str, secret_key: str, region: str, bucket_name):
        self.tempdir = tempdir
        self.ip = address
        self.port = port
        self.address = f'http://{self.ip}:{self.port}'
        self.acc_key = acc_key
        self.secret_key = secret_key
        self.region = region
        self.bucket_name = bucket_name

    def __repr__(self):
        return f"[unknown] {self.address}/{self.bucket_name}"

    @property
    def type(self):
        return 'S3'

    def create_endpoint_conf(self):
        return create_conf(self.address, self.region)

    def get_resource(self):
        """Creates boto3.resource object that can be used to communicate to the given server"""
        return boto3.resource('s3',
            endpoint_url=self.address,
            aws_access_key_id=self.acc_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=None,
            region_name=self.region,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )

    def put_object(self, key: str, body: bytes = b''):
        """Create a single object in the test bucket."""
        self.get_resource().Bucket(self.bucket_name).put_object(Key=key, Body=body)

    def list_objects(self, prefix: str = '') -> list[StoredObject]:
        """The test bucket's objects whose keys start with the prefix."""
        return [StoredObject(o.key, o.size, o.e_tag)
                for o in self.get_resource().Bucket(self.bucket_name).objects.filter(Prefix=prefix)]

    def read_object(self, key: str) -> bytes:
        return self.get_resource().Object(self.bucket_name, key).get()['Body'].read()

    def copy_object(self, src: str, dst: str):
        self.get_resource().Object(self.bucket_name, dst).copy_from(
                CopySource={'Bucket': self.bucket_name, 'Key': src})

    def create_test_bucket(self, test_name: str):
        """Create a unique per-test bucket using boto3."""
        self.bucket_name = _make_bucket_name(test_name)
        create_bucket(self.get_resource(), self.bucket_name)

    def destroy_test_bucket(self):
        """Empty and delete the per-test bucket using boto3.

        A no-op once the bucket is gone, so a caller that cannot tell whether
        its deferred destroy was registered may call this and let the callback
        run too.  A failed destroy leaves the bucket on record, so whichever of
        the two runs second retries it.
        """
        if not self.bucket_name:
            return
        try:
            resource = self.get_resource()
            bucket = resource.Bucket(self.bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except Exception as e:
            # Keep the name so that a retry still has a bucket to delete.
            logging.warning("Failed to destroy test bucket %s: %s", self.bucket_name, e)
            return
        self.bucket_name = None

    async def start(self):
        pass

    async def stop(self):
        pass


# Keep the old name as an alias for backward compatibility
S3_Server = S3Server


class S3MockWrapper(S3Server):
    def __init__(self, log_dir):
        self.server = None
        self.log_dir = log_dir
        # Fields are fully initialized by start(); base-class values are
        # placeholders until then. The server keeps its objects inside the
        # container, so it has no use for a temporary directory either.
        super().__init__('', '', 0, '', '', S3MockServer.DEFAULT_REGION, '')

    async def start(self):
        server = S3MockServer(self.log_dir, logging.getLogger('s3mock'))
        await server.start()
        self.server = server
        self.ip = server.address
        self.port = server.port
        self.address = server.uri
        self.acc_key = server.access_key
        self.secret_key = server.secret_key
        self.bucket_name = server.bucket_name

    async def stop(self):
        if self.server is not None:
            # Dropped only on success, so that a retry still has a
            # server to stop.
            await self.server.stop()
            self.server = None


class GSFront:
    def __init__(self, endpoint, bucket_name, credentials_file):
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.credentials_file = credentials_file

    @property
    def address(self):
        return self.endpoint

    @property
    def type(self):
        return 'GS'

    def create_endpoint_conf(self):
        endpoint = {'name': self.endpoint,
                    'type': 'gs',
                    'credentials_file': self.credentials_file if self.credentials_file else 'none'
                    }
        return [endpoint]

    def put_object(self, key: str, body: bytes = b''):
        """Create a single object in the test bucket via the native JSON
        upload API, as all of the bucket's objects are handled."""
        url = (f'{self.endpoint}/upload/storage/v1/b/{self.bucket_name}/o'
               f'?uploadType=media&name={urllib.parse.quote(key, safe="")}')
        with urllib.request.urlopen(urllib.request.Request(url, data=body, method='POST')) as resp:
            assert resp.status == 200, f'upload of {key} failed: {resp.status}'

    def _bucket_url(self):
        return f'{self.endpoint}/storage/v1/b/{self.bucket_name}'

    def _object_url(self, key: str):
        return f'{self._bucket_url()}/o/{urllib.parse.quote(key, safe="")}'

    def list_objects(self, prefix: str = '') -> list[StoredObject]:
        """The test bucket's objects whose keys start with the prefix."""
        objects, token = [], None
        while True:
            params = {'prefix': prefix}
            if token:
                params['pageToken'] = token
            response = requests.get(f'{self._bucket_url()}/o', params=params, timeout=10)
            response.raise_for_status()
            listing = response.json()
            objects += [StoredObject(o['name'], int(o['size']), o['etag']) for o in listing.get('items', [])]
            token = listing.get('nextPageToken')
            if not token:
                return objects

    def read_object(self, key: str) -> bytes:
        response = requests.get(self._object_url(key), params={'alt': 'media'}, timeout=10)
        response.raise_for_status()
        return response.content

    def copy_object(self, src: str, dst: str):
        dst_path = f'b/{self.bucket_name}/o/{urllib.parse.quote(dst, safe="")}'
        response = requests.post(f'{self._object_url(src)}/copyTo/{dst_path}', json={}, timeout=10)
        response.raise_for_status()

    def create_test_bucket(self, test_name: str):
        """Create a unique per-test bucket using the GCS JSON API."""
        self.bucket_name = _make_bucket_name(test_name)
        response = requests.post(f'{self.endpoint}/storage/v1/b?project=testproject', json={
            'name': self.bucket_name, 'location': 'US', 'storageClass': 'STANDARD',
            'iamConfiguration': {
                'uniformBucketLevelAccess': {
                    'enabled': True,
                }
            }
        }, timeout=10)
        if response.status_code not in [200, 201]:
            raise Exception(f'Could not create test bucket: {response}')

    def destroy_test_bucket(self):
        """Empty and delete the per-test bucket using the GCS JSON API.

        A no-op once the bucket is gone, see S3Server.destroy_test_bucket().
        """
        if not self.bucket_name:
            return
        try:
            for o in self.list_objects():
                response = requests.delete(self._object_url(o.key), timeout=10)
                response.raise_for_status()
            response = requests.delete(self._bucket_url(), timeout=10)
            response.raise_for_status()
        except Exception as e:
            # Keep the name so that a retry still has a bucket to delete.
            logging.warning("Failed to destroy test bucket %s: %s", self.bucket_name, e)
            return
        self.bucket_name = None

    async def start(self):
        pass

    async def stop(self):
        pass


class GSMockWrapper(GSFront):
    """The GCS side of a server of our own, for when the test environment
    has none."""
    def __init__(self, log_dir):
        self.server = None
        self.log_dir = log_dir
        # Fields are fully initialized by start().
        super().__init__(None, None, 'none')

    async def start(self):
        server = S3MockServer(self.log_dir, logging.getLogger('s3mock'))
        await server.start()
        self.server = server
        self.endpoint = server.gs_uri
        self.bucket_name = server.bucket_name

    async def stop(self):
        if self.server is not None:
            # Dropped only on success, so that a retry still has a
            # server to stop.
            await self.server.stop()
            self.server = None


def create_s3_server(pytestconfig, tmpdir, log_dir):
    server = None
    s3_server_address = pytestconfig.getoption('--s3-server-address')
    s3_server_port = pytestconfig.getoption('--s3-server-port')
    aws_acc_key = pytestconfig.getoption('--aws-access-key')
    aws_secret_key = pytestconfig.getoption('--aws-secret-key')
    aws_region = pytestconfig.getoption('--aws-region')
    s3_server_bucket = pytestconfig.getoption('--s3-server-bucket')

    default_address = os.environ.get(S3MockServer.ENV_ADDRESS)
    default_port = os.environ.get(S3MockServer.ENV_PORT)
    default_acc_key = os.environ.get(S3MockServer.ENV_ACCESS_KEY)
    default_secret_key = os.environ.get(S3MockServer.ENV_SECRET_KEY)
    default_region = S3MockServer.DEFAULT_REGION
    default_bucket = os.environ.get(S3MockServer.ENV_BUCKET)

    # Note: bucket_name passed here is overwritten by create_test_bucket() when
    # using the s3_server fixture. The --s3-server-bucket option currently only
    # affects code paths that use server.bucket_name directly without the fixture.
    tempdir = tmpdir.strpath
    if s3_server_address:
        server = S3Server(tempdir,
                          s3_server_address,
                          s3_server_port,
                          aws_acc_key,
                          aws_secret_key,
                          aws_region,
                          s3_server_bucket)
    elif default_address:
        server = S3Server(tempdir,
                          default_address,
                          int(default_port),
                          default_acc_key,
                          default_secret_key,
                          default_region,
                          default_bucket)
    else:
        server = S3MockWrapper(log_dir)
    return server


def create_gs_server(log_dir):
    endpoint = os.environ.get('GS_SERVER_ADDRESS_FOR_TEST')
    bucket = os.environ.get('GS_BUCKET_FOR_TEST')
    credentials_file = os.environ.get('GS_CREDENTIALS_FILE')

    # The tests create buckets of their own, so a server is all it takes.
    if endpoint is not None:
        return GSFront(endpoint, bucket, credentials_file)
    return GSMockWrapper(log_dir)


@pytest.fixture(scope="function")
async def s3_server(request, pytestconfig, tmpdir, suite_log_dir):
    server = create_s3_server(pytestconfig, tmpdir, suite_log_dir)
    await server.start()
    bucket_created = False
    try:
        server.create_test_bucket(request.node.name)
        bucket_created = True
        yield server
    finally:
        if bucket_created:
            server.destroy_test_bucket()
        await server.stop()

