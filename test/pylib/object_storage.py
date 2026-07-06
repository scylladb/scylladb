#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Reusable object-storage helpers for S3 and GCS test fixtures.

Classes, factory functions, and shared pytest fixtures used by
test/cluster/object_store/ and test/cqlpy/ tests.
"""

import os
import logging
import re
import uuid

from test.pylib.minio_server import MinioServer
from test.pylib.dockerized_service import DockerizedServer
from test.pylib.host_registry import HostRegistry
from operator import attrgetter

import pytest
import boto3
import requests


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
        return MinioServer.create_conf(self.address, self.region)

    def get_resource(self):
        """Creates boto3.resource object that can be used to communicate to the given server"""
        return boto3.resource('s3',
            endpoint_url=self.address,
            aws_access_key_id=self.acc_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )

    def create_test_bucket(self, test_name: str):
        """Create a unique per-test bucket using boto3."""
        self.bucket_name = _make_bucket_name(test_name)
        resource = self.get_resource()
        resource.Bucket(self.bucket_name).create()

    def destroy_test_bucket(self):
        """Empty and delete the per-test bucket using boto3."""
        try:
            resource = self.get_resource()
            bucket = resource.Bucket(self.bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except Exception as e:
            logging.warning("Failed to destroy test bucket %s: %s", self.bucket_name, e)

    async def start(self):
        pass

    async def stop(self):
        pass


# Keep the old name as an alias for backward compatibility
S3_Server = S3Server


class MinioWrapper(S3Server):
    def __init__(self, tempdir):
        self.host_registry = HostRegistry()
        self.leased_host = None
        self.server = None
        # Fields are fully initialized by start(); base-class values are
        # placeholders until then.
        super().__init__(tempdir, '', 0, '', '', MinioServer.DEFAULT_REGION, '')

    def create_endpoint_conf(self):
        return MinioServer.create_conf(self.address, self.region)

    async def start(self):
        self.leased_host = await self.host_registry.lease_host()
        self.server = MinioServer(self.tempdir,
                                  self.leased_host,
                                  logging.getLogger('minio'))
        try:
            await self.server.start()
        except Exception:
            await self.host_registry.release_host(self.leased_host)
            self.leased_host = None
            raise
        self.ip = self.server.address
        self.port = self.server.port
        self.address = f'http://{self.ip}:{self.port}'
        self.acc_key = self.server.access_key
        self.secret_key = self.server.secret_key
        self.bucket_name = self.server.bucket_name

    async def stop(self):
        try:
            await self.server.stop()
        finally:
            if self.leased_host is not None:
                await self.host_registry.release_host(self.leased_host)
                self.leased_host = None


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

    def get_resource(self):
        """Creates boto3.resource object that can be used to communicate to the given server"""
        return boto3.resource('s3',
            endpoint_url=self.endpoint,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )

    def create_test_bucket(self, test_name: str):
        """Create a unique per-test bucket using boto3."""
        self.bucket_name = _make_bucket_name(test_name)
        resource = self.get_resource()
        resource.Bucket(self.bucket_name).create()

    def destroy_test_bucket(self):
        """Empty and delete the per-test bucket using boto3."""
        try:
            resource = self.get_resource()
            bucket = resource.Bucket(self.bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except Exception as e:
            logging.warning("Failed to destroy test bucket %s: %s", self.bucket_name, e)

    async def start(self):
        pass

    async def stop(self):
        pass


class GSServerImpl(GSFront):
    def __init__(self, tmpdir):
        super(GSServerImpl, self).__init__(None, 'testbucket', None)
        self.server = None
        self.host = None
        self.port = None
        self.oldvars = {}
        self.vars = { 'GS_SERVER_ADDRESS_FOR_TEST': attrgetter('endpoint'),
                      'GS_BUCKET_FOR_TEST': attrgetter('bucket_name'),
                      'GS_CREDENTIALS_FILE': attrgetter('credentials_file'),
                      }
        self.tmpdir = tmpdir

    def publish(self):
        self.endpoint = f'http://{self.host}:{self.port}'
        self.oldvars = { k : os.environ.get(k) for k in self.vars }
        for k, v in self.vars.items():
            val = v(self)
            if val:
                os.environ[k] = val

    def unpublish(self):
        for k in self.vars:
            v = self.oldvars.get(k)
            if v:
                os.environ[k] = v
            elif os.environ.get(k):
                del os.environ[k]

    def _image_args(self, host, port):
        # pylint: disable=unused-argument
        # note: need to set 'public-host' to the IP we connect to, to make XML (s3) API work for listing
        return ["-scheme", "http", "-log-level", "debug", "--port", f'{port}', '-public-host', '127.0.0.1']

    async def start(self):
        self.server = DockerizedServer("docker.io/fsouza/fake-gcs-server:1.54.0", self.tmpdir,
                                       logfilenamebase="fake-gcs-server",
                                       image_args=self._image_args,
                                       success_string="server started at",
                                       failure_string="address already in use",
                                       port=4443
                                       )
        await self.server.start()
        self.port = self.server.port
        self.host = self.server.host
        self.publish()


    def create_test_bucket(self, test_name: str):
        """Create a unique per-test bucket using GCS HTTP API (fake server doesn't support S3 XML for this)."""
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
        """Empty and delete the per-test bucket using GCS HTTP API."""
        try:
            # List and delete all objects first using boto3 (listing works on fake GCS)
            resource = self.get_resource()
            bucket = resource.Bucket(self.bucket_name)
            bucket.objects.all().delete()
            # Delete the bucket via GCS HTTP API
            requests.delete(f'{self.endpoint}/storage/v1/b/{self.bucket_name}', timeout=10)
        except Exception as e:
            logging.warning("Failed to destroy test bucket %s: %s", self.bucket_name, e)

    async def stop(self):
        if self.server:
            await self.server.stop()
        self.unpublish()


# Keep the old name as an alias for backward compatibility (conftest.py imports it)
GSServer = GSServerImpl


def create_s3_server(pytestconfig, tmpdir):
    server = None
    s3_server_address = pytestconfig.getoption('--s3-server-address')
    s3_server_port = pytestconfig.getoption('--s3-server-port')
    aws_acc_key = pytestconfig.getoption('--aws-access-key')
    aws_secret_key = pytestconfig.getoption('--aws-secret-key')
    aws_region = pytestconfig.getoption('--aws-region')
    s3_server_bucket = pytestconfig.getoption('--s3-server-bucket')

    default_address = os.environ.get(MinioServer.ENV_ADDRESS)
    default_port = os.environ.get(MinioServer.ENV_PORT)
    default_acc_key = os.environ.get(MinioServer.ENV_ACCESS_KEY)
    default_secret_key = os.environ.get(MinioServer.ENV_SECRET_KEY)
    default_region = MinioServer.DEFAULT_REGION
    default_bucket = os.environ.get(MinioServer.ENV_BUCKET)

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
        server = MinioWrapper(tempdir)
    return server


def create_gs_server(tmpdir):
    endpoint = os.environ.get('GS_SERVER_ADDRESS_FOR_TEST')
    bucket = os.environ.get('GS_BUCKET_FOR_TEST')
    credentials_file = os.environ.get('GS_CREDENTIALS_FILE')

    if endpoint is not None and bucket is not None:
        return GSFront(endpoint, bucket, credentials_file)
    return GSServerImpl(tmpdir)


@pytest.fixture(scope="function")
async def s3_server(request, pytestconfig, tmpdir):
    server = create_s3_server(pytestconfig, tmpdir)
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

