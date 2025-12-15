#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
import logging

# use minio_server
from test.pylib.minio_server import MinioServer
from test.pylib.suite.python import add_s3_options
from test.pylib.dockerized_service import DockerizedServer
from operator import attrgetter

import pytest
import boto3
import requests

def pytest_addoption(parser):
    add_s3_options(parser)


def format_tuples(tuples=None, **kwargs):
    '''format a dict to structured values (tuples) in CQL'''
    if tuples is None:
        tuples = {}
    tuples.update(kwargs)
    body = ', '.join(f"'{key}': '{value}'" for key, value in tuples.items())
    return f'{{ {body} }}'


class S3_Server:
    def __init__(self, tempdir: str, address: str, port: int, acc_key: str, secret_key: str, region: str, bucket_name):
        self.tempdir = tempdir
        self.address = address
        self.port = port
        self.acc_key = acc_key
        self.secret_key = secret_key
        self.region = region
        self.bucket_name = bucket_name

    def __repr__(self):
        return f"[unknown] {self.address}:{self.port}/{self.bucket_name}"

    @property
    def type(self):
        return 'S3'

    def create_endpoint_conf(self):
        return MinioServer.create_conf(self.address, self.port, self.region)

    def get_resource(self):
        """Creates boto3.resource object that can be used to communicate to the given server"""
        return boto3.resource('s3',
            endpoint_url=f'http://{self.address}:{self.port}',
            aws_access_key_id=self.acc_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )

    async def start(self):
        pass

    async def stop(self):
        pass

class MinioWrapper(S3_Server):
    def __init__(self, tempdir):
        self.server = MinioServer(tempdir,
                                  '127.0.0.1',
                                  logging.getLogger('minio'))
        super().__init__(
            tempdir,
            self.server.address,
            self.server.port,
            self.server.access_key,
            self.server.access_key,
            MinioServer.DEFAULT_REGION,
            self.server.bucket_name
        )

    def create_endpoint_conf(self):
        return MinioServer.create_conf(self.address, self.port, self.region)

    async def start(self):
        return self.server.start()

    async def stop(self):
        return self.server.stop()

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

    tempdir = tmpdir.strpath
    if s3_server_address:
        server = S3_Server(tempdir,
                           s3_server_address,
                           s3_server_port,
                           aws_acc_key,
                           aws_secret_key,
                           aws_region,
                           s3_server_bucket)
    elif default_address:
        server = S3_Server(tempdir,
                           default_address,
                           int(default_port),
                           default_acc_key,
                           default_secret_key,
                           default_region,
                           default_bucket)
    else:
        server = MinioWrapper(tempdir)
    return server

@pytest.fixture(scope="function")
async def s3_server(pytestconfig, tmpdir):
    server = create_s3_server(pytestconfig, tmpdir)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()

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

    async def start(self):
        pass
    async def stop(self):
        pass

class GSServer(GSFront):
    def __init__(self, tmpdir):
        super(GSServer, self).__init__(None, 'testbucket', None)
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
            v = self.oldvars[k]
            if v:
                os.environ[k] = v
            elif os.environ.get(k):
                del os.environ[k]

    def _docker_args(self, host, port):
        # pylint: disable=unused-argument
        return ["-p", f'{port}:{port}']

    def _image_args(self, host, port):
        # pylint: disable=unused-argument
        return ["-scheme", "http", "-log-level", "debug", "--port", f'{port}', '-public-host', f'127.0.0.1:{port}']

    async def start(self):
        self.server = DockerizedServer("docker.io/fsouza/fake-gcs-server:1.52.3", self.tmpdir, 
                                       logfilenamebase="fake-gcs-server",
                                       docker_args=self._docker_args,
                                       image_args=self._image_args,
                                       success_string="server started at",
                                       failure_string="address already in use"
                                       )
        await self.server.start()
        self.port = self.server.port
        self.host = self.server.host
        self.publish()

        # create bucket. can't use boto, because fake server does not support xml syntax 
        # for anything beyong listing
        response = requests.post(f'{self.endpoint}/storage/v1/b?project=testproject', json = {
            'name': self.bucket_name, 'location' : 'US', 'storageClass' : 'STANDARD',
            'iamConfiguration': {
                'uniformBucketLevelAccess' : {
                    'enabled': True,
                }
            }
        }, timeout = 10)
        if response.status_code not in [200, 201]:
            raise Exception(f'Could not create test bucket: {response}')

    async def stop(self):
        if self.server:
            await self.server.stop()
        self.unpublish()

@pytest.fixture(scope="function", params=['s3','gs'])
async def object_storage(request, pytestconfig, tmpdir):
    server = None

    if request.param == 'gs':
        endpoint = os.environ.get('GS_SERVER_ADDRESS_FOR_TEST')
        bucket = os.environ.get('GS_BUCKET_FOR_TEST')
        credentials_file = os.environ.get('GS_CREDENTIALS_FILE')

        if endpoint is not None and bucket is not None:
            server = GSFront(endpoint, bucket, credentials_file)
        else:
            server = GSServer(tmpdir)
    else:
        server = create_s3_server(pytestconfig, tmpdir)

    try:
        await server.start()
        yield server
    finally:
        await server.stop()

@pytest.fixture(scope="function")
async def s3_storage(pytestconfig, tmpdir):
    server = create_s3_server(pytestconfig, tmpdir)

    try:
        await server.start()
        yield server
    finally:
        await server.stop()