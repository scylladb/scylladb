#!/usr/bin/python3
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""S3 server for testing, backed by the Adobe S3Mock container.

   Provides helpers to setup and manage the S3 endpoint the tests run against.

   Not to be confused with test/pylib/s3_server_mock.py, which is a tiny
   hand-written HTTP server used to inject S3 errors; this one is a full S3
   implementation the tests store real data in.
"""
import argparse
import asyncio
import logging
import os
import random
import string

import boto3

from test.pylib.dockerized_service import DockerizedServer


def create_conf(url: str, region: str):
    """Object storage endpoint configuration, as scylla's object-storage.yaml wants it."""
    endpoint = {'name': url,
                # don't put credentials here. We're exporing env vars, which should
                # be picked up properly by scylla.
                # https://github.com/scylladb/scylla-pkg/issues/3845
                #'aws_access_key_id': acc_key,
                #'aws_secret_access_key': secret_key,
                'aws_region': region,
                'iam_role_arn': '',
                'type': 's3',
                }
    return [endpoint]


class S3MockServer:
    ENV_ADDRESS = 'S3_SERVER_ADDRESS_FOR_TEST'
    ENV_PORT = 'S3_SERVER_PORT_FOR_TEST'
    ENV_BUCKET = 'S3_BUCKET_FOR_TEST'
    ENV_ACCESS_KEY = 'AWS_ACCESS_KEY_ID'
    ENV_SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
    DEFAULT_REGION = 'local'

    IMAGE = 'docker.io/adobe/s3mock:5.2.0'
    # The port S3Mock serves plain HTTP on inside the container. The host port is
    # picked by the container runtime, see DockerizedServer.
    IMAGE_PORT = 9090
    # S3Mock is a Spring Boot application packaged with a buildpack whose memory
    # calculator derives the heap size from the container's memory limit, which on
    # an unconstrained test machine yields an absurd -Xmx. Objects live on disk, so
    # cap the heap at something a test service has no business exceeding.
    JAVA_TOOL_OPTIONS = '-Xmx512m'
    STARTED_MESSAGE = 'Started S3MockApplication'

    def __init__(self, log_dir, logger):
        """
        `log_dir` must be a CI-archived directory (testlog) so that the
        container's log survives for post-mortem analysis.
        """
        self.log_dir = log_dir
        self.logger = logger
        self.server = None
        self.address = None
        self.port = None
        self.bucket_name = 'testbucket'
        # S3Mock does not authenticate anything, but scylla still needs credentials
        # to sign its requests with, so hand it a random pair unless the environment
        # already carries one (which the KMS tests rely on, see aws_kms_fixture.hh).
        self.access_key = os.environ.get(self.ENV_ACCESS_KEY, ''.join(random.choice(string.hexdigits) for i in range(16)))
        self.secret_key = os.environ.get(self.ENV_SECRET_KEY, ''.join(random.choice(string.hexdigits) for i in range(32)))
        self.old_env = dict()

    def __repr__(self):
        return f"[s3mock] {self.address}:{self.port}/{self.bucket_name}"

    @property
    def uri(self):
        return f'http://{self.address}:{self.port}'

    def _docker_args(self, host, port):
        # pylint: disable=unused-argument
        return ['-e', f'JAVA_TOOL_OPTIONS={self.JAVA_TOOL_OPTIONS}']

    def _create_bucket(self):
        resource = boto3.resource('s3',
                                  endpoint_url=self.uri,
                                  aws_access_key_id=self.access_key,
                                  aws_secret_access_key=self.secret_key,
                                  aws_session_token=None,
                                  region_name=self.DEFAULT_REGION,
                                  config=boto3.session.Config(signature_version='s3v4'),
                                  verify=False)
        resource.Bucket(self.bucket_name).create()

    def _set_environ(self):
        self.old_env = dict(os.environ)
        os.environ[self.ENV_ADDRESS] = f'{self.address}'
        os.environ[self.ENV_PORT] = f'{self.port}'
        os.environ[self.ENV_BUCKET] = f'{self.bucket_name}'
        os.environ[self.ENV_ACCESS_KEY] = f'{self.access_key}'
        os.environ[self.ENV_SECRET_KEY] = f'{self.secret_key}'

    def _get_environs(self):
        return [self.ENV_ADDRESS,
                self.ENV_PORT,
                self.ENV_BUCKET,
                self.ENV_ACCESS_KEY,
                self.ENV_SECRET_KEY]

    def get_envs_settings(self):
        return {key: os.environ[key] for key in self._get_environs()}

    def _unset_environ(self):
        for env in self._get_environs():
            if value := self.old_env.get(env):
                os.environ[env] = value
            else:
                del os.environ[env]

    def print_environ(self):
        msgs = []
        for key in self._get_environs():
            value = os.environ[key]
            msgs.append(f'export {key}={value}')
        print('\n'.join(msgs))

    async def start(self):
        self.logger.info('Starting %s', self.IMAGE)
        server = DockerizedServer(self.IMAGE,
                                  self.log_dir,
                                  logfilenamebase='s3mock',
                                  docker_args=self._docker_args,
                                  success_string=self.STARTED_MESSAGE,
                                  failure_string='address already in use',
                                  port=self.IMAGE_PORT)
        await server.start()
        # Assigned only once the container is up, so that stop() has nothing to do
        # if we never got that far.
        self.server = server
        self.address = server.host
        self.port = server.port
        self._set_environ()
        try:
            self.logger.info('Creating bucket %s on %s', self.bucket_name, self.uri)
            self._create_bucket()
        except Exception:
            await self.stop()
            raise

    async def stop(self):
        if self.server is None:
            return

        self.logger.info('Stopping s3mock server')
        # so the test's process environment is not polluted by a test case
        # which launches the S3MockServer by itself.
        self._unset_environ()
        try:
            await self.server.stop()
        finally:
            self.logger.info('Stopped s3mock server')
            self.server = None


async def main():
    parser = argparse.ArgumentParser(description="Start an S3Mock server")
    parser.add_argument('--logdir', default='.')
    args = parser.parse_args()
    server = S3MockServer(args.logdir, logging.getLogger('s3mock'))
    await server.start()
    server.print_environ()
    try:
        _ = input('server started. press any key to stop: ')
    except KeyboardInterrupt:
        pass
    finally:
        await server.stop()

if __name__ == '__main__':
    asyncio.run(main())
