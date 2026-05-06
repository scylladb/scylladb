#!/usr/bin/python3
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Minio server for testing.
   Provides helpers to setup and manage minio server for testing.
"""
import os
import argparse
import asyncio
from asyncio.subprocess import Process
from typing import Generator, Optional
import json
import logging
import pathlib
import random
import subprocess
import shutil
import time
import tempfile
import socket
import string
import yaml
from io import BufferedWriter
from test.pylib.nested_container import (
    MINIO_IMAGE, ensure_image, run_container, kill_container, rm_container,
    mc_in_container, _runtime
)

class MinioServer:
    ENV_ADDRESS = 'S3_SERVER_ADDRESS_FOR_TEST'
    ENV_PORT = 'S3_SERVER_PORT_FOR_TEST'
    ENV_BUCKET = 'S3_BUCKET_FOR_TEST'
    ENV_ACCESS_KEY = 'AWS_ACCESS_KEY_ID'
    ENV_SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
    DEFAULT_REGION = 'local'

    log_file: BufferedWriter

    def __init__(self, tempdir_base, address, logger):
        self.address = address
        self.port = None
        self.tempdir_base = pathlib.Path(tempdir_base)
        tempdir = tempfile.mkdtemp(dir=tempdir_base, prefix="minio-")
        self.tempdir = pathlib.Path(tempdir)
        self.rootdir = self.tempdir / 'minio_root'
        self.mcdir = self.tempdir / 'mc'
        self.logger = logger
        self.container_name: Optional[str] = None
        self.cmd: Optional[Process] = None
        self.default_user = 'minioadmin'
        self.default_pass = 'minioadmin'
        self.bucket_name = 'testbucket'
        self.access_key = os.environ.get(self.ENV_ACCESS_KEY, ''.join(random.choice(string.hexdigits) for i in range(16)))
        self.secret_key = os.environ.get(self.ENV_SECRET_KEY, ''.join(random.choice(string.hexdigits) for i in range(32)))
        self.log_filename = (self.tempdir_base / 'minio').with_suffix(".log")
        self.old_env = dict()
        self.default_config = None

    def __repr__(self):
        return f"[minio] {self.address}:{self.port}/{self.bucket_name}"

    def check_server(self, port):
        s = socket.socket()
        try:
            s.connect((self.address, port))
            return True
        except socket.error:
            return False
        finally:
            s.close()

    def log_to_file(self, str):
        self.log_file.write(str.encode())
        self.log_file.write('\n'.encode())
        self.log_file.flush()

    async def mc(self, *args, ignore_failure=False, timeout=0):
        retry_until: float = time.time() + timeout
        retry_step: float = 0.1

        while True:
            try:
                mc_in_container(MINIO_IMAGE, ['--debug'] + list(args),
                                config_dir=str(self.mcdir),
                                container_name=self.container_name,
                                stdout=self.log_file, stderr=self.log_file)
            except subprocess.CalledProcessError:
                if ignore_failure:
                    self.log_to_file('ignoring')
                    break
                command = ' '.join(args)
                self.log_to_file(f'failed to run "mc {command}"')
                if time.time() >= retry_until:
                    raise
                self.log_to_file(f'retry after {retry_step} seconds')
                await asyncio.sleep(retry_step)
            else:
                break

    def _bucket_policy(self):
        # the default anonymous public policy does not allow us to access
        # the taggings, so let's add the tagging actions manually.
        #
        # the original access policy is dumped using:
        #   mc anonymous set public local/testbucket
        #   mc anonymous get-json local/testbucket
        #
        # we added following actions to the policy for accessing objects in the
        # bucket created for testing:
        # - GetObjectTagging
        # - PutObjectTagging
        # - DeleteObjectTagging
        #
        # the full list of actions can be found at
        #   https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html
        bucket_actions = [
            "s3:ListBucket",
            "s3:ListBucketMultipartUploads",
            "s3:GetBucketLocation",
        ]
        object_actions = [
            "s3:AbortMultipartUpload",
            "s3:DeleteObject",
            "s3:GetObject",
            "s3:ListMultipartUploadParts",
            "s3:PutObject",
            "s3:GetObjectTagging",
            "s3:PutObjectTagging",
            "s3:DeleteObjectTagging"
        ]
        statement = [
            {
                'Action': bucket_actions,
                'Effect': 'Allow',
                'Principal': {'AWS': ['*']},
                'Resource': [ f'arn:aws:s3:::{self.bucket_name}' ]
            },
            {
                'Action': object_actions,
                'Effect': 'Allow',
                'Principal': {'AWS': ['*']},
                'Resource': [ f'arn:aws:s3:::{self.bucket_name}/*' ]
            }
        ]
        return {'Statement': statement,
                'Version': '2012-10-17'}

    def _get_local_ports(self, num_ports: int) -> Generator[int, None, None]:
        with open('/proc/sys/net/ipv4/ip_local_port_range', encoding='ascii') as port_range:
            min_port, max_port = map(int, port_range.read().split())
        for _ in range(num_ports):
            yield random.randint(min_port, max_port)

    @staticmethod
    def create_conf(url: str, region: str):
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

    async def _run_server(self, port):
        self.logger.info(f'Starting minio server container at {self.address}:{port}')
        self.container_name = f'minio-test-{port}'

        # Remove any stale container with the same name
        rm_container(self.container_name)

        cmd = await asyncio.create_subprocess_exec(
            _runtime(), 'run', '--rm',
            '--name', self.container_name,
            '--network', 'host',
            '-e', 'MINIO_BROWSER=off',
            '-e', 'MINIO_FS_OSYNC=off',
            '-v', f'{self.rootdir}:/data',
            MINIO_IMAGE,
            'server', '--address', f'{self.address}:{port}', '/data',
            preexec_fn=os.setsid,
            stderr=self.log_file,
            stdout=self.log_file,
        )
        timeout = time.time() + 30
        while time.time() < timeout:
            if cmd.returncode is not None:
                self.logger.info('minio exited with %s', cmd.returncode)
                raise RuntimeError("Failed to start minio server")
            if self.check_server(port):
                self.logger.info('minio is up and running')
                break

            await asyncio.sleep(0.1)

        return cmd

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
        from test import TOP_SRC_DIR
        containerfile_dir = str(TOP_SRC_DIR / 'tools' / 'toolchain' / 'containers' / 'minio')
        try:
            ensure_image(MINIO_IMAGE, containerfile_dir=containerfile_dir)
        except Exception as e:
            raise RuntimeError(f"Minio container image not available: {e}") from e

        self.log_file = self.log_filename.open("wb")
        os.mkdir(self.rootdir)

        retries = 42  # just retry a fixed number of times
        for port in self._get_local_ports(retries):
            try:
                self.cmd = await self._run_server(port)
                self.port = port
            except RuntimeError:
                pass
            else:
                break
        else:
            raise RuntimeError("Failed to start Minio server after all retries")

        self._set_environ()

        try:
            alias = 'local'
            self.log_to_file(f'Configuring access to {self.address}:{self.port}')
            await self.mc('alias', 'rm', alias, ignore_failure=True)
            # wait for the server to be ready when running the first command which should not fail
            await self.mc('alias', 'set', alias, f'http://{self.address}:{self.port}', self.default_user, self.default_pass, timeout=30)
            self.log_to_file(f'Creating user with key {self.access_key}')
            await self.mc('admin', 'user', 'add', alias, self.access_key, self.secret_key)
            self.log_to_file(f'Configuring bucket {self.bucket_name}')
            await self.mc('mb', f'{alias}/{self.bucket_name}')
            # Write policy file into the container's mounted volume so it's
            # accessible from inside the container at /data/
            policy_path = pathlib.Path(self.rootdir) / 'policy.json'
            policy_path.write_text(json.dumps(self._bucket_policy(), indent=2), encoding='UTF-8')
            try:
                await self.mc('admin', 'policy', 'create', alias, 'test-policy', '/data/policy.json')
            finally:
                policy_path.unlink(missing_ok=True)
            await self.mc('admin', 'policy', 'attach', alias, 'test-policy', '--user', self.access_key)

        except Exception as e:
            self.logger.error(f'MC failed: {e}')
            await self.stop()
            raise RuntimeError(f"Failed to configure Minio server: {e}") from e

    async def stop(self):
        self.logger.info('Killing minio server container')
        if not self.cmd:
            return

        self._unset_environ()
        try:
            if self.container_name:
                kill_container(self.container_name)
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            self.logger.info('Killed minio server')
            self.cmd = None
            self.container_name = None
            shutil.rmtree(self.tempdir)


async def main():
    parser = argparse.ArgumentParser(description="Start a MinIO server")
    parser.add_argument('--tempdir')
    parser.add_argument('--host', default='127.0.0.1')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(suffix='-minio', dir=args.tempdir) as tempdir:
        if args.tempdir is None:
            print(f'{tempdir=}')
        server = MinioServer(tempdir, args.host, logging.getLogger('minio'))
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
