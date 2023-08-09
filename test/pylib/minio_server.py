#!/usr/bin/python3
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Minio server for testing.
   Provides helpers to setup and manage minio server for testing.
"""
import os
import argparse
import asyncio
from asyncio.subprocess import Process
from typing import Optional
import json
import logging
import pathlib
import subprocess
import shutil
import time
import tempfile
import socket
from io import BufferedWriter

class MinioServer:
    log_file: BufferedWriter

    def __init__(self, tempdir_base, hosts, logger):
        self.hosts = hosts
        self.srv_exe = shutil.which('minio')
        self.address = None
        self.port = 9000
        tempdir = tempfile.mkdtemp(dir=tempdir_base, prefix="minio-")
        self.tempdir = pathlib.Path(tempdir)
        self.rootdir = self.tempdir / 'minio_root'
        self.mcdir = self.tempdir / 'mc'
        self.logger = logger
        self.cmd: Optional[Process] = None
        self.default_user = 'minioadmin'
        self.default_pass = 'minioadmin'
        self.bucket_name = 'testbucket'
        self.log_filename = (self.tempdir / 'minio').with_suffix(".log")

    def check_server(self):
        s = socket.socket()
        try:
            s.connect((self.address, self.port))
            return True
        except socket.error as e:
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
        cmd = ['mc',
               '--debug',
               '--config-dir', self.mcdir]
        cmd.extend(args)

        while True:
            try:
                subprocess.check_call(cmd, stdout=self.log_file, stderr=self.log_file)
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

    def _anonymous_public_policy(self):
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

    async def _is_port_available(self, port: int) -> bool:
        try:
            _, writer = await asyncio.open_connection('localhost', port)
            writer.close()
            await writer.wait_closed()
            return False
        except OSError:
            # likely connection refused
            return True

    async def _find_available_port(self, min_port: int, max_port: int) -> int:
        assert min_port < max_port
        for port in range(min_port, max_port):
            if await self._is_port_available(port):
                return port
        raise RuntimeError(f'failed to find available port in [{min_port}, {max_port})')

    async def start(self):
        if self.srv_exe is None:
            self.logger.info("Minio not installed, get it from https://dl.minio.io/server/minio/release/linux-amd64/minio and put into PATH")
            return

        self.port = await self._find_available_port(self.port, self.port + 1000)
        self.address = await self.hosts.lease_host()
        self.log_file = self.log_filename.open("wb")
        os.environ['S3_SERVER_ADDRESS_FOR_TEST'] = f'{self.address}'
        os.environ['S3_SERVER_PORT_FOR_TEST'] = f'{self.port}'
        os.environ['S3_PUBLIC_BUCKET_FOR_TEST'] = f'{self.bucket_name}'

        self.logger.info(f'Starting minio server at {self.address}:{self.port}')
        os.mkdir(self.rootdir)
        self.cmd = await asyncio.create_subprocess_exec(
            self.srv_exe,
            *[ 'server', '--address', f'{self.address}:{self.port}', self.rootdir ],
            preexec_fn=os.setsid,
            stderr=self.log_file,
            stdout=self.log_file,
        )

        timeout = time.time() + 30
        while time.time() < timeout:
            if self.cmd.returncode:
                raise RuntimeError("Failed to start minio server")

            if self.check_server():
                self.logger.info('minio is up and running')
                break

            await asyncio.sleep(0.1)

        try:
            alias = 'local'
            self.log_to_file(f'Configuring access to {self.address}:{self.port}')
            await self.mc('config', 'host', 'rm', alias, ignore_failure=True)
            # wait for the server to be ready when running the first command which should not fail
            await self.mc('config', 'host', 'add', alias, f'http://{self.address}:{self.port}', self.default_user, self.default_pass, timeout=30)
            self.log_to_file(f'Configuring bucket {self.bucket_name}')
            await self.mc('mb', f'{alias}/{self.bucket_name}')
            with tempfile.NamedTemporaryFile(mode='w', encoding='UTF-8', suffix='.json') as policy_file:
                json.dump(self._anonymous_public_policy(), policy_file, indent=2)
                policy_file.flush()
                await self.mc('anonymous', 'set-json', policy_file.name, f'{alias}/{self.bucket_name}')

        except Exception as e:
            self.logger.info(f'MC failed: {e}')
            await self.stop()

    async def stop(self):
        self.logger.info('Killing minio server')
        if not self.cmd:
            return

        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            self.logger.info('Killed minio server')
            self.cmd = None
            shutil.rmtree(self.tempdir)


class HostRegistry:
    def __init__(self, host: str) -> None:
        self._host = host

    async def lease_host(self) -> str:
        assert self._host is not None
        host = self._host
        self._host = None
        return host

    async def release_host(self, host: str) -> None:
        assert self._host is None
        self._host = host


async def main():
    parser = argparse.ArgumentParser(description="Start a MinIO server")
    parser.add_argument('--tempdir')
    parser.add_argument('--host', default='127.0.0.1')
    args = parser.parse_args()
    host_registry = HostRegistry(args.host)
    with tempfile.TemporaryDirectory(suffix='-minio', dir=args.tempdir) as tempdir:
        if args.tempdir is None:
            print(f'{tempdir=}')
        server = MinioServer(tempdir, host_registry, logging.getLogger('minio'))
        await server.start()
        print(f'export S3_SERVER_ADDRESS_FOR_TEST={server.address}')
        print(f'export S3_SERVER_PORT_FOR_TEST={server.port}')
        print(f'export S3_PUBLIC_BUCKET_FOR_TEST={server.bucket_name}')
        try:
            _ = input('server started. press any key to stop: ')
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
