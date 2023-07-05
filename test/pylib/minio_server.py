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

    async def start(self):
        if self.srv_exe is None:
            self.logger.info("Minio not installed, get it from https://dl.minio.io/server/minio/release/linux-amd64/minio and put into PATH")
            return

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
            self.log_to_file(f'Configuring access to {self.address}:{self.port}')
            await self.mc('config', 'host', 'rm', 'local', ignore_failure=True)
            # wait for the server to be ready when running the first command which should not fail
            await self.mc('config', 'host', 'add', 'local', f'http://{self.address}:{self.port}', self.default_user, self.default_pass, timeout=30)
            self.log_to_file(f'Configuring bucket {self.bucket_name}')
            await self.mc('mb', f'local/{self.bucket_name}')
            await self.mc('anonymous', 'set', 'public', f'local/{self.bucket_name}')

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
