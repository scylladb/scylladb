#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Minio server for testing.
   Provides helpers to setup and manage minio server for testing.
"""
import os
import asyncio
from asyncio.subprocess import Process
from typing import Optional
import pathlib
import subprocess
import shutil
import time
import socket
from io import BufferedWriter

class MinioServer:
    log_file: BufferedWriter

    def __init__(self, tempdir, hosts, logger):
        self.hosts = hosts
        self.srv_exe = shutil.which('minio')
        self.address = None
        self.port = 9000
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

    async def start(self):
        if self.srv_exe is None:
            self.logger.info("Minio not installed, get it from https://dl.minio.io/server/minio/release/linux-amd64/minio and put into PATH")
            return

        self.address = await self.hosts.lease_host()
        self.log_file = self.log_filename.open("wb")
        os.environ['MINIO_SERVER_ADDRESS'] = f'{self.address}'

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
            self.logger.info(f'Configuring bucket {self.bucket_name}')
            try:
                subprocess.check_call(['mc', '-C', self.mcdir, 'config', 'host', 'rm', 'local'], stdout=self.log_file, stderr=self.log_file)
            except:
                pass

            subprocess.check_call(['mc', '-C', self.mcdir, 'config', 'host', 'add', 'local', f'http://{self.address}:{self.port}', self.default_user, self.default_pass], stdout=self.log_file, stderr=self.log_file)
            subprocess.check_call(['mc', '-C', self.mcdir, 'mb', f'local/{self.bucket_name}'], stdout=self.log_file, stderr=self.log_file)
            subprocess.check_call(['mc', '-C', self.mcdir, 'anonymous', 'set', 'public', f'local/{self.bucket_name}'], stdout=self.log_file, stderr=self.log_file)
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
            shutil.rmtree(self.rootdir)
