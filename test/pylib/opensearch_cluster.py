#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""OpenSearch cluster for testing.
   Provides helpers to setup and manage OpenSearch cluster for testing.
"""
from opensearchpy import OpenSearch
import os
import argparse
import asyncio
from asyncio.subprocess import Process
from typing import Generator, Optional
import logging
import pathlib
import random
import shutil
import time
import tempfile
import socket
import yaml
from io import BufferedWriter

class OpenSearchCluster:

    ENV_JAVA_HOME = 'OPENSEARCH_JAVA_HOME'
    ENV_CONF_PATH = 'OPENSEARCH_PATH_CONF'
    ENV_ADDRESS = 'OPENSEARCH_ADDRESS'
    ENV_PORT = 'OPENSEARCH_PORT'

    log_file: BufferedWriter

    def __init__(self, tempdir_base, address, logger):
        self.opensearch_dir = next(
            (f for f in pathlib.Path('/opt/scylladb/dependencies/').iterdir() 
             if f.name.startswith("opensearch-") and f.is_dir()), 
            pathlib.Path()
        )
        self.os_exe = shutil.which('opensearch', path=self.opensearch_dir / 'bin')
        self.address = address
        self.port = None
        tempdir = tempfile.mkdtemp(dir=tempdir_base, prefix="opensearch-")
        self.tempdir = pathlib.Path(tempdir)
        self.rootdir = self.tempdir / 'opensearch_data'
        self.config_dir = self.tempdir / 'config'
        self.config_file = self.config_dir / 'opensearch.yml'
        self.logger = logger
        self.cmd: Optional[Process] = None
        self.log_filename = (self.tempdir / 'opensearch').with_suffix(".log")
        self.old_env = dict()

    def __repr__(self):
        return f"[opensearch] {self.address}:{self.port}"

    def check_server(self, port):
        s = socket.socket()
        try:
            s.connect((self.address, port))
            return True
        except socket.error:
            return False
        finally:
            s.close()

    def _get_local_ports(self, num_ports: int) -> Generator[int, None, None]:
        with open('/proc/sys/net/ipv4/ip_local_port_range', encoding='ascii') as port_range:
            min_port, max_port = map(int, port_range.read().split())
        for _ in range(num_ports):
            yield random.randint(min_port, max_port)

    def create_conf_file(self, address: str, port: int, path: str):
        with open(path, 'w', encoding='ascii') as config_file:
            config = {
                'network.host': address,
                'http.port': port,
                'plugins.security.disabled': True,
                'path.data': str(self.rootdir),
            }
            yaml.dump(config, config_file)

    async def _run_cluster(self, port):
        self.logger.info(f'Starting OpenSearch cluster at {self.address}:{port}')
        cmd = await asyncio.create_subprocess_exec(
            self.os_exe,
            stdout=self.log_file,
            stderr=self.log_file,
            start_new_session=True
        )
        timeout = time.time() + 30
        while time.time() < timeout:
            if cmd.returncode is not None:
                self.logger.info('OpenSearch exited with %s', cmd.returncode)
                raise RuntimeError("Failed to start OpenSearch cluster")
            if self.check_server(port):
                self.logger.info('Opensearch is up and running')
                break

            await asyncio.sleep(0.1)

        return cmd

    def _set_environ(self):
        self.old_env = dict(os.environ)
        os.environ[self.ENV_JAVA_HOME] = f'{self.opensearch_dir}/jdk'
        os.environ[self.ENV_CONF_PATH] = f'{self.config_dir}'
        os.environ[self.ENV_ADDRESS] = f'{self.address}'
        os.environ[self.ENV_PORT] = f'{self.port}'

    def _get_environs(self):
        return [self.ENV_JAVA_HOME, self.ENV_CONF_PATH, self.ENV_ADDRESS, self.ENV_PORT]

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
        if self.os_exe is None:
            self.logger.error("OpenSearch not installed")
            shutil.rmtree(self.tempdir)
            return

        self.log_file = self.log_filename.open("wb")
        os.mkdir(self.rootdir)
        shutil.copytree(self.opensearch_dir / 'config', self.config_dir)

        retries = 42  # just retry a fixed number of times
        for port in self._get_local_ports(retries):
            try:
                self.port = port
                self.create_conf_file(self.address, port, self.config_file)
                self._set_environ()
                self.cmd = await self._run_cluster(port)
            except RuntimeError:
                pass
            else:
                break
        else:
            self.logger.error("Failed to start OpenSearch cluster")
            return


    async def stop(self):
        self.logger.info('Killing OpenSearch cluster')
        if not self.cmd:
            return

        self._unset_environ()
        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            self.logger.info('Killed OpenSearch cluster')
            self.cmd = None
            shutil.rmtree(self.tempdir)


async def main():
    parser = argparse.ArgumentParser(description="Start a OpenSearch cluster")
    parser.add_argument('--tempdir')
    parser.add_argument('--host', default='127.0.0.1')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(suffix='-opensearch', dir=args.tempdir) as tempdir:
        if args.tempdir is None:
            print(f'{tempdir=}')
        server = OpenSearchCluster(tempdir, args.host, logging.getLogger('opensearch'))
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
