#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""OpenSearch cluster for testing.
   Provides helpers to setup and manage OpenSearch cluster for testing.
"""
import argparse
import asyncio
from asyncio.subprocess import Process
from typing import Optional
import logging
import pathlib
import shutil
import time
import tempfile
import urllib.request
from io import BufferedWriter

class OpenSearchCluster:

    log_file: BufferedWriter

    def __init__(self, tempdir_base, address, logger):
        self.compose_file = next(
            (f for f in pathlib.Path('./test/').rglob('opensearch.yml') if f.is_file())
        )
        self.docker_exe = shutil.which('podman')
        self.address = address
        self.port = 9200
        tempdir = tempfile.mkdtemp(dir=tempdir_base, prefix="opensearch-")
        self.tempdir = pathlib.Path(tempdir)
        self.logger = logger
        self.cmd: Optional[Process] = None
        self.log_filename = (self.tempdir / 'opensearch').with_suffix(".log")

    def __repr__(self):
        return f"[opensearch] {self.address}:{self.port}"

    def check_server(self, port):
        try:
            with urllib.request.urlopen(f'http://{self.address}:{port}', timeout=2) as response:
                return response.status == 200
        except Exception:
            return False

    async def _run_cluster(self, port):
        self.logger.info(f'Starting OpenSearch cluster at {self.address}:{port}')
        cmd = await asyncio.create_subprocess_exec(
            self.docker_exe,
            *['compose',
            '-f',
            self.compose_file,
            'up'],
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

            await asyncio.sleep(0.2)

        return cmd

    async def start(self):
        if self.docker_exe is None:
            self.logger.error("Container engine is not installed or not in PATH")
            shutil.rmtree(self.tempdir)
            return

        if self.compose_file:
            self.logger.info(f"Found opensearch.yml at: {self.compose_file}")
        else:
            self.logger.error("opensearch.yml not found in ./test/** directory")
            shutil.rmtree(self.tempdir)
            return

        self.log_file = self.log_filename.open("wb")
        self.cmd = await self._run_cluster(self.port)

    async def stop(self):
        self.logger.info('Stopping and removing OpenSearch container')
        if not self.cmd:
            return

        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            self.logger.info('Stopped OpenSearch cluster')
            cmd_stop = await asyncio.create_subprocess_exec(
                self.docker_exe,
                *['compose',
                '-f',
                self.compose_file,
                'down'],
                stdout=self.log_file,
                stderr=self.log_file,
            )
            await cmd_stop.communicate()
            self.logger.info('Removed OpenSearch container')
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
        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()], format='%(message)s')
        server = OpenSearchCluster(tempdir, args.host, logging.getLogger('opensearch'))
        await server.start()
        try:
            _ = input('server started. press any key to stop: ')
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()

if __name__ == '__main__':
    asyncio.run(main())
