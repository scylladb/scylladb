#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path

from test.pylib.host_registry import HostRegistry
from test.pylib.minio_server import MinioServer
from test.pylib.s3_server_mock import MockS3Server
from test.pylib.util import LogPrefixAdapter


class PrepareChildProcessEnv:
    """
    Class responsible to get environment variables from the main thread through the shared file and set them for the process
    """
    def __init__(self, env_file: Path):
        self.env_file = env_file

    def prepare(self) -> None:
        """
        Read the environment variables for S3 and MinIO from the file and set them for the process.
        """
        timeout = 10
        start_time = time.time()
        while True:
            if os.path.exists(self.env_file):
                with open(self.env_file, 'r') as file:
                    for line in file.readlines():
                        key, value = line.strip().split('=', 1)
                        os.environ[key] = value
                break

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for file {self.env_file}")
            # Sleep needed to wait when the controller will create a file with environment variables.
            # Without sleep checking of the file existence will be too fast,
            # so it will finish before the file is created
            time.sleep(0.5)

    def cleanup(self) -> None:
        """
        Fake method to have the same interfaces with Controller class.
        """
        pass


class PrepareMainProcessEnv:
    """
    A class responsible for starting additional services needed by tests.

    It starts up a Minio server and an S3 mock server.
    The environment settings are saved to a file for later consumption by child processes.
    """
    def __init__(self, temp_dir: Path, modes: list[str], env_file: Path):
        self.temp_dir = temp_dir
        pytest_dirs = [self.temp_dir / mode / 'pytest' for mode in modes]
        for directory in [self.temp_dir, *pytest_dirs]:
            if not directory.exists():
                directory.mkdir()
        self.env_file = env_file
        hosts = HostRegistry()
        self.loop = asyncio.new_event_loop()
        address = self.loop.run_until_complete(hosts.lease_host())
        self.ms = MinioServer(self.temp_dir, '127.0.0.1', LogPrefixAdapter(logging.getLogger('minio'), {'prefix': 'minio'}))
        self.mock_s3 = MockS3Server(address, 2012,
                                    LogPrefixAdapter(logging.getLogger('s3_mock'), {'prefix': 's3_mock'}))

    def prepare(self) -> None:
        """
        Start the S3 mock server and MinIO for the tests.
        Create a file with environment variables for connecting to them.
        """
        tasks = [self.loop.create_task(self.ms.start()), self.loop.create_task(self.mock_s3.start())]
        self.loop.run_until_complete(asyncio.gather(*tasks))
        envs = self.ms.get_envs_settings()
        envs.update(self.mock_s3.get_envs_settings())
        with open(self.env_file, 'w') as file:
            for key, value in envs.items():
                file.write(f"{key}={value}\n")

    def cleanup(self) -> None:
        """
        Stop the S3 mock server and MinIO
        Remove the file with environment variables to not mess for consecutive runs.
        """
        tasks = [self.loop.create_task(self.ms.stop()), self.loop.create_task(self.mock_s3.stop())]
        self.loop.run_until_complete(asyncio.gather(*tasks))
        if os.path.exists(self.env_file):
            self.env_file.unlink()

def get_prepare_instance(temp_dir: Path, is_worker: bool, modes: list[str]) -> PrepareChildProcessEnv | PrepareMainProcessEnv:
    """
    xdist helps to execute test in parallel.
    For that purpose it creates one main controller and workers.
    Pytest itself doesn't know if it's a worker or controller, so it will execute all fixtures and methods.
    Tests need S3 mock server and minio to start only once for the whole run, since they can share the one instance and
    share the environment variables with workers.

    So the part of starting the servers executes on non-workers' machines.
    That means when xdist isn't used, servers start as intended in the main process.
    Tests on workers should know the endpoints of the servers, so the controller prepares this information.
    According classes responsible for configuration controller and workers.
    """
    env_file = Path(f"{temp_dir}/minio_env").absolute()
    if is_worker:
        return PrepareChildProcessEnv(env_file)
    else:
        return PrepareMainProcessEnv(temp_dir, modes, env_file)