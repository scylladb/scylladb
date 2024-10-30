#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from test.pylib.host_registry import HostRegistry
from test.pylib.minio_server import MinioServer
from test.pylib.s3_proxy import S3ProxyServer
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
        sleep_for = 0.01
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
            time.sleep(sleep_for)
            sleep_for *=sleep_for

    def cleanup(self) -> None:
        """
        Fake method to have the same interfaces with Controller class.
        """
        pass

    def __enter__(self):
        self.prepare()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class PrepareMainProcessEnv:
    """
    A class responsible for starting additional services needed by tests.

    It starts up a Minio server and an S3 mock server.
    The environment settings are saved to a file for later consumption by child processes.
    Class ensures that the necessary subdirectories exist or clean it if it exists
    """
    def __init__(self, temp_dir: Path, modes: list[str], env_file: Path):
        self.temp_dir = temp_dir
        pytest_dirs = [self.temp_dir / mode / 'pytest' for mode in modes]
        for directory in [self.temp_dir, *pytest_dirs]:
            if not directory.exists():
                os.makedirs(directory, exist_ok=True)
            else:
                shutil.rmtree(directory)
        self.env_file = env_file
        hosts = HostRegistry()
        self.loop = asyncio.new_event_loop()
        address_minio = self.loop.run_until_complete(hosts.lease_host())
        address_s3_mock = self.loop.run_until_complete(hosts.lease_host())
        self.address_s3_proxy = self.loop.run_until_complete(hosts.lease_host())
        self.minio = MinioServer(self.temp_dir, address_minio, LogPrefixAdapter(logging.getLogger('minio'), {'prefix': 'minio'}))
        self.mock_s3 = MockS3Server(address_s3_mock, 2012,
                                    LogPrefixAdapter(logging.getLogger('s3_mock'), {'prefix': 's3_mock'}))
        # S3 proxy initialized later because it needs to know Minis address and port that will be available only after
        # Minio will start
        self.proxy_s3 = None

    def prepare(self) -> None:
        """
        Start the S3 mock server and MinIO for the tests.
        Create a file with environment variables for connecting to them.
        """

        tasks = [
            self.loop.create_task(self.minio.start()),
            self.loop.create_task(self.mock_s3.start()),
        ]
        self.loop.run_until_complete(asyncio.gather(*tasks))
        envs = self.minio.get_envs_settings()
        envs.update(self.mock_s3.get_envs_settings())
        minio_uri = "http://" + envs[self.minio.ENV_ADDRESS] + ":" + envs[self.minio.ENV_PORT]
        self.proxy_s3 = S3ProxyServer(self.address_s3_proxy, 9002, minio_uri, 3, int(time.time()),
                                      LogPrefixAdapter(logging.getLogger('s3_proxy'), {'prefix': 's3_proxy'}))
        self.loop.run_until_complete(self.proxy_s3.start())
        envs.update(self.proxy_s3.get_envs_settings())
        with open(self.env_file, 'w') as file:
            for key, value in envs.items():
                file.write(f"{key}={value}\n")

    def cleanup(self) -> None:
        """
        Stop the S3 mock server and MinIO
        Remove the file with environment variables to not mess for consecutive runs.
        """
        tasks = [
            self.loop.create_task(self.minio.stop()),
            self.loop.create_task(self.mock_s3.stop()),
            self.loop.create_task(self.proxy_s3.stop()),
        ]
        self.loop.run_until_complete(asyncio.gather(*tasks))
        if os.path.exists(self.env_file):
            self.env_file.unlink()

    def __enter__(self):
        try:
            self.prepare()
        except Exception:
            self.cleanup()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

@contextmanager
def get_env_manager(temp_dir: Path, is_worker: bool, modes: list[str]) -> Generator[None, Any, None]:
    """
    xdist helps to execute test in parallel.
    For that purpose it creates one main controller and workers.
    Pytest itself doesn't know if it's a worker or controller, so it will execute all fixtures and methods.
    Tests need S3 mock server and minio to start only once for the whole run, since they can share the one instance and
    share the environment variables with workers.

    So the part of starting the servers executes on non-workers' processes.
    That means when xdist isn't used, servers start as intended in the main process.
    Tests on workers should know the endpoints of the servers, so the controller prepares this information.
    According classes responsible for configuration controller and workers.
    """
    env_file = Path(f"{temp_dir}/services_envs").absolute()
    if is_worker:
        with PrepareChildProcessEnv(env_file):
            yield
    else:
        with PrepareMainProcessEnv(temp_dir, modes, env_file):
            yield
