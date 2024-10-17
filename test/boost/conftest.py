import asyncio
import logging
import os
import shlex
import time
from pathlib import PosixPath, Path
from typing import Dict

import pytest
import xdist
import yaml
from _pytest.nodes import Collector

from test.pylib.cpp.item import CppFile
from test.pylib.host_registry import HostRegistry
from test.pylib.minio_server import MinioServer
from test.pylib.s3_server_mock import MockS3Server
from test.pylib.util import LogPrefixAdapter

all_modes = {'debug': 'Debug', 'release': 'RelWithDebInfo', 'dev': 'Dev', }


def pytest_addoption(parser):
    parser.addoption('--mode', action='store', required=True,
                     help='Scylla build mode. Tests can use it to adjust their behavior.')
    parser.addoption("--tmpdir", action="store", default="testlog", help="""Path to temporary test data and log files. The data is
            further segregated per build mode. Default: ./testlog.""", )


def read_suite_config():
    with open(Path(__file__).resolve().parent / 'suite.yaml', "r") as cfg_file:
        cfg = yaml.safe_load(cfg_file.read())
        if not isinstance(cfg, dict):
            raise RuntimeError("Failed to load tests in {}: suite.yaml is empty")
        return cfg


def pytest_collect_file(file_path: PosixPath, parent: Collector):
    config = parent.config
    suite_config = read_suite_config()
    mode = config.getoption('mode')
    disabled_tests = set(suite_config.get("disable", []))
    disabled_tests.update(suite_config.get("skip_in_" + mode, []))
    run_in_m = set(suite_config.get("run_in_" + mode, []))

    for a in all_modes:
        if a == mode:
            continue
        skip_in_m = set(suite_config.get("run_in_" + a, []))
        disabled_tests.update(skip_in_m - run_in_m)
    args = ['--', '--overprovisioned', '--unsafe-bypass-fsync 1', '--kernel-page-cache 1',
        '--blocked-reactor-notify-ms 2000000', '--collectd 0', '--max-networking-io-control-blocks=100', ]
    custom_args = suite_config.get("custom_args", {})
    if file_path.suffix == ".cc" and '.inc' not in file_path.suffixes:
        no_parallel_run = False
        test_name = file_path.stem
        if test_name in disabled_tests:
            return None
        if test_name in suite_config.get('no_parallel_cases'):
            no_parallel_run = True
        project_root = Path(__file__).resolve().parent.parent.parent
        executable = Path(f'{project_root}/build/{mode}/test/boost') / file_path.stem
        temp_dir = project_root / config.getoption('tmpdir')

        args.extend(custom_args.get(file_path.stem, ['-c2', '-m2G']))
        args = ' '.join(args)
        args = shlex.split(args)
        return CppFile.from_parent(parent=parent, mode=mode, temp_dir=temp_dir, executable=executable, arguments=args,
                                   no_parallel_run=no_parallel_run, path=file_path)


@pytest.hookimpl(wrapper=True)
def pytest_runtestloop(session):
    is_worker = xdist.is_xdist_worker(session)
    temp_dir = Path(session.config.getoption('tmpdir'))
    mode = session.config.getoption('mode')

    prepare_instance = get_prepare_instance(temp_dir, is_worker, mode)
    prepare_instance.prepare_controller()
    prepare_instance.prepare_worker()
    try:
        yield
    finally:
        prepare_instance.cleanup()

class PrepareInstance:
    def __init__(self, temp_dir: Path, mode: str) -> None:
        self.temp_dir = temp_dir
        pytest_dir = self.temp_dir / mode / 'pytest'
        self.env_file = Path(f'{temp_dir}/minio_env').absolute()
        for directory in [self.temp_dir, pytest_dir]:
            if not directory.exists():
                directory.mkdir()

    def start_servers(self) -> None:
        pass

    def stop_servers(self) -> None:
        pass

    def cleanup(self) -> None:
        pass

    def prepare_worker(self) -> None:
        pass

    def prepare_controller(self) -> None:
        pass

    def get_envs(self) -> Dict[str, str]:
        pass

class PrepareWorker(PrepareInstance):
    def prepare_worker(self) -> None:
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
            time.sleep(0.1)


class PrepareController(PrepareInstance):
    def __init__(self, temp_dir, mode):
        super().__init__(temp_dir, mode)
        hosts = HostRegistry()
        self.loop = asyncio.new_event_loop()
        address = self.loop.run_until_complete(hosts.lease_host())
        self.ms = MinioServer(self.temp_dir, '127.0.0.1', LogPrefixAdapter(logging.getLogger('minio'), {'prefix': 'minio'}))
        self.mock_s3 = MockS3Server(address, 2012,
                                  LogPrefixAdapter(logging.getLogger('s3_mock'), {'prefix': 's3_mock'}))

    def start_servers(self) -> None:
        tasks = [self.loop.create_task(self.ms.start()), self.loop.create_task(self.mock_s3.start())]
        self.loop.run_until_complete(asyncio.gather(*tasks))

    def stop_servers(self) -> None:
        tasks = [self.loop.create_task(self.ms.stop()), self.loop.create_task(self.mock_s3.stop())]
        self.loop.run_until_complete(asyncio.gather(*tasks))

    def get_envs(self) -> Dict[str, str]:
        envs = self.ms.get_envs_settings()
        envs.update(self.mock_s3.get_envs_settings())
        return envs

    def prepare_controller(self) -> None:
        self.start_servers()
        with open(self.env_file, 'w') as file:
            for key, value in self.get_envs().items():
                file.write(f'{key}={value}\n')

    def cleanup(self) -> None:
        self.stop_servers()
        # if os.path.exists(self.env_file):
        #     self.env_file.unlink()

def get_prepare_instance(temp_dir: Path, is_worker: bool, mode: str) -> PrepareInstance:
    if is_worker:
        return PrepareWorker(temp_dir, mode)
    else:
        return PrepareController(temp_dir, mode)
