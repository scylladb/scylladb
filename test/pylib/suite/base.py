#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import logging
import os
import pathlib
import shutil
import time

import universalasync

from test import TEST_RUNNER
from test.pylib.artifact_registry import ArtifactRegistry as artifacts
from test.pylib.host_registry import HostRegistry
from test.pylib.ldap_server import start_ldap
from test.pylib.minio_server import MinioServer
from test.pylib.resource_gather import setup_cgroup
from test.pylib.s3_proxy import S3ProxyServer
from test.pylib.s3_server_mock import MockS3Server
from test.pylib.util import LogPrefixAdapter


PYTEST_TESTS_LOGS_FOLDER = "pytest_tests_logs"


def prepare_dir(dirname: pathlib.Path, pattern: str, save_log_on_success: bool) -> None:
    # Ensure the dir exists.
    dirname.mkdir(parents=True, exist_ok=True)

    if not save_log_on_success:
        # Remove old artifacts.
        if pattern == '*':
            shutil.rmtree(dirname, ignore_errors=True)
        else:
            for p in dirname.glob(pattern):
                p.unlink()

@universalasync.async_to_sync_wraps
async def prepare_environment(tempdir_base: pathlib.Path, modes: list[str], gather_metrics: bool, save_log_on_success: bool,
                        toxiproxy_byte_limit: int) -> None:
    prepare_dirs(tempdir_base, modes, gather_metrics, save_log_on_success=save_log_on_success)
    await start_3rd_party_services(tempdir_base=tempdir_base, toxiproxy_byte_limit=toxiproxy_byte_limit)

def prepare_dirs(tempdir_base: pathlib.Path, modes: list[str], gather_metrics: bool, save_log_on_success: bool = False) -> None:
    setup_cgroup(gather_metrics)
    prepare_dir(tempdir_base, "*.log", save_log_on_success)
    prepare_dir(tempdir_base/ PYTEST_TESTS_LOGS_FOLDER, "*.log", save_log_on_success)
    for directory in ['report', 'ldap_instances']:
        full_path_directory = tempdir_base / directory
        prepare_dir(full_path_directory, '*', save_log_on_success)
    for mode in modes:
        prepare_dir(tempdir_base / mode, "*.log", save_log_on_success)
        prepare_dir(tempdir_base / mode, "*.reject", save_log_on_success)
        prepare_dir(tempdir_base / mode / "xml", "*.xml", save_log_on_success)
        prepare_dir(tempdir_base / mode / "failed_test", "*", save_log_on_success)
        prepare_dir(tempdir_base / mode / "allure", "*.xml", save_log_on_success)
        if TEST_RUNNER != "pytest":
            prepare_dir(tempdir_base / mode / "pytest", "*", save_log_on_success)


def _make_service_logger(logger_name: str, log_file: pathlib.Path) -> logging.Logger:
    """Return a logger that writes exclusively to *log_file*.

    Disables propagation to the root logger so service start/stop messages
    never appear on the console.
    """
    svc_logger = logging.getLogger(logger_name)
    svc_logger.propagate = False
    svc_logger.setLevel(logging.DEBUG)
    if not svc_logger.handlers:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        svc_logger.addHandler(handler)
    return svc_logger


@universalasync.async_to_sync_wraps
async def start_3rd_party_services(tempdir_base: pathlib.Path, toxiproxy_byte_limit: int):
    hosts = HostRegistry()

    finalize = start_ldap(
        host=await hosts.lease_host(),
        port=5000,
        instance_root=tempdir_base / 'ldap_instances',
        toxiproxy_byte_limit=toxiproxy_byte_limit)
    async def make_async_finalize():
        finalize()

    artifacts.add_exit_artifact(None, make_async_finalize)
    ms = MinioServer(
        tempdir_base=str(tempdir_base),
        address=await hosts.lease_host(),
        logger=LogPrefixAdapter(
            logger=_make_service_logger("minio", tempdir_base / "minio.log"),
            extra={"prefix": "minio"},
        ),
    )
    await ms.start()
    artifacts.add_exit_artifact(None, ms.stop)

    mock_s3_server = MockS3Server(
        host=await hosts.lease_host(),
        port=2012,
        logger=LogPrefixAdapter(
            logger=_make_service_logger("s3_mock", tempdir_base / "s3_mock.log"),
            extra={"prefix": "s3_mock"},
        ),
    )
    await mock_s3_server.start()
    artifacts.add_exit_artifact(None, mock_s3_server.stop)

    minio_uri = f"http://{os.environ[ms.ENV_ADDRESS]}:{os.environ[ms.ENV_PORT]}"
    proxy_s3_server = S3ProxyServer(
        host=await hosts.lease_host(),
        port=9002,
        minio_uri=minio_uri,
        max_retries=3,
        seed=int(time.time()),
        logger=LogPrefixAdapter(
            logger=_make_service_logger("s3_proxy", tempdir_base / "s3_proxy.log"),
            extra={"prefix": "s3_proxy"},
        ),
    )
    await proxy_s3_server.start()
    artifacts.add_exit_artifact(None, proxy_s3_server.stop)
