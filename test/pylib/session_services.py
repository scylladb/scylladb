#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import json
import logging
import os
import pathlib
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import pytest

from test.pylib.host_registry import HostRegistry, Host
from test.pylib.ldap_server import start_ldap
from test.pylib.minio_server import MinioServer
from test.pylib.s3_proxy import S3ProxyServer
from test.pylib.s3_server_mock import MockS3Server
from test.pylib.util import LogPrefixAdapter

if TYPE_CHECKING:
    from test.pylib.runner import TestSuiteConfig


logger = logging.getLogger(__name__)

SESSION_SERVICE_ENV_VERSION = 1
SESSION_SERVICE_ENV_FILENAME = "session_service_env.json"
SESSION_SERVICE_S3 = "s3"
SESSION_SERVICE_LDAP = "ldap"
SESSION_SERVICES = frozenset({SESSION_SERVICE_S3, SESSION_SERVICE_LDAP})

S3_ENV_KEYS = (
    MinioServer.ENV_ADDRESS,
    MinioServer.ENV_PORT,
    MinioServer.ENV_BUCKET,
    MinioServer.ENV_ACCESS_KEY,
    MinioServer.ENV_SECRET_KEY,
    "MOCK_S3_SERVER_PORT",
    "MOCK_S3_SERVER_HOST",
    "PROXY_S3_SERVER_PORT",
    "PROXY_S3_SERVER_HOST",
)
LDAP_ENV_KEYS = (
    "SEASTAR_LDAP_PORT",
    "SEASTAR_LDAP_HOST",
    "SASLAUTHD_MUX_PATH",
)
SESSION_SERVICE_ENV_KEYS = tuple(dict.fromkeys((*S3_ENV_KEYS, *LDAP_ENV_KEYS)))

_ORIGINAL_SESSION_SERVICE_ENV: dict[str, str | None] | None = None


def session_service_env_path(tmpdir: str | pathlib.Path) -> pathlib.Path:
    return pathlib.Path(tmpdir).absolute() / "pytest_log" / SESSION_SERVICE_ENV_FILENAME


def _make_service_logger(logger_name: str, log_file: pathlib.Path) -> logging.Logger:
    svc_logger = logging.getLogger(logger_name)
    svc_logger.propagate = False
    svc_logger.setLevel(logging.DEBUG)
    if not svc_logger.handlers:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        svc_logger.addHandler(handler)
    return svc_logger


def normalize_session_services(services: Iterable[str]) -> frozenset[str]:
    result = frozenset(str(service) for service in services)
    unknown = result - SESSION_SERVICES
    if unknown:
        raise pytest.UsageError(f"Unknown session service(s): {', '.join(sorted(unknown))}")
    return result


def _services_from_value(value: object) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, str):
        return normalize_session_services([value])
    if isinstance(value, Iterable):
        return normalize_session_services(str(service) for service in value)
    raise pytest.UsageError(f"Session service annotation must be a string or a list of strings, got {value!r}")


def _suite_relative_test_key(item: pytest.Item, suite_config: TestSuiteConfig | None) -> str | None:
    if suite_config is None:
        return None
    try:
        relative = pathlib.Path(item.path).relative_to(suite_config.path)
    except ValueError:
        try:
            relative = pathlib.Path(item.path).resolve().relative_to(suite_config.path.resolve())
        except ValueError:
            return None
    return relative.with_suffix("").as_posix()


def _suite_config_service_requirements(item: pytest.Item, suite_config: TestSuiteConfig | None) -> frozenset[str]:
    if suite_config is None:
        return frozenset()
    raw = suite_config.cfg.get("requires_services")
    if raw is None:
        return frozenset()
    if not isinstance(raw, dict):
        return _services_from_value(raw)

    services: set[str] = set()
    for selector in ("*", "default"):
        services.update(_services_from_value(raw.get(selector)))

    if test_key := _suite_relative_test_key(item, suite_config):
        services.update(_services_from_value(raw.get(test_key)))
        services.update(_services_from_value(raw.get(pathlib.Path(test_key).name)))
    return normalize_session_services(services)


def _marker_service_requirements(item: pytest.Item) -> frozenset[str]:
    marker = item.get_closest_marker("requires_service")
    if marker is None:
        return frozenset()

    services: list[str] = []
    services.extend(str(service) for service in marker.args)
    for key in ("service", "name"):
        if key in marker.kwargs:
            services.append(str(marker.kwargs[key]))
    if "services" in marker.kwargs:
        value = marker.kwargs["services"]
        if isinstance(value, str):
            services.append(value)
        else:
            services.extend(str(service) for service in value)
    unknown_kwargs = set(marker.kwargs) - {"service", "name", "services"}
    if unknown_kwargs:
        raise pytest.UsageError(f"Unknown @pytest.mark.requires_service arguments: {', '.join(sorted(unknown_kwargs))}")
    return normalize_session_services(services)


def session_service_requirements_for_item(item: pytest.Item, suite_config: TestSuiteConfig | None) -> frozenset[str]:
    return normalize_session_services(
        set(_suite_config_service_requirements(item, suite_config)) | set(_marker_service_requirements(item))
    )


class SessionServiceManager:
    """Start and stop session-scoped services required by selected tests."""

    def __init__(self, tempdir_base: str | pathlib.Path, toxiproxy_byte_limit: int) -> None:
        self.tempdir_base = pathlib.Path(tempdir_base).absolute()
        self.toxiproxy_byte_limit = toxiproxy_byte_limit
        self.hosts = HostRegistry()
        self.active_services: frozenset[str] = frozenset()
        self._hosts_by_role: dict[str, Host] = {}
        self._ldap_finalize = None
        self._minio_server: MinioServer | None = None
        self._mock_s3_server: MockS3Server | None = None
        self._proxy_s3_server: S3ProxyServer | None = None
        self._original_env = {key: os.environ.get(key) for key in SESSION_SERVICE_ENV_KEYS}
        self._write_environment()

    async def ensure_services(self, services: Iterable[str]) -> None:
        desired = normalize_session_services(services)
        if desired == self.active_services:
            return

        previous = self.active_services
        stopped = set(previous - desired)
        started: list[str] = []
        try:
            for service in sorted(stopped, reverse=True):
                await self._stop_service(service)
            for service in sorted(desired - previous):
                await self._start_service(service)
                started.append(service)
        except Exception:
            for service in reversed(started):
                await self._stop_service(service)
            self.active_services = normalize_session_services(previous - stopped)
            self._write_environment()
            raise

        self.active_services = desired
        self._write_environment()

    async def stop_all(self) -> None:
        await self.ensure_services(())

    async def _lease_host(self, role: str) -> Host:
        if role not in self._hosts_by_role:
            self._hosts_by_role[role] = await self.hosts.lease_host()
        return self._hosts_by_role[role]

    async def _start_service(self, service: str) -> None:
        logger.info("Starting session service %s", service)
        if service == SESSION_SERVICE_LDAP:
            await self._start_ldap()
        elif service == SESSION_SERVICE_S3:
            await self._start_s3_stack()
        else:
            raise pytest.UsageError(f"Unknown session service: {service}")

    async def _stop_service(self, service: str) -> None:
        logger.info("Stopping session service %s", service)
        if service == SESSION_SERVICE_LDAP:
            self._stop_ldap()
        elif service == SESSION_SERVICE_S3:
            await self._stop_s3_stack()
        else:
            raise pytest.UsageError(f"Unknown session service: {service}")

    async def _start_s3_stack(self) -> None:
        try:
            minio = MinioServer(
                tempdir_base=str(self.tempdir_base),
                address=await self._lease_host("minio"),
                logger=LogPrefixAdapter(
                    logger=_make_service_logger("minio", self.tempdir_base / "minio.log"),
                    extra={"prefix": "minio"},
                ),
            )
            await minio.start()
            self._minio_server = minio

            mock_s3_server = MockS3Server(
                host=await self._lease_host("s3_mock"),
                port=2012,
                logger=LogPrefixAdapter(
                    logger=_make_service_logger("s3_mock", self.tempdir_base / "s3_mock.log"),
                    extra={"prefix": "s3_mock"},
                ),
            )
            await mock_s3_server.start()
            self._mock_s3_server = mock_s3_server

            minio_uri = f"http://{os.environ[minio.ENV_ADDRESS]}:{os.environ[minio.ENV_PORT]}"
            proxy_s3_server = S3ProxyServer(
                host=await self._lease_host("s3_proxy"),
                port=9002,
                minio_uri=minio_uri,
                max_retries=3,
                seed=int(time.time()),
                logger=LogPrefixAdapter(
                    logger=_make_service_logger("s3_proxy", self.tempdir_base / "s3_proxy.log"),
                    extra={"prefix": "s3_proxy"},
                ),
            )
            await proxy_s3_server.start()
            self._proxy_s3_server = proxy_s3_server
        except Exception:
            await self._stop_s3_stack()
            raise

    async def _stop_s3_stack(self) -> None:
        if self._proxy_s3_server is not None:
            await self._proxy_s3_server.stop()
            self._proxy_s3_server = None
        if self._mock_s3_server is not None:
            await self._mock_s3_server.stop()
            self._mock_s3_server = None
        if self._minio_server is not None:
            await self._minio_server.stop()
            self._minio_server = None
        self._restore_environment(S3_ENV_KEYS)

    async def _start_ldap(self) -> None:
        self._ldap_finalize = start_ldap(
            host=await self._lease_host("ldap"),
            port=5000,
            instance_root=self.tempdir_base / "ldap_instances",
            toxiproxy_byte_limit=self.toxiproxy_byte_limit,
        )

    def _stop_ldap(self) -> None:
        if self._ldap_finalize is not None:
            self._ldap_finalize()
            self._ldap_finalize = None
        self._restore_environment(LDAP_ENV_KEYS)

    def _restore_environment(self, keys: Iterable[str]) -> None:
        for key in keys:
            if (value := self._original_env.get(key)) is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _current_environment(self) -> dict[str, str]:
        keys: list[str] = []
        if SESSION_SERVICE_S3 in self.active_services:
            keys.extend(S3_ENV_KEYS)
        if SESSION_SERVICE_LDAP in self.active_services:
            keys.extend(LDAP_ENV_KEYS)
        return {key: os.environ[key] for key in keys if key in os.environ}

    def _write_environment(self) -> None:
        path = session_service_env_path(self.tempdir_base)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": SESSION_SERVICE_ENV_VERSION,
            "active_services": sorted(self.active_services),
            "managed_env": list(SESSION_SERVICE_ENV_KEYS),
            "env": self._current_environment(),
        }
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        tmp_path.replace(path)


def apply_session_service_environment(tmpdir: str | pathlib.Path) -> None:
    global _ORIGINAL_SESSION_SERVICE_ENV
    if _ORIGINAL_SESSION_SERVICE_ENV is None:
        _ORIGINAL_SESSION_SERVICE_ENV = {key: os.environ.get(key) for key in SESSION_SERVICE_ENV_KEYS}

    path = session_service_env_path(tmpdir)
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        payload = {"version": SESSION_SERVICE_ENV_VERSION, "managed_env": list(SESSION_SERVICE_ENV_KEYS), "env": {}}
    except json.JSONDecodeError as exc:
        raise pytest.UsageError(f"Failed to parse session service environment from {path}: {exc}") from exc

    if payload.get("version") != SESSION_SERVICE_ENV_VERSION:
        raise pytest.UsageError(f"Unsupported session service environment version in {path}")

    service_env = {str(key): str(value) for key, value in payload.get("env", {}).items()}
    managed_env = tuple(str(key) for key in payload.get("managed_env", SESSION_SERVICE_ENV_KEYS))
    for key in managed_env:
        if key in service_env:
            os.environ[key] = service_env[key]
        elif (original_value := _ORIGINAL_SESSION_SERVICE_ENV.get(key)) is not None:
            os.environ[key] = original_value
        else:
            os.environ.pop(key, None)
