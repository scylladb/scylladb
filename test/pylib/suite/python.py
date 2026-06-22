#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import logging
import os
import pathlib
from functools import cache
from typing import TYPE_CHECKING

from test import path_to
from test.pylib.artifact_registry import ArtifactRegistry as artifacts
from test.pylib.pool import Pool
from test.pylib.scylla_cluster import ScyllaCluster, ScyllaServer, merge_cmdline_options, get_current_version_description

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable, Awaitable

    from pytest import Parser


class PythonTestSuite:
    def __init__(self, path: pathlib.Path, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        self.suite_path = path
        self.log_dir = pathlib.Path(options.tmpdir) / mode
        self.name = str(self.suite_path.name)
        self.cfg = cfg
        self.options = options
        self.mode = mode
        self.suite_key = os.path.join(path, mode)
        # environment variables that should be the base of all processes running in this suit
        self.base_env = {}
        if self.need_coverage():
            # Set the coverage data from each instrumented object to use the same file (and merged into it with locking)
            # as long as we don't need test specific coverage data, this looks sufficient. The benefit of doing this in
            # this way is that the storage will not be bloated with coverage files (each can weigh 10s of MBs so for several
            # thousands of tests it can easily reach 10 of GBs)
            # ref: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html#running-the-instrumented-program
            self.base_env["LLVM_PROFILE_FILE"] = str(self.log_dir / "coverage" / self.name / "%m.profraw")

        self.scylla_exe = options.exe_path or path_to(self.mode, "scylla")

        cluster_cfg = self.cfg.get("cluster", {"initial_size": 1})
        cluster_size = cluster_cfg["initial_size"]
        env_pool_size = os.getenv("CLUSTER_POOL_SIZE")
        if options.cluster_pool_size is not None:
            pool_size = options.cluster_pool_size
        elif env_pool_size is not None:
            pool_size = int(env_pool_size)
        else:
            pool_size = cfg.get("pool_size", 2)
        self.dirties_cluster = set(cfg.get("dirties_cluster", []))

        self.create_cluster = self.get_cluster_factory(cluster_size, options)
        async def recycle_cluster(cluster: ScyllaCluster) -> None:
            """When a dirty cluster is returned to the cluster pool,
               stop it and release the used IPs. We don't necessarily uninstall() it yet,
               which would delete the log file and directory - we might want to preserve
               these if it came from a failed test.
            """
            await cluster.stop()
            for srv in cluster.servers.values():
                if srv.log_file is not None:
                    srv.log_file.close()
                srv.maintenance_socket_dir.cleanup()
            # Close API client to release connector resources
            if cluster.api is not None:
                cluster.api.close()
                cluster.api = None
            await cluster.release_ips()

        self.clusters = Pool(pool_size, self.create_cluster, recycle_cluster)

    def need_coverage(self):
        return self.options.coverage and (self.mode in self.options.coverage_modes) and bool(
            self.cfg.get("coverage", True))

    def get_cluster_factory(self, cluster_size: int, options: argparse.Namespace) -> Callable[..., Awaitable]:
        def create_server(create_cfg: ScyllaCluster.CreateServerParams):
            cmdline_options = self.cfg.get("extra_scylla_cmdline_options", [])
            if type(cmdline_options) == str:
                cmdline_options = [cmdline_options]
            cmdline_options = merge_cmdline_options(cmdline_options, create_cfg.cmdline_from_test)
            cmdline_options = merge_cmdline_options(cmdline_options, options.extra_scylla_cmdline_options.split())
            # There are multiple sources of config options, with increasing priority
            # (if two sources provide the same config option, the higher priority one wins):
            # 1. the defaults
            # 2. suite-specific config options (in "extra_scylla_config_options")
            # 3. config options from tests (when servers are added during a test)
            default_config_options = \
                {"authenticator": "PasswordAuthenticator",
                 "authorizer": "CassandraAuthorizer"}
            default_config_options["tablets_initial_scale_factor"] = 4 if self.mode == "release" else 2
            config_options = default_config_options | \
                             self.cfg.get("extra_scylla_config_options", {}) | \
                             create_cfg.config_from_test

            server = ScyllaServer(
                mode=self.mode,
                version=(create_cfg.version or get_current_version_description(self.scylla_exe)),
                vardir=self.log_dir,
                logger=create_cfg.logger,
                cluster_name=create_cfg.cluster_name,
                ip_addr=create_cfg.ip_addr,
                seeds=create_cfg.seeds,
                cmdline_options=cmdline_options,
                config_options=config_options,
                property_file=create_cfg.property_file,
                append_env=self.base_env,
                server_encryption=create_cfg.server_encryption)

            return server

        async def create_cluster(logger: logging.Logger | logging.LoggerAdapter) -> ScyllaCluster:
            cluster = ScyllaCluster(logger, cluster_size, create_server)

            async def stop() -> None:
                await cluster.stop()

            # Suite artifacts are removed when
            # the entire suite ends successfully.
            artifacts.add_suite_artifact(self, stop)
            if not self.options.save_log_on_success:
                # If a test fails, we might want to keep the data dirs.
                async def uninstall() -> None:
                    await cluster.uninstall()

                artifacts.add_suite_artifact(self, uninstall)
            artifacts.add_exit_artifact(self, stop)

            await cluster.install_and_start()
            # If cluster failed to start, raise the exception immediately
            # so the pool doesn't return a broken cluster to tests
            if cluster.start_exception is not None:
                # Clean up the broken cluster before raising
                try:
                    await cluster.stop()
                    if cluster.api is not None:
                        await cluster.api.close()
                        cluster.api = None
                    await cluster.release_ips()
                except:
                    pass  # Ignore cleanup errors
                raise cluster.start_exception
            return cluster

        return create_cluster


# Use cache to execute this function once per pytest session.
@cache
def add_host_option(parser: Parser) -> None:
    parser.addoption("--host", default="localhost",
                     help="a DB server host to connect to")


# Use cache to execute this function once per pytest session.
@cache
def add_cql_connection_options(parser: Parser) -> None:
    """Add pytest options for a CQL connection."""

    cql_options = parser.getgroup("CQL connection options")
    cql_options.addoption("--port", default="9042",
                          help="CQL port to connect to")
    cql_options.addoption("--ssl", action="store_true",
                          help="Connect to CQL via an encrypted TLSv1.2 connection", default=False)
    cql_options.addoption("--auth_username",
                          help="username for authentication", default=None)
    cql_options.addoption("--auth_password",
                          help="password for authentication", default=None)


# Use cache to execute this function once per pytest session.
@cache
def add_s3_options(parser: Parser) -> None:
    """Options for tests which use S3 server (i.e., cluster/object_store and cqlpy/test_tools.py)"""

    s3_options = parser.getgroup("S3 server settings")
    s3_options.addoption('--s3-server-address', default=None)
    s3_options.addoption('--s3-server-port', type=int, default=None)
    s3_options.addoption('--aws-access-key', default=None)
    s3_options.addoption('--aws-secret-key', default=None)
    s3_options.addoption('--aws-region', default=None)
    s3_options.addoption('--s3-server-bucket', default=None)
