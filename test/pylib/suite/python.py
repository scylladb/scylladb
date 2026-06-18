#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import logging
import os
import pathlib
from contextlib import asynccontextmanager
from functools import cache
from typing import TYPE_CHECKING

from test import path_to
from test.pylib.artifact_registry import ArtifactRegistry as artifacts
from test.pylib.pool import Pool
from test.pylib.scylla_cluster import ScyllaCluster, ScyllaServer, merge_cmdline_options, get_current_version_description
from test.pylib.suite.base import Test, TestSuite
from test.pylib.util import LogPrefixAdapter

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable, Awaitable, AsyncGenerator
    from typing import Optional, Union

    from pytest import Parser


class PythonTestSuite(TestSuite):
    """A collection of Python pytests against a single Scylla instance"""

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        self.scylla_exe = path_to(self.mode, "scylla")

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

        async def create_cluster(logger: Union[logging.Logger, logging.LoggerAdapter]) -> ScyllaCluster:
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


    async def add_test(self, shortname, casename) -> None:
        test = PythonTest(self.next_id((shortname, self.suite_key)), shortname, casename, self)
        self.tests.append(test)

class PythonTest(Test):
    """Run a pytest collection of cases against a standalone Scylla"""

    def __init__(self, test_no: int, shortname: str, casename: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        self.casename = casename

    @asynccontextmanager
    async def run_ctx(self) -> AsyncGenerator[ScyllaCluster]:
        """A test's setup/teardown context manager.

        Important part of this code is getting a ScyllaDB node from the pool and providing an address to the host
        as a `--host` argument.  This node returned to the pool after test is finished.  If the test was failed then
        the node will be marked as dirty.
        """
        loggerPrefix = self.mode + '/' + self.uname
        logger = LogPrefixAdapter(logging.getLogger(loggerPrefix), {'prefix': loggerPrefix})
        cluster: ScyllaCluster | None = None
        server_log_filename: pathlib.Path | None = None
        is_before_test_ok = False
        is_after_test_ok = False
        try:
            cluster: ScyllaCluster = await self.suite.clusters.get(logger)
            cluster.before_test(self.uname)
            logger.info("Leasing Scylla cluster %s for test %s", cluster, self.uname)
            server_log_filename = cluster.server_log_filename()
            is_before_test_ok = True
            cluster.take_log_savepoint()

            yield cluster

            if self.shortname in self.suite.dirties_cluster:
                cluster.is_dirty = True
            cluster.after_test(self.uname, self.success)
            is_after_test_ok = True
        except Exception as e:
            if not is_before_test_ok:
                print(f"Test {self.name} pre-check failed: {str(e)}\ncheck server logs: {server_log_filename}")
                logger.info(f"Discarding cluster after failed start for test %s...", self.name)
            elif not is_after_test_ok:
                print(f"Test {self.name} post-check failed: {str(e)}\ncheck server logs: {server_log_filename}")
                logger.info(f"Discarding cluster after failed test %s...", self.name)
            self.success = False
            if cluster is not None:
                cluster.is_dirty = True
            raise
        finally:
            if cluster is not None:
                await self.suite.clusters.put(cluster, is_dirty=cluster.is_dirty)
                logger.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")


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
