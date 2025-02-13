#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import collections
import logging
import os
import pathlib
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

from scripts import coverage
from test.pylib.pool import Pool
from test.pylib.scylla_cluster import ScyllaCluster, ScyllaServer, merge_cmdline_options
from test.pylib.suite.base import Test, TestSuite, path_to, read_log, run_test
from test.pylib.util import LogPrefixAdapter

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable, Awaitable
    from typing import Optional, Union


class PythonTestSuite(TestSuite):
    """A collection of Python pytests against a single Scylla instance"""

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        self.scylla_exe = path_to(self.mode, "scylla")
        self.scylla_env = dict(self.base_env)
        if self.mode == "coverage":
            self.scylla_env.update(coverage.env(self.scylla_exe, distinct_id=self.name))
        self.scylla_env['SCYLLA'] = self.scylla_exe

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
            for srv in cluster.running.values():
                srv.log_file.close()
                srv.maintenance_socket_dir.cleanup()
            await cluster.stop()
            await cluster.release_ips()

        self.clusters = Pool(pool_size, self.create_cluster, recycle_cluster)

    def get_cluster_factory(self, cluster_size: int, options: argparse.Namespace) -> Callable[..., Awaitable]:
        def create_server(create_cfg: ScyllaCluster.CreateServerParams):
            cmdline_options = self.cfg.get("extra_scylla_cmdline_options", [])
            if type(cmdline_options) == str:
                cmdline_options = [cmdline_options]
            cmdline_options = merge_cmdline_options(cmdline_options, create_cfg.cmdline_from_test)
            cmdline_options = merge_cmdline_options(cmdline_options, options.extra_scylla_cmdline_options)
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
                exe=self.scylla_exe,
                vardir=os.path.join(self.options.tmpdir, self.mode),
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
            cluster = ScyllaCluster(logger, self.hosts, cluster_size, create_server)

            async def stop() -> None:
                await cluster.stop()

            # Suite artifacts are removed when
            # the entire suite ends successfully.
            self.artifacts.add_suite_artifact(self, stop)
            if not self.options.save_log_on_success:
                # If a test fails, we might want to keep the data dirs.
                async def uninstall() -> None:
                    await cluster.uninstall()

                self.artifacts.add_suite_artifact(self, uninstall)
            self.artifacts.add_exit_artifact(self, stop)

            await cluster.install_and_start()
            return cluster

        return create_cluster

    @property
    def pattern(self) -> str:
        return ["*_test.py", "test_*.py"]

    async def add_test(self, shortname, casename) -> None:
        test = PythonTest(self.next_id((shortname, self.suite_key)), shortname, casename, self)
        self.tests.append(test)

    async def run(self, test: 'Test', options: argparse.Namespace):
        if not os.access(self.scylla_exe, os.F_OK):
            raise FileNotFoundError(f"{self.scylla_exe} does not exist.")
        if not os.access(self.scylla_exe, os.X_OK):
            raise PermissionError(f"{self.scylla_exe} is not executable.")
        return await super().run(test, options)


class PythonTest(Test):
    """Run a pytest collection of cases against a standalone Scylla"""

    def __init__(self, test_no: int, shortname: str, casename: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = "python"
        self.core_args = ["-m", "pytest"]
        self.casename = casename
        self.xmlout = os.path.join(self.suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        self.server_log: Optional[str] = None
        self.server_log_filename: Optional[pathlib.Path] = None
        self.is_before_test_ok = False
        self.is_after_test_ok = False

    def _prepare_pytest_params(self, options: argparse.Namespace):
        self.args = [
            "-s",  # don't capture print() output inside pytest
            "--log-level=DEBUG",   # Capture logs
            "-o",
            "junit_family=xunit2",
            "-o",
            "junit_suite_name={}".format(self.suite.name),
            "--junit-xml={}".format(self.xmlout),
            "-rs",
            "--run_id={}".format(self.id),
            "--mode={}".format(self.mode)
        ]
        self.args.append(f"--alluredir={self.allure_dir}")
        if not options.save_log_on_success:
            self.args.append("--allure-no-capture")
        if options.markers:
            self.args.append(f"-m={options.markers}")

            # https://docs.pytest.org/en/7.1.x/reference/exit-codes.html
            no_tests_selected_exit_code = 5
            self.valid_exit_codes = [0, no_tests_selected_exit_code]

        arg = str(self.suite.suite_path / (self.shortname + ".py"))
        if self.casename is not None:
            arg += '::' + self.casename
        self.args.append(arg)

    def reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        super().reset()
        self.server_log = None
        self.server_log_filename = None
        self.is_before_test_ok = False
        self.is_after_test_ok = False

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))
        if self.server_log is not None:
            print("Server log of the first server:")
            print(self.server_log)

    async def run(self, options: argparse.Namespace) -> Test:

        self._prepare_pytest_params(options)

        loggerPrefix = self.mode + '/' + self.uname
        logger = LogPrefixAdapter(logging.getLogger(loggerPrefix), {'prefix': loggerPrefix})
        cluster = await self.suite.clusters.get(logger)
        try:
            cluster.before_test(self.uname)
            prepare_cql = self.suite.cfg.get("prepare_cql", None)
            if prepare_cql and not hasattr(cluster, 'prepare_cql_executed'):
                cc = next(iter(cluster.running.values())).control_connection
                if not isinstance(prepare_cql, collections.abc.Iterable):
                    prepare_cql = [prepare_cql]
                for stmt in prepare_cql:
                    cc.execute(stmt)
                cluster.prepare_cql_executed = True
            logger.info("Leasing Scylla cluster %s for test %s", cluster, self.uname)
            self.args.insert(0, "--host={}".format(cluster.endpoint()))
            self.is_before_test_ok = True
            cluster.take_log_savepoint()
            status = await run_test(self, options, env=self.suite.scylla_env)
            # if self.shortname in self.suite.dirties_cluster:
            cluster.is_dirty = True
            cluster.after_test(self.uname, status)
            self.is_after_test_ok = True
            self.success = status
        except Exception as e:
            self.server_log = cluster.read_server_log()
            self.server_log_filename = cluster.server_log_filename()
            if not self.is_before_test_ok:
                print("Test {} pre-check failed: {}".format(self.name, str(e)))
                print("Server log of the first server:\n{}".format(self.server_log))
                logger.info(f"Discarding cluster after failed start for test %s...", self.name)
            elif not self.is_after_test_ok:
                print("Test {} post-check failed: {}".format(self.name, str(e)))
                print("Server log of the first server:\n{}".format(self.server_log))
                logger.info(f"Discarding cluster after failed test %s...", self.name)
        await self.suite.clusters.put(cluster, is_dirty=cluster.is_dirty)
        logger.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        super().write_junit_failure_report(xml_res)
        if self.server_log_filename is not None:
            system_err = ET.SubElement(xml_res, 'system-err')
            system_err.text = read_log(self.server_log_filename)
