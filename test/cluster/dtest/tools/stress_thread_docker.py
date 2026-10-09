#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import base64
import concurrent.futures
import datetime
import logging
import random
import shlex
import time
from collections import deque, namedtuple
from functools import cache, cached_property

from ccmlib.common import wait_for
from ccmlib.scylla_node import ScyllaNode
from docker.errors import DockerException

import docker
from alternator_utils import random_string
from tools.docker_utils import container_reload, container_remove, dump_container_logs, get_docker_client
from tools.docker_versions import get_docker_version
from tools.retrying import retrying

logger = logging.getLogger(__name__)


class DockerBasedStressThread:
    # Expects getting a docker image name like: 'scylladb/hydra-loaders:cassandra-harry-jdk11-20220816'
    # And a cluster node to issue stress tool command on.

    def __init__(  # noqa: PLR0913
        self,
        node,
        stress_cmd,
        container_name,
        container_command_line="-c 'sleep infinity'",
        timeout=600,
        stress_num=1,
        env: dict[str, str] | None = None,
        volumes: dict[str, str] | list[str] | None = None,
        capture_output: bool = True,
        mem_limit: str = "256m",
        nano_cpus: int = 1_000_000_000,
    ):
        self.timeout = timeout
        self.node: ScyllaNode = node
        self.executor = None
        self.env = env
        self.volumes = volumes
        self.results_future = []
        self.max_workers = stress_num
        self.stress_cmd = stress_cmd
        self.capture_output = capture_output
        self.mem_limit = mem_limit
        self.nano_cpus = nano_cpus
        self.docker_image_param_name = get_docker_version(container_name)
        short_uuid = base64.urlsafe_b64encode(random.SystemRandom().randbytes(6)).decode()
        self.name = "-".join([container_name, datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f"), short_uuid])
        self.container_command_line = container_command_line
        self.container = None
        self.create_stress_container()

    @cached_property
    def docker_client(self):
        return get_docker_client()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Kill container first to unblock any worker thread stuck reading Docker exec output.
        self.remove_stress_container()
        self.shutdown_executor()

    def shutdown_executor(self, timeout=60):
        if self.executor:
            self.executor.shutdown(wait=True, cancel_futures=True)
            self.executor = None

    @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException, message="remove stress container")
    def remove_stress_container(self):
        if self.container:
            # Use a shorter API timeout than the default client's, so a hung Docker daemon fails the removal
            # before callers give up waiting for the stress result, instead of blocking past their deadline.
            container = get_docker_client(timeout=60).containers.prepare_model(self.container.attrs)
            container_reload(container)
            container_remove(container, force=True)
            self.container = None

    def pull_image(self):
        existing_images = self.docker_client.images.list(filters={"reference": self.docker_image_param_name})
        if not existing_images:
            self.docker_client.images.pull(self.docker_image_param_name)

    def create_stress_container(self):
        if self.container:
            raise Exception("stress docker already exists for this instance")

        @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException)
        def start_stresstool_docker():
            self.pull_image()
            self.container = self.docker_client.containers.run(
                network_mode="host",
                entrypoint="/bin/bash",
                name=self.name,
                image=self.docker_image_param_name,
                command=self.container_command_line,
                volumes=self.volumes,
                environment=self.env,
                detach=True,
                labels=["dtest"],
                mem_limit=self.mem_limit,
                nano_cpus=self.nano_cpus,
            )
            container_reload(self.container)

        start_stresstool_docker()
        if self.container:
            wait_for(func=self.is_container_running, timeout=60, step=2)

    def is_container_running(self) -> bool:
        container_reload(self.container)
        if self.container.status != "running":
            dump_container_logs(self.container)
            return False
        return True

    def configure_executer(self):
        logger.debug("Starting a %s Worker thread", self.__class__.__name__)
        self.executor = concurrent.futures.ThreadPoolExecutor(  # pylint: disable=consider-using-with
            max_workers=self.max_workers
        )

    def run(self):
        self.configure_executer()
        self.results_future = [self.executor.submit(self._run_stress)]

        return self

    def _kill_process_by_token(self, token: str, signal: int):
        exec_instance = self.docker_client.api.exec_create(self.container.id, cmd=f'bash -c "PID=`cat /tmp/{token}`; pkill -{signal} -P $PID"', stdout=True, stderr=True)
        exec_output = self.docker_client.api.exec_start(
            exec_instance["Id"],
            tty=False,
            stream=True,
            demux=True,
        )
        err = ""
        for _, stderr_out in exec_output:
            err += stderr_out

        exit_metadata = self.docker_client.api.exec_inspect(exec_instance["Id"])
        if exit_metadata["Running"]:
            logger.error("Process is still running")
        if exit_metadata["ExitCode"] != 0:
            if err:
                logger.error("Failed to kill process: " + err)
            else:
                logger.error("Failed to kill process")

    def _run_stress(self, max_tail_lines=100):  # noqa: PLR0912
        token = random_string(10)
        stress_cmd = shlex.join(shlex.quote(part) for part in shlex.split(self.stress_cmd))
        cmd = f'bash -c "echo $$ >/tmp/{token};{stress_cmd}"'
        logger.debug("running command: %s", self.stress_cmd)

        exec_instance = self.docker_client.api.exec_create(self.container.id, cmd=cmd, stdout=True, stderr=True)
        exec_output = self.docker_client.api.exec_start(
            exec_instance["Id"],
            tty=False,
            stream=True,
            demux=True,
        )

        if self.capture_output:
            stdout_tail = deque(maxlen=max_tail_lines)
            stderr_tail = deque(maxlen=max_tail_lines)
        else:
            stdout_tail = None
            stderr_tail = None

        end_time = datetime.datetime.now() + datetime.timedelta(days=10)
        is_killed = False
        if self.timeout and self.timeout > 5:
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=self.timeout - 5)

        for stdout_out, stderr_out in exec_output:
            if stdout_out is not None:
                _stdout_out = stdout_out.decode("utf-8", "replace")
                for line in _stdout_out.splitlines(keepends=True):
                    if stdout_tail is not None:
                        stdout_tail.append(line)
                    logger.debug(line.rstrip())
            if stderr_out is not None:
                _stderr_out = stderr_out.decode("utf-8", "replace")
                for line in _stderr_out.splitlines(keepends=True):
                    if stderr_tail is not None:
                        stderr_tail.append(line)
                    logger.debug(line.rstrip())
            if not is_killed and datetime.datetime.now() > end_time:
                logger.error("stress tool is running longer than expected, sending kill signal to terminate it")
                self._kill_process_by_token(token, 6)  # send SIGABRT to the stress command
                is_killed = True

        exit_metadata = self.docker_client.api.exec_inspect(exec_instance["Id"])
        assert not exit_metadata["Running"]
        exit_code = exit_metadata["ExitCode"]

        if self.capture_output:
            stdout = "".join(stdout_tail)
            stderr = "".join(stderr_tail)
        else:
            stdout = ""
            stderr = ""

        ret = namedtuple("Subprocess_Return", "stdout stderr rc")
        return ret(stdout=stdout, stderr=stderr, rc=exit_code)

    def wait_for_stress_results(self):
        results = []
        logger.debug("Wait for %s stress threads results. Using timeout: %s", self.max_workers, self.timeout)
        try:
            for future in concurrent.futures.as_completed(self.results_future, timeout=self.timeout):
                results.append(future.result())
        finally:
            self.shutdown_executor()

        return results[-1]
