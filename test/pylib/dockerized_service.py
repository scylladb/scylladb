#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import shutil
import socket
import asyncio
import pathlib
from typing import Callable

# How many times start() launches the container before giving up.
START_MAX_ATTEMPTS = 8

class DockerizedServer:
    """class for running an external dockerized service image, typically mock server"""
    # pylint: disable=too-many-instance-attributes

    def __init__(self, image, tmpdir, logfilenamebase, 
                 success_string : Callable[[str, int], bool] | str,
                 failure_string : Callable[[str, int], bool] | str,
                 docker_args : Callable[[str, int], list[str]] | list[str] = [],
                 image_args : Callable[[str, int], list[str]] | list[str] = [],
                 host = '127.0.0.1'):
        self.image = image
        self.host = host
        self.tmpdir = tmpdir
        self.logfilenamebase = logfilenamebase
        self.docker_args: Callable[[str, int], list[str]] = (lambda host,port : docker_args) if isinstance(docker_args, list) else docker_args
        self.image_args: Callable[[str, int], list[str]] = (lambda host,port : image_args) if isinstance(image_args, list) else image_args
        self.is_success_line = lambda line, port : success_string in line if isinstance(success_string, str) else success_string
        self.is_failure_line = lambda line, port : failure_string in line if isinstance(failure_string, str) else failure_string
        self.logfile = None
        self.port = None
        self.proc = None

    def _is_any_failure_line(self, line, port):
        return self.is_failure_line(line, port) or "Address already in use" in line or "port is already allocated" in line

    async def _is_serving(self, proc, logfilename, port):
        """Checks that a container which printed its start message really serves on port.

        The start message alone proves nothing: e.g. local-kms prints it before binding
        the port, and then exits on "address already in use". Since the nested containers
        run in the host network, the port can be taken by any socket on the host after
        we probed it. Give such a failure time to show up, then check that the process
        is alive, that no failure line was logged, and that the port accepts connections.
        """
        await asyncio.sleep(0.5)
        if proc.returncode is not None:
            return False
        with logfilename.open("r") as f:
            if any(self._is_any_failure_line(line, port) for line in f):
                return False
        for _ in range(10):
            try:
                _, writer = await asyncio.open_connection(self.host, port)
                writer.close()
                await writer.wait_closed()
                return True
            except OSError:
                await asyncio.sleep(0.5)
        return False

    async def start(self):
        """Starts docker image on a random port"""
        exe = pathlib.Path(next(exe for exe in [shutil.which(path) 
                                                for path in ["podman", "docker"]] 
                                                if exe is not None)).resolve()

        for _ in range(START_MAX_ATTEMPTS):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((self.host, 0))
            port = sock.getsockname()[1]

            logfilename = (pathlib.Path(self.tmpdir) 
                           / f'{self.logfilenamebase}-{port}').with_suffix(".log")
            self.logfile = logfilename.open("wb")

            docker_args = self.docker_args(self.host, port)
            image_args = self.image_args(self.host, port)

            args = ["run", "--rm", "-p", f'{port}:{port}'] + docker_args + [self.image] + image_args

            sock.close()

            proc = await asyncio.create_subprocess_exec(exe, *args, stderr=self.logfile)
            failed = False

            # In any sane world we would just pipe stderr to a pipe and launch a background
            # task to just readline from there to both check the start message as well as 
            # add it to the log (preferrably via logger).
            # This works fine when doing this in a standalone python script. 
            # However, for some reason, when run in a pytest fixture, the pipe will fill up,
            # without or reader waking up and doing anyhing, and for any test longer than very
            # short, we will fill the stderr buffer and hang.
            # I cannot figure out how to get around this, so we workaround it
            # instead by directing stderr to a log file, and simply repeatedly
            # try to read the info from this file until we are happy.
            async with asyncio.timeout(120):
                done = False
                while not done and not failed:
                    with logfilename.open("r") as f:
                        for line in f:
                            if self.is_success_line(line, port):
                                print(f'Got start message: {line}')
                                done = True
                                break
                            if self._is_any_failure_line(line, port):
                                print(f'Got fail message: {line}')
                                failed = True
                                break
                    if not done and not failed:
                        # Yield to the event loop, otherwise the timeout can never fire.
                        await asyncio.sleep(0.1)

            if done and not await self._is_serving(proc, logfilename, port):
                print(f'Container does not serve on port {port}, log:\n{logfilename.read_text()}')
                failed = True

            if failed:
                self.logfile.close()
                if proc.returncode is None:
                    proc.terminate()
                await proc.wait()
                continue

            self.proc = proc
            self.port = port
            return

        raise RuntimeError(f'Could not start {self.image} after {START_MAX_ATTEMPTS} attempts')

    async def stop(self):
        """Stops docker image"""
        if self.proc:
            self.proc.terminate()
            await self.proc.wait()
        if self.logfile:
            self.logfile.close()
