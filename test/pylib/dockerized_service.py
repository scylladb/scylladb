#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import shutil
import itertools
import asyncio
import pathlib
import re
import os
import subprocess
from typing import Callable

logger = logging.getLogger("DockerizedServer")

class DockerizedServer:
    """class for running an external dockerized service image, typically mock server"""
    # pylint: disable=too-many-instance-attributes

    newid = itertools.count(start=1).__next__   # Sequential unique id

    def __init__(self, image, tmpdir, logfilenamebase, 
                 success_string : Callable[[str, int], bool] | str,
                 failure_string : Callable[[str, int], bool] | str,
                 docker_args : Callable[[str, int], list[str]] | list[str] = [],
                 image_args : Callable[[str, int], list[str]] | list[str] = [],
                 host = '127.0.0.1',
                 port = None):
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
        self.service_port = port
        self.echo_thread = None

    async def start(self):
        """Starts docker image on a random port"""
        exe = pathlib.Path(next(exe for exe in [shutil.which(path) 
                                                for path in ["podman", "docker"]] 
                                                if exe is not None)).resolve()
        sid = f"{os.getpid()}-{DockerizedServer.newid()}"
        name = f'{self.logfilenamebase}-{sid}'
        logfilename = (pathlib.Path(self.tmpdir) / name).with_suffix(".log")
        self.logfile = logfilename.open("wb")

        docker_args = self.docker_args(self.host, self.service_port)
        image_args = self.image_args(self.host, self.service_port)

        args = [exe, "run", "--name", name, "--rm" ]
        if self.service_port is None:
            args = args + ["-P"]
        else:
            args = args + ["-p", str(self.service_port)]

        args = args + docker_args + [self.image] + image_args

        # This seems weird, using the blocking IO subprocess.
        # However, we want to use a pipe reader so we can push the 
        # output into the test log (because we are bad at propagating
        # log files etc from CI)
        # But the pipe reader needs to read until EOF, otherwise the
        # docker process will eventually hang. So we can't await a 
        # coroutine.
        # We _can_, sort of, use pool.create_task(...) to send a coro
        # to the background, and use a signal for waiting, like here,
        # thus ensuring the coro runs forever, sort of... However, 
        # this currently breaks, probably due to some part of the 
        # machinery/tests that don't async fully, causing us to not
        # process the log, and thus hand/fail, bla bla.
        # The solution is to make the process synced, and use a 
        # background thread (execution pool) for the processing.
        # This way we know the pipe reader will not suddenly get
        # blocked at inconvinient times.
        proc = subprocess.Popen(args, stderr=subprocess.PIPE)
        loop = asyncio.get_running_loop()
        ready_fut = loop.create_future()

        def process_io(): 
            f = ready_fut
            try:
                while True:
                    data = proc.stderr.readline()
                    if not data:
                        if f:
                            loop.call_soon_threadsafe(f.set_exception, RuntimeError("Log EOF"))
                        logger.debug("EOF received")
                        break
                    line = data.decode()
                    self.logfile.write(data)
                    logger.debug(line)
                    if f and self.is_success_line(line, self.service_port):
                        logger.info('Got start message: %s', line)
                        loop.call_soon_threadsafe(f.set_result, True)
                        f = None
                    if f and self.is_failure_line(line, self.service_port):
                        logger.info('Got fail message: %s', line)
                        loop.call_soon_threadsafe(f.set_result, False)
                        f = None
            except Exception as e:
                logger.error("Exception in log processing: %s", e)
                if f:
                    loop.call_soon_threadsafe(f.set_exception, e)

        self.echo_thread = loop.run_in_executor(None, process_io)
        ok = await ready_fut
        if not ok:
            self.logfile.close()
            proc.kill()
            proc.wait()
            raise RuntimeError("Could not parse expected launch message from container")

        check_proc = await asyncio.create_subprocess_exec(exe
                                                          , *["container", "port", name]
                                                          , stdout=asyncio.subprocess.PIPE
        )
        while True:
            data = await check_proc.stdout.readline()
            if not data:
                break
            s = data.decode()
            m = re.search(r"\d+\/\w+ -> [\w+\.\[\]\:]+:(\d+)", s)
            if m:
                self.port = int(m.group(1))

        await check_proc.wait()
        if not self.port:
            proc.kill()
            proc.wait()
            raise RuntimeError("Could not query port from container")
        self.proc = proc

    async def stop(self):
        """Stops docker image"""
        if self.proc:
            logger.debug("Stopping docker process")
            self.proc.terminate()
            self.proc.wait()
            self.proc = None
        if self.echo_thread:
            logger.debug("Waiting for IO thread")
            await self.echo_thread
            self.echo_thread = None
        if self.logfile:
            logger.debug("Closing log file")
            self.logfile.close()
            self.logfile = None
