# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Optional
import pytest
import os
import re


logger = logging.getLogger(__name__)

class ScyllaLogFile():
    """
    Class for browsing a Scylla log file.
    Based on scylla-ccm implementation of log browsing.
    """
    def __init__(self, thread_pool: ThreadPoolExecutor, logfile_path: str):
        self.thread_pool = thread_pool # used for asynchronous IO operations
        self.file = logfile_path
        if not os.path.isfile(self.file):
            pytest.fail("Log file {} does not exist".format(self.file))

    async def _run_in_executor(self, func, *args, loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.thread_pool, func, *args)

    async def mark(self) -> int:
        """
        Returns "a mark" to the current position of this node Scylla log.
        This is for use with the from_mark parameter of watch_log_for method,
        allowing to watch the log from the position when this method was called.
        """
        with open(self.file, 'r') as f:
            await self._run_in_executor(f.seek, 0, os.SEEK_END)
            return await self._run_in_executor(f.tell)

    async def wait_for(self, pattern: str | re.Pattern, from_mark: Optional[int] = None, timeout: int = 600) -> int:
        """
        wait_for() checks if the log contains the given message.
        Because it may take time for the log message to be flushed, and sometimes
        we may want to look for messages about various delayed events, this
        function doesn't give up when it reaches the end of file, and rather
        retries until a given timeout. The timeout may be long, because successful
        tests will not wait for it. Note, however, that long timeouts will make
        xfailing tests slow.
        The timeout is in seconds.
        If from_mark is given, the log is searched from that position, otherwise
        from the beginning.
        """
        prog = re.compile(pattern)
        loop = asyncio.get_running_loop()
        line = ""

        with open(self.file, 'r') as f:
            if from_mark is not None:
                await self._run_in_executor(f.seek, from_mark, loop=loop)

            async with asyncio.timeout(timeout):
                logger.debug("Waiting for log message: %s", pattern)
                while True:
                    line += await self._run_in_executor(f.readline, loop=loop)
                    if line:
                        if prog.search(line):
                            logger.debug("Found log message: %s", line)
                            return await self._run_in_executor(f.tell)
                        elif line[-1] != '\n':
                            continue
                        line = ""
                    else:
                        await asyncio.sleep(0.01)

    async def grep(self, expr: str | re.Pattern, filter_expr: Optional[str | re.Pattern] = None,
             from_mark: Optional[int] = None) -> list[tuple[str, re.Match[str]]]:
        """
        Returns a list of lines matching the regular expression in the Scylla log.
        The list contains tuples of (line, match), where line is the full line
        from the log file, and match is the re.Match object for the matching
        expression.
        If filter_expr is given, only lines which do not match it are returned.
        If from_mark is given, the log is searched from that position, otherwise
        from the beginning.
        """
        matchings = []
        pattern = re.compile(expr)
        filter_pattern = re.compile(filter_expr) if filter_expr else None
        loop = asyncio.get_running_loop()

        with open(self.file) as f:
            if from_mark:
                await self._run_in_executor(f.seek, from_mark, loop=loop)
            line = await self._run_in_executor(f.readline, loop=loop)
            while line:
                m = pattern.search(line)
                if m and not (filter_pattern and re.search(filter_pattern, line)):
                    matchings.append((line, m))
                line = await self._run_in_executor(f.readline, loop=loop)
        return matchings
