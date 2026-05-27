# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from __future__ import annotations

from test.pylib.util import universalasync_typed_wrap
import asyncio
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple

import pytest

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from concurrent.futures import ThreadPoolExecutor
    from typing import Callable


logger = logging.getLogger(__name__)

MarkType = int

class FileSnapshot:
    """
    Allows examining a snapshot of a file, assuming the file is append-only.
    The snapshot is determined at the time of opening.

    Acts as a synchronous context manager for direct I/O operations.
    Consume all lines like this:

        with FileSnapshot(path) as snap:
            while (line := snap.next_line()):
                # process line

    """

    def __init__(self, path: Path, from_mark: MarkType = None):
        self.path = path
        self.from_mark = from_mark

    def __enter__(self):
        size = self.path.stat().st_size
        self.file = self.path.open(encoding="utf-8")
        self.to_mark = size
        if self.from_mark:
            self.file.seek(self.from_mark)
        return self

    def next_line(self) -> Optional[str]:
        pos = self.file.tell()
        if pos >= self.to_mark:
            return None
        line = self.file.readline()
        if not line:
            return None
        if pos + len(line) > self.to_mark:
            return line[:self.to_mark - pos]
        return line

    def __exit__(self, exc_type, exc, tb):
        self.file.close()


@universalasync_typed_wrap
class ScyllaLogFile:
    """Browse a Scylla log file.

    Based on scylla-ccm implementation of log browsing.
    """
    def __init__(self, thread_pool: ThreadPoolExecutor, logfile_path: str | Path):
        self.thread_pool = thread_pool  # used for asynchronous IO operations
        self.file = Path(logfile_path)
        if not self.file.is_file():
            pytest.fail(f"Log file {self.file.name} does not exist")

    async def _run_in_executor[T, **P](self,
                                       func: Callable[P, T],
                                       *args: P.args,
                                       loop: AbstractEventLoop | None = None) -> T:
        if loop is None:
            loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.thread_pool, func, *args)

    async def mark(self) -> int:
        """Return "a mark" to the current position of this node Scylla log.

        This is for use with the from_mark parameter of watch_log_for method, allowing to watch the log from
        the position when this method was called.
        """
        return (await self._run_in_executor(self.file.stat)).st_size

    async def wait_for(self,
                       *exprs: str | re.Pattern[str],
                       from_mark: int | None = None,
                       timeout: float = 600) -> tuple[int, list[tuple[str, re.Match[str]]]]:
        """Wait until all patterns appear in the log until the timeout expires.

        The `exprs` are regular expressions.

        If `from_mark` argument is given, the log is searched from that position, otherwise from the beginning.

        The `timeout` (in seconds) may be long, because successful tests will not wait for it.  Note, however, that
        long timeouts will make xfailing tests slow.

        Return a tuple with the last read position and list of tuples with matched lines and re.Match instances.
        """
        logger.debug("Waiting for log message(s): %s", exprs)

        loop = asyncio.get_running_loop()

        exprs = [re.compile(pattern) for pattern in exprs]
        matches = []

        with self.file.open(encoding="utf-8") as log_file:
            if from_mark is not None:
                await self._run_in_executor(log_file.seek, from_mark, loop=loop)

            async with asyncio.timeout(timeout):
                line = ""
                while exprs:
                    # Because it may take time for the log message to be flushed, and sometimes we may want to look
                    # for messages about various delayed events, this function doesn't give up when it reaches
                    # the end of file, and rather retries until a given timeout.
                    line += await self._run_in_executor(log_file.readline, loop=loop)
                    if line:
                        for pattern in exprs.copy():
                            if match := pattern.search(line):
                                logger.debug("Found log message: %s", line)
                                matches.append((line, match))
                                exprs.remove(pattern)
                        if line[-1] == "\n":
                            line = ""
                    else:
                        await asyncio.sleep(0.01)

            return await self._run_in_executor(log_file.tell, loop=loop), matches

    def _for_each_line_snapshot(self, from_mark: int | None = None):
        """
        Yields lines from the file, without considering lines appended after the call started.
        Uses direct I/O for performance.
        """
        with FileSnapshot(self.file, from_mark) as snap:
            while line := snap.next_line():
                yield line

    async def grep(self,
                   expr: str | re.Pattern[str],
                   filter_expr: str | re.Pattern[str] | None = None,
                   from_mark: int | None = None,
                   max_count: int = 0) -> list[tuple[str, re.Match[str]]]:
        """Search the log for lines matching the regular expression.

        If `filter_expr` argument is given, only lines which do not match it are returned.

        If `from_mark` argument is given, the log is searched from that position, otherwise from the beginning.

        If `max_count` is greater than 0, return list of matches when it'll reach `max_count` length.

        Return a list of tuples (line, match), where line is the full line from the log, and match is the re.Match[str]
        object for the matching expression.
        """
        loop = asyncio.get_running_loop()
        def _grep_sync():
            expr_compiled = re.compile(expr)
            filter_func = re.compile(filter_expr).search if filter_expr else lambda _: False
            matches = []
            for line in self._for_each_line_snapshot(from_mark=from_mark):
                if match := not filter_func(line) and expr_compiled.search(line):
                    matches.append((line, match))
                    if len(matches) == max_count:
                        break
            return matches
        return await loop.run_in_executor(self.thread_pool, _grep_sync)

    async def grep_for_errors(self,
                              distinct_errors: bool = False,
                              from_mark: int | None = None) -> list[str] | list[list[str]]:
        """Search the log for messages with ERROR level.

        If `distinct_errors` is False then collect all messages after found one till a first message with INFO level.
        Otherwise, add just matched line to a return list.

        If `from_mark` argument is given, the log is searched from that position, otherwise from the beginning.

        Return a list of error messages.  Error message can be just one line or a list of lines.
        """
        loop = asyncio.get_running_loop()
        def _grep_errors_sync():
            # Each line in scylla-*.log starts with log level, so, use re.match to search from the beginning of each line.
            error_pattern = re.compile(r"ERROR\b")
            info_pattern = re.compile(r"INFO\b")
            matches = []
            with FileSnapshot(self.file, from_mark) as snap:
                line = snap.next_line()
                while line:
                    if error_pattern.match(line):
                        if distinct_errors:
                            if line not in matches:
                                matches.append(line)
                        else:
                            matches.append([line])
                            while line := snap.next_line():
                                if info_pattern.match(line):
                                    break
                                matches[-1].append(line)
                            continue
                    line = snap.next_line()
            return matches
        return await loop.run_in_executor(self.thread_pool, _grep_errors_sync)

    async def find_backtraces(self, from_mark: int | None = None) -> list[str]:
        """
        Find and extract all backtraces from the log file.

        Each backtrace starts with a line "Backtrace:" followed by lines that start with exactly 2 spaces.  
        If `from_mark` argument is given, the log is searched from that position, otherwise from the beginning.  
        Return a list of strings, where each string is a complete backtrace (all lines joined together).  
        """
        loop = asyncio.get_running_loop()
        def _find_backtraces_sync():
            backtraces = []
            with FileSnapshot(self.file, from_mark) as snap:
                line = snap.next_line()
                while line:
                    if line.strip() == "Backtrace:":
                        # Found a backtrace, collect all lines that start with exactly 2 spaces
                        backtrace_lines = [line]
                        while True:
                            next_line = snap.next_line()
                            if not next_line:
                                # End of file
                                line = ""
                                break
                            if next_line.startswith("  ") and not next_line.startswith("   "):
                                # Line starts with exactly 2 spaces (backtrace entry)
                                backtrace_lines.append(next_line)
                            else:
                                # End of backtrace
                                line = next_line
                                break
                        if backtrace_lines:
                            # Join all backtrace lines into a single string
                            backtraces.append(''.join(backtrace_lines))
                        # Continue from current line (already read in the inner loop)
                        continue
                    line = snap.next_line()
            return backtraces
        return await loop.run_in_executor(self.thread_pool, _find_backtraces_sync)
