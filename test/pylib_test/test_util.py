import asyncio
import os
import tempfile
import pathlib
from concurrent.futures import ThreadPoolExecutor

import pytest
from test.pylib.util import Blocking, read_last_line, run_sync, scale_timeout_by_mode

def test_read_last_line():
    test_cases = [
        (b"This is the first line.\nThis is the second line.\nThis is the third line.", 'This is the third line.'),
        (b"This is another file.\nIt has a few lines.\nThe last line is what we're interested in.", 'The last line is what we\'re interested in.'),
        (b"This file has only one line.", 'This file has only one line.'),
        (b"\n", ""),
        (b"\n\n\n", ""),
        (b"", ""),
        (b"abc\n", 'abc'),
        (b"abc", '...bc', 2),
        (b"lalala\nbububu", "bububu"),
        (b"line1\nline2\nline3\n", "...line3", 6),
        (b"line1\nline2\nline3", "line3", 6),
        (b"line1\nline2\nline3\n", "line3", 7),
        (b"\xbe\xbe\xbe\xbebububu\n", "bububu")
    ]
    for test_case in test_cases:
        with tempfile.NamedTemporaryFile(dir=os.getenv('TMPDIR', '/tmp')) as f:
            f.write(test_case[0])
            f.flush()
            file_path = pathlib.Path(f.name)
            actual = read_last_line(file_path, test_case[2]) if len(test_case) == 3 else read_last_line(file_path)
            assert(actual == test_case[1])


@pytest.mark.parametrize(
    "mode,base_timeout,expected",
    [
        ("debug", 10, 30),
        ("sanitize", 10, 30),
        ("release", 10, 10),
        ("dev", 10, 20),
        ("coverage", 10, 30),
        ("custom_exe", 10, 10),
    ],
)
def test_scale_timeout_by_mode(mode, base_timeout, expected):
    assert scale_timeout_by_mode(mode, base_timeout) == expected


def test_run_sync_leaves_the_current_loop_alone():
    """run_sync() runs the coroutine on a loop of its own, closes it and keeps the thread's current loop current."""

    async def running_loop() -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def check() -> None:
        # A thread with a current loop that isn't running, like dtest's main
        # thread between the async fixtures of pytest-asyncio.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            used = run_sync(running_loop())
            assert used is not loop
            assert used.is_closed()
            assert asyncio.get_event_loop() is loop
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    with ThreadPoolExecutor(1) as executor:
        executor.submit(check).result()


class _Pending:
    def __await__(self):
        yield


class _Server:
    def __init__(self) -> None:
        self.name = "s1"

    async def add(self, a: int, b: int) -> int:
        await asyncio.sleep(0)
        return a + b

    def describe(self) -> str:
        return f"server {self.name}"

    def pending(self) -> _Pending:
        return _Pending()


def test_blocking_runs_coroutines_and_passes_the_rest_through():
    """Blocking runs calls that return a coroutine, rejects other awaitables and passes everything else through."""

    server = _Server()
    view = Blocking(server)

    assert view.add(1, 2) == 3
    assert view.describe() == "server s1"
    assert view.name == "s1"
    with pytest.raises(TypeError, match="cannot run"):
        view.pending()

    view.name = "s2"
    assert server.name == "s2"
