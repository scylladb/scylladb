import asyncio
import os
import tempfile
import pathlib
import pytest
from test.pylib.util import read_last_line, gather_safely_and_ignore_specific_exceptions

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


def test_gather_safely_and_ignore_specific_exceptions_string_pattern_does_not_match_per_character() -> None:
    async def _fails() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(gather_safely_and_ignore_specific_exceptions(
            [_fails()],
            "connection is closed",
        ))


@pytest.mark.parametrize(
    "error_message, patterns",
    [
        pytest.param("connection was closed", ("connection was closed",), id="iterable-patterns"),
        pytest.param("some message", ("runtimeerror",), id="exception-type-name"),
        pytest.param("  Timed Out While Closing ", ("", "   ", " timed out "), id="normalized-patterns"),
    ],
)
def test_gather_safely_and_ignore_specific_exceptions_legacy_mode_matches_expected(error_message, patterns) -> None:
    async def _fails() -> None:
        raise RuntimeError(error_message)

    results = asyncio.run(gather_safely_and_ignore_specific_exceptions(
        [_fails()],
        patterns,
    ))

    assert isinstance(results[0], RuntimeError)


def test_gather_safely_and_ignore_specific_exceptions_varargs_form() -> None:
    async def _ok() -> int:
        return 7

    async def _fails() -> None:
        raise RuntimeError("Connection IS Closed")

    results = asyncio.run(gather_safely_and_ignore_specific_exceptions(
        _ok(),
        _fails(),
        "connection is closed",
    ))

    assert results[0] == 7
    assert isinstance(results[1], RuntimeError)


def test_gather_safely_and_ignore_specific_exceptions_raises_unexpected_error() -> None:
    async def _fails() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(gather_safely_and_ignore_specific_exceptions(
            [_fails()],
            ["connection is closed"],
        ))


@pytest.mark.parametrize(
    "ignore_patterns, error_match",
    [
        pytest.param(1, "string or an iterable of strings", id="non-iterable-patterns"),  # type: ignore[arg-type]
        pytest.param(["ok", 2], "iterable of strings", id="mixed-iterable-patterns"),  # type: ignore[list-item]
    ],
)
def test_gather_safely_and_ignore_specific_exceptions_rejects_invalid_patterns(ignore_patterns, error_match) -> None:
    with pytest.raises(TypeError, match=error_match):
        asyncio.run(gather_safely_and_ignore_specific_exceptions([], ignore_patterns))


@pytest.mark.parametrize(
    "args, error_match",
    [
        pytest.param(([1], "boom"), "awaitables iterable should contain only awaitables", id="invalid-awaitable-iterable"),  # type: ignore[list-item]
        pytest.param((1, "connection is closed"), "invalid argument type for gather", id="invalid-vararg-type"),  # type: ignore[arg-type]
    ],
)
def test_gather_safely_and_ignore_specific_exceptions_rejects_invalid_inputs(args, error_match) -> None:
    with pytest.raises(TypeError, match=error_match):
        asyncio.run(gather_safely_and_ignore_specific_exceptions(*args))


def test_gather_safely_and_ignore_specific_exceptions_rejects_awaitable_after_patterns() -> None:
    async def _ok() -> int:
        return 1

    coro = _ok()
    with pytest.raises(TypeError, match="awaitables must come before"):
        asyncio.run(gather_safely_and_ignore_specific_exceptions("connection is closed", coro))
    coro.close()


def test_gather_safely_and_ignore_specific_exceptions_fails_when_no_patterns_match() -> None:
    async def _fails() -> None:
        raise ValueError("not ignored")

    with pytest.raises(ValueError, match="not ignored"):
        asyncio.run(gather_safely_and_ignore_specific_exceptions([_fails()], []))


def test_gather_safely_and_ignore_specific_exceptions_fails_on_unexpected_with_mixed_results() -> None:
    async def _ok() -> int:
        return 9

    async def _ignored() -> None:
        raise RuntimeError("connection is closed")

    async def _unexpected() -> None:
        raise ValueError("fatal")

    with pytest.raises(ValueError, match="fatal"):
        asyncio.run(gather_safely_and_ignore_specific_exceptions(
            [_ok(), _ignored(), _unexpected()],
            ["connection is closed"],
        ))

