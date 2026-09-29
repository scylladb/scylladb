#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""The boost listing cache: list a test binary once per build, not once per worker."""
import json
import multiprocessing
import pathlib

import pytest

from test.pylib import sched_dir
from test.pylib.cpp.boost import _listing_cache_dir, _listing_cache_file, _shared_listing

IDENT = (lambda tree: tree, lambda raw: raw)


@pytest.fixture(autouse=True)
def _cache_in_tmp(tmp_path, monkeypatch):
    """Keep each test's cache in a run directory of its own."""
    monkeypatch.setattr(sched_dir, "_root", tmp_path / "sched")
    _listing_cache_dir.cache_clear()
    yield
    _listing_cache_dir.cache_clear()


def _exe(tmp_path: pathlib.Path, body: bytes = b"binary") -> pathlib.Path:
    exe = tmp_path / "some_test"
    exe.write_bytes(body)
    return exe


def test_second_caller_does_not_run_the_binary(tmp_path):
    exe = _exe(tmp_path)
    calls = []

    def compute():
        calls.append(1)
        return {"some_test": ["a", "b"]}

    first = _shared_listing(exe, False, "plain", compute, *IDENT)
    second = _shared_listing(exe, False, "plain", compute, *IDENT)
    assert first == second == {"some_test": ["a", "b"]}
    assert len(calls) == 1, "the listing must be computed once and then read from disk"
    assert _listing_cache_dir().is_dir()


def test_rebuilding_the_binary_invalidates_the_listing(tmp_path):
    exe = _exe(tmp_path)
    before = _listing_cache_file(exe, False, "plain")
    _shared_listing(exe, False, "plain", lambda: {"x": ["old"]}, *IDENT)
    exe.write_bytes(b"binary rebuilt, different size")
    after = _listing_cache_file(exe, False, "plain")
    assert before != after, "the cache key must follow the binary's size and mtime"
    assert _shared_listing(exe, False, "plain", lambda: {"x": ["new"]}, *IDENT) == {"x": ["new"]}


def test_labels_survive_the_round_trip(tmp_path):
    """Labels are sets; JSON has no sets, so they are stored sorted and rebuilt."""
    exe = _exe(tmp_path)
    tree = {"some_test": [["case_a", {"slow", "flaky"}], ["case_b", set()]]}
    enc = lambda t: {k: [[n, sorted(l)] for n, l in v] for k, v in t.items()}
    dec = lambda r: {k: [[n, set(l)] for n, l in v] for k, v in r.items()}
    assert _shared_listing(exe, False, "json", lambda: tree, enc, dec) == tree
    # and again, this time off the disk
    assert _shared_listing(exe, False, "json", lambda: {}, enc, dec) == tree


def test_a_corrupt_cache_file_is_recomputed_not_raised(tmp_path):
    exe = _exe(tmp_path)
    _shared_listing(exe, False, "plain", lambda: {"x": ["a"]}, *IDENT)
    _listing_cache_file(exe, False, "plain").write_text("{ this is not json")
    assert _shared_listing(exe, False, "plain", lambda: {"x": ["b"]}, *IDENT) == {"x": ["b"]}


def test_an_unwritable_build_tree_still_lists(tmp_path, monkeypatch):
    """The cache is an optimisation: if it cannot be stored, collection must still work."""
    exe = _exe(tmp_path)
    (tmp_path / "blocked").write_text("a file where the cache directory would go")
    monkeypatch.setattr(sched_dir, "_root", tmp_path / "blocked" / "sched")
    _listing_cache_dir.cache_clear()
    assert _listing_cache_file(exe, False, "plain") is None
    assert _shared_listing(exe, False, "plain", lambda: {"x": ["a"]}, *IDENT) == {"x": ["a"]}


def _worker(args):
    exe, marker_dir, tmpdir, i = args
    sched_dir.configure(tmpdir)          # what pytest_configure does in a real worker
    marker = pathlib.Path(marker_dir) / f"ran-{i}"

    def compute():
        marker.write_text("1")
        return {"some_test": ["only-once"]}

    return _shared_listing(pathlib.Path(exe), False, "plain", compute, *IDENT)


def test_racing_workers_list_once_between_them(tmp_path):
    """Sixty workers starting together is the case this exists for."""
    exe = _exe(tmp_path)
    markers = tmp_path / "markers"
    markers.mkdir()
    with multiprocessing.Pool(16) as pool:
        results = pool.map(_worker, [(str(exe), str(markers), str(tmp_path), i) for i in range(16)])
    assert all(r == {"some_test": ["only-once"]} for r in results)
    assert len(list(markers.iterdir())) == 1, "the lock must keep all but one worker off the binary"


def test_a_rebuild_does_not_leave_its_old_listing_behind(tmp_path):
    exe = _exe(tmp_path)
    _shared_listing(exe, False, "plain", lambda: {"x": ["old"]}, *IDENT)
    exe.write_bytes(b"binary rebuilt, a different size entirely")
    _shared_listing(exe, False, "plain", lambda: {"x": ["new"]}, *IDENT)
    listings = list(_listing_cache_dir().glob(f"{exe.name}.*.json"))
    assert len(listings) == 1, "the entry from the previous build must be dropped"
    assert json.loads(listings[0].read_text()) == {"x": ["new"]}


def test_nothing_is_carried_between_runs(tmp_path):
    """The controller empties the run directory at session start and removes it at the end."""
    first = _listing_cache_dir()
    (first / "left-behind.json").write_text("{}")
    sched_dir.prepare()                 # the controller, at session start
    _listing_cache_dir.cache_clear()
    second = _listing_cache_dir()
    assert list(second.iterdir()) == [], "a new run must not see the previous one's listings"
    sched_dir.remove()                  # the controller, at session end
    assert not (tmp_path / "sched").exists(), "and a run takes its directory with it"


def test_both_listings_of_one_binary_are_kept(tmp_path):
    """combined_tests is listed plain and as json; storing one must not drop the other."""
    exe = _exe(tmp_path)
    _shared_listing(exe, True, "plain", lambda: {"x": ["p"]}, *IDENT)
    _shared_listing(exe, True, "json", lambda: {"x": ["j"]}, *IDENT)
    calls = []
    assert _shared_listing(exe, True, "plain", lambda: calls.append(1) or {}, *IDENT) == {"x": ["p"]}
    assert _shared_listing(exe, True, "json", lambda: calls.append(1) or {}, *IDENT) == {"x": ["j"]}
    assert calls == [], "both listings must still be cached"


def test_one_binary_in_two_build_modes_keeps_both_listings(tmp_path):
    """build/dev/.../foo_test and build/debug/.../foo_test share a name, not a listing."""
    dev = tmp_path / "dev" / "foo_test"; debug = tmp_path / "debug" / "foo_test"
    for exe in (dev, debug):
        exe.parent.mkdir(parents=True)
        exe.write_bytes(exe.parent.name.encode())
    _shared_listing(dev, False, "plain", lambda: {"x": ["dev"]}, *IDENT)
    _shared_listing(debug, False, "plain", lambda: {"x": ["debug"]}, *IDENT)
    calls = []
    assert _shared_listing(dev, False, "plain", lambda: calls.append(1) or {}, *IDENT) == {"x": ["dev"]}
    assert _shared_listing(debug, False, "plain", lambda: calls.append(1) or {}, *IDENT) == {"x": ["debug"]}
    assert calls == []
