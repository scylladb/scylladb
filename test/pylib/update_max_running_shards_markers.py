#! /usr/bin/env python3

#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Backfill `max_running_shards` markers from measured runs.

Run the test, then let what it actually did write the marker:

    ./test.py --mode dev test/cluster/my_new_test.py
    ./test/pylib/update_max_running_shards_markers.py testlog/sqlite_*.db

No flag needed: an unmarked test has no claim, so nothing caps what is
recorded.  Re-measuring one that already has a claim needs
`pytest --measure-running-shards` -- see measured_shards().

Idempotent: it writes a missing marker, raises one that is too low, and leaves
the rest alone, a claim being an upper bound.
"""

import argparse
import ast
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# Run directly, sys.path[0] is this directory, so `test` is not importable yet.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from test import TEST_DIR
from test.pylib.db.writer import CLUSTER_METRICS_TABLE
from test.pylib.running_shards import MARKER


def parse_nodeid(nodeid):
    """(file, class or None, function) for a nodeid, parameters stripped.

    One function carries one marker, so parametrized cases fold together.
    Raises ValueError for a nodeid this cannot place in the source, e.g. one
    naming a nested class.
    """
    file_name, *rest = nodeid.split("::")
    if not rest:
        raise ValueError("no test in the nodeid")
    *class_name, func_name = rest
    if len(class_name) > 1:
        raise ValueError("nested classes are not supported")
    func_name = func_name.split("[", maxsplit=1)[0]  # drop the parameters
    return Path(file_name), (class_name[0] if class_name else None), func_name


def measured_shards(db_paths, headroom):
    """Peak shards measured for each test that passed, with no claim in force.

    Returns file -> class (or None) -> function -> shards.  Takes the maximum
    over every row that maps to one marker: modes, hosts, --repeat copies and
    parameters.

    Only runs that passed outright, with no claim, count.  A test that failed,
    was skipped part-way or xfailed stopped before its peak, and a run with a
    claim cannot exceed it, so all of them report too little.  Writing such a
    value as a claim makes the test fail on its own claim later.  That is why
    raising a claim needs --measure-running-shards.

    Both facts are read off the row itself: tests.id is shared by every run of
    the same test, so a join through it would let one run's outcome vouch for
    another run's peak.
    """
    per_test = {}

    for db_path in db_paths:
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found at: {db_path}")
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute(
                f"SELECT nodeid, MAX(max_running_shards) FROM {CLUSTER_METRICS_TABLE} "
                f"WHERE status IN ('passed', 'xpassed') AND claim IS NULL GROUP BY nodeid"
            ).fetchall()
        finally:
            conn.close()

        for nodeid, shards in rows:
            try:
                key = parse_nodeid(nodeid)
            except ValueError as err:
                print(f"Skipping {nodeid}: {err}")
                continue
            per_test[key] = max(per_test.get(key, 0), shards)

    by_file = defaultdict(lambda: defaultdict(dict))
    for (file_name, class_name, func_name), shards in per_test.items():
        if not shards:
            # It leased a cluster but started no server, so it has nothing to
            # claim.  A marker of 0 would also break the first time it did
            # start one.
            continue
        by_file[file_name][class_name][func_name] = shards + headroom
    return by_file


def find_marker(func_node: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.Call | None:
    """The max_running_shards decorator on `func_node`, if any.

    Any spelling of it, `@pt.mark.max_running_shards(2)` included.  Only
    written_as_pytest_mark() can be rewritten, but one this did not see at all
    would get a second marker added above it -- and the one below, being
    closer to the test, is the one that counts.
    """
    for decorator in func_node.decorator_list:
        match decorator:
            case ast.Call(func=ast.Attribute(attr=str(attr))) if attr == MARKER:
                return decorator
    return None


def written_as_pytest_mark(marker: ast.Call) -> bool:
    """Whether the marker is spelled `pytest.mark.<MARKER>`, the only spelling this writes."""
    match marker.func:
        case ast.Attribute(value=ast.Attribute(value=ast.Name(id="pytest"), attr="mark")):
            return True
    return False


def claim_of(marker: ast.Call) -> int | None:
    """The shard count an existing marker claims, or None if it is not a plain literal.

    A computed or non-integer claim is left for a human: this script must not
    guess at what it meant.
    """
    args = list(marker.args) + [kw.value for kw in marker.keywords if kw.arg == "amount"]
    match args:
        case [ast.Constant(value=int(claim))] if not isinstance(claim, bool):
            return claim
    return None


class MarkerEditor(ast.NodeVisitor):
    """Collects the line edits that bring one file's markers up to date."""

    def __init__(self, tests: dict[str | None, dict[str, int]], file_path: Path) -> None:
        self.tests = tests
        self.file_path = file_path
        # Line inserts and whole-line replacements, both 0-based, applied in
        # reverse so earlier edits do not shift later ones.
        self.inserts: list[tuple[int, str]] = []
        self.replacements: list[tuple[int, int, str]] = []
        self._class_name: str | None = None
        # Only a module-level function, or a method of one class, is a test that
        # a nodeid can name.  Anything deeper is a helper defined inside one.
        self._depth = 0

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        outer, self._class_name = self._class_name, node.name
        self.generic_visit(node)
        self._class_name = outer

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        shards = self.tests.get(self._class_name, {}).get(node.name)
        if shards is not None and not self._depth:
            self._update(node, shards)
        self._depth += 1
        self.generic_visit(node)
        self._depth -= 1

    visit_AsyncFunctionDef = visit_FunctionDef

    def _update(self, node: ast.FunctionDef | ast.AsyncFunctionDef, shards: int) -> None:
        where = f"{self.file_path.name}::{node.name}"
        marker = find_marker(node)
        if marker is None:
            indent = " " * node.col_offset
            # Above the whole decorator stack, so the claim is the first thing
            # read about the test.  AST lines are 1-based, list indices are not.
            first = min([node.lineno] + [d.lineno for d in node.decorator_list]) - 1
            self.inserts.append((first, f"{indent}@pytest.mark.{MARKER}({shards})"))
            return

        claim = claim_of(marker)
        if claim is None or not written_as_pytest_mark(marker):
            print(f"Skipping {where}: cannot read or rewrite its {MARKER} marker, measured {shards}")
        elif claim < shards:
            indent = " " * (marker.col_offset - 1)  # the '@' sits one column left
            self.replacements.append(
                (marker.lineno - 1, marker.end_lineno - 1,
                 f"{indent}@pytest.mark.{MARKER}({shards})"))
            print(f"Raising {where}: {MARKER} {claim} -> {shards}")


def module_binds_pytest(tree: ast.Module, above: int) -> bool:
    """Whether `import pytest` runs at module level before line `above`.

    Where a decorator reads the name: an import inside a helper, under another
    name, or below the marker leaves it unbound at collection time.
    """
    return any(node.lineno <= above and isinstance(node, ast.Import)
               and any(a.name == "pytest" and a.asname in (None, "pytest") for a in node.names)
               for node in tree.body)


def module_body_start(tree: ast.Module) -> int:
    """The line a module's own code starts on, past its docstring."""
    return tree.body[0].end_lineno if ast.get_docstring(tree) else tree.body[0].lineno - 1


def update_file(file_path: Path, tests: dict[str | None, dict[str, int]]) -> bool:
    """Bring one file's markers up to date. Returns whether it was changed."""
    if not file_path.exists():
        print(f"Warning: test file not found, skipping: {file_path}")
        return False

    source = file_path.read_text()
    try:
        tree = ast.parse(source)
    except SyntaxError as err:
        print(f"Error parsing {file_path}: {err}")
        return False

    editor = MarkerEditor(tests, file_path)
    editor.visit(tree)
    if not editor.inserts and not editor.replacements:
        return False

    # split("\n") round-trips exactly: splitlines() also breaks on form feeds
    # and U+2028, and re-joining would rewrite those lines.  A file ending in a
    # newline yields a final "" here, so the join restores it as it was.
    lines = source.split("\n")
    # One pass, bottom-up, treating an insert as a replacement of zero lines.
    # Every position comes from the original AST.  Replacing a marker written
    # over several lines with one line changes the line count, so it must not
    # run before the edits below it are applied.
    edits = [(start, end + 1, marker) for start, end, marker in editor.replacements]
    edits += [(line, line, marker) for line, marker in editor.inserts]
    for start, end, marker in sorted(edits, reverse=True):
        lines[start:end] = [marker]

    first_marker = min((line for line, _ in editor.inserts), default=0)
    if editor.inserts and not module_binds_pytest(tree, above=first_marker):
        # A marker we write needs the name.  After the last import above it, so
        # it lands below the licence header and the docstring; with no such
        # import, at the top of the module body, which is above the marker
        # whether the test is a function or a method of a class.
        above = [node.end_lineno for node in tree.body
                 if isinstance(node, (ast.Import, ast.ImportFrom)) and node.end_lineno <= first_marker]
        if above:
            lines.insert(max(above), "import pytest")
        else:
            at = module_body_start(tree)
            lines[at:at] = ["import pytest", ""]
        print(f"Adding 'import pytest' to {file_path}")

    print(f"Updating {file_path}: {len(editor.inserts)} new, {len(editor.replacements)} raised")
    file_path.write_text("\n".join(lines))
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description=f"Write {MARKER} markers into the test sources from measured runs.",
        epilog="Produce the measurements with ./test.py --measure-running-shards.",
    )
    parser.add_argument("db_path", nargs="+", type=Path,
                        help="SQLite .db file(s) written by a measurement run, e.g. testlog/sqlite_*.db")
    parser.add_argument("--path", type=Path,
                        help="only update files under PATH")
    parser.add_argument("--headroom-shards", type=int, default=0, metavar="N",
                        help="claim N shards more than measured. A claim is an upper bound, so "
                             "headroom keeps a test that varies its cluster from failing on the "
                             "run where it uses most (default: 0, claim exactly what was measured)")
    args = parser.parse_args()
    if args.headroom_shards < 0:
        # A negative value would write a claim below the measured peak, and far
        # enough below it a zero or negative marker.  claimed_shards() rejects
        # those, so the tool would write source that fails collection.
        parser.error("--headroom-shards cannot be negative")

    by_file = measured_shards(db_paths=args.db_path, headroom=args.headroom_shards)
    if not by_file:
        print(f"No unrestricted {CLUSTER_METRICS_TABLE} rows found. A claim caps the peak "
              f"recorded under it, so measuring takes --measure-running-shards -- was the "
              f"run made with it, and did any cluster test pass?")
        return 1

    changed = 0
    for file_rel_path, tests in sorted(by_file.items()):
        file_abs_path = TEST_DIR / file_rel_path
        if args.path and not file_abs_path.is_relative_to(args.path.absolute()):
            continue
        changed += update_file(file_abs_path, tests)

    print(f"{changed} file(s) updated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
