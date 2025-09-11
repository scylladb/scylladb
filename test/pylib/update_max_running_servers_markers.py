#! /usr/bin/env python3

#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import argparse
import ast
import sqlite3
from collections import defaultdict
from pathlib import Path

from test import TEST_DIR
from test.pylib.db.writer import CLUSTER_METRICS_TABLE


MARKER_NAME = "max_running_servers"
ARGUMENT_NAME = "amount"
COLUMN_NAME = "max_running_servers"


def get_test_data(db_paths: list[Path]) -> dict[Path, dict[str | None, dict[str, set[int]]]]:
    """Connect to the SQLite database and retrieves test data.

    Args:
        db_paths: list of Path to the SQLite database files.

    Returns:
        A dictionary mapping file paths to a nested dictionary, which maps
        class names (or None for standalone functions) to another dictionary
        mapping test function names to their max_running_servers (can be
        multiple values.)
        e.g., {Path('...'): {'MyClass': {'test_a': {1}}, None: {'test_b': {2}}}}
    """
    tests_by_file = defaultdict(lambda: defaultdict(lambda: defaultdict(set[int])))

    for db_path in db_paths:
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found at: {db_path}")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        query = f"SELECT test_name, {COLUMN_NAME} FROM {CLUSTER_METRICS_TABLE};"

        try:
            cursor.execute(query)
            for test_name, max_running_servers in cursor.fetchall():
                try:
                    file_name, *class_name, func_name = test_name.split("::")
                    if len(class_name) > 1:
                        raise ValueError(f"Unsupported test name format (nested classes?)")
                    class_name = class_name[0] if class_name else None
                    func_name = func_name.rsplit(".", maxsplit=2)[0]  # cut '.<mode>.<run_id>' suffix
                    func_name = func_name.split("[", maxsplit=1)[0]  # cut test parameters
                except ValueError as err:
                    print(f"Error parsing {test_name}: {err}")
                else:
                    tests_by_file[Path(file_name)][class_name][func_name].add(max_running_servers)
        finally:
            conn.close()

    return tests_by_file


def has_marker(func_node: ast.FunctionDef | ast.AsyncFunctionDef, marker_name: str) -> bool:
    """Check if a function node already has a specific pytest marker."""

    for decorator in func_node.decorator_list:
        if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
            # Handles @pytest.mark.marker_name(...)
            if (isinstance(decorator.func.value, ast.Attribute) and
                    isinstance(decorator.func.value.value, ast.Name) and
                    decorator.func.value.value.id == "pytest" and
                    decorator.func.value.attr == "mark" and
                    decorator.func.attr == marker_name):
                return True
    return False


class MarkerVisitor(ast.NodeVisitor):
    """AST visitor to find test functions and methods to mark."""

    def __init__(self, tests_to_mark: dict[str | None, dict[str, set[int]]], file_path: Path):
        self.tests_to_mark = tests_to_mark
        self.file_path = file_path
        self.insertions: list[tuple[int, str]] = []
        self._current_class_name: str | None = None
        self.pytest_imported = False

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "pytest":
                self.pytest_imported = True
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        original_class = self._current_class_name
        self._current_class_name = node.name
        self.generic_visit(node)
        self._current_class_name = original_class

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        class_tests = self.tests_to_mark.get(self._current_class_name, {})
        if node.name in class_tests:
            if not has_marker(node, MARKER_NAME):
                arg = class_tests[node.name]
                if len(arg) > 1:
                    print(f"Warning: We have multiple values for {node.name}: {list(arg)}. Use maximum.")
                arg = max(arg)
                indentation = " " * node.col_offset
                marker_line = f"{indentation}@pytest.mark.{MARKER_NAME}({ARGUMENT_NAME}={arg})"

                # AST line numbers are 1-based, list indices are 0-based.
                self.insertions.append((node.lineno - 1, marker_line))
            else:
                print(f"Info: Marker '{MARKER_NAME}' already exists on '{node.name}' in {self.file_path}. Skipping.")
        self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef


def add_markers_to_file(file_path: Path, tests_to_mark: dict[str | None, dict[str, set[int]]]):
    """Add pytest markers to the specified functions in a Python file.

    Also add 'import pytest' if it's missing.

    Args:
        file_path: The path to the Python file to modify.
        tests_to_mark: A dict mapping class names/None to function names to the marker argument.
    """
    if not file_path.exists():
        print(f"Warning: Test file not found, skipping: {file_path}")
        return

    source_code = file_path.read_text()
    lines = source_code.splitlines()

    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        print(f"Error parsing {file_path}: {e}")
        return

    visitor = MarkerVisitor(tests_to_mark, file_path)
    visitor.visit(tree)

    if not visitor.insertions:
        return  # no changes to make

    # Apply insertions in reverse to avoid messing up line indices.
    for line_num, marker in sorted(visitor.insertions, reverse=True):
        lines.insert(line_num, marker)

    # If we are adding markers, ensure pytest is imported.
    if not visitor.pytest_imported:
        import_line_num = 0

        # Find the best place to insert the import statement.
        if tree.body:
            last_import_node = None
            for node in tree.body:
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    last_import_node = node
                elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                    # This is likely a module docstring.
                    last_import_node = node

            if last_import_node:
                import_line_num = last_import_node.end_lineno

        if import_line_num > min(visitor.insertions)[0]:
            print(f"Error: in {file_path} the last import is below the first test to add the marker")
            return

        lines.insert(import_line_num, "import pytest")
        print(f"Adding 'import pytest' to {file_path}")

    print(f"Updating {file_path} with {len(visitor.insertions)} new marker(s).")
    file_path.write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Add pytest markers to test files based on data from an SQLite database(s)."
    )
    parser.add_argument(
        "db_path",
        nargs="+",
        type=Path,
        help="path to the SQLite .db file"
    )
    parser.add_argument(
        "--path",
        type=Path,
        help="process files only under the PATH",
    )
    args = parser.parse_args()

    try:
        tests_by_file = get_test_data(db_paths=args.db_path)
        if not tests_by_file:
            print("No test data found in the database.")
            return

        for file_rel_path, tests in tests_by_file.items():
            file_abs_path = TEST_DIR / file_rel_path
            if args.path and not file_abs_path.is_relative_to(args.path.absolute()):
                continue
            add_markers_to_file(file_abs_path, tests)

    except Exception as e:
        print(f"\nAn error occurred: {e}")


if __name__ == "__main__":
    main()
