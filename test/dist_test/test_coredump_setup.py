#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import builtins
import errno
import importlib.util
import logging
import os


# ---------------------------------------------------------------------------
# Load scyllasetup from dist/docker/ using importlib so we test the real
# production code rather than a duplicated copy of the logic.
# ---------------------------------------------------------------------------

_SCYLLASETUP_PATH = os.path.join(
    os.path.dirname(__file__), '..', '..', 'dist', 'docker', 'scyllasetup.py',
)

def _load_scyllasetup():
    spec = importlib.util.spec_from_file_location('scyllasetup', _SCYLLASETUP_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


scyllasetup = _load_scyllasetup()


def _make_setup(tmp_path, core_pattern_content):
    """Create a ScyllaSetup-like object with paths redirected to tmp_path.

    Returns (setup_obj, core_pattern_file, coredump_dir).
    """
    core_pattern_file = tmp_path / "core_pattern"
    core_pattern_file.write_text(core_pattern_content)

    coredump_dir = tmp_path / "coredump"
    # coredump_dir is intentionally NOT pre-created; the method should create it.

    # Build a minimal object that has coredumpSetup bound to it,
    # with the class-level path constants overridden for testing.
    setup = object.__new__(scyllasetup.ScyllaSetup)
    setup.CORE_PATTERN_PATH = str(core_pattern_file)
    setup._coredump_dir = str(coredump_dir)

    return setup, core_pattern_file, coredump_dir


class TestCoredumpSetup:
    """Unit tests for ScyllaSetup.coredumpSetup()."""

    def test_pipe_pattern_is_overridden(self, tmp_path):
        """A pipe-based pattern is replaced with a file-based one."""
        setup, core_file, coredump_dir = _make_setup(
            tmp_path,
            "|/usr/share/apport/apport -p%p -s%s -c%c -d%d -P%P -u%u -g%g -- %E\n",
        )
        setup.coredumpSetup()
        result = core_file.read_text().strip()
        assert result == f"{coredump_dir}/core.%e.%p.%t"
        assert coredump_dir.is_dir()

    def test_file_pattern_is_left_alone(self, tmp_path):
        """A file-based pattern is not modified."""
        original = "core.%e.%p.%t"
        setup, core_file, _ = _make_setup(tmp_path, original)
        setup.coredumpSetup()
        assert core_file.read_text().strip() == original

    def test_systemd_coredump_pattern_is_overridden(self, tmp_path):
        """systemd-coredump pipe pattern is replaced."""
        setup, core_file, coredump_dir = _make_setup(
            tmp_path,
            "|/usr/lib/systemd/systemd-coredump %p %u %g %s %t %e\n",
        )
        setup.coredumpSetup()
        result = core_file.read_text().strip()
        assert result == f"{coredump_dir}/core.%e.%p.%t"

    def test_readonly_pattern_file_logs_warning(self, tmp_path, monkeypatch, caplog):
        """When the pattern file is not writable, a warning is logged."""
        setup, core_file, _ = _make_setup(
            tmp_path,
            "|/usr/share/apport/apport -p%p\n",
        )
        real_open = builtins.open

        def raising_open(path, mode="r", *args, **kwargs):
            if path == str(core_file) and mode == "w":
                raise OSError(errno.EACCES, "Permission denied")
            return real_open(path, mode, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", raising_open)

        with caplog.at_level(logging.WARNING):
            setup.coredumpSetup()
        assert "permission" in caplog.text.lower()
        # Original content is preserved.
        assert core_file.read_text().startswith("|")

    def test_readonly_filesystem_logs_warning(self, tmp_path, monkeypatch, caplog):
        """A read-only filesystem error logs the same host-side guidance."""
        setup, core_file, _ = _make_setup(
            tmp_path,
            "|/usr/share/apport/apport -p%p\n",
        )
        real_open = builtins.open

        def raising_open(path, mode="r", *args, **kwargs):
            if path == str(core_file) and mode == "w":
                raise OSError(errno.EROFS, "Read-only file system")
            return real_open(path, mode, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", raising_open)

        with caplog.at_level(logging.WARNING):
            setup.coredumpSetup()

        assert "permission" in caplog.text.lower()
        assert "kernel.core_pattern" in caplog.text
        assert core_file.read_text().startswith("|")

    def test_unexpected_oserror_logs_debug(self, tmp_path, monkeypatch, caplog):
        """An unexpected OSError errno is logged at DEBUG, not propagated."""
        setup, core_file, _ = _make_setup(
            tmp_path,
            "|/usr/share/apport/apport -p%p\n",
        )
        real_open = builtins.open

        def raising_open(path, mode="r", *args, **kwargs):
            if path == str(core_file) and mode == "w":
                raise OSError(errno.ENOSPC, "No space left on device")
            return real_open(path, mode, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", raising_open)

        with caplog.at_level(logging.DEBUG):
            setup.coredumpSetup()  # should not raise

        assert "Unexpected OSError" in caplog.text
        assert core_file.read_text().startswith("|")

    def test_coredump_dir_is_created(self, tmp_path):
        """The coredump directory is created even when no override happens."""
        setup, _, coredump_dir = _make_setup(tmp_path, "core")
        assert not coredump_dir.exists()
        setup.coredumpSetup()
        assert coredump_dir.is_dir()

    def test_missing_pattern_file(self, tmp_path):
        """If the pattern file doesn't exist, the method returns silently."""
        setup, core_file, coredump_dir = _make_setup(tmp_path, "dummy")
        core_file.unlink()  # remove it
        setup.coredumpSetup()  # should not raise
        assert coredump_dir.is_dir()

    def test_empty_pattern(self, tmp_path):
        """An empty pattern is not pipe-based, so no override."""
        setup, core_file, _ = _make_setup(tmp_path, "")
        setup.coredumpSetup()
        assert core_file.read_text() == ""

    def test_makedirs_failure_does_not_override_pattern(self, tmp_path, monkeypatch, caplog):
        """If coredump dir creation fails, core_pattern is not modified."""
        setup, core_file, coredump_dir = _make_setup(
            tmp_path,
            "|/usr/share/apport/apport -p%p\n",
        )
        real_makedirs = os.makedirs

        def failing_makedirs(path, *args, **kwargs):
            if path == str(coredump_dir):
                raise OSError(errno.EACCES, "Permission denied")
            return real_makedirs(path, *args, **kwargs)

        monkeypatch.setattr(os, "makedirs", failing_makedirs)

        with caplog.at_level(logging.WARNING):
            setup.coredumpSetup()

        assert "Could not create coredump directory" in caplog.text
        # core_pattern must NOT have been modified
        assert core_file.read_text().startswith("|")
