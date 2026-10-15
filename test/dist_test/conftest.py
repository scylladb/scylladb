#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Harness for unit testing dist/common/scripts.

Each script is run in-process as ``__main__`` on top of a pyfakefs fake
filesystem, with every subprocess entry point replaced by `FakeRun`.

Coverage report (from the repo root):

    uv run --project test --with pytest-cov python -m pytest test/dist_test \\
        --confcutdir=test/dist_test --cov=dist/common/scripts --cov-report=term-missing
"""
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / 'dist' / 'common' / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

import scylla_util  # noqa: E402

# Read while the real filesystem is still visible; pyfakefs hides it.
_sources = {p.name: p.read_text() for p in SCRIPTS_DIR.iterdir() if p.is_file()}


class FakeRun:
    """Stands in for subprocess.run/call/check_call/check_output/Popen.

    Every command is recorded in `calls` as a string.  The first `responses`
    key that is a substring of the command picks its (returncode, stdout);
    unmatched commands succeed with empty output.
    """

    def __init__(self):
        self.calls = []
        self.responses = {}

    def set(self, cmd, stdout='', returncode=0):
        self.responses[cmd] = (returncode, stdout)

    def _result(self, cmd, kwargs):
        cmd_str = cmd if isinstance(cmd, str) else ' '.join(str(c) for c in cmd)
        self.calls.append(cmd_str)
        returncode, stdout = next((v for k, v in self.responses.items() if k in cmd_str), (0, ''))
        if callable(stdout):
            stdout = stdout()
        text = kwargs.get('encoding') or kwargs.get('text') or kwargs.get('universal_newlines')
        if not text:
            stdout = stdout.encode()
        return subprocess.CompletedProcess(cmd, returncode, stdout, '' if text else b'')

    def run(self, cmd, *args, check=False, **kwargs):
        res = self._result(cmd, kwargs)
        if check:
            res.check_returncode()
        return res

    def call(self, cmd, *args, **kwargs):
        return self._result(cmd, kwargs).returncode

    def check_call(self, cmd, *args, **kwargs):
        self._result(cmd, kwargs).check_returncode()
        return 0

    def check_output(self, cmd, *args, **kwargs):
        res = self._result(cmd, kwargs)
        res.check_returncode()
        return res.stdout

    def popen(self, cmd, *args, **kwargs):
        res = self._result(cmd, kwargs)
        return SimpleNamespace(args=cmd, pid=4242, returncode=res.returncode, stdout=None,
                               communicate=lambda *a, **kw: (res.stdout, res.stderr),
                               wait=lambda *a, **kw: res.returncode, poll=lambda: res.returncode)

    def __contains__(self, cmd):
        return any(cmd in c for c in self.calls)


class Scripts:
    def __init__(self, monkeypatch, fs, fake_run):
        self.monkeypatch = monkeypatch
        self.fs = fs
        self.cmd = fake_run

    def distro(self, name, like=None, version='9'):
        """Pretend to run on distro `name` (e.g. 'rhel', 'ubuntu', 'gentoo')."""
        import distro
        like = {'rhel': 'fedora', 'centos': 'rhel fedora', 'ubuntu': 'debian',
                'opensuse-leap': 'suse opensuse'}.get(name, '') if like is None else like
        self.monkeypatch.setattr(distro, 'id', lambda: name)
        self.monkeypatch.setattr(distro, 'like', lambda: like)
        self.monkeypatch.setattr(distro, 'version', lambda *a, **kw: version)
        self.monkeypatch.setattr(distro, 'major_version', lambda *a, **kw: version.split('.')[0])
        self.monkeypatch.setattr(distro, 'name', lambda *a, **kw: name)

    def file(self, path, contents=''):
        return self.fs.create_file(path, contents=contents)

    def read(self, path):
        return Path(path).read_text()

    def run(self, name, *argv, uid=0):
        """Run script `name` as __main__ with `argv`; returns its SystemExit code (0 if it didn't exit)."""
        self.monkeypatch.setattr(sys, 'argv', [f'/opt/scylladb/scripts/{name}', *argv])
        self.monkeypatch.setattr('os.getuid', lambda: uid)
        try:
            exec(compile(_sources[name], str(SCRIPTS_DIR / name), 'exec'), {'__name__': '__main__', '__file__': str(SCRIPTS_DIR / name)})
        except SystemExit as e:
            return e.code if e.code is not None else 0
        return 0


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item):
    # test.py's reporting hooks write logs/artifacts, which must reach the real filesystem
    try:
        return (yield)
    finally:
        if fs := item.funcargs.get('fs'):
            fs.pause()


@pytest.fixture
def fake_run(monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr(subprocess, 'run', fake.run)
    monkeypatch.setattr(subprocess, 'call', fake.call)
    monkeypatch.setattr(subprocess, 'check_call', fake.check_call)
    monkeypatch.setattr(subprocess, 'check_output', fake.check_output)
    monkeypatch.setattr(subprocess, 'Popen', fake.popen)
    monkeypatch.setattr(scylla_util, 'run', fake.run)
    return fake


@pytest.fixture
def scripts(monkeypatch, fs, fake_run):
    monkeypatch.setattr('time.sleep', lambda s: None)
    s = Scripts(monkeypatch, fs, fake_run)
    s.distro('rhel')
    return s
