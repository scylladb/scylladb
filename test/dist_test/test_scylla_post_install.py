#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import importlib.util
import os
import sys

import pytest


# ---------------------------------------------------------------------------
# Load the real production script by path, as test_coredump_setup.py does.
# It imports scylla_util (which imports scylla_sysconfdir), so dist/common/
# scripts has to go on sys.path first.
# ---------------------------------------------------------------------------

_SCRIPTS_DIR = os.path.normpath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'dist', 'common', 'scripts',
))
_POST_INSTALL_PATH = os.path.join(_SCRIPTS_DIR, 'scylla_post_install.py')


def _load_post_install():
    if _SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, _SCRIPTS_DIR)
    spec = importlib.util.spec_from_file_location('scylla_post_install', _POST_INSTALL_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


scylla_post_install = _load_post_install()
scylla_util = sys.modules['scylla_util']

HELPER_SLICE_D = 'etc/systemd/system/scylla-helper.slice.d'
JOURNALD_D = 'etc/systemd/system/systemd-journald.service.d'

BIG_MEMORY = 64 * 1024 * 1024   # kB (64GiB) - above the percentage threshold
SMALL_MEMORY = 8 * 1024 * 1024  # kB (8GiB) - below it


class FakeSystemctl:
    """Stands in for the systemctl binary, recording what was asked of it."""

    def __init__(self, version=254):
        self.version = version
        self.calls = []

    def __call__(self, args, capture=False):
        self.calls.append(list(args))
        if args == ['--version']:
            return 'systemd {} ({}.5-1)\n+PAM +AUDIT +SELINUX\n'.format(self.version, self.version)
        return True

    def restarted(self):
        return [c[1] for c in self.calls if c[0] == 'try-restart']

    def reloaded(self):
        return ['daemon-reload'] in self.calls


def make_post_install(tmp_path, *, memory_kb=BIG_MEMORY, online_cpus='0-3',
                      cpuset=None, cgroup_controllers='cpuset cpu io memory pids',
                      kernel_version='6.1.0', systemctl=None):
    """Build a PostInstall rooted at tmp_path, with a fake machine underneath."""
    (tmp_path / 'proc').mkdir(exist_ok=True)
    (tmp_path / 'proc/meminfo').write_text(
        'MemFree:         1000 kB\nMemTotal:       {} kB\n'.format(memory_kb))

    cpu_dir = tmp_path / 'sys/devices/system/cpu'
    cpu_dir.mkdir(parents=True, exist_ok=True)
    (cpu_dir / 'online').write_text(online_cpus + '\n')

    if cgroup_controllers is not None:
        cgroup_dir = tmp_path / 'sys/fs/cgroup'
        cgroup_dir.mkdir(parents=True, exist_ok=True)
        (cgroup_dir / 'cgroup.controllers').write_text(cgroup_controllers + '\n')

    scylla_d = tmp_path / 'etc/scylla.d'
    scylla_d.mkdir(parents=True, exist_ok=True)
    if cpuset is not None:
        (scylla_d / 'cpuset.conf').write_text(cpuset)

    post_install = scylla_post_install.PostInstall(root=tmp_path, kernel_version=kernel_version)
    post_install.run_systemctl = systemctl if systemctl is not None else FakeSystemctl()
    return post_install


CPUSET_PINNED = '''# DO NO EDIT
# This file should be automatically configure by scylla_cpuset_setup
#
# CPUSET="--cpuset 0 --smp 1"
CPUSET="--cpuset 2-3 "
'''

CPUSET_COMMENTED_ONLY = '''# DO NO EDIT
#
# CPUSET="--cpuset 0 --smp 1"
'''


class TestCpuListHelpers:
    """The cpu list helpers shared with scylla_io_setup."""

    @pytest.mark.parametrize('text,cpus', [
        ('0', {0}),
        ('0-1', {0, 1}),
        ('2-3,6', {2, 3, 6}),
        ('2-23,26-47', set(range(2, 24)) | set(range(26, 48))),
        ('0,2,4', {0, 2, 4}),
    ])
    def test_parse_cpu_list(self, text, cpus):
        assert scylla_util.parse_cpu_list(text) == cpus

    @pytest.mark.parametrize('text', ['0', '0-1', '2-3,6', '2-23,26-47', '0,2,4'])
    def test_cpu_list_round_trip(self, text):
        assert scylla_util.cpu_list_to_str(scylla_util.parse_cpu_list(text)) == text

    def test_parse_cpu_list_ignores_blanks(self):
        assert scylla_util.parse_cpu_list(' 0, 2-3 ,') == {0, 2, 3}

    def test_cpu_list_to_str_empty(self):
        assert scylla_util.cpu_list_to_str(set()) == ''


class TestParseCpusetConf:
    """Parsing the forms /etc/scylla.d/cpuset.conf takes in the wild."""

    def _parse(self, tmp_path, content):
        path = tmp_path / 'cpuset.conf'
        path.write_text(content)
        return scylla_util.parse_cpuset_conf(path)

    def test_cpuset_with_trailing_whitespace(self, tmp_path):
        d = self._parse(tmp_path, 'CPUSET="--cpuset 2-23,26-47 "\n')
        assert d['cpuset'] == set(range(2, 24)) | set(range(26, 48))
        assert d['smp'] is None

    def test_cpuset_with_smp(self, tmp_path):
        d = self._parse(tmp_path, 'CPUSET="--cpuset 0 --smp 1"\n')
        assert d['cpuset'] == {0}
        assert d['smp'] == 1

    def test_cpuset_range(self, tmp_path):
        assert self._parse(tmp_path, 'CPUSET="--cpuset 0-1"\n')['cpuset'] == {0, 1}

    def test_commented_example_is_ignored(self, tmp_path):
        assert self._parse(tmp_path, CPUSET_COMMENTED_ONLY)['cpuset'] is None

    def test_live_line_wins_over_comment(self, tmp_path):
        assert self._parse(tmp_path, CPUSET_PINNED)['cpuset'] == {2, 3}

    def test_last_line_wins(self, tmp_path):
        content = 'CPUSET="--cpuset 0-1"\nCPUSET="--cpuset 2-3"\n'
        assert self._parse(tmp_path, content)['cpuset'] == {2, 3}

    def test_missing_file(self, tmp_path):
        assert scylla_util.parse_cpuset_conf(tmp_path / 'nope.conf') == {'cpuset': None, 'smp': None}


class TestFreeCpus:
    def test_free_cpus_is_the_complement(self, tmp_path):
        p = make_post_install(tmp_path, online_cpus='0-3', cpuset=CPUSET_PINNED)
        assert p.free_cpus() == {0, 1}

    def test_no_free_cpus_when_scylla_owns_all(self, tmp_path):
        p = make_post_install(tmp_path, online_cpus='0-3', cpuset='CPUSET="--cpuset 0-3"\n')
        assert p.free_cpus() == set()

    def test_no_free_cpus_when_unpinned(self, tmp_path):
        p = make_post_install(tmp_path, online_cpus='0-3', cpuset=CPUSET_COMMENTED_ONLY)
        assert p.free_cpus() == set()

    def test_no_free_cpus_without_cpuset_conf(self, tmp_path):
        p = make_post_install(tmp_path, online_cpus='0-3', cpuset=None)
        assert p.free_cpus() == set()


class TestMemoryDropin:
    def test_written_on_a_small_machine(self, tmp_path):
        make_post_install(tmp_path, memory_kb=SMALL_MEMORY).run()
        content = (tmp_path / HELPER_SLICE_D / 'memory.conf').read_text()
        assert 'MemoryHigh=1200M' in content
        assert 'MemoryMax=1400M' in content

    def test_absent_on_a_large_machine(self, tmp_path):
        make_post_install(tmp_path, memory_kb=BIG_MEMORY).run()
        assert not (tmp_path / HELPER_SLICE_D / 'memory.conf').exists()


class TestCapabilitiesDropin:
    def test_written_on_a_recent_kernel(self, tmp_path):
        make_post_install(tmp_path, kernel_version='5.8.0-1.el8.x86_64').run()
        capabilities = tmp_path / 'etc/systemd/system/scylla-server.service.d/capabilities.conf'
        assert 'AmbientCapabilities=CAP_PERFMON' in capabilities.read_text()

    def test_absent_on_an_old_kernel(self, tmp_path):
        make_post_install(tmp_path, kernel_version='5.4.0-99-generic').run()
        assert not (tmp_path / 'etc/systemd/system/scylla-server.service.d/capabilities.conf').exists()


class TestAllowedCpusDropin:
    def test_written_when_scylla_has_dedicated_cores(self, tmp_path):
        make_post_install(tmp_path, online_cpus='0-3', cpuset=CPUSET_PINNED).run()
        assert (tmp_path / HELPER_SLICE_D / 'cpuset.conf').read_text() == \
            '[Slice]\nAllowedCPUs=0-1\n'

    def test_skipped_when_there_are_no_free_cores(self, tmp_path):
        make_post_install(tmp_path, online_cpus='0-3', cpuset='CPUSET="--cpuset 0-3"\n').run()
        assert not (tmp_path / HELPER_SLICE_D / 'cpuset.conf').exists()

    def test_skipped_on_cgroup_v1(self, tmp_path):
        make_post_install(tmp_path, cpuset=CPUSET_PINNED, cgroup_controllers=None).run()
        assert not (tmp_path / HELPER_SLICE_D / 'cpuset.conf').exists()

    def test_skipped_without_the_cpuset_controller(self, tmp_path):
        make_post_install(tmp_path, cpuset=CPUSET_PINNED, cgroup_controllers='cpu io memory').run()
        assert not (tmp_path / HELPER_SLICE_D / 'cpuset.conf').exists()

    def test_skipped_on_old_systemd(self, tmp_path):
        make_post_install(tmp_path, cpuset=CPUSET_PINNED,
                          systemctl=FakeSystemctl(version=239)).run()
        assert not (tmp_path / HELPER_SLICE_D / 'cpuset.conf').exists()

    def test_stale_dropin_is_removed(self, tmp_path):
        """A node reconfigured to give Scylla every core must not stay pinned."""
        stale = tmp_path / HELPER_SLICE_D / 'cpuset.conf'
        stale.parent.mkdir(parents=True)
        stale.write_text('[Slice]\nAllowedCPUs=0-1\n')
        make_post_install(tmp_path, online_cpus='0-3', cpuset='CPUSET="--cpuset 0-3"\n').run()
        assert not stale.exists()


class TestJournaldDropin:
    def test_journald_joins_the_helper_slice(self, tmp_path):
        make_post_install(tmp_path, cpuset=CPUSET_PINNED).run()
        assert (tmp_path / JOURNALD_D / 'scylla-helper-slice.conf').read_text() == \
            '[Service]\nSlice=scylla-helper.slice\n'

    def test_written_even_without_free_cores(self, tmp_path):
        """Containment is worth having even with no core to spare."""
        make_post_install(tmp_path, online_cpus='0-3', cpuset='CPUSET="--cpuset 0-3"\n').run()
        assert (tmp_path / JOURNALD_D / 'scylla-helper-slice.conf').exists()

    def test_journald_is_restarted(self, tmp_path):
        systemctl = FakeSystemctl()
        make_post_install(tmp_path, cpuset=CPUSET_PINNED, systemctl=systemctl).run()
        assert systemctl.reloaded()
        assert 'systemd-journald.service' in systemctl.restarted()


class TestLimitNofile:
    """An older install's per-machine LimitNOFILE drop-in is retired, not left
    to override the LimitNOFILE=infinity the unit now ships."""

    def test_existing_dropin_is_rewritten(self, tmp_path):
        limitnofile = tmp_path / 'etc/systemd/system/scylla-server.service.d/limitnofile.conf'
        limitnofile.parent.mkdir(parents=True)
        limitnofile.write_text('[Service]\nLimitNOFILE=800000\n')
        make_post_install(tmp_path).run()
        assert limitnofile.read_text() == '[Service]\nLimitNOFILE=infinity\n'

    def test_not_created_when_absent(self, tmp_path):
        make_post_install(tmp_path).run()
        assert not (tmp_path / 'etc/systemd/system/scylla-server.service.d'
                    / 'limitnofile.conf').exists()

    def test_already_infinity_is_not_rewritten(self, tmp_path):
        limitnofile = tmp_path / 'etc/systemd/system/scylla-server.service.d/limitnofile.conf'
        limitnofile.parent.mkdir(parents=True)
        limitnofile.write_text('[Service]\nLimitNOFILE=infinity\n')
        p = make_post_install(tmp_path)
        p.run()
        assert limitnofile not in p.changed


class TestCoredumpTimeout:
    def test_rewritten_when_runtimemaxsec_is_missing(self, tmp_path):
        timeout = tmp_path / 'etc/systemd/system/systemd-coredump@.service.d/timeout.conf'
        timeout.parent.mkdir(parents=True)
        timeout.write_text('[Service]\n')
        make_post_install(tmp_path).run()
        assert 'RuntimeMaxSec=infinity' in timeout.read_text()
        assert 'TimeoutSec=infinity' in timeout.read_text()

    def test_left_alone_when_already_set(self, tmp_path):
        timeout = tmp_path / 'etc/systemd/system/systemd-coredump@.service.d/timeout.conf'
        timeout.parent.mkdir(parents=True)
        original = '[Service]\nRuntimeMaxSec=120\n'
        timeout.write_text(original)
        make_post_install(tmp_path).run()
        assert timeout.read_text() == original

    def test_not_created_when_absent(self, tmp_path):
        make_post_install(tmp_path).run()
        assert not (tmp_path / 'etc/systemd/system/systemd-coredump@.service.d').exists()


class TestApply:
    def test_idempotent_second_run_restarts_nothing(self, tmp_path):
        """A re-run must not disturb running processes. daemon-reload still
        happens, as in the shell version."""
        make_post_install(tmp_path, cpuset=CPUSET_PINNED).run()

        systemctl = FakeSystemctl()
        p = make_post_install(tmp_path, cpuset=CPUSET_PINNED, systemctl=systemctl)
        p.run()
        assert p.changed == set()
        assert systemctl.reloaded()
        assert systemctl.restarted() == []

    def test_only_journald_is_restarted(self, tmp_path):
        """The slice limits apply live on daemon-reload, so the other members
        must be left running."""
        systemctl = FakeSystemctl()
        make_post_install(tmp_path, online_cpus='0-3', cpuset=CPUSET_PINNED,
                          systemctl=systemctl).run()
        assert systemctl.restarted() == ['systemd-journald.service']

    def test_cpuset_removal_alone_restarts_nothing(self, tmp_path):
        """Dropping the pinning reverts on daemon-reload; nothing needs a kick."""
        make_post_install(tmp_path, online_cpus='0-3', cpuset=CPUSET_PINNED).run()

        systemctl = FakeSystemctl()
        make_post_install(tmp_path, online_cpus='0-3', cpuset='CPUSET="--cpuset 0-3"\n',
                          systemctl=systemctl).run()
        assert not (tmp_path / HELPER_SLICE_D / 'cpuset.conf').exists()
        assert systemctl.restarted() == []


class TestRobustness:
    """Post-install must never fail the package transaction."""

    def test_unreadable_meminfo_does_not_stop_the_other_steps(self, tmp_path):
        p = make_post_install(tmp_path, cpuset=CPUSET_PINNED)
        (tmp_path / 'proc/meminfo').unlink()
        p.run()
        assert (tmp_path / JOURNALD_D / 'scylla-helper-slice.conf').exists()
        assert (tmp_path / HELPER_SLICE_D / 'cpuset.conf').exists()

    def test_missing_sysfs_does_not_stop_the_other_steps(self, tmp_path):
        p = make_post_install(tmp_path, memory_kb=SMALL_MEMORY, cpuset=CPUSET_PINNED)
        (tmp_path / 'sys/devices/system/cpu/online').unlink()
        p.run()
        assert (tmp_path / HELPER_SLICE_D / 'memory.conf').exists()
        assert (tmp_path / JOURNALD_D / 'scylla-helper-slice.conf').exists()

    def test_main_returns_zero_on_a_broken_root(self, tmp_path, monkeypatch):
        """main() swallows everything - and must never reach the host's systemd."""
        calls = []

        def fake_systemctl(self, args, capture=False):
            calls.append(list(args))
            return True

        monkeypatch.setattr(scylla_post_install.PostInstall, 'run_systemctl', fake_systemctl)
        assert scylla_post_install.main(['--root', str(tmp_path / 'nonexistent')]) == 0
        assert all(c[0] in ('daemon-reload', 'try-restart') for c in calls), calls


class TestSystemdVersion:
    """systemd_version() parses the real `systemctl --version` banner."""

    @pytest.mark.parametrize('banner,expected', [
        ('systemd 249 (249.11-0ubuntu3.12)\n+PAM +AUDIT +SELINUX\n', 249),
        ('systemd 239 (239-78.el8)\n+PAM +AUDIT\n', 239),
        ('systemd 254 (254.5-1.fc39)\n', 254),
        ('systemd 257 (257.4-1)\n+PAM\n', 257),
        ('not a systemd banner\n', None),
    ])
    def test_parses_the_banner(self, tmp_path, banner, expected):
        p = make_post_install(tmp_path)
        p.run_systemctl = lambda args, capture=False: banner
        assert p.systemd_version() == expected

    def test_none_when_systemctl_is_missing(self, tmp_path):
        p = make_post_install(tmp_path)
        p.run_systemctl = lambda args, capture=False: None
        assert p.systemd_version() is None


class TestParseVersion:
    @pytest.mark.parametrize('text,expected', [
        ('5.8.0-1.el8.x86_64', (5, 8, 0)),
        ('6.1.0', (6, 1, 0)),
        ('5.4', (5, 4)),
        ('unknown', ()),
    ])
    def test_parse_version(self, text, expected):
        assert scylla_post_install.parse_version(text) == expected

    def test_perfmon_threshold(self):
        assert scylla_post_install.parse_version('5.8.0') >= \
            scylla_post_install.PERFMON_KERNEL_VERSION
        assert scylla_post_install.parse_version('5.4.0') < \
            scylla_post_install.PERFMON_KERNEL_VERSION
