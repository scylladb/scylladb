#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""scylla_cpuset_setup and scylla_prepare pin scylla-helper.slice to the CPUs
outside Scylla's cpuset, through setup_helper_slice_cpuset() in scylla_util.

On cloud images cpuset.conf is first written at first boot, after the package
install. Without the pinning there, new nodes stayed unpinned (CLOUDEVOPS-2311).
"""
import importlib.util
import os
import subprocess
import sys
import types

import pytest


# Load the real scylla_util by path. It imports scylla_sysconfdir, so
# dist/common/scripts has to go on sys.path first.
_SCRIPTS_DIR = os.path.normpath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'dist', 'common', 'scripts',
))


def _load_scylla_util():
    if _SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, _SCRIPTS_DIR)
    # scylla_util imports distro and traceback_with_variables, which the test
    # venv does not install. The functions under test use neither, so give
    # the import an empty module and remove it again after the load.
    stubs = [name for name in ('distro', 'traceback_with_variables')
             if name not in sys.modules and importlib.util.find_spec(name) is None]
    for name in stubs:
        sys.modules[name] = types.ModuleType(name)
    spec = importlib.util.spec_from_file_location(
        'scylla_util', os.path.join(_SCRIPTS_DIR, 'scylla_util.py'))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # scylla_util sets sys.excepthook when it loads. Keep the hook pytest uses.
    excepthook = sys.excepthook
    try:
        spec.loader.exec_module(mod)
    finally:
        sys.excepthook = excepthook
        for name in stubs:
            del sys.modules[name]
    return mod


scylla_util = _load_scylla_util()

DROPIN = 'etc/systemd/system/scylla-helper.slice.d/cpuset.conf'


class FakeSystemd:
    """Stands in for systemctl. Records the commands it gets."""

    def __init__(self, version=254):
        self.version = version
        self.calls = []
        self.fail = False

    def run(self, cmd, check=False, timeout=None):
        self.calls.append(list(cmd))
        if self.fail:
            raise subprocess.CalledProcessError(1, cmd)

    def reloaded(self):
        return ['systemctl', 'daemon-reload'] in self.calls


@pytest.fixture
def systemd(monkeypatch):
    fake = FakeSystemd()
    monkeypatch.setattr(scylla_util, 'run', fake.run)
    monkeypatch.setattr(scylla_util, 'systemd_version', lambda: fake.version)
    return fake


def make_machine(tmp_path, *, online_cpus='0-3',
                 cgroup_controllers='cpuset cpu io memory pids', systemd_running=True):
    cpu_dir = tmp_path / 'sys/devices/system/cpu'
    cpu_dir.mkdir(parents=True)
    (cpu_dir / 'online').write_text(online_cpus + '\n')
    if cgroup_controllers is not None:
        cgroup_dir = tmp_path / 'sys/fs/cgroup'
        cgroup_dir.mkdir(parents=True)
        (cgroup_dir / 'cgroup.controllers').write_text(cgroup_controllers + '\n')
    if systemd_running:
        (tmp_path / 'run/systemd/system').mkdir(parents=True)


def allowed_cpus(tmp_path):
    """The AllowedCPUs= of the drop-in, or None without a drop-in."""
    path = tmp_path / DROPIN
    if not path.exists():
        return None
    content = path.read_text()
    assert content.startswith('[Slice]\nAllowedCPUs=')
    return content.removeprefix('[Slice]\nAllowedCPUs=').strip()


class TestCpuList:
    @pytest.mark.parametrize('text,cpus', [
        ('0', {0}),
        ('0-3', {0, 1, 2, 3}),
        ('0,2-3,7', {0, 2, 3, 7}),
        ('0-1,\n', {0, 1}),
    ])
    def test_parse(self, text, cpus):
        assert scylla_util.parse_cpu_list(text) == cpus

    @pytest.mark.parametrize('text', ['0', '0-3', '0,2-3,7', '1,3,5'])
    def test_round_trip(self, text):
        assert scylla_util.cpu_list_to_str(scylla_util.parse_cpu_list(text)) == text


class TestSystemdVersion:
    @pytest.mark.parametrize('banner,version', [
        ('systemd 239 (239-78.el8)\n+PAM +AUDIT', 239),
        ('systemd 257 (257.4-1)\n+PAM', 257),
    ])
    def test_parses_the_banner(self, monkeypatch, banner, version):
        monkeypatch.setattr(scylla_util, 'out', lambda cmd, **kwargs: banner)
        assert scylla_util.systemd_version() == version


class TestCpusetConfCpus:
    @pytest.mark.parametrize('content,cpuset', [
        pytest.param('CPUSET="--cpuset 2-7 "\n', '2-7', id='cpuset'),
        pytest.param('CPUSET="--cpuset 1,3-5 --smp 4"\n', '1,3-5', id='cpuset-and-smp'),
        pytest.param('CPUSET="--cpuset=4-5"\n', '4-5', id='cpuset-equals'),
        pytest.param('  CPUSET="--cpuset 6"\n', '6', id='leading-space'),
        pytest.param('# old\nCPUSET="--cpuset 0 "\nCPUSET="--cpuset 2-3 "\n', '2-3', id='last-line-wins'),
        pytest.param('CPUSET="--smp 4"\n', None, id='smp-only'),
        pytest.param('CPUSET=""\n', None, id='empty'),
        pytest.param('', None, id='no-cpuset-line'),
    ])
    def test_reads_the_cpuset(self, tmp_path, content, cpuset):
        conf = tmp_path / 'etc/scylla.d/cpuset.conf'
        conf.parent.mkdir(parents=True)
        conf.write_text(content)
        assert scylla_util.cpuset_conf_cpus(root=tmp_path) == cpuset

    def test_missing_file(self, tmp_path):
        assert scylla_util.cpuset_conf_cpus(root=tmp_path) is None
        assert not (tmp_path / 'etc').exists()


class TestSetupHelperSliceCpuset:
    def test_pins_the_slice_to_the_free_cpus(self, tmp_path, systemd):
        make_machine(tmp_path)
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        assert allowed_cpus(tmp_path) == '0-1'
        assert systemd.reloaded()

    def test_new_cpuset_updates_the_dropin(self, tmp_path, systemd):
        make_machine(tmp_path, online_cpus='0-7')
        scylla_util.setup_helper_slice_cpuset('2-7', root=tmp_path)
        scylla_util.setup_helper_slice_cpuset('1-7', root=tmp_path)
        assert allowed_cpus(tmp_path) == '0'

    def test_rerun_reloads_again(self, tmp_path, systemd):
        """An earlier daemon-reload may have failed. A rerun with the same
        cpuset must still apply the drop-in."""
        make_machine(tmp_path)
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        systemd.calls.clear()
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        assert systemd.reloaded()

    @pytest.mark.parametrize('cpuset', [
        pytest.param(None, id='not-pinned'),
        pytest.param('0-3', id='no-spare-cpu'),
    ])
    def test_no_dropin_without_a_spare_cpu(self, tmp_path, systemd, cpuset):
        make_machine(tmp_path)
        scylla_util.setup_helper_slice_cpuset(cpuset, root=tmp_path)
        assert allowed_cpus(tmp_path) is None
        assert systemd.reloaded()

    @pytest.mark.parametrize('cpuset', [
        pytest.param(None, id='not-pinned'),
        pytest.param('0-3', id='no-spare-cpu'),
    ])
    def test_existing_dropin_widens_to_every_cpu(self, tmp_path, systemd, cpuset):
        """When AllowedCPUs= goes away, the kernel leaves the running helpers
        on the old CPUs. Seen on a lab node with kernel 6.17 and systemd 255."""
        make_machine(tmp_path)
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        scylla_util.setup_helper_slice_cpuset(cpuset, root=tmp_path)
        assert allowed_cpus(tmp_path) == '0-3'

    @pytest.mark.parametrize('machine,version', [
        pytest.param({'cgroup_controllers': None}, 254, id='cgroup-v1'),
        pytest.param({'cgroup_controllers': 'cpu io memory'}, 254, id='no-cpuset-controller'),
        pytest.param({}, 243, id='systemd-243'),
    ])
    def test_dropin_is_removed_where_allowedcpus_cannot_apply(self, tmp_path, systemd,
                                                               machine, version):
        make_machine(tmp_path, **machine)
        systemd.version = version
        stale = tmp_path / DROPIN
        stale.parent.mkdir(parents=True)
        stale.write_text('[Slice]\nAllowedCPUs=0\n')
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        assert allowed_cpus(tmp_path) is None
        assert systemd.reloaded()

    def test_reload_only_on_change(self, tmp_path, systemd):
        """scylla_prepare runs before every Scylla start. It reloads systemd
        only when the drop-in changed."""
        make_machine(tmp_path)
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path, always_reload=False)
        assert systemd.reloaded()
        systemd.calls.clear()
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path, always_reload=False)
        assert systemd.calls == []
        scylla_util.setup_helper_slice_cpuset('3', root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) == '0-2'
        assert systemd.reloaded()

    def test_no_reload_without_a_dropin(self, tmp_path, systemd):
        make_machine(tmp_path)
        scylla_util.setup_helper_slice_cpuset(None, root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) is None
        assert systemd.calls == []

    def test_cpu_count_change_updates_the_dropin(self, tmp_path, systemd):
        """After a resize, the online CPUs differ from the drop-in."""
        make_machine(tmp_path, online_cpus='0-3')
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        (tmp_path / 'sys/devices/system/cpu/online').write_text('0-7\n')
        systemd.calls.clear()
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) == '0-1,4-7'
        assert systemd.reloaded()

    def test_removal_reloads(self, tmp_path, systemd):
        make_machine(tmp_path)
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        systemd.version = 243
        systemd.calls.clear()
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) is None
        assert systemd.reloaded()

    @pytest.mark.parametrize('first,second', [
        pytest.param(None, '2-3', id='new-dropin'),
        pytest.param('2-3', '3', id='changed-dropin'),
    ])
    def test_failed_reload_is_retried(self, tmp_path, systemd, first, second):
        """When daemon-reload fails, the old drop-in comes back, so the next
        scylla_prepare run sees a change and reloads again."""
        make_machine(tmp_path)
        if first:
            scylla_util.setup_helper_slice_cpuset(first, root=tmp_path)
        before = allowed_cpus(tmp_path)
        systemd.fail = True
        with pytest.raises(subprocess.CalledProcessError):
            scylla_util.setup_helper_slice_cpuset(second, root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) == before
        systemd.fail = False
        systemd.calls.clear()
        scylla_util.setup_helper_slice_cpuset(second, root=tmp_path, always_reload=False)
        assert systemd.reloaded()

    def test_nothing_without_systemd(self, tmp_path, systemd):
        """The container image runs scylla_cpuset_setup without systemd."""
        make_machine(tmp_path, systemd_running=False)
        scylla_util.setup_helper_slice_cpuset('2-3', root=tmp_path)
        assert not (tmp_path / 'etc').exists()
        assert systemd.calls == []
