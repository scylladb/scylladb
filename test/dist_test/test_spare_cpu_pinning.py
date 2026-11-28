#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""scylla_cpuset_setup and scylla_prepare pin scylla-helper.slice and
systemd-journald to the CPUs outside Scylla's cpuset, through
setup_spare_cpu_pinning() in scylla_util.

On cloud images cpuset.conf is first written at first boot, after the package
install. Without the pinning there, new nodes stayed unpinned (CLOUDEVOPS-2311,
SCYLLADB-6198).
"""
import importlib.machinery
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
    # scylla_util imports distro, traceback_with_variables and yaml, which the
    # test venv may not install. The functions under test use none of them,
    # so give the import an empty module and remove it again after the load.
    stubs = [name for name in ('distro', 'traceback_with_variables', 'yaml')
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


def _load_script(script):
    """Load a script of dist/common/scripts as a module, on top of the
    scylla_util above. The scripts import distro and yaml, with the same
    stubs as scylla_util."""
    stubs = [name for name in ('distro', 'yaml')
             if name not in sys.modules and importlib.util.find_spec(name) is None]
    for name in stubs:
        sys.modules[name] = types.ModuleType(name)
    old_util = sys.modules.get('scylla_util')
    sys.modules['scylla_util'] = scylla_util
    try:
        loader = importlib.machinery.SourceFileLoader(
            script, os.path.join(_SCRIPTS_DIR, script))
        spec = importlib.util.spec_from_loader(script, loader)
        mod = importlib.util.module_from_spec(spec)
        loader.exec_module(mod)
    finally:
        if old_util is None:
            del sys.modules['scylla_util']
        else:
            sys.modules['scylla_util'] = old_util
        for name in stubs:
            del sys.modules[name]
    return mod


scylla_prepare = _load_script('scylla_prepare')
scylla_cpuset_setup = _load_script('scylla_cpuset_setup')
scylla_io_setup = _load_script('scylla_io_setup')

SLICE_DROPIN = 'etc/systemd/system/scylla-helper.slice.d/cpuset.conf'
JOURNALD_DROPIN = 'etc/systemd/system/systemd-journald.service.d/scylla-cpuset.conf'
SECTIONS = {SLICE_DROPIN: 'Slice', JOURNALD_DROPIN: 'Service'}


class FakeSystemd:
    """Stands in for systemctl. Records the commands it gets, and the
    AllowedCPUs= of the slice and journald drop-ins at each daemon-reload."""

    def __init__(self, root, version=254):
        self.root = root
        self.version = version
        self.calls = []
        self.dropin_at_reload = []
        self.fail = False
        self.hang = False

    def run(self, cmd, check=False, timeout=None):
        self.calls.append(list(cmd))
        self.timeout = timeout
        if list(cmd) == ['systemctl', 'daemon-reload']:
            self.dropin_at_reload.append((dropin_cpus(self.root, SLICE_DROPIN),
                                          dropin_cpus(self.root, JOURNALD_DROPIN)))
        if self.hang:
            raise subprocess.TimeoutExpired(cmd, timeout)
        result = subprocess.CompletedProcess(cmd, 1 if self.fail else 0)
        if check:
            result.check_returncode()
        return result

    def reloaded(self):
        return ['systemctl', 'daemon-reload'] in self.calls


@pytest.fixture
def systemd(monkeypatch, tmp_path):
    fake = FakeSystemd(tmp_path)
    monkeypatch.setattr(scylla_util, 'run', fake.run)
    monkeypatch.setattr(scylla_util, 'systemd_version', lambda: fake.version)
    monkeypatch.setattr(scylla_util, 'is_container', lambda: False)
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


def dropin_cpus(tmp_path, dropin):
    """The AllowedCPUs= of one drop-in, or None without that drop-in."""
    path = tmp_path / dropin
    if not path.exists():
        return None
    prefix = f'[{SECTIONS[dropin]}]\nAllowedCPUs='
    content = path.read_text()
    assert content.startswith(prefix)
    return content.removeprefix(prefix).strip()


def write_dropin(tmp_path, dropin, cpus):
    path = tmp_path / dropin
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'[{SECTIONS[dropin]}]\nAllowedCPUs={cpus}\n')


def allowed_cpus(tmp_path):
    """The AllowedCPUs= of the drop-ins, or None without drop-ins. The slice
    and journald drop-ins must agree."""
    cpus = dropin_cpus(tmp_path, SLICE_DROPIN)
    assert dropin_cpus(tmp_path, JOURNALD_DROPIN) == cpus
    return cpus


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
        def fake_out(cmd, **kwargs):
            assert cmd == 'systemctl --version'
            assert kwargs.get('timeout') == scylla_util.SYSTEMCTL_TIMEOUT
            return banner
        monkeypatch.setattr(scylla_util, 'out', fake_out)
        assert scylla_util.systemd_version() == version


class TestParseCpusetConf:
    @pytest.mark.parametrize('content,parsed', [
        pytest.param('CPUSET="--cpuset 1-3 --smp 2 "\n', ('1-3', 2), id='cpuset-and-smp'),
        pytest.param('CPUSET="--smp 2 --cpuset 1-3"\n', ('1-3', 2), id='smp-first'),
        pytest.param('CPUSET="--cpuset=0,2-7 --smp=7"\n', ('0,2-7', 7), id='equals'),
        pytest.param('CPUSET="--smp 4"\n', (None, 4), id='smp-only'),
        pytest.param('#CPUSET="--cpuset 0"\nCPUSET=""\n', (None, None), id='commented'),
        pytest.param('CPUSET="--cpuset 0"\nCPUSET="--smp 1"\n', (None, 1), id='last-line-wins'),
        pytest.param('', (None, None), id='no-cpuset-line'),
        # A broken value reads as unset, so scylla_cpuset_setup rewrites it.
        pytest.param('CPUSET="--cpuset 1-3oops --smp 2x"\n', (None, None), id='invalid'),
        pytest.param('CPUSET="--cpuset 1-3-5"\n', (None, None), id='invalid-range'),
        pytest.param("CPUSET='--cpuset 1-3'\n", ('1-3', None), id='single-quotes'),
    ])
    def test_parse(self, content, parsed):
        assert scylla_util.parse_cpuset_conf(content) == parsed


class TestIrqCpuMask:
    """scylla_prepare builds the perftune IRQ mask from the --cpuset value. A
    --smp option on the same line once reached hwloc-calc, and Scylla did not
    start."""

    @pytest.mark.parametrize('content,cmd', [
        pytest.param('CPUSET="--cpuset 0,2-7 --smp 7 "\n',
                     '/opt/scylladb/bin/hwloc-calc --pi all ~PU:0 ~PU:2-7', id='cpuset-and-smp'),
        pytest.param('CPUSET="--cpuset 1-7 "\n',
                     '/opt/scylladb/bin/hwloc-calc --pi all ~PU:1-7', id='cpuset'),
        pytest.param('CPUSET="--smp 4 "\n', '/opt/scylladb/bin/hwloc-calc all', id='smp-only'),
        pytest.param('CPUSET=""\n', '/opt/scylladb/bin/hwloc-calc all', id='empty'),
    ])
    def test_hwloc_command(self, tmp_path, monkeypatch, content, cmd):
        conf = tmp_path / 'etc/scylla.d/cpuset.conf'
        conf.parent.mkdir(parents=True)
        conf.write_text(content)
        calls = []
        monkeypatch.setattr(scylla_prepare.os.path, 'exists', lambda path: True)
        monkeypatch.setattr(scylla_prepare, 'cpuset_conf_cpus',
                            lambda: scylla_util.cpuset_conf_cpus(root=tmp_path))
        monkeypatch.setattr(scylla_prepare, 'out', lambda c: calls.append(c) or '0x00000001')
        assert scylla_prepare.get_irq_cpu_mask() == '0x00000001'
        assert calls == [cmd]


class TestCpusetConfCpus:
    @pytest.mark.parametrize('content,cpuset', [
        pytest.param('CPUSET="--cpuset 2-7 "\n', '2-7', id='cpuset'),
        pytest.param('CPUSET="--cpuset 1,3-5 --smp 4"\n', '1,3-5', id='cpuset-and-smp'),
        pytest.param('CPUSET="--cpuset=4-5"\n', '4-5', id='cpuset-equals'),
        pytest.param('CPUSET=--cpuset=2-7\n', '2-7', id='unquoted'),
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


class TestSetupSpareCpuPinning:
    def test_pins_the_slice_to_the_free_cpus(self, tmp_path, systemd):
        make_machine(tmp_path)
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert allowed_cpus(tmp_path) == '0-1'
        assert systemd.reloaded()
        # scylla_prepare runs this before Scylla starts. A stuck daemon-reload
        # must not block the start.
        assert systemd.timeout == scylla_util.SYSTEMCTL_TIMEOUT

    def test_new_cpuset_updates_the_dropin(self, tmp_path, systemd):
        make_machine(tmp_path, online_cpus='0-7')
        scylla_util.setup_spare_cpu_pinning('2-7', root=tmp_path)
        scylla_util.setup_spare_cpu_pinning('1-7', root=tmp_path)
        assert allowed_cpus(tmp_path) == '0'

    def test_rerun_reloads_again(self, tmp_path, systemd):
        """An earlier daemon-reload may have failed. A rerun with the same
        cpuset must still apply the drop-in."""
        make_machine(tmp_path)
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        systemd.calls.clear()
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert systemd.reloaded()

    @pytest.mark.parametrize('cpuset', [
        pytest.param(None, id='not-pinned'),
        pytest.param('0-3', id='no-spare-cpu'),
    ])
    def test_no_dropin_without_a_spare_cpu(self, tmp_path, systemd, cpuset):
        make_machine(tmp_path)
        scylla_util.setup_spare_cpu_pinning(cpuset, root=tmp_path)
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
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        scylla_util.setup_spare_cpu_pinning(cpuset, root=tmp_path)
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
        write_dropin(tmp_path, SLICE_DROPIN, '0')
        write_dropin(tmp_path, JOURNALD_DROPIN, '0')
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert allowed_cpus(tmp_path) is None
        assert systemd.reloaded()

    def test_reload_only_on_change(self, tmp_path, systemd):
        """scylla_prepare runs before every Scylla start. It reloads systemd
        only when the drop-in changed."""
        make_machine(tmp_path)
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path, always_reload=False)
        assert systemd.reloaded()
        systemd.calls.clear()
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path, always_reload=False)
        assert systemd.calls == []
        scylla_util.setup_spare_cpu_pinning('3', root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) == '0-2'
        # The reload must see the new drop-in, not the old one.
        assert systemd.dropin_at_reload[-1] == ('0-2', '0-2')

    def test_no_reload_without_a_dropin(self, tmp_path, systemd):
        make_machine(tmp_path)
        scylla_util.setup_spare_cpu_pinning(None, root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) is None
        assert systemd.calls == []

    def test_cpu_count_change_updates_the_dropin(self, tmp_path, systemd):
        """After a resize, the online CPUs differ from the drop-in."""
        make_machine(tmp_path, online_cpus='0-3')
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        (tmp_path / 'sys/devices/system/cpu/online').write_text('0-7\n')
        systemd.calls.clear()
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) == '0-1,4-7'
        assert systemd.reloaded()

    def test_removal_reloads(self, tmp_path, systemd):
        make_machine(tmp_path)
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        systemd.version = 243
        systemd.calls.clear()
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) is None
        # The reload must run after the drop-in is gone.
        assert systemd.reloaded()
        assert systemd.dropin_at_reload[-1] == (None, None)

    @pytest.mark.parametrize('first,second', [
        pytest.param(None, '2-3', id='new-dropin'),
        pytest.param('2-3', '3', id='changed-dropin'),
    ])
    def test_failed_reload_is_retried(self, tmp_path, systemd, first, second):
        """When daemon-reload fails, the old drop-in comes back, so the next
        scylla_prepare run sees a change and reloads again."""
        make_machine(tmp_path)
        if first:
            scylla_util.setup_spare_cpu_pinning(first, root=tmp_path)
        before = allowed_cpus(tmp_path)
        systemd.fail = True
        with pytest.raises(subprocess.CalledProcessError):
            scylla_util.setup_spare_cpu_pinning(second, root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) == before
        systemd.fail = False
        systemd.calls.clear()
        scylla_util.setup_spare_cpu_pinning(second, root=tmp_path, always_reload=False)
        assert systemd.reloaded()

    @pytest.mark.parametrize('other_dropin', [False, True])
    def test_failed_reload_removes_only_new_directories(self, tmp_path, systemd, other_dropin):
        """A failed reload removes a drop-in directory only when this run
        made it. A directory that holds memory.conf stays."""
        make_machine(tmp_path)
        slice_dir = (tmp_path / SLICE_DROPIN).parent
        if other_dropin:
            slice_dir.mkdir(parents=True)
            (slice_dir / 'memory.conf').write_text('[Slice]\nMemoryHigh=1200M\n')
        systemd.fail = True
        with pytest.raises(subprocess.CalledProcessError):
            scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert allowed_cpus(tmp_path) is None
        assert slice_dir.exists() == other_dropin
        assert not (tmp_path / JOURNALD_DROPIN).parent.exists()

    @pytest.mark.parametrize('always_reload', [True, False])
    def test_failed_journald_write_still_pins_the_slice(self, tmp_path, systemd, always_reload):
        """A journald drop-in that cannot be written must not keep the slice
        drop-in from applying. The error still reaches the caller."""
        make_machine(tmp_path)
        # A regular file where the drop-in directory should be.
        blocker = tmp_path / JOURNALD_DROPIN
        blocker.parent.parent.mkdir(parents=True, exist_ok=True)
        blocker.parent.write_text('')
        with pytest.raises(OSError):
            scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path, always_reload=always_reload)
        assert dropin_cpus(tmp_path, SLICE_DROPIN) == '0-1'
        assert systemd.reloaded()

    def test_upgrade_adds_the_journald_dropin(self, tmp_path, systemd):
        """A node that an older version pinned has only the slice drop-in."""
        make_machine(tmp_path)
        write_dropin(tmp_path, SLICE_DROPIN, '0-1')
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path, always_reload=False)
        assert allowed_cpus(tmp_path) == '0-1'
        assert systemd.reloaded()

    def test_only_existing_dropins_widen(self, tmp_path, systemd):
        """Without a spare CPU, widen the slice drop-in of an older version,
        but do not add a journald drop-in."""
        make_machine(tmp_path)
        write_dropin(tmp_path, SLICE_DROPIN, '0-1')
        scylla_util.setup_spare_cpu_pinning('0-3', root=tmp_path)
        assert dropin_cpus(tmp_path, SLICE_DROPIN) == '0-3'
        assert dropin_cpus(tmp_path, JOURNALD_DROPIN) is None

    def test_other_journald_dropins_stay(self, tmp_path, systemd):
        """systemd-journald.service.d is not ours. Remove only our file."""
        make_machine(tmp_path)
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        other = tmp_path / 'etc/systemd/system/systemd-journald.service.d/other.conf'
        other.write_text('[Service]\n')
        systemd.version = 243
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert allowed_cpus(tmp_path) is None
        assert other.exists()

    def test_timeout_keeps_the_new_dropin(self, tmp_path, systemd):
        """On a timeout only systemctl stops. systemd can still apply the new
        drop-ins, so the new drop-ins stay on disk."""
        make_machine(tmp_path)
        systemd.hang = True
        with pytest.raises(subprocess.TimeoutExpired):
            scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert allowed_cpus(tmp_path) == '0-1'

    def test_nothing_without_systemd(self, tmp_path, systemd):
        """The container image runs scylla_cpuset_setup without systemd."""
        make_machine(tmp_path, systemd_running=False)
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert not (tmp_path / 'etc').exists()
        assert systemd.calls == []

    def test_nothing_in_a_container(self, tmp_path, systemd, monkeypatch):
        """A container with the host's /run mounted sees a running systemd.
        A daemon-reload there could reach the host's systemd."""
        make_machine(tmp_path)
        monkeypatch.setattr(scylla_util, 'is_container', lambda: True)
        scylla_util.setup_spare_cpu_pinning('2-3', root=tmp_path)
        assert not (tmp_path / 'etc').exists()
        assert systemd.calls == []


class TestCpusetSetupChange:
    """scylla_cpuset_setup deletes perftune.yaml and rewrites cpuset.conf only
    when the --cpuset or --smp value changes."""

    @pytest.mark.parametrize('conf,cpuset,smp,changed', [
        pytest.param('', '2-3', None, True, id='no-cpuset-line'),
        pytest.param('CPUSET="--cpuset 2-3 "\n', '2-3', None, False, id='same-cpuset'),
        pytest.param('CPUSET="--cpuset 2-3 --smp 2 "\n', '2-3', '2', False, id='same-cpuset-and-smp'),
        pytest.param('CPUSET="--smp 2 --cpuset 2-3 "\n', '2-3', '2', False, id='smp-first'),
        pytest.param('CPUSET="--cpuset=2-3 "\n', '2-3', None, False, id='equals-sign'),
        pytest.param('CPUSET="--smp 4 "\n', None, '4', False, id='same-smp'),
        pytest.param('CPUSET="--cpuset 1 "\nCPUSET="--cpuset 2-3 "\n', '2-3', None, False,
                     id='last-line-wins'),
        pytest.param('CPUSET="--cpuset 1-3 "\n', '2-3', None, True, id='new-cpuset'),
        pytest.param('CPUSET="--cpuset 2-3 --smp 2 "\n', '2-3', None, True, id='smp-removed'),
        pytest.param('CPUSET="--cpuset 2-3 "\n', '2-3', '2', True, id='smp-added'),
        pytest.param('CPUSET="--cpuset 0-3, "\n', '0-3', None, True, id='broken-value'),
    ])
    def test_change(self, conf, cpuset, smp, changed):
        assert scylla_cpuset_setup.cpuset_conf_changed(conf, cpuset, smp) == changed


class TestIoSetupCpuinfo:
    """scylla_io_setup gives iotune the --cpuset or --smp value of
    cpuset.conf, and sizes the open file limit from the shard count."""

    @pytest.mark.parametrize('conf,cpuset,smp,shards', [
        pytest.param('', None, None, 8, id='no-cpuset-line'),
        pytest.param('CPUSET=""\n', None, None, 8, id='empty'),
        pytest.param('CPUSET="--cpuset 2-3 "\n', {2, 3}, None, 2, id='cpuset'),
        pytest.param('CPUSET="--cpuset 0,2-3 --smp 1 "\n', {0, 2, 3}, 1, 1, id='cpuset-and-smp'),
        pytest.param('CPUSET="--smp 3 "\n', None, 3, 3, id='smp'),
        # The old scylla_io_setup regex skipped the next two lines and ran
        # iotune on every CPU.
        pytest.param('CPUSET="--smp 1 --cpuset 2-3 "\n', {2, 3}, 1, 1, id='smp-first'),
        pytest.param('CPUSET=--cpuset 2-3\n', {2, 3}, None, 2, id='unquoted'),
        pytest.param('# CPUSET="--cpuset 1 "\nCPUSET="--cpuset 2-3 "\n', {2, 3}, None, 2,
                     id='comment'),
        pytest.param('CPUSET="--cpuset 1 "\nCPUSET="--cpuset 2-3 "\n', {2, 3}, None, 2,
                     id='last-line-wins'),
    ])
    def test_cpuset_and_smp(self, tmp_path, monkeypatch, conf, cpuset, smp, shards):
        (tmp_path / 'scylla.d').mkdir()
        (tmp_path / 'scylla.d/cpuset.conf').write_text(conf)
        monkeypatch.setattr(scylla_io_setup, 'etcdir', lambda: str(tmp_path))
        monkeypatch.setattr(scylla_io_setup.scylla_cpuinfo, '_scylla_cpuinfo__system_cpus',
                            lambda self: {n: {'processor': str(n)} for n in range(8)})
        info = scylla_io_setup.scylla_cpuinfo()
        assert info.cpuset() == cpuset
        assert info.smp() == smp
        assert info.nr_shards() == shards
