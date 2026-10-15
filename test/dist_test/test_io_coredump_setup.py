#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import os
import resource

import pytest

CPUINFO = ''.join(f'processor\t: {i}\ncore id\t\t: {i // 2}\n\n' for i in range(4))
IOTUNE = ('/usr/bin/iotune --format envfile --options-file /etc/scylla.d/io.conf'
          ' --properties-file /etc/scylla.d/io_properties.yaml')


@pytest.fixture
def io_setup(scripts, monkeypatch):
    """A machine ready for scylla_io_setup; returns the list of setrlimit() calls."""
    scripts.file('/proc/cpuinfo', CPUINFO)
    scripts.file('/etc/scylla.d/dev-mode.conf')
    scripts.file('/etc/scylla.d/cpuset.conf')
    scripts.file('/etc/scylla.d/io.conf')
    scripts.file('/etc/scylla.d/io_properties.yaml')
    scripts.file('/etc/scylla/scylla.yaml', 'commitlog_directory: /var/lib/scylla/commitlog\n')
    for d in ('data', 'commitlog', 'hints'):
        scripts.fs.create_dir(f'/var/lib/scylla/{d}')
    monkeypatch.setattr(os, 'statvfs', lambda p: os.statvfs_result((0, 4096, 0, 0, 10 ** 7, 0, 0, 0, 0, 0)))
    monkeypatch.setattr(resource, 'getrlimit', lambda r: (1024, 4096))
    setrlimit = []
    monkeypatch.setattr(resource, 'setrlimit', lambda r, limits: setrlimit.append(limits))
    return setrlimit


def test_io_setup(scripts, io_setup):
    """iotune evaluates every existing scylla dir, nofile is raised to the hard limit, and io.conf is world-readable for scylla."""
    assert scripts.run('scylla_io_setup') == 0
    assert scripts.cmd.calls == [IOTUNE + ' --evaluation-directory /var/lib/scylla/data'
                                 ' --evaluation-directory /var/lib/scylla/commitlog'
                                 ' --evaluation-directory /var/lib/scylla/hints']
    assert io_setup == [(4096, 4096)]
    assert os.stat('/etc/scylla.d/io.conf').st_mode & 0o777 == 0o644


@pytest.mark.parametrize('cpuset,expected', [('--cpuset 0-1,3 ', '--cpuset 0,1,3'),
                                             ('--smp 2 ', '--smp 2')])
def test_io_setup_cpuset(scripts, io_setup, cpuset, expected):
    """iotune runs on the same CPUs scylla will use, as set in cpuset.conf."""
    scripts.fs.get_object('/etc/scylla.d/cpuset.conf').set_contents(f'CPUSET="{cpuset}"\n')
    scripts.run('scylla_io_setup')
    assert scripts.cmd.calls[0].endswith(expected)


@pytest.mark.parametrize('product,expected', [('i4i.large', True), ('m5.large', False)])
def test_io_setup_i4i(scripts, io_setup, product, expected):
    """i4i instances get a 4K random-write buffer, which their NVMe disks need for correct results."""
    scripts.file('/sys/devices/virtual/dmi/id/product_name', product)
    scripts.run('scylla_io_setup')
    assert ('--random-write-io-buffer-size 4096' in scripts.cmd.calls[0]) == expected


def test_io_setup_conf_from_env(scripts, io_setup, monkeypatch):
    """SCYLLA_CONF points io_setup at a non-default scylla.yaml, as in non-root and container installs."""
    monkeypatch.setenv('SCYLLA_CONF', '/conf')
    scripts.file('/conf/scylla.yaml', 'workdir: /w\ndata_file_directories: [/w/d]\n')
    scripts.fs.create_dir('/w/d')
    scripts.run('scylla_io_setup')
    assert scripts.cmd.calls == [IOTUNE + ' --evaluation-directory /w/d']


def test_io_setup_dev_mode(scripts, io_setup):
    """Developer mode skips iotune, since it has no real disks to measure."""
    scripts.fs.get_object('/etc/scylla.d/dev-mode.conf').set_contents('DEV_MODE=--developer-mode=1\n')
    assert scripts.run('scylla_io_setup') == 0
    assert scripts.cmd.calls == []


@pytest.mark.parametrize('broken', ['missing', 'file', 'small'])
def test_io_setup_bad_data_dir(scripts, io_setup, monkeypatch, broken):
    """A missing, non-directory or too-small data dir fails setup before running iotune on it."""
    os.rmdir('/var/lib/scylla/data')
    if broken == 'file':
        scripts.file('/var/lib/scylla/data')
    elif broken == 'small':
        os.mkdir('/var/lib/scylla/data')
        monkeypatch.setattr(os, 'statvfs', lambda p: os.statvfs_result((0, 4096, 0, 0, 10, 0, 0, 0, 0, 0)))
    assert scripts.run('scylla_io_setup') == 1
    assert scripts.cmd.calls == []


def test_io_setup_iotune_fails(scripts, io_setup):
    """An iotune failure fails io_setup, so scylla_setup doesn't report success without io properties."""
    scripts.cmd.set('iotune', returncode=1)
    assert scripts.run('scylla_io_setup') == 1


def fail(*args):
    raise ValueError('nope')


def test_io_setup_rlimits_unlimited_hard_keeps_soft(scripts, io_setup, monkeypatch):
    """With no hard nofile limit, a soft limit already above what iotune needs (10 per shard + 500) is kept."""
    monkeypatch.setattr(resource, 'getrlimit', lambda r: (1024, resource.RLIM_INFINITY))
    scripts.run('scylla_io_setup')
    assert io_setup == [(1024, resource.RLIM_INFINITY)]


def test_io_setup_setrlimit_failure_exits(scripts, io_setup, monkeypatch):
    """A failure to raise the nofile limit aborts setup, since iotune would fail later with too many open files."""
    monkeypatch.setattr(resource, 'setrlimit', fail)
    assert scripts.run('scylla_io_setup') == 1


def test_io_setup_getrlimit_failure_is_ignored(scripts, io_setup, monkeypatch):
    """When the nofile limit can't be read, setup still runs iotune with whatever limit it has."""
    monkeypatch.setattr(resource, 'getrlimit', fail)
    assert scripts.run('scylla_io_setup') == 0


@pytest.mark.parametrize('distro', ['rhel', 'ubuntu', 'gentoo'])
@pytest.mark.parametrize('iotune_rc', [0, 1])
def test_kernel_check(scripts, monkeypatch, distro, iotune_rc, capsys):
    """The kernel check installs xfsprogs, runs iotune --fs-check on a loop XFS image, and cleans it up."""
    scripts.distro(distro)
    monkeypatch.setattr('shutil.which', lambda x: None)
    scripts.file('/var/tmp/kernel-check.img')
    scripts.cmd.set('iotune --fs-check', returncode=iotune_rc)
    assert scripts.run('scylla_kernel_check') == iotune_rc
    assert {'rhel': 'yum install -y xfsprogs', 'ubuntu': 'install -y xfsprogs',
            'gentoo': 'emerge -uq xfsprogs'}[distro] in scripts.cmd
    assert scripts.cmd.calls[-5:] == [
        'dd if=/dev/zero of=/var/tmp/kernel-check.img bs=1M count=300',
        'mkfs.xfs /var/tmp/kernel-check.img',
        'mount /var/tmp/kernel-check.img /var/tmp/mnt -o loop',
        'iotune --fs-check --evaluation-directory /var/tmp/mnt --default-log-level error',
        'umount /var/tmp/mnt',
    ]
    assert not os.path.exists('/var/tmp/mnt') and not os.path.exists('/var/tmp/kernel-check.img')
    assert ('supported kernel' in capsys.readouterr().out) == (iotune_rc == 0)


@pytest.mark.parametrize('status', ['enabled', 'disabled'])
def test_selinux_setup(scripts, status):
    """SELinux is disabled persistently and at runtime only when it is enabled."""
    scripts.file('/etc/sysconfig/selinux', 'SELINUX=enforcing\nSELINUXTYPE=targeted\n')
    scripts.cmd.set('sestatus', f'SELinux status:                 {status}')
    scripts.run('scylla_selinux_setup')
    enforcing = status == 'enabled'
    assert ('setenforce 0' in scripts.cmd) == enforcing
    assert ('SELINUX=disabled\n' in scripts.read('/etc/sysconfig/selinux')) == enforcing


def test_selinux_setup_not_redhat(scripts):
    """SELinux setup is a no-op outside the Red Hat family."""
    scripts.distro('ubuntu')
    assert scripts.run('scylla_selinux_setup') == 0
    assert scripts.cmd.calls == []


@pytest.fixture
def coredump(scripts):
    """Make every external command of scylla_coredump_setup succeed with a usable core."""
    scripts.file('/var/lib/systemd/coredump/core.1')
    scripts.fs.create_dir('/etc/sysctl.d')
    scripts.fs.create_dir('/etc/systemd/system')
    scripts.cmd.set('coredumpctl --version', 'systemd 252\n+PAM +ZSTD')
    scripts.cmd.set('coredumpctl --no-pager', '  Storage: /var/lib/systemd/coredump/core.1 (present)')
    return scripts


CONF = '[Coredump]\nStorage=external\nCompress={}\nProcessSizeMax=1024G\nExternalSizeMax=1024G'


@pytest.mark.parametrize('distro', ['rhel', 'ubuntu', 'opensuse-leap'])
def test_coredump_setup(coredump, monkeypatch, distro):
    """systemd-coredump is configured for large external cores, and the test core is generated then removed."""
    coredump.distro(distro, version='8')
    monkeypatch.setattr('shutil.which', lambda x: None)
    coredump.cmd.set('systemctl cat abrt-ccpp.service', returncode=1)
    assert coredump.run('scylla_coredump_setup') == 0
    assert coredump.read('/etc/systemd/coredump.conf') == CONF.format('yes')
    assert coredump.read('/etc/systemd/system/systemd-coredump@.service.d/timeout.conf') == \
        '[Service]\nRuntimeMaxSec=infinity\nTimeoutSec=infinity'
    assert coredump.read('/etc/sysctl.d/99-scylla-coredump.conf') == \
        'kernel.core_pattern=|/usr/lib/systemd/systemd-coredump %p %u %g %s %t %e'
    assert not os.path.exists('/var/lib/systemd/coredump/core.1')
    assert ('install -y systemd-coredump' in coredump.cmd) == (distro != 'rhel')
    assert ('systemctl  restart systemd-coredump.socket' in coredump.cmd) == (distro == 'opensuse-leap')
    assert 'systemctl  disable abrt-ccpp.service' not in coredump.cmd


def test_coredump_setup_raiddir(coredump):
    """--dump-to-raiddir bind-mounts the coredump dir onto the scylla RAID, and old systemd gets no timeout drop-in."""
    coredump.file('/usr/lib/sysctl.d/50-coredump.conf')
    coredump.cmd.set('coredumpctl --version', 'systemd 239\n+PAM')
    coredump.cmd.set('systemctl cat systemd-coredump@.service', returncode=1)
    coredump.run('scylla_coredump_setup', '--dump-to-raiddir')
    assert coredump.read('/etc/systemd/coredump.conf') == CONF.format('no')
    assert 'Where=/var/lib/systemd/coredump' in coredump.read('/etc/systemd/system/var-lib-systemd-coredump.mount')
    assert os.path.isdir('/var/lib/scylla/coredump')
    assert 'systemctl  start var-lib-systemd-coredump.mount' in coredump.cmd
    assert 'sysctl -p /usr/lib/sysctl.d/50-coredump.conf' in coredump.cmd
    assert 'systemctl  disable abrt-ccpp.service' in coredump.cmd
    assert not os.path.exists('/etc/systemd/system/systemd-coredump@.service.d')


def test_coredump_setup_gentoo(coredump):
    """Gentoo only gets the sysctl core_pattern, it has no systemd coredump.conf to write."""
    coredump.distro('gentoo')
    assert coredump.run('scylla_coredump_setup') == 0
    assert coredump.cmd.calls[-1] == 'sysctl -p /etc/sysctl.d/99-scylla-coredump.conf'
    assert not os.path.exists('/etc/systemd/coredump.conf')


@pytest.mark.parametrize('info', ['', 'Storage: none', 'Storage: journal',
                                  'Storage: /var/lib/systemd/coredump/core.2 (inaccessible)', None],
                         ids=['empty', 'none', 'journal', 'inaccessible', 'coredumpctl-fails'])
def test_coredump_setup_no_core(coredump, info):
    """Setup fails when the test crash didn't produce a usable core file."""
    if info is None:
        coredump.cmd.set('coredumpctl --no-pager', returncode=1)
    else:
        coredump.cmd.set('coredumpctl --no-pager', info)
    assert coredump.run('scylla_coredump_setup') == 1


def test_coredump_setup_old_systemd(coredump):
    """Old coredumpctl, which prints 'Coredump:' instead of 'Storage:', is still parsed."""
    coredump.cmd.set('coredumpctl --no-pager', 'Coredump: /var/lib/systemd/coredump/core.1')
    assert coredump.run('scylla_coredump_setup') == 0
    assert not os.path.exists('/var/lib/systemd/coredump/core.1')


@pytest.mark.parametrize('modules', ['', 'systemd-container-coredump', 'abc\nsystemd-container-coredump'])
def test_coredump_setup_rhel9_selinux(coredump, monkeypatch, modules):
    """On RHEL 9 the systemd-container-coredump SELinux module is installed once, wherever semodule lists it."""
    coredump.distro('rhel', version='9')
    monkeypatch.setattr('shutil.which', lambda x: None)
    coredump.file('/usr/share/selinux/packages/targeted/systemd-container-coredump.pp.bz2')
    coredump.cmd.set('getenforce', 'Enforcing')
    coredump.cmd.set('semodule -l', modules)
    coredump.run('scylla_coredump_setup')
    assert 'yum install -y libselinux-utils' in coredump.cmd
    assert 'yum install -y policycoreutils' in coredump.cmd
    assert ('semodule -v -e systemd-container-coredump' in coredump.cmd) == (modules == '')
