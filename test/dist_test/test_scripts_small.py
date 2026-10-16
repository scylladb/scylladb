#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import subprocess

import pytest

from test.dist_test.conftest import SCRIPTS_DIR, _sources


@pytest.mark.parametrize('name', sorted(_sources))
def test_syntax(name):
    path = SCRIPTS_DIR / name
    if _sources[name].startswith('#!/bin/bash'):
        subprocess.run(['bash', '-n', str(path)], check=True)
    else:
        compile(_sources[name], str(path), 'exec')


@pytest.mark.parametrize('name', ['scylla_cpuset_setup', 'scylla_memory_setup', 'scylla_ntp_setup'])
def test_requires_root(scripts, name, capsys):
    assert scripts.run(name, uid=1000) == 1
    assert 'Requires root permission' in capsys.readouterr().out


def test_cpuset_setup(scripts):
    scripts.file('/etc/scylla.d/perftune.yaml')
    scripts.file('/etc/scylla.d/cpuset.conf', 'CPUSET="--cpuset 1 "\n')
    assert scripts.run('scylla_cpuset_setup', '--cpuset', '4', '--smp', '3') == 0
    assert scripts.read('/etc/scylla.d/cpuset.conf') == 'CPUSET="--cpuset 4 --smp 3 "\n'
    assert not scripts.fs.exists('/etc/scylla.d/perftune.yaml')


def test_cpuset_setup_new_conf(scripts):
    scripts.fs.create_dir('/etc/scylla.d')
    scripts.run('scylla_cpuset_setup', '--smp', '2')
    assert scripts.read('/etc/scylla.d/cpuset.conf') == 'CPUSET="--smp 2 "\n'


def test_cpuset_setup_no_args(scripts):
    assert scripts.run('scylla_cpuset_setup') == 1


def test_dev_mode_setup(scripts):
    scripts.fs.create_dir('/etc/scylla.d')
    scripts.run('scylla_dev_mode_setup', '--developer-mode', '1')
    assert scripts.read('/etc/scylla.d/dev-mode.conf') == 'DEV_MODE=--developer-mode=1\n'
    scripts.run('scylla_dev_mode_setup', '--developer-mode', '0')
    assert scripts.read('/etc/scylla.d/dev-mode.conf') == ''


def test_memory_setup(scripts):
    scripts.fs.create_dir('/etc/scylla.d')
    scripts.run('scylla_memory_setup', '--lock-memory', '--memory', '4G', '--reserve-memory', '1G')
    assert scripts.read('/etc/scylla.d/memory.conf') == 'MEM_CONF="--lock-memory=1 --memory=4G --reserve-memory=1G"\n'
    assert scripts.run('scylla_memory_setup') == 1


@pytest.mark.parametrize('key,expected', [('commitlog_directory', '/c\n'),
                                          ('seeds', 'a\nb\n'),
                                          ('opts', 'x:1\n'),
                                          ('missing', "key 'missing' not found\n")])
def test_config_get(scripts, capsys, key, expected):
    scripts.file('/etc/scylla/scylla.yaml', 'commitlog_directory: /c\nseeds: [a, b]\nopts: {x: 1}\n')
    scripts.run('scylla_config_get.py', '--get', key)
    assert capsys.readouterr().out == expected


CHRONY_CONF = 'pool 2.fedora.pool.ntp.org iburst\n'
NTP_CONF = 'server 0.us.pool.ntp.org iburst\nserver 1.us.pool.ntp.org iburst\n'


@pytest.mark.parametrize('binary', ['/lib/systemd/systemd-timesyncd', 'chronyd', 'ntpd'])
def test_ntp_setup_already_active(scripts, monkeypatch, binary):
    if binary.startswith('/'):
        scripts.file(binary)
    monkeypatch.setattr('shutil.which', lambda x: x == binary)
    scripts.cmd.set('is-active', 'active')
    assert scripts.run('scylla_ntp_setup') == 0
    assert 'chronyc -a makestep' not in scripts.cmd


@pytest.mark.parametrize('distro', ['rhel', 'ubuntu'])
def test_ntp_setup_nothing_installed(scripts, monkeypatch, distro):
    scripts.distro(distro)
    monkeypatch.setattr('shutil.which', lambda x: None)
    scripts.file('/etc/chrony.conf', CHRONY_CONF)
    scripts.cmd.set('systemctl  cat chronyd', returncode=1)
    scripts.run('scylla_ntp_setup')
    if distro == 'rhel':
        assert 'yum install -y chrony' in scripts.cmd
        assert 'systemctl  restart chrony' in scripts.cmd
        assert 'chronyc -a makestep' in scripts.cmd
    else:
        assert 'apt-get -o DPkg::Lock::Timeout=300 install -y systemd-timesyncd' in scripts.cmd
        assert scripts.cmd.calls[-1] == 'timedatectl set-ntp true'


def test_ntp_setup_timesyncd_inactive(scripts, monkeypatch):
    scripts.file('/lib/systemd/systemd-timesyncd')
    monkeypatch.setattr('shutil.which', lambda x: None)
    scripts.run('scylla_ntp_setup')
    assert scripts.cmd.calls[-1] == 'timedatectl set-ntp true'
    assert 'install' not in ' '.join(scripts.cmd.calls)


def test_ntp_setup_chrony_subdomain(scripts, monkeypatch):
    monkeypatch.setattr('shutil.which', lambda x: x == 'chronyd')
    scripts.file('/etc/chrony/chrony.conf', CHRONY_CONF)
    scripts.run('scylla_ntp_setup', '--subdomain', 'centos')
    assert scripts.read('/etc/chrony/chrony.conf') == 'pool 2.centos.pool.ntp.org iburst\n'
    assert 'systemctl  enable chronyd' in scripts.cmd


def test_ntp_setup_chrony_no_conf(scripts, monkeypatch):
    monkeypatch.setattr('shutil.which', lambda x: x == 'chronyd')
    with pytest.raises(FileNotFoundError):
        scripts.run('scylla_ntp_setup')


@pytest.mark.parametrize('subdomain', [[], ['--subdomain', 'centos']])
def test_ntp_setup_ntpd(scripts, monkeypatch, subdomain):
    monkeypatch.setattr('shutil.which', lambda x: x == 'ntpd')
    scripts.file('/etc/ntp.conf', NTP_CONF)
    scripts.cmd.set('systemctl  cat ntpd', returncode=1)
    scripts.run('scylla_ntp_setup', *subdomain)
    assert 'yum install -y ntpdate' in scripts.cmd
    server = '0.centos.pool.ntp.org' if subdomain else '0.us.pool.ntp.org'
    assert f'ntpdate {server}' in scripts.cmd
    assert scripts.cmd.calls[-1] == 'systemctl  start ntp'


# ---- scylla_util ------------------------------------------------------------

import logging
import os
import sys
from types import SimpleNamespace
from subprocess import CalledProcessError

import scylla_util
import scylla_blocktune


def test_excepthook(scripts, monkeypatch, capsys):
    monkeypatch.setattr(logging.getLogger('scylla_util'), 'handlers', [])
    monkeypatch.setattr('sys.argv', ['/opt/scylladb/scripts/scylla_setup'])
    try:
        raise ValueError('boom')
    except ValueError as e:
        scylla_util.scylla_excepthook(type(e), e, e.__traceback__)
    log = f'/var/tmp/scylla/scylla_setup-{os.getpid()}-debug.log'
    assert f'Debug log created: {log}' in capsys.readouterr().out
    assert 'boom' in scripts.read(log)


def test_out(scripts, capsys):
    scripts.cmd.set('good', ' ok \n')
    scripts.cmd.set('bad', 'oops', returncode=2)
    assert scylla_util.out('good') == 'ok'
    assert scylla_util.out('bad', ignore_error=True) == 'oops'
    with pytest.raises(CalledProcessError):
        scylla_util.out('bad')
    assert "Command 'bad' returned non-zero exit status: 2" in capsys.readouterr().out


@pytest.mark.parametrize('flag,attr,expected', [
    (None, 'scriptsdir', '/opt/scylladb/scripts'),
    (None, 'bindir', '/usr/bin'),
    (None, 'etcdir', '/etc'),
    (None, 'datadir', '/var/lib/scylla'),
    ('SCYLLA-NONROOT-FILE', 'bindir', '/opt/scylladb/bin'),
    ('SCYLLA-NONROOT-FILE', 'etcdir', '/opt/scylladb/etc'),
    ('SCYLLA-NONROOT-FILE', 'datadir', '/opt/scylladb'),
    (None, 'scylladir', '/opt/scylladb'),
    (None, 'scyllabindir', '/opt/scylladb/bin'),
    (None, 'sysconfdir', '/etc/sysconfig'),
])
def test_dirs(scripts, monkeypatch, flag, attr, expected):
    monkeypatch.setattr('sys.argv', ['/opt/scylladb/scripts/libexec/scylla_setup'])
    if flag:
        scripts.file(f'/opt/scylladb/{flag}')
    assert getattr(scylla_util, attr)() == expected


def test_offline_container(scripts, monkeypatch):
    monkeypatch.setattr('sys.argv', ['/opt/scylladb/scripts/scylla_setup'])
    assert not scylla_util.is_offline() and not scylla_util.is_container()
    scripts.file('/opt/scylladb/SCYLLA-OFFLINE-FILE')
    scripts.file('/opt/scylladb/SCYLLA-CONTAINER-FILE')
    assert scylla_util.is_offline() and scylla_util.is_container()


@pytest.mark.parametrize('distro,like,version,expected', [
    ('rhel', 'fedora', '9', 'rhel'),
    ('ubuntu', 'debian', '22.04', 'debian'),
    ('opensuse-leap', 'suse opensuse', '15', 'suse'),
    ('amzn', 'centos rhel fedora', '2', 'amzn2'),
    ('gentoo', '', '2', 'gentoo'),
    ('arch', '', '', 'arch'),
])
def test_distro_detection(scripts, distro, like, version, expected):
    scripts.distro(distro, like=like, version=version)
    assert scylla_util.pkg_distro() == expected
    assert scylla_util.is_redhat_variant() == (expected in ('rhel', 'amzn2'))
    assert scylla_util.is_gentoo() == (distro == 'gentoo')
    assert scylla_util.is_arch() == (distro == 'arch')


@pytest.mark.parametrize('conf,expected', [('DEV_MODE=--developer-mode=1\n', True),
                                           ('DEV_MODE=--developer-mode true\n', True),
                                           ('# DEV_MODE=--developer-mode=1\n', False),
                                           ('', False)])
def test_is_developer_mode(scripts, conf, expected):
    scripts.file('/etc/scylla.d/dev-mode.conf', conf)
    assert scylla_util.is_developer_mode() == expected


def test_match_patterns_in_files(scripts):
    scripts.file('/sys/class/dmi/id/board_vendor', 'Amazon EC2\n')
    assert scylla_util.match_patterns_in_files([('google', '/nope'), ('amazon', '/sys/class/dmi/id/board_vendor')])
    assert not scylla_util.match_patterns_in_files([('google', '/sys/class/dmi/id/board_vendor')])


@pytest.mark.parametrize('mask,expected', [('0x1', '0'), ('0x0000000f', '0-3'),
                                           ('0x00000000,0x0000002d', '0,2-3,5'), ('0xff00', '8-15')])
def test_hex2list(mask, expected):
    assert scylla_util.hex2list(mask) == expected


def test_is_unused_disk(scripts, monkeypatch):
    # pyfakefs doesn't support O_EXCL without O_CREAT
    monkeypatch.setattr(scylla_util.os, 'open', lambda *a: 3)
    monkeypatch.setattr(scylla_util.os, 'close', lambda fd: None)
    scripts.file('/dev/nvme0n1')
    assert not scylla_util.is_unused_disk('/dev/nvme0n1')  # not in /sys/class/block
    scripts.fs.create_dir('/sys/class/block/nvme0n1')
    assert scylla_util.is_unused_disk('/dev/nvme0n1')
    scripts.cmd.set('lsblk -n -oPARTTYPE /dev/nvme0n1', scylla_util.SYSTEM_PARTITION_UUIDS[1])
    assert not scylla_util.is_unused_disk('/dev/nvme0n1')

    def busy(*args):
        raise OSError('busy')
    monkeypatch.setattr(scylla_util.os, 'open', busy)
    assert not scylla_util.is_unused_disk('/dev/nvme0n1')


def test_colorprint(capsys):
    scylla_util.colorprint('{red}x{nocolor} {y}', y='z')
    assert capsys.readouterr().out == '\033[1;31mx\033[0m z\n'


def test_scylla_dirs(scripts):
    scripts.file('/etc/scylla/scylla.yaml', 'data_file_directories: [" "]\nhints_directory: /h\n')
    assert scylla_util.get_scylla_dirs() == ['/var/lib/scylla/data', '/var/lib/scylla/commitlog',
                                             '/var/lib/scylla/schema_commitlog', '/h',
                                             '/var/lib/scylla/view_hints']
    scripts.file('/w.yaml', 'workdir: /w\ndata_file_directories: [/d1, /d2]\ncommitlog_directory: /c\n')
    y = scylla_util.parse_scylla_dirs_with_default('/w.yaml')
    assert (y['data_file_directories'], y['commitlog_directory'], y['saved_caches_directory']) == \
        (['/d1', '/d2'], '/c', '/w/saved_caches')
    assert scylla_util.perftune_base_command() == (
        '/opt/scylladb/scripts/perftune.py --tune disks --dir /var/lib/scylla/data --dir /var/lib/scylla/commitlog '
        '--dir /var/lib/scylla/schema_commitlog --dir /h --dir /var/lib/scylla/view_hints')


def test_is_valid_nic(scripts):
    scripts.fs.create_dir('/sys/class/net/eth0')
    assert scylla_util.is_valid_nic('eth0')
    assert not scylla_util.is_valid_nic('eth1')
    assert not scylla_util.is_valid_nic('')


def test_get_set_nic_and_disks_config_value(scripts):
    scripts.file('/new', 'SET_NIC_AND_DISKS=yes\n')
    scripts.file('/old', 'SET_NIC=no\n')
    scripts.file('/both', 'SET_NIC_AND_DISKS=yes\nSET_NIC=no\n')
    assert scylla_util.get_set_nic_and_disks_config_value(scylla_util.sysconfig_parser('/new')) == 'yes'
    assert scylla_util.get_set_nic_and_disks_config_value(scylla_util.sysconfig_parser('/old')) == 'no'
    with pytest.raises(Exception, match='Only one of'):
        scylla_util.get_set_nic_and_disks_config_value(scylla_util.sysconfig_parser('/both'))


def test_swap_exists(scripts):
    assert not scylla_util.swap_exists()
    scripts.cmd.set('swapon', '/swapfile file 1G 0B -2')
    assert scylla_util.swap_exists()


@pytest.mark.parametrize('topology,expected', [(None, False), ('package_cpus', True), ('core_cpus', True),
                                               ('core_siblings', True), ('thread_siblings', True), ('other', False)])
def test_check_sysfs_numa_topology_is_valid(scripts, topology, expected):
    if topology:
        scripts.file(f'/sys/devices/system/cpu/cpu0/topology/{topology}')
    assert scylla_util.check_sysfs_numa_topology_is_valid() == expected


@pytest.mark.parametrize('distro,cmd', [('rhel', 'yum install -y cpupowerutils'),
                                        ('ubuntu', 'apt-get -o DPkg::Lock::Timeout=300 install -y linux-cpupower'),
                                        ('gentoo', 'emerge -uq sys-power/cpupower'),
                                        ('opensuse-leap', 'zypper install -y cpupower'),
                                        ('amzn', 'yum install -y cpupowerutils')])
def test_pkg_install(scripts, monkeypatch, distro, cmd):
    monkeypatch.setattr('sys.argv', ['/opt/scylladb/scripts/scylla_setup'])
    scripts.distro(distro, like='centos rhel fedora' if distro == 'amzn' else None, version='2')
    scylla_util.pkg_install('cpupowerutils')
    assert cmd in scripts.cmd
    # offline installs only report the missing package
    scripts.file('/opt/scylladb/SCYLLA-OFFLINE-FILE')
    with pytest.raises(SystemExit):
        scylla_util.pkg_install('cpupowerutils')
    scripts.cmd.calls.clear()
    scylla_util.pkg_install('cpupowerutils', offline_exit=False)
    assert cmd in scripts.cmd


def test_pkg_install_unsupported(scripts, capsys):
    scripts.distro('arch', like='')
    with pytest.raises(SystemExit):
        scylla_util.pkg_install('cpupowerutils')
    assert 'Package "cpupower" required.' in capsys.readouterr().out


def test_apt_is_updated(scripts):
    assert not scylla_util.apt_is_updated()
    scripts.fs.create_dir('/var/lib/apt/lists')
    assert scylla_util.apt_is_updated()
    os.utime('/var/lib/apt/lists', (0, 0))
    assert not scylla_util.apt_is_updated()
    scripts.file('/var/lib/apt/periodic/update-success-stamp')
    assert scylla_util.apt_is_updated()
    scylla_util.apt_install('foo')
    assert scripts.cmd.calls == ['apt-get -o DPkg::Lock::Timeout=300 install -y foo']


@pytest.mark.parametrize('stderrs,raises', [(['E: Could not get lock /var/lib/apt/lists/lock'], False),
                                            (['E: Could not get lock x'] * scylla_util.APT_GET_UPDATE_NUM_RETRY, True),
                                            (['E: some other error'], True)])
def test_apt_install_update_retry(scripts, monkeypatch, stderrs, raises):
    scripts.distro('ubuntu')
    stderrs = list(stderrs)
    real_run = scylla_util.run

    def run(cmd, **kwargs):
        if cmd == 'apt-get update' and stderrs:
            raise CalledProcessError(100, cmd, stderr=stderrs.pop(0))
        return real_run(cmd, **kwargs)
    monkeypatch.setattr(scylla_util, 'run', run)
    if raises:
        with pytest.raises(CalledProcessError):
            scylla_util.apt_install('foo')
    else:
        scylla_util.apt_install('foo')
        assert 'apt-get -o DPkg::Lock::Timeout=300 install -y foo' in scripts.cmd


@pytest.mark.parametrize('distro,cmd', [('rhel', 'yum remove -y foo'),
                                        ('ubuntu', 'apt-get -o DPkg::Lock::Timeout=300 remove -y foo'),
                                        ('gentoo', 'emerge --deselect foo')])
def test_pkg_uninstall(scripts, distro, cmd):
    scripts.distro(distro)
    scylla_util.pkg_uninstall('foo')
    assert scripts.cmd.calls == [cmd]


def test_pkg_uninstall_unsupported(scripts, capsys):
    scripts.distro('opensuse-leap')
    scylla_util.pkg_uninstall('foo')
    assert 'WARNING: Package "foo" should be removed.' in capsys.readouterr().out


def test_systemd_unit(scripts, monkeypatch):
    monkeypatch.setattr('sys.argv', ['/opt/scylladb/scripts/scylla_setup'])
    scripts.cmd.set('is-active', 'active\n')
    unit = scylla_util.systemd_unit('scylla-server.service')
    for op in ('start', 'stop', 'restart', 'enable', 'disable', 'mask', 'unmask'):
        getattr(unit, op)()
    assert unit.is_active() == 'active'
    scylla_util.systemd_unit.reload()
    assert str(unit) == 'scylla-server.service'
    assert [c.split()[-1] for c in scripts.cmd.calls] == ['scylla-server.service'] * 9 + ['daemon-reload']
    assert scylla_util.systemd_unit.available('foo')
    scripts.cmd.set('cat bar', returncode=1)
    assert not scylla_util.systemd_unit.available('bar')
    with pytest.raises(scylla_util.SystemdException):
        scylla_util.systemd_unit('bar')
    scripts.file('/opt/scylladb/SCYLLA-NONROOT-FILE')
    scylla_util.systemd_unit('scylla-server.service').start()
    assert scripts.cmd.calls[-1] == 'systemctl --user start scylla-server.service'


def test_sysconfig_parser(scripts):
    from pathlib import Path
    scripts.file('/etc/sysconfig/scylla-server', 'A=1\nB="x y"\n')
    cfg = scylla_util.sysconfig_parser(Path('/etc/sysconfig/scylla-server'))
    assert (cfg.get('A'), cfg.get('B'), cfg.has_option('C')) == ('1', 'x y', False)
    cfg.set('A', 'say "hi" now')
    cfg.set('C', '3')
    assert cfg.get('A') == 'say "hi" now'
    cfg.commit()
    assert scripts.read('/etc/sysconfig/scylla-server') == 'A="say \\"hi\\" now"\nB="x y"\nC=3\n'


def test_sysconfig_parser_trailing_quote(scripts):
    scripts.file('/f')
    cfg = scylla_util.sysconfig_parser('/f')
    cfg.set('A', 'say "hi"')
    assert cfg.get('A') == 'say "hi"'


def test_get_product(scripts):
    assert scylla_util.get_product(None) == 'scylla'
    scripts.file('/etc/SCYLLA-PRODUCT-FILE', 'scylla-enterprise\n')
    assert scylla_util.get_product(None) == 'scylla-enterprise'


# ---- scripts ------------------------------------------------------------------

@pytest.mark.parametrize('name', ['scylla_cpuscaling_setup', 'scylla_fstrim_setup', 'scylla_rsyslog_setup',
                                  'scylla_stop', 'scylla_dev_mode_setup'])
def test_requires_root_more(scripts, name):
    assert scripts.run(name, uid=1000) == 1


def test_cpuscaling_setup_unsupported_cpu(scripts, capsys):
    assert scripts.run('scylla_cpuscaling_setup') == 0
    assert "doesn't supported CPU scaling" in capsys.readouterr().out
    assert not scripts.cmd.calls


@pytest.mark.parametrize('distro,like,conf,expected', [
    ('rhel', None, '/etc/sysconfig/cpupower',
     'CPUPOWER_START_OPTS="frequency-set -g performance"\nCPUPOWER_STOP_OPTS="frequency-set -g ondemand"\n'),
    ('amzn', 'centos rhel fedora', '/etc/sysconfig/scylla-cpupower',
     'CPUPOWER_START_OPTS="frequency-set -g performance"\nCPUPOWER_STOP_OPTS="frequency-set -g ondemand"\n'),
    ('opensuse-leap', None, '/etc/sysconfig/scylla-cpupower',
     'CPUPOWER_START_OPTS="frequency-set -g performance"\nCPUPOWER_STOP_OPTS="frequency-set -g ondemand"\n'),
    ('gentoo', '', '/etc/conf.d/cpupower', 'START_OPTS="-g performance"\nSTOP_OPTS="-g ondemand"\n'),
    ('arch', '', '/etc/default/cpupower', 'governor=performance\n'),
    ('ubuntu', None, '/etc/default/cpufrequtils', 'GOVERNOR=performance\n'),
])
def test_cpuscaling_setup(scripts, monkeypatch, distro, like, conf, expected):
    scripts.distro(distro, like=like, version='2')
    if distro == 'arch':  # pkg_install() can't install on arch
        monkeypatch.setattr('shutil.which', lambda x: '/usr/bin/cpupower')
    for d in ('/etc/sysconfig', '/etc/conf.d', '/etc/default', '/etc/systemd/system'):
        scripts.fs.create_dir(d)
    assert scripts.run('scylla_cpuscaling_setup', '--force') == 0
    assert scripts.read(conf) == expected
    assert scripts.cmd.calls[-1].startswith('systemctl  restart ')
    if conf == '/etc/sysconfig/scylla-cpupower':
        assert 'EnvironmentFile=/etc/sysconfig/scylla-cpupower' in scripts.read('/etc/systemd/system/scylla-cpupower.service')
        assert 'systemctl daemon-reload' in scripts.cmd


@pytest.mark.parametrize('has_ondemand', [True, False])
def test_cpuscaling_setup_debian_ondemand(scripts, has_ondemand):
    scripts.distro('ubuntu')
    scripts.fs.create_dir('/etc/default')
    if not has_ondemand:
        scripts.cmd.set('systemctl  cat ondemand', returncode=1)
    scripts.run('scylla_cpuscaling_setup', '--force')
    assert ('systemctl  disable ondemand' in scripts.cmd) == has_ondemand
    assert scripts.read('/etc/default/cpufrequtils') == 'GOVERNOR=performance\n'


def test_cpuscaling_setup_unsupported_distro(scripts, monkeypatch, capsys):
    scripts.distro('alpine', like='')
    monkeypatch.setattr('shutil.which', lambda x: '/usr/bin/cpupower')
    scripts.file('/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor')
    assert scripts.run('scylla_cpuscaling_setup') == 0
    assert 'Unsupported distribution' in capsys.readouterr().out


@pytest.fixture
def blockdev(scripts, monkeypatch):
    """A /var/lib/scylla filesystem on a partition of a RAID with two member disks."""
    monkeypatch.setattr(scylla_blocktune, 'tuned_blockdevs', set())
    # tune_blockdev() imports os.path functions locally, which pyfakefs doesn't patch
    real_path, fake_path = sys.modules['os.path'], scylla_blocktune.os.path
    for f in ('exists', 'realpath'):
        real_f, fake_f = getattr(real_path, f), getattr(fake_path, f)
        monkeypatch.setattr(real_path, f, lambda p, *a, real_f=real_f, fake_f=fake_f, **kw:
                            (fake_f if str(p).startswith('/sys/') else real_f)(p, *a, **kw))
    # fake stat results have no st_rdev; scylla dirs are on 8:1, /dev/md0 is 9:0
    real_stat = scylla_blocktune.os.stat
    monkeypatch.setattr(scylla_blocktune.os, 'stat', lambda p, *a, **kw: SimpleNamespace(
        st_dev=8 * 256 + 1, st_rdev=9 * 256) if p.startswith(('/var/lib/scylla', '/dev/md0')) else real_stat(p, *a, **kw))
    scripts.file('/etc/scylla/scylla.yaml', 'workdir: /var/lib/scylla\n')
    scripts.file('/sys/dev/block/8:1/partition', '1')
    scripts.file('/sys/dev/block/queue/scheduler', 'mq-deadline [none]')
    scripts.file('/sys/dev/block/queue/nomerges', '0')
    for slave in ('sda', 'sdb'):
        scripts.file(f'/sys/dev/block/slaves/{slave}/queue/scheduler', '[mq-deadline] none')
        scripts.file(f'/sys/dev/block/slaves/{slave}/queue/nomerges', '0')
    return scripts


# try_write() never closes the files it writes, so under pyfakefs the data never
# lands; check what the script reports it tunes instead.
@pytest.mark.parametrize('argv', [[], ['--config', '/etc/scylla/scylla.yaml'], ['--filesystem', '/var/lib/scylla/data']])
def test_blocktune(blockdev, capsys, argv):
    blockdev.run('scylla-blocktune', *argv)
    out = capsys.readouterr().out
    for dev in ('', 'slaves/sda/', 'slaves/sdb/'):
        assert f'tuning: /sys/dev/block/{dev}queue/nomerges 2\n' in out
    assert 'tuning: /sys/dev/block/slaves/sda/queue/scheduler noop\n' in out


def test_blocktune_scheduler_none(blockdev, capsys):
    blockdev.run('scylla-blocktune')
    assert 'already tuned: /sys/dev/block/queue/scheduler' in capsys.readouterr().out


def test_blocktune_set_nomerges_dev(blockdev, capsys):
    blockdev.fs.create_dir('/sys/dev/block/9:0/slaves')
    os.symlink('/sys/dev/block/slaves/sda', '/sys/dev/block/9:0/slaves/sda')
    blockdev.run('scylla-blocktune', '--set-nomerges', '0', '--dev', '/dev/md0', '--dev', '/dev/md0')
    assert capsys.readouterr().out == ('tuning /sys/dev/block/9:0\n'
                                       'tuning /sys/dev/block/slaves/sda\n'
                                       'already tuned: /sys/dev/block/slaves/sda/queue/nomerges\n'
                                       'tuning /sys/dev/block/9:0\n')


def test_blocktune_write_failure(capsys):
    scylla_blocktune.try_write('/nonexistent/queue/nomerges', '2')
    assert 'warning: unable to tune /nonexistent/queue/nomerges to 2' in capsys.readouterr().out


def test_fstrim(scripts, capsys):
    scripts.file('/etc/scylla/scylla.yaml', 'commitlog_directory: /commitlog/cl\n')
    scripts.fs.create_dir('/var/lib/scylla/data')
    scripts.fs.create_dir('/commitlog/cl')
    scripts.fs.add_mount_point('/var/lib/scylla')
    scripts.cmd.set('fstrim /var/lib/scylla', returncode=1)
    scripts.run('scylla_fstrim')
    assert scripts.cmd.calls[0] == '/opt/scylladb/scripts/scylla-blocktune --set-nomerges 1'
    assert sorted(scripts.cmd.calls[1:3]) == ['fstrim /', 'fstrim /var/lib/scylla']
    assert scripts.cmd.calls[3] == '/opt/scylladb/scripts/scylla-blocktune --set-nomerges 2'
    assert "/var/lib/scylla doesn't support TRIM." in capsys.readouterr().out


def test_fstrim_setup(scripts):
    scripts.run('scylla_fstrim_setup')
    assert scripts.cmd.calls == ['systemctl  cat scylla-fstrim.timer', 'systemctl  enable scylla-fstrim.timer',
                                 'systemctl  cat scylla-fstrim.timer', 'systemctl  start scylla-fstrim.timer']


@pytest.mark.parametrize('contents,rotated', [('some logs', True), ('', False)])
def test_logrotate(scripts, contents, rotated):
    scripts.file('/opt/scylladb/scylla-server.log', contents)
    scripts.run('scylla_logrotate')
    assert len(os.listdir('/opt/scylladb')) == 1
    assert os.path.exists('/opt/scylladb/scylla-server.log') != rotated


@pytest.mark.parametrize('server,expected', [('10.0.0.1', '10.0.0.1:1514'), ('10.0.0.1:5000', '10.0.0.1:5000')])
def test_rsyslog_setup(scripts, server, expected):
    scripts.fs.create_dir('/etc/rsyslog.d')
    scripts.run('scylla_rsyslog_setup', '--remote-server', server)
    assert scripts.read('/etc/rsyslog.d/scylla.conf') == \
        f"if $programname ==  'scylla' then @@{expected};RSYSLOG_SyslogProtocol23Format\n"
    assert scripts.cmd.calls[-1] == 'systemctl  restart rsyslog.service'


@pytest.mark.parametrize('mode,cmds', [
    ('posix', []),
    ('virtio', ['ip tuntap del mode tap dev tap0']),
    ('dpdk', ['/opt/scylladb/scripts/dpdk-devbind.py -u 0000:00:03.0',
              '/opt/scylladb/scripts/dpdk-devbind.py -b virtio-pci 0000:00:03.0']),
])
def test_stop(scripts, mode, cmds):
    scripts.file('/etc/sysconfig/scylla-server',
                 f'NETWORK_MODE={mode}\nTAP=tap0\nETHPCIID=0000:00:03.0\nETHDRV=virtio-pci\n')
    scripts.run('scylla_stop')
    assert scripts.cmd.calls == cmds


def test_cpuset_setup_container_same_args(scripts):
    scripts.file('/opt/scylladb/SCYLLA-CONTAINER-FILE')
    scripts.file('/etc/scylla.d/perftune.yaml')
    scripts.file('/etc/scylla.d/cpuset.conf', 'CPUSET="--smp 2 "\n')
    assert scripts.run('scylla_cpuset_setup', '--smp', '2', uid=1000) == 0
    assert scripts.fs.exists('/etc/scylla.d/perftune.yaml')  # unchanged config keeps perftune.yaml
