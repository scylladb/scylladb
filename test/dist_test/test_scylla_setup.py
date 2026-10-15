#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import stat
import sys

import pytest

import scylla_util
from test.dist_test.conftest import SCRIPTS_DIR, _sources

S = '/opt/scylladb/scripts/'


def setup_calls(scripts):
    """The setup sub-scripts scylla_setup ran, without the scripts dir prefix."""
    return [c.removeprefix(S) for c in scripts.cmd.calls if c.startswith(S)]


def answer(monkeypatch, *answers):
    it = iter(answers)
    monkeypatch.setattr('builtins.input', lambda *a: next(it))


def load(scripts, monkeypatch):
    """scylla_setup's namespace, without running its main."""
    monkeypatch.setattr(sys, 'argv', [S + 'scylla_setup'])
    ns = {'__name__': 'scylla_setup'}
    exec(compile(_sources['scylla_setup'], str(SCRIPTS_DIR / 'scylla_setup'), 'exec'), ns)
    return ns


@pytest.fixture
def root(scripts):
    scripts.file('/sys/class/net/eth0')
    scripts.cmd.set('scylla --version', '2026.1.0')
    return scripts


def test_requires_root(scripts, capsys):
    assert scripts.run('scylla_setup', '--no-raid-setup', uid=1000) == 1
    assert 'Requires root permission' in capsys.readouterr().out


@pytest.mark.parametrize('distro', ['rhel', 'ubuntu'])
def test_non_interactive(root, distro, capsys):
    root.distro(distro)
    root.fs.create_dir('/etc/rsyslog.d')
    assert root.run('scylla_setup', '--disks', '/dev/sdb', '--raid-level-5', '--online-discard', '0',
                    '--ntp-domain', 'centos', '--setup-nic-and-disks',
                    '--swap-directory', '/', '--swap-size', '2', '--rsyslog-server', '10.0.0.1:514') == 0
    assert setup_calls(root) == [
        'scylla_kernel_check',
        *(['scylla_selinux_setup'] if distro == 'rhel' else []),
        'scylla_ntp_setup --subdomain centos',
        'scylla_raid_setup --disks /dev/sdb --enable-on-nextboot --raid-level=5 --online-discard=0',
        'scylla_coredump_setup --dump-to-raiddir',
        'scylla_sysconfig_setup --nic eth0 --setup-nic-and-disks',
        'scylla_io_setup',
        'scylla_cpuscaling_setup',
        'scylla_fstrim_setup',
        'scylla_memory_setup --lock-memory',
        'scylla_swap_setup --swap-directory / --swap-size 2',
        'scylla_rsyslog_setup --remote-server 10.0.0.1:514',
    ]
    assert 'systemctl  enable scylla-server.service' in root.cmd
    assert 'systemctl  start scylla-perf-collector.service' in root.cmd
    assert 'ScyllaDB setup finished.' in capsys.readouterr().out


def test_skip_everything(root, capsys):
    root.cmd.set('scylla --version', '')
    root.cmd.set('swapon', '/swapfile file 1G 0B -2')
    assert root.run('scylla_setup', '--no-raid-setup', '--no-kernel-check', '--no-enable-service',
                    '--no-selinux-setup', '--no-ntp-setup', '--no-coredump-setup', '--no-sysconfig-setup',
                    '--no-io-setup', '--no-cpuscaling-setup', '--no-fstrim-setup', '--no-memory-setup',
                    '--no-swap-setup', '--no-rsyslog-setup', '--no-perf-collector-setup') == 0
    assert setup_calls(root) == []
    assert 'systemctl  disable scylla-perf-collector.service' in root.cmd
    assert 'A Scylla executable was not found' in capsys.readouterr().out


def test_developer_mode(root):
    root.run('scylla_setup', '--no-raid-setup', '--developer-mode', '--no-sysconfig-setup')
    calls = setup_calls(root)
    assert 'scylla_dev_mode_setup --developer-mode 1' in calls
    assert 'scylla_io_setup' not in calls


def test_raid_mount_exists(root, capsys):
    root.file('/etc/systemd/system/var-lib-scylla.mount')
    root.run('scylla_setup', '--disks', '/dev/sdb')
    assert 'var-lib-scylla.mount already exists, skipping RAID setup' in capsys.readouterr().out
    assert 'scylla_coredump_setup --dump-to-raiddir' in setup_calls(root)
    assert not any(c.startswith('scylla_raid_setup') for c in setup_calls(root))


@pytest.mark.parametrize('argv', [['--nic', 'eth9'], []])
def test_bad_nic(scripts, argv):
    assert scripts.run('scylla_setup', '--no-raid-setup', *argv) == 1


def test_setup_script_failure(root, capsys):
    root.cmd.set('scylla_ntp_setup', returncode=1)
    assert root.run('scylla_setup', '--no-raid-setup') == 1
    assert 'NTP setup failed.' in capsys.readouterr().out


@pytest.mark.parametrize('limit,dev_mode', [('1024', True), ('100000', False)])
def test_nonroot(scripts, limit, dev_mode):
    scripts.file('/opt/scylladb/SCYLLA-NONROOT-FILE')
    scripts.cmd.set('systemctl --no-pager show user@1000.service', f'LimitNOFILE={limit}')
    assert scripts.run('scylla_setup', '--no-raid-setup', '--no-sysconfig-setup', uid=1000) == 0
    assert setup_calls(scripts) == (['scylla_dev_mode_setup --developer-mode 1'] if dev_mode else ['scylla_io_setup'])
    assert 'systemctl --user enable scylla-server.service' in scripts.cmd
    assert 'scylla-perf-collector' not in ' '.join(scripts.cmd.calls)


@pytest.mark.parametrize('distro,tools,skipped', [
    ('rhel', [], ['scylla_kernel_check', 'scylla_coredump_setup', 'scylla_cpuscaling_setup']),
    ('rhel', ['mkfs.xfs', 'coredumpctl', 'cpupower'], []),
    ('ubuntu', ['mkfs.xfs', 'coredumpctl'], ['scylla_cpuscaling_setup']),
])
def test_offline(scripts, monkeypatch, distro, tools, skipped):
    scripts.distro(distro)
    scripts.file('/opt/scylladb/SCYLLA-OFFLINE-FILE')
    monkeypatch.setattr('shutil.which', lambda x: x in tools)
    scripts.run('scylla_setup', '--no-raid-setup', '--no-sysconfig-setup')
    calls = setup_calls(scripts)
    assert 'scylla_ntp_setup' not in calls
    for script in skipped:
        assert script not in calls
    for script in {'scylla_kernel_check', 'scylla_coredump_setup', 'scylla_cpuscaling_setup'} - set(skipped):
        assert script in calls


LSBLK = '''/dev/sda 8:0 0 476.9G 0 disk
/dev/sda1 8:1 0 650M 0 part /boot/efi
/dev/sdb 8:16 0 100G 0 disk
/dev/sdc 8:32 0 100G 0 disk
/dev/sr0 11:0 1 1024M 0 rom
/dev/loop0 7:0 0 4K 1 loop /snap/x
'''


def test_interactive(root, monkeypatch, capsys):
    root.fs.create_dir('/etc/rsyslog.d')
    root.file('/sys/class/block/sda/sda1')
    for nic in ['eth1', 'eth2']:
        root.fs.create_dir(f'/sys/class/net/{nic}/device')
    for dev in ['/dev/sdb', '/dev/sdc']:
        root.fs.create_file(dev, st_mode=stat.S_IFBLK | 0o660)
    root.file('/dev/regular')
    monkeypatch.setattr(scylla_util, 'is_unused_disk', lambda dev: dev != '/dev/sda1')
    root.cmd.set('lsblk --help', ' -p, --paths          print complete device path')
    root.cmd.set('lsblk -pnr', LSBLK)
    root.cmd.set('scylla_selinux_setup', returncode=1)
    answer(monkeypatch,
           'maybe', '',            # kernel check: invalid answer is asked again, empty takes default
           'n', 'y',               # verify packages, enable service
           'yes', '',              # SELinux; its failure waits for a key
           'yes',                  # NTP
           'yes', 'yes', 'no', '',  # RAID, are you sure, RAID5, online discard
           'done', '', '/dev/nope', '/dev/regular', '/dev/sdb,/dev/sdc', '/dev/sdb', 'done',
           '',                     # coredump
           'y', 'y', 'bogus', 'eth1', 'y',  # sysconfig, NIC/disk tuning, NIC, clocksource
           'y', 'y', 'y', 'y',     # io, cpuscaling, fstrim, memory
           'y', 'n', '/nope', '/', 'n', '5x', '5',  # swap, on /?, directory, auto size?, size
           'y', '10.0.0.1:514',    # rsyslog
           'y')                    # perf collector
    assert root.run('scylla_setup') == 0
    assert setup_calls(root) == [
        'scylla_kernel_check',
        'scylla_selinux_setup',
        'scylla_ntp_setup',
        'scylla_raid_setup --disks /dev/sdb,/dev/sdc --enable-on-nextboot --raid-level=0 --online-discard=1',
        'scylla_coredump_setup --dump-to-raiddir',
        'scylla_sysconfig_setup --nic eth1 --setup-nic-and-disks --set-clocksource',
        'scylla_io_setup',
        'scylla_cpuscaling_setup',
        'scylla_fstrim_setup',
        'scylla_memory_setup --lock-memory',
        'scylla_swap_setup --swap-directory / --swap-size 5',
        'scylla_rsyslog_setup --remote-server 10.0.0.1:514',
    ]
    out = capsys.readouterr().out
    assert 'Please select unmounted disks from the following list: /dev/sdb, /dev/sdc' in out
    assert '/dev/nope not found' in out
    assert '/dev/regular is not block device' in out
    assert '/dev/sdb is already added' in out
    assert '"5x" is not valid number' in out
    assert '--disks /dev/sdb,/dev/sdc' in out  # non-interactive suggestion
    assert 'Please restart your machine' in out


def test_interactive_say_no(root, monkeypatch, capsys):
    root.cmd.set('lsblk --help', '')
    answer(monkeypatch, *['n'] * 5, 'yes', 'yes', 'yes', 'yes', 'cancel', *['n'] * 20)
    root.fs.create_file('/dev/sdb', st_mode=stat.S_IFBLK | 0o660)
    monkeypatch.setattr(scylla_util, 'is_unused_disk', lambda dev: True)
    assert root.run('scylla_setup') == 0
    assert setup_calls(root) == []
    assert 'Please select unmounted disks from the following list: /dev/sdb' in capsys.readouterr().out


def test_interactive_no_free_disks(root, monkeypatch, capsys):
    answer(monkeypatch, 'n', 'n', 'n', 'n', 'n', 'yes', 'yes', 'no', 'no', *['n'] * 20)
    root.run('scylla_setup')
    assert 'No free disks were detected' in capsys.readouterr().out
    assert not any(c.startswith('scylla_raid_setup') for c in setup_calls(root))


def test_interactive_select_unlisted_disk(root, monkeypatch):
    root.cmd.set('lsblk --help', '')
    root.fs.create_file('/dev/sdb', st_mode=stat.S_IFBLK | 0o660)
    root.fs.create_file('/dev/sdz', st_mode=stat.S_IFBLK | 0o660)
    monkeypatch.setattr(scylla_util, 'is_unused_disk', lambda dev: dev == '/dev/sdb')
    answer(monkeypatch, 'n', 'n', 'n', 'n', 'n', 'yes', 'yes', 'no', 'no', '/dev/sdz', 'done', *['n'] * 20)
    assert root.run('scylla_setup') == 0


@pytest.mark.parametrize('distro,setup,expected', [
    ('ubuntu', lambda s: s.cmd.set('dpkg -s scylla-tools', returncode=1), 1),
    ('rhel', lambda s: None, 0),
    ('opensuse-leap', lambda s: s.cmd.set('rpm -q', returncode=1), 1),
    ('gentoo', lambda s: s.file('/var/db/pkg/dev-db/scylla-tools-1.0'), 0),
    ('gentoo', lambda s: None, 1),
])
def test_do_verify_package(scripts, monkeypatch, distro, setup, expected):
    scripts.distro(distro)
    setup(scripts)
    ns = load(scripts, monkeypatch)
    if expected:
        with pytest.raises(SystemExit):
            ns['do_verify_package_and_exit']('scylla-tools')
    else:
        ns['do_verify_package_and_exit']('scylla-tools')


@pytest.mark.parametrize('installed', [True, False])
def test_do_verify_package_offline(scripts, monkeypatch, installed):
    scripts.file('/opt/scylladb/SCYLLA-OFFLINE-FILE')
    if installed:
        scripts.file('/opt/scylladb/share/cassandra/bin/cqlsh')
    assert load(scripts, monkeypatch)['do_verify_package']('scylla-tools') == (0 if installed else 1)


def test_do_verify_package_unknown_distro(scripts, monkeypatch):
    scripts.distro('arch')
    with pytest.raises(SystemExit):
        load(scripts, monkeypatch)['do_verify_package']('scylla-tools')


def test_list_block_devices_without_lsblk_paths(scripts, monkeypatch):
    for d in ['/dev/sda', '/dev/nvme0n1', '/dev/mapper/control', '/dev/mapper/vg-data']:
        scripts.file(d)
    assert sorted(load(scripts, monkeypatch)['list_block_devices']()) == ['/dev/mapper/vg-data', '/dev/nvme0n1', '/dev/sda']


def test_get_unused_disks(scripts, monkeypatch):
    for d in ['/dev/sda', '/dev/sdb', '/dev/sdc']:
        scripts.file(d)
    scripts.file('/sys/class/block/sda/sda1')
    monkeypatch.setattr(scylla_util, 'is_unused_disk', lambda dev: dev != '/dev/sdc')
    assert load(scripts, monkeypatch)['get_unused_disks']() == ['/dev/sdb']


@pytest.mark.parametrize('nics,expected', [([], SystemExit), (['eth0'], 'eth0')])
def test_interactive_choose_nic(scripts, monkeypatch, nics, expected):
    scripts.fs.create_dir('/sys/class/net/lo')
    for nic in nics:
        scripts.fs.create_dir(f'/sys/class/net/{nic}/bonding')
    choose = load(scripts, monkeypatch)['interactive_choose_nic']
    if expected is SystemExit:
        with pytest.raises(SystemExit):
            choose()
    else:
        assert choose() == expected


def test_interactive_setup_script_failure(root, monkeypatch):
    root.cmd.set('scylla_kernel_check', returncode=1)
    answer(monkeypatch, 'y')
    with pytest.raises(Exception, match='scylla_kernel_check'):
        root.run('scylla_setup')
