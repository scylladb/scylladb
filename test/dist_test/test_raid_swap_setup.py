#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import re
import subprocess
from collections import namedtuple
from types import SimpleNamespace

import psutil
import pytest
import pyudev
import pyfakefs.helpers
from pyfakefs.helpers import FakeStatResult

import scylla_util

UUID_LINK = '/dev/disk/by-uuid/abcd'
Part = namedtuple('Part', 'mountpoint fstype')


@pytest.fixture
def raid(scripts, monkeypatch):
    """A machine with two unused block devices and every tool installed."""
    for d in ('/dev/sdb', '/dev/sdc', '/dev/md0'):
        scripts.file(d)
    scripts.file('/sys/dev/block/0:0/queue/logical_block_size', '512')
    for d in ('/etc/systemd/system', '/etc/mdadm'):
        scripts.fs.create_dir(d)
    monkeypatch.setattr(pyfakefs.helpers, 'USER_ID', 0)  # the script chowns as root
    monkeypatch.setattr('stat.S_ISBLK', lambda mode: True)
    monkeypatch.setattr(FakeStatResult, 'st_rdev', 0, raising=False)  # major:minor 0:0
    monkeypatch.setattr(scylla_util, 'is_unused_disk', lambda dev: True)
    monkeypatch.setattr('shutil.which', lambda x: f'/usr/bin/{x}')
    monkeypatch.setattr('platform.release', lambda: '6.1.0-generic')
    monkeypatch.setattr(psutil, 'disk_partitions', lambda all=False: [])
    monkeypatch.setattr('pwd.getpwnam', lambda n: SimpleNamespace(pw_uid=123))
    monkeypatch.setattr('grp.getgrnam', lambda n: SimpleNamespace(gr_gid=456))
    monkeypatch.setattr(pyudev, 'Context', lambda: None)
    scripts.udev = SimpleNamespace(
        device_links=[UUID_LINK], properties={'ID_FS_UUID': 'abcd', 'ID_FS_TYPE': 'xfs', 'ID_FS_USAGE': 'filesystem'},
        sys_path='', sys_name='', sys_number='', device_path='', tags=[], subsystem='block', driver=None,
        device_type='disk', device_node='', device_number=0, is_initialized=True, time_since_initialized=0)
    monkeypatch.setattr(pyudev.Devices, 'from_device_file', lambda ctx, f: scripts.udev)
    scripts.cmd.set('systemd-escape -p --suffix=mount', 'var-lib-scylla.mount')
    scripts.cmd.set('mdadm --detail --scan', 'ARRAY /dev/md0 metadata=1.2')
    scripts.cmd.set('matchpathcon -n', 'system_u:object_r:systemd_coredump_var_lib_t:s0')
    return scripts


def mount_unit(raid):
    return raid.read('/etc/systemd/system/var-lib-scylla.mount')


@pytest.mark.parametrize('distro', ['rhel', 'ubuntu'])
def test_raid_single_disk(raid, distro):
    """A single disk gets a plain XFS volume, mounted on /var/lib/scylla by a unit scylla-server waits for."""
    raid.distro(distro)
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdb') == 0
    assert 'mkfs.xfs  /dev/sdb -K -m rmapbt=0 -m reflink=0' in raid.cmd
    assert 'mdadm' not in raid.cmd
    assert mount_unit(raid) == f'''[Unit]
Description=Scylla data directory
Before=local-fs.target scylla-server.service
DefaultDependencies=no

[Mount]
What={UUID_LINK}
Where=/var/lib/scylla
Type=xfs
Options=noatime,discard,lazytime

[Install]
WantedBy=local-fs.target'''
    assert raid.read('/etc/systemd/system/scylla-server.service.d/mounts.conf') == \
        '[Unit]\nRequiresMountsFor=/var/lib/scylla\n'
    assert 'systemctl  start var-lib-scylla.mount' in raid.cmd
    assert raid.fs.exists('/var/lib/scylla/view_hints')
    if distro == 'ubuntu':
        assert raid.cmd.calls[-1] == 'update-initramfs -u'
    else:
        assert 'semanage fcontext -a -t systemd_coredump_var_lib_t "/var/lib/scylla/coredump(/.*)?"' in raid.cmd
        assert raid.fs.exists('/.autorelabel')


@pytest.mark.parametrize('kernel,block_opt,lazytime', [('5.4.0', '-b size=1024', ',lazytime'),
                                                       ('3.10.0', '-b size=1024', ''),
                                                       ('x.y', '-b size=1024', '')])
def test_raid_raid0_old_kernel(raid, monkeypatch, kernel, block_opt, lazytime):
    """RAID0 must use the first free /dev/mdX, and lazytime only on kernels that support it."""
    monkeypatch.setattr('platform.release', lambda: kernel)
    raid.file('/sys/block/md0/md/array_state', 'active')
    raid.file('/sys/block/md1/md/array_state', 'clear')
    raid.file('/dev/md1')
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdb,/dev/sdc', '--online-discard', 'False') == 0
    assert 'mdadm --create --verbose --force --run /dev/md1 --level=0 -c1024 --raid-devices=2 /dev/sdb /dev/sdc' in raid.cmd
    assert f'mkfs.xfs {block_opt} /dev/md1' in raid.cmd
    assert raid.read('/etc/mdadm.conf') == 'ARRAY /dev/md0 metadata=1.2\nMAILADDR root'
    assert f'Options=noatime{lazytime}\n' in mount_unit(raid)


@pytest.mark.parametrize('argv', [['--raiddev', '/dev/md0'], []], ids=['raiddev', 'auto'])
def test_raid_no_free_md(raid, capsys, argv):
    """When every /dev/mdX is taken, RAID setup must fail instead of reusing an active array."""
    for i in range(10):
        raid.file(f'/sys/block/md{i}/md/array_state', 'active')
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdb', '--force-raid', *argv) == 1
    assert ("Can't find unused /dev/mdX" in capsys.readouterr().out) == (not argv)


@pytest.mark.parametrize('unit', ['mdmonitor.service', 'mdadm.service'])
def test_raid_level1_starts_md_service(raid, unit):
    """Mirrored RAID needs the md monitor running, whichever unit name the distro uses."""
    if unit == 'mdadm.service':
        raid.cmd.set('systemctl  cat mdmonitor.service', returncode=1)
    raid.distro('ubuntu')
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb,/dev/sdc', '--raid-level', '1')
    assert '--level=1' in ' '.join(raid.cmd.calls)
    assert f'systemctl  start {unit}' in raid.cmd
    assert raid.read('/etc/mdadm/mdadm.conf').endswith('MAILADDR root')


def test_raid_level1_mount_unit_has_no_md_dependency(raid):
    """The mount unit is ordered before local-fs.target, so depending on the md monitor unit (after sysinit.target) is a cycle."""
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb,/dev/sdc', '--raid-level', '1')
    assert 'md' not in ''.join(l for l in mount_unit(raid).splitlines() if l.startswith(('After=', 'Wants=')))


@pytest.mark.parametrize('role,where', [('data', '/var/lib/scylla/data'), ('commitlog', '/var/lib/scylla/commitlog')])
def test_raid_volume_role(raid, role, where):
    """--volume-role must mount the volume on the matching sub directory of --root."""
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb', '--volume-role', role, '--root', '/var/lib/scylla/')
    assert f'systemd-escape -p --suffix=mount {where}' in raid.cmd
    assert f'Where={where}\n' in mount_unit(raid)


def test_raid_invalid_role(raid, capsys):
    """An unknown --volume-role must be rejected before touching any disk."""
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdb', '--volume-role', 'foo') == 1
    assert 'Invalid role specified (foo)' in capsys.readouterr().out


@pytest.mark.parametrize('option', ['--online-discard', '--blk-discard'])
def test_raid_invalid_bool_option(raid, capsys, option):
    """A non-boolean --online-discard/--blk-discard gets a usage error, not a traceback or a silent true."""
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdb', option, 'maybe') == 2
    assert f"argument {option}: invalid strtobool value: 'maybe'" in capsys.readouterr().err


@pytest.mark.parametrize('problem,message', [('missing', '/dev/sdx is not found'),
                                             ('not_blk', '/dev/sdx is not block device'),
                                             ('busy', '/dev/sdx is busy'),
                                             ('mounted', '/var/lib/scylla is already mounted'),
                                             ('unit_exists', 'mount unit /etc/systemd/system/var-lib-scylla.mount already exists')])
def test_raid_precondition_errors(raid, monkeypatch, capsys, problem, message):
    """Missing, non-block, busy or already mounted disks must be refused before anything is formatted."""
    if problem != 'missing':
        raid.file('/dev/sdx')
    if problem == 'not_blk':
        monkeypatch.setattr('stat.S_ISBLK', lambda mode: False)
    if problem == 'busy':
        monkeypatch.setattr(scylla_util, 'is_unused_disk', lambda dev: False)
    if problem == 'mounted':
        monkeypatch.setattr('os.path.ismount', lambda p: True)
    if problem == 'unit_exists':
        raid.file('/etc/systemd/system/var-lib-scylla.mount')
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdx') == 1
    assert message in capsys.readouterr().out


@pytest.mark.parametrize('distro,install,pkgs', [
    ('rhel', 'yum install -y', ['xfsprogs', 'mdadm', 'libselinux-utils', 'policycoreutils', 'policycoreutils-python-utils']),
    ('ubuntu', 'apt-get -o DPkg::Lock::Timeout=300 install -y', ['xfsprogs', 'mdadm', 'initramfs-tools'])],
    ids=['rhel', 'ubuntu'])
def test_raid_installs_missing_tools(raid, monkeypatch, distro, install, pkgs):
    """Missing mkfs/mdadm/SELinux tools must be installed with the distro's package manager."""
    raid.distro(distro)
    monkeypatch.setattr('shutil.which', lambda x: None)
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    for pkg in pkgs:
        assert f'{install} {pkg}' in raid.cmd


def test_raid_blk_discard(raid):
    """--blk-discard must discard only the disks that support discard."""
    raid.file('/sys/block/sdb/queue/discard_granularity', '512')
    raid.file('/sys/block/sdc/queue/discard_granularity', '0')
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb,/dev/sdc', '--blk-discard', 'True')
    assert 'blkdiscard /dev/sdb' in raid.cmd
    assert 'blkdiscard /dev/sdc' not in raid.cmd


def test_raid_no_blk_discard_by_default(raid):
    """blkdiscard wipes disks and is slow, so it must not run unless --blk-discard is given."""
    raid.file('/sys/block/sdb/queue/discard_granularity', '512')
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    assert 'blkdiscard /dev/sdb' not in raid.cmd


@pytest.mark.parametrize('link', ['/dev/disk/by-label/x', '/dev/disk/by-partuuid/x', '/dev/disk/by-path/x',
                                  '/dev/disk/by-id/x', None],
                         ids=['by-label', 'by-partuuid', 'by-path', 'by-id', 'no-link'])
def test_raid_mount_without_uuid(raid, caplog, link):
    """Without a filesystem UUID from udev, the mount unit must use another stable link, or the device itself."""
    raid.udev.device_links = [link] if link else []
    raid.udev.properties = {}
    with caplog.at_level(logging.ERROR):
        raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    assert f'What={link or "/dev/sdb"}\n' in mount_unit(raid)
    assert 'Failed to detect uuid' in caplog.text
    assert 'ID_FS_TYPE is not "xfs"' in caplog.text


def test_raid_mount_failure_dumps_udev(raid, caplog):
    """When mounting fails, the udev properties must be logged to explain why."""
    raid.udev.properties['ID_FS_TYPE'] = 'ext4'
    raid.cmd.set('systemctl  start var-lib-scylla.mount', returncode=1)
    with caplog.at_level(logging.ERROR), pytest.raises(subprocess.CalledProcessError):
        raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    assert 'dumping udev env parameters on /dev/sdb' in caplog.text
    assert 'ID_FS_TYPE: ext4' in caplog.text


def test_raid_mount_failure_names_missing_link(raid, caplog):
    """When mounting fails, the error must name the missing device link."""
    raid.cmd.set('systemctl  start var-lib-scylla.mount', returncode=1)
    with caplog.at_level(logging.ERROR), pytest.raises(subprocess.CalledProcessError):
        raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    assert f'{UUID_LINK} is not found' in caplog.text
    assert 'is broken link' not in caplog.text


def test_raid_mount_failure_names_broken_link(raid, caplog):
    """A device link pointing nowhere is reported as broken, not as missing."""
    raid.fs.create_symlink(UUID_LINK, '/dev/nowhere')
    raid.cmd.set('systemctl  start var-lib-scylla.mount', returncode=1)
    with caplog.at_level(logging.ERROR), pytest.raises(subprocess.CalledProcessError):
        raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    assert f'{UUID_LINK} is broken link' in caplog.text
    assert f'{UUID_LINK} is not found' not in caplog.text


def test_raid_enable_on_nextboot_and_existing_mounts_conf(raid):
    """--enable-on-nextboot must enable the mount unit and keep existing RequiresMountsFor= entries."""
    raid.file('/etc/systemd/system/scylla-server.service.d/mounts.conf', '[Unit]\nRequiresMountsFor=/a\n')
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb', '--enable-on-nextboot')
    assert 'systemctl  enable var-lib-scylla.mount' in raid.cmd
    assert raid.read('/etc/systemd/system/scylla-server.service.d/mounts.conf') == \
        '[Unit]\nRequiresMountsFor=/a\nRequiresMountsFor=/var/lib/scylla\n'


def test_raid_selinux_enabled_restorecon(raid, monkeypatch):
    """With SELinux enabled, the new volume must be relabeled."""
    monkeypatch.setattr(psutil, 'disk_partitions', lambda all=False: [Part('/proc', 'proc'),
                                                                      Part('/sys/fs/selinux', 'selinuxfs')])
    raid.file('/sys/fs/selinux/enforce')
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    assert raid.cmd.calls[-1] == 'restorecon -F -v -R /var/lib/scylla'


def test_raid_offline_without_selinux_tools(raid, monkeypatch, capsys):
    """An offline install without SELinux tools must skip relabeling, not fail."""
    raid.file('/opt/scylladb/SCYLLA-OFFLINE-FILE')
    monkeypatch.setattr('shutil.which', lambda x: None if x == 'restorecon' else f'/usr/bin/{x}')
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdb') == 0
    assert 'Unable to find SELinux tools, skip relabeling.' in capsys.readouterr().out


@pytest.mark.parametrize('existing', ['', '/a(/.*)? ctx\n'], ids=['new', 'append'])
def test_raid_offline_without_semanage(raid, monkeypatch, existing):
    """Offline without semanage, the coredump file context must be added to file_contexts.local and compiled."""
    raid.file('/opt/scylladb/SCYLLA-OFFLINE-FILE')
    raid.file('/etc/selinux/targeted/contexts/files/file_contexts.local', existing)
    monkeypatch.setattr('shutil.which', lambda x: None if x == 'semanage' else f'/usr/bin/{x}')
    raid.run('scylla_raid_setup', '--disks', '/dev/sdb')
    assert raid.read('/etc/selinux/targeted/contexts/files/file_contexts.local') == \
        f'{existing}{existing and chr(10)}/var/lib/scylla/coredump(/.*)?   system_u:object_r:systemd_coredump_var_lib_t:s0\n'
    assert 'sefcontext_compile /etc/selinux/targeted/contexts/files/file_contexts.local' in raid.cmd


def test_raid_offline_without_selinux_policy(raid, monkeypatch, capsys):
    """Offline without SELinux policy files, relabeling must be skipped, not fail."""
    raid.file('/opt/scylladb/SCYLLA-OFFLINE-FILE')
    monkeypatch.setattr('shutil.which', lambda x: None if x == 'semanage' else f'/usr/bin/{x}')
    assert raid.run('scylla_raid_setup', '--disks', '/dev/sdb') == 0
    assert 'Unable to find SELinux policy files, skip relabeling.' in capsys.readouterr().out


# --- scylla_swap_setup ---

GiB = 1024 ** 3


@pytest.fixture
def swap(scripts, monkeypatch):
    """A machine with / on xfs and /mnt on ext4, 96GiB of RAM and 100GiB free, for scylla_swap_setup."""
    scripts.fs.create_dir('/etc/systemd/system')
    scripts.fs.create_dir('/mnt')
    monkeypatch.setattr('pathlib.Path.is_mount', lambda p: str(p) in ('/', '/mnt'))
    scripts.disk = SimpleNamespace(free=100 * GiB)
    scripts.mem = SimpleNamespace(total=96 * GiB)
    scripts.parts = [Part('/', 'xfs'), Part('/mnt', 'ext4')]
    monkeypatch.setattr(psutil, 'disk_usage', lambda p: scripts.disk)
    monkeypatch.setattr(psutil, 'virtual_memory', lambda: scripts.mem)
    monkeypatch.setattr(psutil, 'disk_partitions', lambda: scripts.parts)
    scripts.cmd.set('systemd-escape -p --suffix=swap', 'swapfile.swap')
    scripts.cmd.set('systemd-escape -p --suffix=mount /mnt', 'mnt.mount')
    scripts.cmd.set('systemd-escape -p --suffix=mount', '-.mount')

    def create_swapfile():  # dd/fallocate are faked, but the script chmods the file they create
        scripts.file(re.search(r'/\S*swapfile', scripts.cmd.calls[-1]).group())
        return ''
    scripts.cmd.set('dd if=/dev/zero', create_swapfile)
    scripts.cmd.set('fallocate', create_swapfile)
    return scripts


@pytest.mark.parametrize('mem_gib,disk_gib,expected_mb', [(96, 100, 16 * 1024), (12, 100, 4 * 1024), (96, 10, 5 * 1024)])
def test_swap_auto_size(swap, mem_gib, disk_gib, expected_mb):
    """The automatic swap size must follow the RAM size and be capped by the free disk space."""
    swap.mem.total, swap.disk.free = mem_gib * GiB, disk_gib * GiB
    assert swap.run('scylla_swap_setup') == 0
    assert f'dd if=/dev/zero of=/swapfile bs=1M count={expected_mb}' in swap.cmd
    assert 'mkswap -f /swapfile' in swap.cmd
    assert swap.read('/etc/systemd/system/swapfile.swap') == '''[Unit]
Description=swapfile
DefaultDependencies=no
After=-.mount
Conflicts=umount.target
Before=umount.target

[Swap]
What=/swapfile

[Install]
WantedBy=multi-user.target'''
    assert swap.cmd.calls[-2:] == ['systemctl  enable swapfile.swap', 'systemctl  start swapfile.swap']


@pytest.mark.parametrize('argv', [['--swap-size', '2'], ['--swap-size-bytes', str(2 * GiB)]], ids=['size-gb', 'size-bytes'])
def test_swap_explicit_size_on_ext4(swap, argv):
    """An explicit size must be honored; on ext4 the file is preallocated and the unit waits for its mount."""
    swap.fs.create_dir('/mnt/swaps')
    swap.cmd.set('systemd-escape -p --suffix=swap', 'mnt-swaps-swapfile.swap')
    swap.run('scylla_swap_setup', '--swap-directory', '/mnt/swaps', *argv)
    assert 'fallocate -l 2048MiB /mnt/swaps/swapfile' in swap.cmd
    assert 'After=mnt.mount\n' in swap.read('/etc/systemd/system/mnt-swaps-swapfile.swap')


@pytest.mark.parametrize('setup,argv,message', [
    ('swap_exists', [], 'swap already configured'),
    (None, ['--swap-size', '1', '--swap-size-bytes', '1'], 'Cannot specify both'),
    ('swapfile', [], 'swapfile /swapfile already exists'),
    ('unit', [], 'swap unit /etc/systemd/system/swapfile.swap already exists'),
    (None, ['--swap-size', '200'], 'does not have enough disk space. 200.00GB space required.'),
    ('small_disk', [], 'swap directory / does not have enough disk space.'),
], ids=['swap-exists', 'both-sizes', 'swapfile-exists', 'unit-exists', 'too-big', 'small-disk'])
def test_swap_errors(swap, capsys, setup, argv, message):
    """Conflicting options, an existing swap, or too little disk space must stop setup with a clear message."""
    if setup == 'swap_exists':
        swap.cmd.set('swapon --noheadings --raw', '/swapfile file 1G 0B -2')
    elif setup == 'swapfile':
        swap.file('/swapfile')
    elif setup == 'unit':
        swap.file('/etc/systemd/system/swapfile.swap')
    elif setup == 'small_disk':
        swap.disk.free = GiB
    assert swap.run('scylla_swap_setup', *argv) == 1
    assert message in capsys.readouterr().out


def test_swap_unknown_fs_type(swap):
    """A swap directory on an unlisted filesystem must still get a swapfile, using the safe dd path."""
    swap.parts = []
    swap.run('scylla_swap_setup')
    assert 'dd if=/dev/zero of=/swapfile bs=1M count=16384' in swap.cmd
