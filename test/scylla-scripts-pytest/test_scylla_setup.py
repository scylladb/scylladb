import os
import sys
import subprocess
import typing
import types
import copy
import json
from pwd import struct_passwd
from grp import struct_group
from pathlib import Path
from importlib.machinery import SourceFileLoader
from unittest.mock import patch, call
from textwrap import dedent
from dataclasses import dataclass
from multiprocessing.pool import Pool

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock.plugin import MockerFixture

scripts_path = Path(__file__).parent.parent.parent / 'dist/common/scripts'
sys.path.insert(1, str(scripts_path))

# 2nd and 5th numbers to simulate having enough disk space for io_setup
mocked_statvfs = os.statvfs_result([0, 10000000000, 0, 0, 10000000000, 0, 0, 0.0, 0.0, 0.0])


@dataclass
class MockStatResult:
    st_rdev: int = 255
    st_mode: int = 0
    st_dev: int = 255


original_stat = os.stat


def mock_stat(p):
    if p in ['/dev/md0', '/dev/sdb', '/var/lib/scylla/data', '/var/lib/scylla/commitlog']:
        return MockStatResult()
    return original_stat(p)


class FileModule:
    argv: typing.List[str] = None
    path: str = None

    def __init__(self, mocker):
        self.mocker: MockerFixture = mocker
        self.run_side_effect_mapping = {
            'scylla --version': dict(stdout='4.5.1', returncode=0)
        }

    def distro(self, name):
        self.mocker.patch('distro.id', return_value=name)
        self.mocker.patch('distro.like', return_value=name)
        if name == 'amzn':
            self.mocker.patch('distro.version', return_value='2')

    def patch(self, *args, **kwargs):
        return self.mocker.patch(*args, **kwargs)

    def _run_side_effect(self, *args, **_):
        for cmd, output in self.run_side_effect_mapping.items():
            if cmd in args[0]:
                return subprocess.CompletedProcess(args=args, **output)

        raise Exception(f'{args} not expected')

    def mock_run(self, side_effect_mapping: dict):
        self.run_side_effect_mapping.update(side_effect_mapping)
        return self.patch('subprocess.run', side_effect=self._run_side_effect)

    def run(self):
        argv = copy.copy(self.argv) if self.argv else []
        argv.insert(0, str(Path('/opt/scylla/scripts') / Path(self.path).name))
        self.patch("sys.argv", new=argv)
        self.patch('os.getuid', return_value=0)
        loader = SourceFileLoader('__main__', self.path)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)


@pytest.fixture(name='file_module')
def fixture_file_module(mocker: MockerFixture):
    yield FileModule(mocker=mocker)


def test_scylla_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_setup')
    file_module.argv = ['--disks', '/dev/sdb', '--rsyslog-server', '127.0.0.1', '--swap-directory', '/', '--swap-size', '1']

    file_module.distro('rhel')
    fs.create_dir('/etc/scylla.d')
    fs.create_dir('/etc/rsyslog.d')
    fs.create_file('/sys/class/net/eth0')
    fs.create_file('/etc/sysconfig/scylla-housekeeping', contents=dedent("""
        REPO_FILES=/etc/yum.repos.d/scylla*.repo
    """))

    file_module.mock_run({'rpm -q': dict(returncode=0),
                          'systemctl': dict(returncode=0),
                          'scylla_selinux_setup': dict(returncode=0), # FIXME: selinux_reboot_required can't ever be True
                          'scripts/scylla': dict(returncode=0, stdout=''),
                          'swapon --noheadings --raw': dict(returncode=0, stdout='')})

    file_module.run()

    assert fs.get_object('/etc/scylla.d/housekeeping.cfg').contents == '[housekeeping]\ncheck-version: True\n'


def test_scylla_setup_interactive(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_setup')
    file_module.argv = []

    file_module.distro('rhel')
    fs.create_dir('/etc/scylla.d')
    fs.create_dir('/etc/rsyslog.d')

    fs.create_file('/sys/class/net/eth0')
    fs.create_file('/etc/sysconfig/scylla-housekeeping', contents=dedent("""
        REPO_FILES=/etc/yum.repos.d/scylla*.repo
    """))
    input_mock = file_module.patch('builtins.input')

    mock_run = file_module.mock_run({'rpm -q': dict(returncode=0),
                          'systemctl': dict(returncode=0),
                          'scripts/scylla': dict(returncode=0, stdout=''),
                          'swapon --noheadings --raw': dict(returncode=0, stdout=''),
                          'lsblk --help': dict(returncode=0, stdout=' -p, --paths          print complete device path'),
                          'lsblk -pnr': dict(returncode=0, stdout=dedent("""
                            /dev/loop63 7:63 0 131.9M 1 loop /snap/chromium/2000
                            /dev/sda 8:0 0 476.9G 0 disk 
                            /dev/sda1 8:1 0 650M 0 part /boot/efi
                            /dev/sda2 8:2 0 128M 0 part 
                            /dev/sda3 8:3 0 99.1G 0 part /media/fruch/OS
                            /dev/sda4 8:4 0 990M 0 part 
                            /dev/sda5 8:5 0 376.1G 0 part /
                          """))})

    # all the interactive answers, + configure swap and rsyslog
    input_mock.side_effect = ['yes'] * 10 + ['/dev/sda', 'done'] + ['yes'] * 9 + ['no', '/', 'no', '5'] + ['yes', '127.0.0.1:5000']

    fs.create_file('/dev/sda')
    file_module.patch('stat.S_ISBLK')
    file_module.patch('scylla_util.is_unused_disk', return_value=True)

    file_module.run()

    assert call('/opt/scylla/scripts/scylla_swap_setup --swap-directory / --swap-size 5', shell=True) in mock_run.call_args_list
    assert call('/opt/scylla/scripts/scylla_rsyslog_setup --remote-server 127.0.0.1:5000', shell=True) in mock_run.call_args_list


def test_scylla_setup_non_root(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_setup')
    file_module.argv = ['--io-setup', '1', '--no-raid-setup']

    fs.create_file('/opt/scylla/SCYLLA-NONROOT-FILE')

    file_module.distro('rhel')
    fs.create_file('/etc/sysconfig/scylla-housekeeping')
    fs.create_file('/sys/class/net/eth0')
    fs.create_file('/etc/scylla.d/housekeeping.cfg', contents=dedent("""
        REPO_FILES=/etc/yum.repos.d/scylla*.repo
    """))

    file_module.mock_run({'systemctl --no-pager show user@1000.service': dict(returncode=0, stdout="LimitNOFILE=100000"),
                          'rpm -q': dict(returncode=0),
                          'systemctl': dict(returncode=0),
                          'scripts/scylla': dict(returncode=0, stdout=''),
                          'swapon --noheadings --raw': dict(returncode=0, stdout='swap_exists')})

    file_module.run()


def test_scylla_setup_offline(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_setup')
    file_module.argv = ['--io-setup', '1', '--no-raid-setup']

    fs.create_file('/opt/scylla/SCYLLA-OFFLINE-FILE')

    fs.create_dir('/opt/scylla/jmx/scylla-jmx')
    fs.create_file('/opt/scylla/share/cassandra/bin/cqlsh')
    fs.create_file('/opt/scylla/node_exporter/node_exporter')

    file_module.distro('rhel')
    fs.create_file('/etc/sysconfig/scylla-housekeeping')
    fs.create_file('/sys/class/net/eth0')
    fs.create_file('/etc/scylla.d/housekeeping.cfg', contents=dedent("""
        REPO_FILES=/etc/yum.repos.d/scylla*.repo
    """))

    file_module.mock_run({'systemctl --no-pager show user@1000.service': dict(returncode=0, stdout="LimitNOFILE=100000"),
                          'systemctl': dict(returncode=0),
                          'scripts/scylla': dict(returncode=0, stdout=''),
                          'swapon --noheadings --raw': dict(returncode=0, stdout='swap_exists')})
    file_module.run()


def test_scylla_ntp_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_ntp_setup')

    file_module.distro('rhel')
    fs.create_file('/etc/chrony/chrony.conf')

    file_module.mock_run({'yum install': dict(returncode=0),
                          'systemctl': dict(returncode=0),
                          'chronyc -a makestep': dict(returncode=0)})

    file_module.run()


def test_scylla_ntp_setup_subdomain(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_ntp_setup')
    file_module.argv = ['--subdomain', 'test.pool.ntp']

    fs.create_file('/usr/bin/ntpd')
    ntp_conf_mock = fs.create_file('/etc/ntp.conf', contents=dedent( '''
        server		0.us.pool.ntp.org		iburst
        server          1.us.pool.ntp.org               iburst
        server          2.us.pool.ntp.org               iburst
        server          3.us.pool.ntp.org               iburst
    '''))
    file_module.distro('rhel')
    file_module.patch('shutil.which', side_effect=lambda x: x == 'ntpd')

    file_module.mock_run({'yum install': dict(returncode=0),
                          'systemctl': dict(returncode=0, stdout=''),
                          'ntpdate 0.test.pool.ntp.pool.ntp.org': dict(returncode=0)})

    file_module.run()

    assert 'test.pool.ntp' in ntp_conf_mock.contents


def test_scylla_ntp_setup_chronyd(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_ntp_setup')

    file_module.distro('rhel')
    file_module.mock_run({'yum install': dict(returncode=0),
                          'systemctl': dict(returncode=0),
                          'chronyc -a makestep': dict(returncode=0)})
    fs.create_file('/etc/chrony/chrony.conf')

    file_module.run()


def test_scylla_ntp_setup_chronyd_subdomain(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_ntp_setup')
    file_module.argv = ['--subdomain', 'test.pool.ntp']

    file_module.distro('rhel')
    file_module.mock_run({'yum install': dict(returncode=0),
                          'systemctl': dict(returncode=0),
                          'chronyc -a makestep': dict(returncode=0)})

    conf_mock = fs.create_file('/etc/chrony/chrony.conf', contents=dedent('''
        # Use public servers from the pool.ntp.org project.
        # Please consider joining the pool (http://www.pool.ntp.org/join.html).
        pool 2.fedora.pool.ntp.org iburst
    '''))

    file_module.run()

    assert 'test.pool.ntp' in conf_mock.contents


def test_scylla_ntp_setup_timesyncd(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_ntp_setup')

    file_module.distro('rhel')
    fs.create_dir('/lib/systemd/systemd-timesyncd')
    mock_run = file_module.patch('subprocess.run')

    file_module.run()

    assert mock_run.call_args_list[-1].args[0] == 'timedatectl set-ntp true'


def test_scylla_selinux_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_selinux_setup')

    selinux_conf_mock = fs.create_file('/etc/sysconfig/selinux', contents='SELINUX="enabled"')

    file_module.distro('rhel')
    file_module.mock_run({'sestatus': dict(returncode=0, stdout='SELinux status:                 enabled'),
                          'setenforce 0': dict(returncode=0)})
    file_module.run()

    assert 'SELINUX=disabled' in selinux_conf_mock.contents


@pytest.mark.parametrize('distro', argvalues=['rhel', 'debian', 'gentoo', 'suse'])
def test_scylla_core_dump_setup(file_module: FileModule, fs: FakeFilesystem, distro: str):

    file_module.path = str(scripts_path / 'scylla_coredump_setup')
    file_module.argv = ['--dump-to-raiddir', '--compress']
    file_module.distro(distro)

    coredump_conf_mock = fs.create_file('/etc/systemd/system/var-lib-systemd-coredump.mount')
    coredump_mount_mock = fs.create_file('/etc/systemd/coredump.conf')
    timeout_conf_mock = fs.create_file('/etc/systemd/system/systemd-coredump@.service.d/timeout.conf')
    sysctl_coredump_mock = fs.create_file('/etc/sysctl.d/99-scylla-coredump.conf')

    fs.create_file('/path/to/cordump')
    file_module.mock_run({'coredumpctl --no-pager --no-legend info': dict(returncode=0, stdout=dedent("""
        Storage: /path/to/cordump
        """)),
                          'systemctl': dict(returncode=0, stdout=''),
                          'sysctl -p': dict(returncode=0, stdout=''),
                          'apt-get install': dict(returncode=0),
                          'zypper install': dict(returncode=0)})
    with patch('time.sleep'), \
        patch('subprocess.Popen'):
        file_module.run()

    if distro == 'gentoo':
        return
    assert coredump_mount_mock.contents == dedent("""
        [Coredump]
        Storage=external
        Compress=yes
        ProcessSizeMax=1024G
        ExternalSizeMax=1024G
        """).strip()
    assert timeout_conf_mock.contents == dedent("""
        [Service]
        RuntimeMaxSec=infinity
        TimeoutSec=infinity
        """).strip()
    assert sysctl_coredump_mock.contents == dedent("""
        kernel.core_pattern=|/usr/lib/systemd/systemd-coredump %p %u %g %s %t %e"
    """).strip()

    assert coredump_conf_mock.contents == dedent("""
        [Unit]
        Description=Save coredump to scylla data directory
        Conflicts=umount.target
        Before=scylla-server.service
        After=local-fs.target
        DefaultDependencies=no
        
        [Mount]
        What=/var/lib/scylla/coredump
        Where=/var/lib/systemd/coredump
        Type=none
        Options=bind
        
        [Install]
        WantedBy=multi-user.target
    """).strip()


@pytest.mark.parametrize('distro', argvalues=['rhel', 'debian', 'gentoo', 'suse',
                                              pytest.param('amzn',
                                                           marks=pytest.mark.xfail(reason="pkg_install() doesn't support it")),
                                              pytest.param('arch',
                                                           marks=pytest.mark.xfail(reason="pkg_install() doesn't support it"))])
def test_scylla_cpuscaling_setup(file_module: FileModule, fs: FakeFilesystem, distro: str):
    file_module.path = str(scripts_path / 'scylla_cpuscaling_setup')
    file_module.argv = ['--force']
    file_module.distro(distro)

    file_module.mock_run({'apt-get install': dict(returncode=0),
                          'yum install': dict(returncode=0),
                          'zypper install': dict(returncode=0),
                          'systemctl  cat': dict(returncode=1),
                          'systemctl  enable': dict(returncode=0),
                          'systemctl  restart': dict(returncode=0),
                          'systemctl daemon-reload': dict(returncode=0),
                          'emerge -uq sys-power/cpupower': dict(returncode=0)})

    cpufrequtils_mock = fs.create_file('/etc/default/cpufrequtils')
    cpupower_mock = None
    scylla_cpupower_service = None
    if distro == 'rhel':
        cpupower_mock = fs.create_file('/etc/sysconfig/cpupower')
    elif distro == 'suse':
        cpupower_mock = fs.create_file('/etc/sysconfig/scylla-cpupower')
        scylla_cpupower_service = fs.create_file('/etc/systemd/system/scylla-cpupower.service')
    elif distro == 'gentoo':
        cpupower_mock = fs.create_file('/etc/conf.d/cpupower')
    file_module.run()

    # verify the correct data was written to correct file
    if cpupower_mock:
        assert dedent("""
            CPUPOWER_START_OPTS="frequency-set -g performance"
            CPUPOWER_STOP_OPTS="frequency-set -g ondemand"
        """).strip() == cpupower_mock.contents.strip() or \
            dedent("""
                    START_OPTS="-g performance"
                    STOP_OPTS="-g ondemand"
                """).strip() == cpupower_mock.contents.strip()
    if scylla_cpupower_service:
        assert scylla_cpupower_service.contents.strip() == dedent("""
            [Unit]
            Description=Scylla cpupower service
            After=syslog.target
            
            [Service]
            Type=oneshot
            RemainAfterExit=yes
            EnvironmentFile=/etc/sysconfig/scylla-cpupower
            ExecStart=/usr/bin/cpupower $CPUPOWER_START_OPTS
            ExecStop=/usr/bin/cpupower $CPUPOWER_STOP_OPTS
            
            [Install]
            WantedBy=multi-user.target
        """).strip()
    if distro == 'debian':
        assert 'GOVERNOR=performance' in cpufrequtils_mock.contents


def test_scylla_cpuset_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_cpuset_setup')
    file_module.argv = ['--cpuset', '4', '--smp', '3']

    cpuset_conf_mock = fs.create_file('/etc/scylla.d/cpuset.conf')
    file_module.run()

    # verify the correct data was written to correct file
    assert 'CPUSET="--cpuset 4 --smp 3 "' in cpuset_conf_mock.contents


def test_scylla_dev_mode_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_dev_mode_setup')
    file_module.argv = ['--developer-mode', '1']

    dev_mode_mock = fs.create_file('/etc/scylla.d/dev-mode.conf')

    file_module.run()
    # verify the correct data was written to correct file
    assert 'DEV_MODE=--developer-mode=1' in dev_mode_mock.contents


def test_scylla_fstrim(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_fstrim')

    fs.create_file('/etc/scylla/scylla.yaml', contents=dedent("""
    data_file_directories:
      - /var/lib/scylla/data
    """))

    with patch('subprocess.call', autospec=True) as call_mock, \
            patch('os.path.ismount', return_value=True):
        call_mock.return_value = 0
        file_module.run()

        call_mock.assert_has_calls(any_order=True, calls=[
            call(['/opt/scylladb/scripts/scylla-blocktune', '--set-nomerges', '1']),
            call(['fstrim', '/var/lib/scylla/data']),
            call(['fstrim', '/var/lib/scylla/commitlog']),
            call(['/opt/scylladb/scripts/scylla-blocktune', '--set-nomerges', '2'])])


@pytest.mark.parametrize('distro', argvalues=['rhel', 'debian', 'gentoo', 'suse', 'amzn', 'fedora', 'oracle'])
def test_scylla_fstrim_setup(file_module: FileModule, fs: FakeFilesystem, distro: str):
    file_module.path = str(scripts_path / 'scylla_fstrim_setup')
    file_module.distro(distro)
    mock_run = file_module.mock_run({'systemctl ': dict(returncode=0)})
    file_module.run()

    assert 'systemctl  cat scylla-fstrim.timer' in mock_run.call_args_list[0].args[0]
    assert 'systemctl  enable scylla-fstrim.timer' in mock_run.call_args_list[1].args[0]
    assert 'systemctl  cat scylla-fstrim.timer' in mock_run.call_args_list[2].args[0]
    assert 'systemctl  start scylla-fstrim.timer' in mock_run.call_args_list[3].args[0]

    if distro in ('rhel', 'arch', 'suse', 'fedora', 'oracle'):
        assert 'systemctl  cat fstrim.timer' in mock_run.call_args_list[4].args[0]
        assert 'systemctl  disable fstrim.timer' in mock_run.call_args_list[5].args[0]


def test_scylla_io_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_io_setup')

    fs.add_real_file('/proc/cpuinfo')

    fs.create_file('/etc/scylla/scylla.yaml', contents=dedent("""
    data_file_directories:
      - /var/lib/scylla/data
    """))
    fs.create_dir('/var/lib/scylla/data')
    fs.create_dir('/var/lib/scylla/commitlog')
    fs.create_dir('/var/lib/scylla/hints')
    fs.create_dir('/var/lib/scylla/view_hints')
    fs.create_dir('/var/lib/scylla/saved_caches')

    fs.create_file('/etc/scylla.d/cpuset.conf')
    fs.create_file('/etc/scylla.d/dev-mode.conf')
    fs.create_file('/etc/scylla.d/io_properties.yaml')
    fs.create_file('/etc/scylla.d/io.conf')

    with patch('os.statvfs', return_value=mocked_statvfs), \
            patch('subprocess.check_call') as check_call_mock:
        file_module.run()
        assert check_call_mock.call_args_list[0].args[0] == ['/usr/bin/iotune', '--format', 'envfile',
                                                             '--options-file', '/etc/scylla.d/io.conf',
                                                             '--properties-file', '/etc/scylla.d/io_properties.yaml',
                                                             '--evaluation-directory', '/var/lib/scylla/data',
                                                             '--evaluation-directory', '/var/lib/scylla/commitlog',
                                                             '--evaluation-directory', '/var/lib/scylla/hints',
                                                             '--evaluation-directory', '/var/lib/scylla/view_hints',
                                                             '--evaluation-directory', '/var/lib/scylla/saved_caches']


@pytest.mark.parametrize('argv',
                         argvalues=([],
                                    ['--set-nomerges', '2', '--config', '/etc/scylla/scylla.yaml'],
                                    ['--set-nomerges', '2', '--dev', '/dev/sdb'],
                                    ['--set-nomerges', '2', '--filesystem', '/var/lib/scylla/data']),
                         ids=('no_args', 'with_config', 'with_dev', 'with_filesystem'))
def test_scylla_blocktune(file_module: FileModule, fs: FakeFilesystem, argv: list):
    file_module.path = str(scripts_path / 'scylla-blocktune')
    file_module.argv = argv

    fs.create_file('/etc/scylla/scylla.yaml', contents=dedent("""
    data_file_directories:
      - /var/lib/scylla/data
    """))
    fs.create_dir('/var/lib/scylla/data')
    fs.create_dir('/var/lib/scylla/commitlog')
    fs.create_dir('/var/lib/scylla/schema_commitlog')

    fs.create_file('/dev/sdb')
    sched_block = fs.create_file('/sys/dev/block/0:255/queue/scheduler')
    mock_block = fs.create_file('/sys/dev/block/0:255/queue/nomerges')
    fs.create_file('/sys/dev/block/0:255/partition')
    slave1_mock = fs.create_file('/sys/dev/block/0:255/slaves/0:1/queue/nomerges')
    slave2_mock = fs.create_file('/sys/dev/block/0:255/slaves/0:2/queue/nomerges')

    with patch('os.stat', side_effect=mock_stat):
        file_module.run()

    assert mock_block.contents == '2\n'
    assert slave1_mock.contents == '2\n'
    assert slave2_mock.contents == '2\n'
    if not argv:
        assert sched_block.contents == 'noop\n'


def test_scylla_housekeeping(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla-housekeeping')
    file_module.argv = ['-c', '/var/lib/scylla-housekeeping/housekeeping.conf',
                        '--uuid-file', '/var/lib/scylla-housekeeping/housekeeping.uuid',
                        '--repo-files',  '/etc/apt/sources.list.d/scylla*.list',
                        'version', '--mode', 'i']

    fs.create_file('/var/lib/scylla-housekeeping/housekeeping.uuid',
                   contents="e4ed3ea6-85c8-11e9-98aa-18568088a0bd")
    # repo_type, repo_id = get_repo_file(args.repo_files) part isn't working any more with
    # newer version repo files, can it be removed ?
    fs.create_file('/etc/apt/sources.list.d/scylla.list', contents=
                   'deb  [arch=amd64] http://downloads.scylladb.com/downloads/scylla/deb/'
                   'debian-ubuntu/scylladb-4.5 stable main')

    fs.create_file('/var/lib/scylla-housekeeping/housekeeping.conf', contents=dedent("""
        [housekeeping]
        check-version=true
    """))

    def pool__init__(self, *args, **kwargs):
        self._state = ''

    with patch.object(Pool, 'apply_async') as apply_async, \
            patch('multiprocessing.pool.Pool.__init__', new=pool__init__):
        apply_async.return_value.get.side_effect = [
            (0, json.dumps("4.5.0")),
            (0, json.dumps({"version": "4.6.3", "latest_patch_version": "4.6.3"}))
        ]
        file_module.run()

    assert apply_async.call_args_list[0].kwargs.get('args')[0] == \
           'http://localhost:10000/storage_service/scylla_release_version'

    assert apply_async.call_args_list[1].kwargs.get('args')[0] == \
           'https://i6a5h9l1kl.execute-api.us-east-1.amazonaws.com/prod/check_version?' \
           'version=4.5.0&sts=i&uu=e4ed3ea6-85c8-11e9-98aa-18568088a0bd'


@pytest.mark.parametrize('distro', argvalues=['rhel', 'debian', 'gentoo', 'suse', 'fedora', 'oracle',
                                              pytest.param('amzn',
                                                           marks=pytest.mark.xfail(reason="pkg_install() doesn't support it")),
                                              pytest.param('arch',
                                                           marks=pytest.mark.xfail(reason="pkg_install() doesn't support it"))])
def test_scylla_kernel_check(file_module: FileModule, fs: FakeFilesystem, distro: str):
    file_module.path = str(scripts_path / 'scylla_kernel_check')

    file_module.distro(distro)
    fs.create_file('/var/tmp/kernel-check.img')
    file_module.mock_run({'yum install': dict(returncode=0),
                          'apt-get install': dict(returncode=0),
                          'emerge -uq': dict(returncode=0),
                          'zypper install': dict(returncode=0),
                          'dd if=/dev/zero': dict(returncode=0),
                          'mkfs.xfs /var/tmp/kernel-check.img': dict(returncode=0),
                          'mount /var/tmp/kernel-check.img /var/tmp/mnt -o loop': dict(returncode=0),
                          'iotune --fs-check --evaluation-directory /var/tmp/mnt': dict(returncode=0),
                          'umount /var/tmp/mnt': dict(returncode=0)})
    with pytest.raises(SystemExit) as ex:
        file_module.run()

    assert ex.value.args[0] == 0


def test_scylla_logrotate(file_module: FileModule, fs: FakeFilesystem):
    """
    not sure this file is being used by any code (we are not logging into a file anymore)
    """
    file_module.path = str(scripts_path / 'scylla_logrotate')

    fs.create_file('/opt/scylla/scylla-server.log', contents='has some logs in it')

    with patch('pathlib.Path.rename') as rename_mock:
        file_module.run()

    assert 'scylla-server.log.' in str(rename_mock.call_args_list[0].args[0])


def test_scylla_memory_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_memory_setup')
    file_module.argv = ['--lock-memory', '--memory', '4G']

    memory_conf_mock = fs.create_file('/etc/scylla.d/memory.conf')
    file_module.run()
    assert 'MEM_CONF="--lock-memory=1 --memory=4G"' in memory_conf_mock.contents


@pytest.mark.parametrize('network_mode', argvalues=['net', 'virtio', 'dpdk'])
def test_scylla_prepare(file_module: FileModule, fs: FakeFilesystem, network_mode: str):

    fs.add_real_file('/proc/cpuinfo')
    fs.create_file('/etc/sysconfig/scylla-server', contents=dedent(f"""
        AMI=true
        NETWORK_MODE={network_mode}
        IFNAME=
        SET_NIC_AND_DISKS=yes
        SET_CLOCKSOURCE=yes
        DISABLE_WRITEBACK_CACHE=yes
        
        TAP=a
        USER=b
        GROUP=c
        BRIDGE=d
        
        ETHPCIID=
        NR_HUGEPAGES=
    """))

    fs.create_file('/etc/scylla.d/cpuset.conf', contents=dedent("""
        CPUSET=2,4-5,9,12
    """))

    fs.create_file('/etc/scylla/scylla.yaml', contents=dedent("""
        data_file_directories:
          - /var/lib/scylla/data
    """))

    file_module.path = str(scripts_path / 'scylla_prepare')
    file_module.mock_run({'--mode mq --get-cpu-mask-quiet': dict(returncode=0, stdout='0x0001'),
                          '--mode sq --get-cpu-mask-quiet': dict(returncode=0, stdout='0x0004'),
                          '--mode sq_split --get-cpu-mask-quiet': dict(returncode=0, stdout='0x1234'),
                          'perftune.py': dict(returncode=0, stdout='INSERT YAML HERE'),
                          'ip tuntap': dict(returncode=0),
                          'ip link': dict(returncode=0),
                          'chown b.c /dev/vhost-net': dict(returncode=0),
                          'modprobe uio': dict(returncode=0),
                          '/opt/scylladb/scripts/dpdk-devbind.py': dict(returncode=0),
                          'hugeadm --create-mounts': dict(returncode=0),
                          '/opt/scylladb/bin/hwloc-calc --pi all': dict(returncode=0, stdout='0xffff,0xfff1')})

    file_module.patch('distro.name', return_value='Ubuntu')
    file_module.run()
    if network_mode == 'net':
        assert fs.get_object('/etc/scylla.d/perftune.yaml').contents == 'INSERT YAML HERE'


@pytest.mark.parametrize('distro', argvalues=['rhel', 'debian', 'gentoo', 'suse', 'fedora', 'oracle'])
def test_scylla_raid_setup(file_module: FileModule, fs: FakeFilesystem, distro: str):

    fs.create_dir('/etc/systemd/system/scylla-server.service.d')
    fs.create_file('/sys/dev/block/0:255/queue/logical_block_size', contents="2048")
    fs.create_file('/dev/sdb')
    fs.create_dir('/sys/class/block/sdb')
    fs.create_file('/sys/block/sdb/queue/discard_granularity', contents='1')
    if distro == 'debian':
        mdadm_conf_mock = fs.create_file('/etc/mdadm/mdadm.conf')
    else:
        mdadm_conf_mock = fs.create_file('/etc/mdadm.conf')

    file_module.path = str(scripts_path / 'scylla_raid_setup')
    file_module.argv = ['--disks', '/dev/sdb', '--force-raid', '--online-discard', '1', '--raid-level', '1']
    file_module.distro(distro)

    file_module.mock_run({
        'yum install': dict(returncode=0),
        'apt-get install': dict(returncode=0),
        'emerge -uq': dict(returncode=0),
        'zypper install': dict(returncode=0),
        'systemd-escape -p --suffix=mount /var/lib/scylla': dict(returncode=0, stdout='var-lib-scylla.mount'),
        'lsblk -n -oPARTTYPE /dev/sdb': dict(returncode=0, stdout=''),
        'udevadm settle': dict(returncode=0),
        'mdadm': dict(returncode=0, stdout=''),
        'mkfs.xfs': dict(returncode=0),
        'blkid': dict(returncode=0, stdout='uuid'),
        'systemctl': dict(returncode=0),
        'update-initramfs -u': dict(returncode=0),
        'wipefs -a': dict(returncode=0)})

    with patch('os.stat', new=mock_stat),\
            patch('os.open'), \
            patch('os.close'), \
            patch('subprocess.Popen'), \
            patch('stat.S_ISBLK'), \
            patch('pwd.getpwnam', return_value=struct_passwd(("scylla", "", 125, 125, "", "", ""))),\
            patch('grp.getgrnam', return_value=struct_group(("scylla", "", 125, 0))):
        file_module.run()

    assert mdadm_conf_mock.contents == '\nMAILADDR root'
    assert fs.get_object('/etc/systemd/system/var-lib-scylla.mount').contents.strip() == dedent("""
        [Unit]
        Description=Scylla data directory
        Before=scylla-server.service
        After=local-fs.target mdmonitor.service
        Wants=mdmonitor.service
        DefaultDependencies=no
        
        [Mount]
        What=/dev/disk/by-uuid/uuid
        Where=/var/lib/scylla
        Type=xfs
        Options=noatime,discard
        
        [Install]
        WantedBy=multi-user.target
    """).strip()

    assert fs.get_object('/etc/systemd/system/scylla-server.service.d/mounts.conf').contents.strip() == dedent("""
        [Unit]
        RequiresMountsFor=/var/lib/scylla
        """).strip()


def test_scylla_rsyslog_setup(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_rsyslog_setup')
    file_module.argv = ['--remote-server', '127.0.0.1:5000']

    rsyslog_conf = fs.create_file('/etc/rsyslog.d/scylla.conf')
    file_module.mock_run({'systemctl  restart': dict(returncode=0), 'systemctl  cat': dict(returncode=0)})
    file_module.run()

    assert rsyslog_conf.contents == \
           "if $programname ==  'scylla' then @@127.0.0.1:5000;RSYSLOG_SyslogProtocol23Format\n"


def test_scylla_config_get(file_module: FileModule, fs: FakeFilesystem):
    file_module.path = str(scripts_path / 'scylla_config_get.py')
    file_module.argv = ['--get', 'commitlog_directory']

    fs.create_file('/etc/scylla/scylla.yaml', contents=dedent("""
    commitlog_directory: /var/lib/scylla/data/commitlog
    """))

    file_module.run()
