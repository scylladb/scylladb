import os
import sys
import subprocess
import typing
import types
import builtins
from functools import partial
from pathlib import Path
from importlib.machinery import SourceFileLoader
from unittest.mock import patch, call, mock_open
from textwrap import dedent
from dataclasses import dataclass

import pytest

scripts_path = Path(__file__).parent.parent.parent / 'dist/common/scripts'
sys.path.insert(1, str(scripts_path))


def exists_mock(p, expected: typing.Union[list, str]):
    expected = expected if isinstance(expected, list) else [expected]
    print(expected, p)
    if str(p) in expected:
        return p
    else:
        return False


def which_mock(expected: typing.Union[list, str], p):
    expected = expected if isinstance(expected, list) else [expected]
    print(expected, p)
    if p in expected:
        return p
    else:
        return None


def run_mock(*args, **kwargs):
    print(args, kwargs)
    ret = subprocess.CompletedProcess(args=args, returncode=0, stdout='', stderr='')
    if args[0] == 'scylla --version':
        ret.stdout = '4.5.1'
    if args[0] == 'swapon --noheadings --raw':
        ret.stdout = 'swap_exists'
    if args[0] == 'sestatus':
        ret.stdout = 'SELinux status:                 enabled'
    if '--get-cpu-mask-quiet' in args[0]:
        ret.stdout = '0x1234'
    elif 'perftune.py' in args[0]:
        ret.stdout = 'INSERT YAML HERE'
    if 'systemd-escape -p --suffix=mount /var/lib/scylla' == args[0]:
        ret.stdout = 'var-lib-scylla.mount'
    if 'coredumpctl --no-pager --no-legend info' in args[0]:
        ret.stdout = dedent("""
        Storage: /path/to/cordump
        """)
    return ret

original_open = builtins.open


def _open(file, mode='r', expected: typing.List[typing.Tuple[str, 'MagicMock']]=None, *args, **kwargs):
    if 'site-packages' in str(file) or 'LC_MESSAGES' in str(file):  # python packages reading their internal info, don't mess with that
        return original_open(file, mode=mode)
    print(file)
    for name, mock_file in expected:
        if name in str(file):
            return mock_file(file)
    raise ValueError(f'unexpected {file}')

# third number only to get a specific device path
mocked_stat = os.stat_result([0, 0, 1000, 0, 0, 0, 10, 0.0, 0.0, 0.0])
# 2nd and 5th numbers to simulate having enough disk space for io_setup
mocked_statvfs = os.statvfs_result([0, 10000000000, 0, 0, 10000000000, 0, 0, 0.0, 0.0, 0.0])


@dataclass
class MockStatResult:
    st_rdev: int = 255
    st_mode: int = 0
    st_dev: int = 0
orignal_stat = os.stat


def mock_stat(p):
    if p in ['/dev/md0', '/dev/sdb', '/var/lib/scylla/data', '/var/lib/scylla/commitlog']:
        return MockStatResult()
    return orignal_stat(p)


def run_module_as_main(module_filename: str, argv: typing.List[str] = None) -> None:
    argv = argv if argv else []
    argv.insert(0, module_filename)
    with patch("sys.argv", new=argv), \
             patch('os.getuid', return_value=0):
        loader = SourceFileLoader('__main__', module_filename)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)


def test_scylla_setup():
    scylla_housekeeping_mock = mock_open(read_data=dedent("""
        REPO_FILES=/etc/yum.repos.d/scylla*.repo
    """))
    f = str(scripts_path / 'scylla_setup')
    argv = ['--io-setup', '1', '--no-raid-setup']
    with patch('os.path.exists', new=partial(exists_mock, expected=['/sys/class/net/eth0',
                                                                    '/etc/sysconfig/scylla-housekeeping'])), \
            patch('subprocess.run', new=run_mock), \
            patch('builtins.open', new=partial(_open, expected=[
                ('/etc/sysconfig/scylla-housekeeping', scylla_housekeeping_mock),
                ('/etc/scylla.d/housekeeping.cfg', scylla_housekeeping_mock)])),\
            patch('distro.like', return_value='rhel'),\
            patch('os.chmod'):
        run_module_as_main(f, argv=argv)


def test_scylla_ntp_setup():
    f = str(scripts_path / 'scylla_ntp_setup')
    with patch('subprocess.run', new=run_mock):
        run_module_as_main(f)


def test_scylla_ntp_setup_subdomain():
    ntp_conf = '''
server		0.us.pool.ntp.org		iburst
server          1.us.pool.ntp.org               iburst
server          2.us.pool.ntp.org               iburst
server          3.us.pool.ntp.org               iburst
        '''
    ntp_conf_mock = mock_open(read_data=ntp_conf)

    f = str(scripts_path / 'scylla_ntp_setup')
    argv = ['--subdomain', 'test.pool.ntp']
    with patch('os.path.exists', new=lambda _: False), \
            patch('shutil.which', new=partial(which_mock, 'ntpd')), \
            patch('pathlib.Path.exists', new=lambda p: str(p) == '/etc/ntp.conf'), \
            patch('pathlib.Path.open', new_callable=lambda: ntp_conf_mock), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f, argv=argv)

    assert ntp_conf_mock.return_value.write.call_count == 1, "write to ntp.conf didn't happen"
    assert 'test.pool.ntp' in ntp_conf_mock.return_value.write.call_args.args[0]


def test_scylla_ntp_setup_chronyd():
    f = str(scripts_path / 'scylla_ntp_setup')
    with patch('os.path.exists', new=lambda _: False), \
            patch('shutil.which', new=partial(which_mock, 'chronyd')), \
            patch('pathlib.Path.exists', new=lambda _: True), \
            patch('pathlib.Path.open', new_callable=lambda:  mock_open()), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f)


def test_scylla_ntp_setup_chronyd_subdomain():
    chrony_conf = '''
# Use public servers from the pool.ntp.org project.
# Please consider joining the pool (http://www.pool.ntp.org/join.html).
pool 2.fedora.pool.ntp.org iburst
    '''
    conf_mock = mock_open(read_data=chrony_conf)
    f = str(scripts_path / 'scylla_ntp_setup')
    argv = ['--subdomain', 'test.pool.ntp']
    with patch('os.path.exists', new=lambda _: False), \
            patch('shutil.which', new=partial(which_mock, 'chronyd')), \
            patch('pathlib.Path.exists', new=lambda p: str(p) == '/etc/chrony/chrony.conf'), \
            patch('pathlib.Path.open', new_callable=lambda: conf_mock), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f, argv=argv)
    assert conf_mock.return_value.write.call_count == 1, "write to chrony.conf didn't happen"
    assert 'test.pool.ntp' in conf_mock.return_value.write.call_args.args[0]


def test_scylla_ntp_setup_timesyncd():
    f = str(scripts_path / 'scylla_ntp_setup')
    with patch('os.path.exists', new=partial(exists_mock, expected='/lib/systemd/systemd-timesyncd')), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f)


def test_scylla_selinux_setup():
    f = str(scripts_path / 'scylla_selinux_setup')
    selinux_conf = 'SELINUX="enabled"'
    selinux_conf_mock = mock_open(read_data=selinux_conf)

    with patch('distro.like', return_value='rhel'), \
            patch('scylla_util.open', new_callable=lambda: selinux_conf_mock), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f)

    assert 'SELINUX="disabled"' in selinux_conf_mock.return_value.write.call_args.args[0]


def test_scylla_core_dump_setup():

    for distro in ['rhel', 'debian', 'gentoo', 'suse']:
        f = str(scripts_path / 'scylla_coredump_setup')
        argv = ['--dump-to-raiddir', '--compress']

        coredump_conf_mock = mock_open()
        coredump_mount_mock = mock_open()
        timeout_conf_mock = mock_open()

        with patch('os.makedirs'), \
                patch('distro.like', return_value=distro), \
                patch('distro.id', return_value=distro), \
                patch('time.sleep'), \
                patch('os.remove'), \
                patch('builtins.open', side_effect=partial(_open, expected=[
                    ('/etc/systemd/system/systemd-coredump@.service.d/timeout.conf', timeout_conf_mock),
                    ('/etc/systemd/coredump.conf', coredump_mount_mock),
                    ('/etc/systemd/system/var-lib-systemd-coredump.mount', coredump_conf_mock)])), \
                patch('subprocess.run', new=run_mock), \
                patch('scylla_util.run', new=run_mock):
            run_module_as_main(f, argv=argv)

        if distro == 'gentoo':
            continue  # on gentoo, there no systemd-cordump configuration

        assert coredump_mount_mock.return_value.write.call_args.args[0] == dedent("""
            [Coredump]
            Storage=external
            Compress=yes
            ProcessSizeMax=1024G
            ExternalSizeMax=1024G
            """).strip()
        assert timeout_conf_mock.return_value.write.call_args.args[0] == dedent("""
            [Service]
            TimeoutStartSec=infinity
            """).strip()
        assert coredump_mount_mock.return_value.write.call_args.args[0] == dedent("""
            [Coredump]
            Storage=external
            Compress=yes
            ProcessSizeMax=1024G
            ExternalSizeMax=1024G
        """).strip()


def test_scylla_cpuscaling_setup():
    f = str(scripts_path / 'scylla_cpuscaling_setup')
    file_mock = mock_open()
    argv = ['--force']

    with patch('os.makedirs'), \
            patch('distro.like', return_value='debian'), \
            patch('scylla_util.open', new_callable=lambda: file_mock), \
            patch('subprocess.run', new=run_mock), \
            patch('scylla_util.run', new=run_mock):
        run_module_as_main(f, argv=argv)

    # verify the correct data was written to correct file
    assert '/etc/default/cpufrequtils' == file_mock.call_args_list[0].args[0]
    assert 'GOVERNOR="performance"' in file_mock.return_value.write.call_args.args[0]


def test_scylla_cpuset_setup():
    f = str(scripts_path / 'scylla_cpuset_setup')
    file_mock = mock_open()
    argv = ['--cpuset', '4', '--smp', '3']

    with patch('os.makedirs'), \
            patch('scylla_util.open', new_callable=lambda: file_mock), \
            patch('subprocess.run', new=run_mock), \
            patch('scylla_util.run', new=run_mock):

        run_module_as_main(f, argv=argv)

    # verify the correct data was written to correct file
    assert '/etc/scylla.d/cpuset.conf' == file_mock.call_args_list[0].args[0]
    assert 'CPUSET="--cpuset 4 --smp 3 "' in file_mock.return_value.write.call_args.args[0]


def test_scylla_dev_mode_setup():
    f = str(scripts_path / 'scylla_dev_mode_setup')
    file_mock = mock_open()
    argv = ['--developer-mode', '1']

    with patch('os.makedirs'), \
            patch('scylla_util.open', new_callable=lambda: file_mock), \
            patch('subprocess.run', new=run_mock), \
            patch('scylla_util.run', new=run_mock):

        run_module_as_main(f, argv=argv)

    # verify the correct data was written to correct file
    assert '/etc/scylla.d/dev-mode.conf' == file_mock.call_args_list[0].args[0]
    assert 'DEV_MODE="--developer-mode=1"' in file_mock.return_value.write.call_args.args[0]


def test_scylla_fstrim():
    f = str(scripts_path / 'scylla_fstrim')
    file_mock = mock_open(read_data=dedent("""
    data_file_directories:
      - /var/lib/scylla/data
    """))

    with patch('subprocess.call', autospec=True) as call_mock, \
            patch('os.path.ismount', return_value=True), \
            patch('scylla_util.open', new_callable=lambda: file_mock), \
            patch('subprocess.run', new=run_mock), \
            patch('scylla_util.run', new=run_mock):
        call_mock.return_value = 0
        run_module_as_main(f)

        call_mock.assert_has_calls(any_order=True, calls=[
            call(['/opt/scylladb/scripts/scylla-blocktune', '--set-nomerges', '1']),
            call(['fstrim', '/var/lib/scylla/data']),
            call(['fstrim', '/var/lib/scylla/commitlog']),
            call(['/opt/scylladb/scripts/scylla-blocktune', '--set-nomerges', '2'])])


def test_scylla_fstrim_setup():
    f = str(scripts_path / 'scylla_fstrim_setup')
    with patch('distro.like', return_value='rhel'), \
            patch('scylla_util.run', new=run_mock):
        run_module_as_main(f)


def test_scylla_io_setup():
    f = str(scripts_path / 'scylla_io_setup')
    scylla_yaml_mock = mock_open(read_data=dedent("""
    data_file_directories:
      - /var/lib/scylla/data
    """))

    with patch('os.path.exists', return_value=True), \
            patch('os.path.isdir', return_value=True), \
            patch('os.listdir'), \
            patch('os.statvfs', return_value=mocked_statvfs), \
            patch('os.stat', return_value=mocked_stat), \
            patch('builtins.open', side_effect=partial(_open, expected=[('/proc/cpuinfo', original_open),
                                                                        ('/etc/scylla.d/cpuset.conf', mock_open()),
                                                                        ('/etc/scylla.d/dev-mode.conf', mock_open()),
                                                                        ('/etc/scylla/scylla.yaml', scylla_yaml_mock)])), \
            patch('scylla_blocktune.open', side_effect=partial(_open, expected=[('', mock_open())])), \
            patch('scylla_util.run', new=run_mock), \
            patch('subprocess.check_call') as check_call_mock, \
            patch('os.chmod'):
        run_module_as_main(f)
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
                                    ['--set-nomerges', '0', '--config', '/etc/scylla/scylla.yaml'],
                                    ['--set-nomerges', '0', '--dev', '/dev/sdb'],
                                    ['--set-nomerges', '0', '--filesystem', '/var/lib/scylla/data']),
                         ids=('no_args', 'with_config', 'with_dev', 'with_filesystem'))
def test_scylla_blocktune(argv):
    f = str(scripts_path / 'scylla-blocktune')

    file_mock = mock_open(read_data="""
api_address: 127.0.0.1
    """)

    with patch('os.makedirs'), \
            patch('os.listdir'), \
            patch('scylla_util.open', new_callable=lambda: file_mock), \
            patch('scylla_blocktune.open', new_callable=lambda: file_mock), \
            patch('subprocess.run', new=run_mock), \
            patch('os.stat', return_value=mocked_stat),\
            patch('scylla_util.run', new=run_mock), \
            patch('os.stat', new=mock_stat):
        run_module_as_main(f, argv=argv)

    if '--dev' not in argv and '--filesystem' not in argv:
        # verify the correct data was written to correct file
        assert '/etc/scylla/scylla.yaml' == file_mock.call_args_list[0].args[0]


def test_scylla_housekeeping():
    f = str(scripts_path / 'scylla-housekeeping')
    argv = ['--uuid-file', '/var/lib/scylla-housekeeping/housekeeping.uuid',
            '--repo-files',  '/etc/apt/sources.list.d/scylla*.list',
            'version', '--version', '4.5.0', '--mode', 'i']
    uuid_file_mock = mock_open(read_data="""e4ed3ea6-85c8-11e9-98aa-18568088a0bd""")

    # repo_type, repo_id = get_repo_file(args.repo_files) part isn't working any more with
    # newer version repo files, can it be removed ?
    list_file_mock = mock_open(read_data=
        'deb  [arch=amd64] http://downloads.scylladb.com/downloads/scylla/deb/debian-ubuntu/scylladb-4.5 stable main')
    from multiprocessing.pool import Pool
    import json
    with patch('subprocess.run', new=run_mock), \
            patch('os.chmod'), \
            patch('os.path.getmtime'),\
            patch('glob.glob', side_effect=lambda _: ['/etc/apt/sources.list.d/scylla.list']), \
            patch('scylla_util.run', new=run_mock), \
            patch.object(Pool, 'apply_async') as apply_async, \
            patch('builtins.open', side_effect=partial(_open, expected=[
                ('/var/lib/scylla-housekeeping/housekeeping.uuid', uuid_file_mock),
                ('/etc/apt/sources.list.d/scylla.list', list_file_mock)])):
        apply_async.return_value.get.return_value = (0, json.dumps({"version": "4.6.3", "latest_patch_version": "4.6.3"}))
        run_module_as_main(f, argv=argv)

    assert apply_async.call_args_list[0].kwargs.get('args')[0] == 'https://i6a5h9l1kl.execute-api.us-east-1.amazonaws.com/prod/check_version?version=4.5.0&sts=i&uu=e4ed3ea6-85c8-11e9-98aa-18568088a0bd'


def test_scylla_kernel_check():
    f = str(scripts_path / 'scylla_kernel_check')
    with pytest.raises(SystemExit) as ex, \
            patch('os.remove'), \
            patch('shutil.rmtree'), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f)

    assert ex.value.args[0] == 0


def test_scylla_logrotate():
    """
    not sure this file is being used by any code (we are not logging into a file anymore)
    """
    f = str(scripts_path / 'scylla_logrotate')
    with patch('pathlib.Path.exists', new=lambda _: True), \
            patch('pathlib.Path.stat', new=lambda _: mocked_stat),\
            patch('pathlib.Path.rename') as rename_mock:
        run_module_as_main(f)

    assert 'scylla-server.log.' in str(rename_mock.call_args_list[0].args[0])


def test_scylla_memory_setup():

    f = str(scripts_path / 'scylla_memory_setup')
    argv = ['--lock-memory', '--memory', '4G']

    file_mock = mock_open()
    with patch('builtins.open', new_callable=lambda: file_mock):
        run_module_as_main(f, argv=argv)

    assert '/etc/scylla.d/memory.conf' == file_mock.call_args_list[0].args[0]
    assert 'MEM_CONF="--lock-memory=1 --memory=4G"' in file_mock.return_value.write.call_args.args[0]


def test_scylla_prepare():
    sysconfig_scylla_server_mock = mock_open(read_data=dedent("""
    AMI=true
    NETWORK_MODE=net
    SET_NIC_AND_DISKS=yes
    IFNAME=eth0
    """))

    cpuset_conf_mock = mock_open(read_data=dedent("""
    CPUSET=2,4-5,9,12
    """))

    scylla_yaml_mock = mock_open(read_data=dedent("""
    data_file_directories:
      - /var/lib/scylla/data
    """))

    preftune_yaml_mock = mock_open()
    f = str(scripts_path / 'scylla_prepare')
    with patch('builtins.open', side_effect=partial(_open, expected=[
        ('/proc/cpuinfo', original_open),
        ('/etc/sysconfig/scylla-server', sysconfig_scylla_server_mock),
        ('/etc/scylla.d/cpuset.conf', cpuset_conf_mock),
        ('/etc/scylla.d/perftune.yaml', preftune_yaml_mock),
        ('/etc/scylla/scylla.yaml', scylla_yaml_mock)])), \
            patch('subprocess.run', new=run_mock), \
            patch('os.chmod'), \
            patch('os.path.exists', new=partial(exists_mock, expected='/etc/scylla.d/cpuset.conf')):
        run_module_as_main(f)

    assert preftune_yaml_mock.return_value.write.call_args_list[0].args[0] == 'INSERT YAML HERE'


def test_scylla_raid_setup():
    mdadm_conf_mock = mock_open()

    f = str(scripts_path / 'scylla_raid_setup')
    argv = ['--disks', '/dev/sdb', '--force-raid', '--online-discard', '1']

    with patch('builtins.open', side_effect=partial(_open, expected=[
        ('/sys/dev/block/0:255/queue/logical_block_size', mock_open(read_data="2048")),
        ('/etc/systemd/system/var-lib-scylla.mount', mock_open()),
        ('/etc/systemd/system/scylla-server.service.d/mounts.conf', mock_open()),
        ('/etc/mdadm.conf', mdadm_conf_mock)])),\
            patch('distro.like', return_value='rhel'), \
            patch('os.path.exists', new=partial(exists_mock, expected='/dev/sdb')), \
            patch('os.open'), \
            patch('os.stat', new=mock_stat),\
            patch('stat.S_ISBLK'), \
            patch('os.path.isdir'), \
            patch('subprocess.run', new=run_mock),\
            patch('os.chown'),\
            patch('pwd.getpwnam'),\
            patch('grp.getgrnam'):
        run_module_as_main(f, argv=argv)

    assert mdadm_conf_mock.return_value.write.call_args.args[0] == '\nMAILADDR root'


def test_scylla_rsyslog_setup():
    f = str(scripts_path / 'scylla_rsyslog_setup')
    argv = ['--remote-server', '127.0.0.1:5000']

    rsyslog_conf = mock_open()
    with patch('builtins.open', side_effect=partial(_open, expected=[('/etc/rsyslog.d/scylla.conf', rsyslog_conf)])), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f, argv=argv)
    assert rsyslog_conf.return_value.write.call_args.args[0] == \
           "if $programname ==  'scylla' then @@127.0.0.1:5000;RSYSLOG_SyslogProtocol23Format\n"


def test_scylla_config_get():
    f = str(scripts_path / 'scylla_config_get.py')
    argv = ['--get', 'commitlog_directory']
    scylla_yaml_conf = mock_open(read_data=dedent("""
    commitlog_directory: /var/lib/scylla/data/commitlog
    """))
    with patch('builtins.open', side_effect=partial(_open, expected=[('/etc/scylla/scylla.yaml', scylla_yaml_conf)])), \
            patch('subprocess.run', new=run_mock):
        run_module_as_main(f, argv=argv)