#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest

from test.dist_test.conftest import SCRIPTS_DIR

# The shipped default, read before pyfakefs hides the real filesystem.
SYSCONFIG = (SCRIPTS_DIR.parent / 'sysconfig' / 'scylla-server').read_text()
NUMA = '/sys/devices/system/cpu/cpu0/topology/core_cpus'


def sysconfig(**overrides):
    lines = SYSCONFIG.splitlines()
    for key, val in overrides.items():
        lines = [f'{key}={val}' if l.startswith(f'{key}=') else l for l in lines]
        if val is None:
            lines = [l for l in lines if not l.startswith(f'{key}=')]
    return '\n'.join(lines) + '\n'


@pytest.fixture
def prepare(scripts, monkeypatch):
    monkeypatch.setattr('platform.machine', lambda: 'x86_64')
    scripts.file('/proc/cpuinfo', 'processor\t: 0\nflags\t\t: fpu sse4_2 pclmulqdq\n')
    scripts.file('/etc/scylla/scylla.yaml', 'data_file_directories: [/var/lib/scylla/data]\n')
    scripts.file(NUMA)
    return scripts


def test_prepare_missing_cpu_flags(prepare, capsys):
    prepare.fs.remove('/proc/cpuinfo')
    prepare.file('/proc/cpuinfo', 'flags\t\t: fpu sse4_2\n')
    assert prepare.run('scylla_prepare') == 1
    assert 'lacks the following features: pclmulqdq' in capsys.readouterr().out


def test_prepare_requires_root(prepare):
    assert prepare.run('scylla_prepare', uid=1000) == 1


def test_prepare_virtio(prepare):
    prepare.file('/etc/sysconfig/scylla-server', sysconfig(NETWORK_MODE='virtio'))
    assert prepare.run('scylla_prepare') == 0
    assert prepare.cmd.calls == ['ip tuntap del mode tap dev tap0',
                                 'ip tuntap add mode tap dev tap0 user scylla one_queue vnet_hdr',
                                 'ip link set dev tap0 up',
                                 'ip link set dev tap0 master virbr0',
                                 'chown scylla.scylla /dev/vhost-net']


@pytest.mark.parametrize('distro_name', ['Ubuntu', 'CentOS'])
def test_prepare_dpdk(prepare, distro_name):
    prepare.distro(distro_name)
    prepare.file('/etc/sysconfig/scylla-server', sysconfig(NETWORK_MODE='dpdk', ETHPCIID='0000:00:03.0'))
    hugepages = prepare.file('/sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages')
    assert prepare.run('scylla_prepare') == 0
    assert '/opt/scylladb/scripts/dpdk-devbind.py --force --bind=uio_pci_generic 0000:00:03.0' in prepare.cmd
    assert hugepages.contents == '64'
    assert ('hugeadm --create-mounts' in prepare.cmd) == (distro_name == 'Ubuntu')


PERFTUNE_RUN = ('/opt/scylladb/scripts/perftune.py --tune disks --dir /var/lib/scylla/data '
                '--dir /var/lib/scylla/commitlog --dir /var/lib/scylla/schema_commitlog '
                '--dir /var/lib/scylla/hints --dir /var/lib/scylla/view_hints '
                '--options-file /etc/scylla.d/perftune.yaml')


@pytest.mark.parametrize('irq_mask,expected_mask', [('0x0000fff0', '0x0000fff0'), ('0x0,,0x0', '0x0000ffff')])
def test_prepare_posix_creates_perftune_yaml(prepare, irq_mask, expected_mask):
    prepare.file('/etc/sysconfig/scylla-server',
                 sysconfig(IFNAME='', SET_NIC_AND_DISKS='yes', SET_CLOCKSOURCE='yes', DISABLE_WRITEBACK_CACHE='yes'))
    prepare.file('/etc/scylla.d/cpuset.conf', 'CPUSET="--cpuset 0,2-3 "\n')
    prepare.cmd.set('hwloc-calc --pi all ~PU:0 ~PU:2-3', irq_mask)
    prepare.cmd.set('hwloc-calc all', '0x0000ffff')
    prepare.cmd.set('--dump-options-file', 'tune: [net]')
    assert prepare.run('scylla_prepare') == 0
    assert (f'/opt/scylladb/scripts/perftune.py --tune net --nic "eth0" --irq-cpu-mask "{expected_mask}" '
            '--tune system --tune-clock --write-back-cache=false --dump-options-file') in prepare.cmd
    assert prepare.read('/etc/scylla.d/perftune.yaml') == 'tune: [net]'
    assert prepare.cmd.calls[-1] == PERFTUNE_RUN


def test_prepare_posix_existing_perftune_yaml(prepare):
    prepare.file('/etc/sysconfig/scylla-server', sysconfig(SET_CLOCKSOURCE='yes'))
    prepare.file('/etc/scylla.d/perftune.yaml', 'mine')
    assert prepare.run('scylla_prepare') == 0
    assert prepare.cmd.calls == [PERFTUNE_RUN]
    assert prepare.read('/etc/scylla.d/perftune.yaml') == 'mine'


def test_prepare_posix_nothing_to_tune(prepare):
    prepare.file('/etc/sysconfig/scylla-server', sysconfig())
    assert prepare.run('scylla_prepare') == 0
    assert prepare.cmd.calls == []


@pytest.mark.parametrize('overrides', [dict(SET_NIC_AND_DISKS='yes'), dict(SET_CLOCKSOURCE='yes')])
def test_prepare_posix_corrupted_numa(prepare, capsys, overrides):
    prepare.fs.remove(NUMA)
    prepare.file('/etc/sysconfig/scylla-server', sysconfig(**overrides))
    assert prepare.run('scylla_prepare') == 0
    assert 'NUMA topology information is corrupted' in capsys.readouterr().out
    assert prepare.cmd.calls == []


def test_prepare_posix_missing_cpuset_conf(prepare, capsys):
    prepare.file('/etc/sysconfig/scylla-server', sysconfig(SET_NIC_AND_DISKS='yes'))
    assert prepare.run('scylla_prepare') == 1
    assert 'please re-run scylla_setup' in capsys.readouterr().out


def test_prepare_posix_perftune_fails(prepare, capsys):
    prepare.file('/etc/sysconfig/scylla-server', sysconfig(SET_CLOCKSOURCE='yes'))
    prepare.file('/etc/scylla.d/perftune.yaml')
    prepare.cmd.set('--options-file /etc/scylla.d/perftune.yaml', returncode=1)
    assert prepare.run('scylla_prepare') == 1
    assert 'while tuning system using perftune.yaml' in capsys.readouterr().out


@pytest.fixture
def sysconfig_setup(scripts):
    scripts.file('/etc/scylla/scylla.yaml', 'data_file_directories: [/var/lib/scylla/data]\n')
    scripts.file(NUMA)
    return scripts


def test_sysconfig_setup_requires_root(sysconfig_setup):
    assert sysconfig_setup.run('scylla_sysconfig_setup', uid=1000) == 1


def test_sysconfig_setup_no_change(sysconfig_setup):
    sysconfig_setup.file('/etc/sysconfig/scylla-server', SYSCONFIG)
    assert sysconfig_setup.run('scylla_sysconfig_setup') == 0
    assert sysconfig_setup.read('/etc/sysconfig/scylla-server') == SYSCONFIG


def test_sysconfig_setup_all_options(sysconfig_setup, capsys):
    sysconfig_setup.file('/etc/sysconfig/scylla-server', SYSCONFIG)
    sysconfig_setup.file('/sys/class/net/eth1')
    sysconfig_setup.cmd.set('--get-cpu-mask', 'WARNING: some perftune warning\n0x00000006')
    assert sysconfig_setup.run('scylla_sysconfig_setup', '--nic', 'eth1', '--mode', 'posix',
                               '--user', 'u', '--group', 'g', '--homedir', '/h', '--confdir', '/c',
                               '--setup-nic-and-disks', '--set-clocksource', '--disable-writeback-cache') == 0
    assert 'WARNING: some perftune warning' in capsys.readouterr().out
    assert '/opt/scylladb/scripts/scylla_cpuset_setup --cpuset 1-2' in sysconfig_setup.cmd
    assert sysconfig_setup.read('/etc/sysconfig/scylla-server') == sysconfig(
        IFNAME='eth1', USER='u', GROUP='g', SCYLLA_HOME='/h', SCYLLA_CONF='/c',
        SET_NIC_AND_DISKS='yes', SET_CLOCKSOURCE='yes', DISABLE_WRITEBACK_CACHE='yes')


def test_sysconfig_setup_bad_cpu_mask(sysconfig_setup):
    sysconfig_setup.file('/etc/sysconfig/scylla-server', SYSCONFIG)
    sysconfig_setup.cmd.set('--get-cpu-mask', 'garbage')
    with pytest.raises(Exception, match='Failed to retrieve CPU mask: garbage'):
        sysconfig_setup.run('scylla_sysconfig_setup', '--setup-nic-and-disks')


def test_sysconfig_setup_invalid_nic(sysconfig_setup, capsys):
    sysconfig_setup.file('/etc/sysconfig/scylla-server', SYSCONFIG)
    assert sysconfig_setup.run('scylla_sysconfig_setup', '--nic', 'eth9') == 1
    assert 'NIC eth9 not found.' in capsys.readouterr().out


@pytest.mark.parametrize('arg', ['--setup-nic-and-disks', '--set-clocksource'])
def test_sysconfig_setup_corrupted_numa(sysconfig_setup, arg):
    sysconfig_setup.fs.remove(NUMA)
    sysconfig_setup.file('/etc/sysconfig/scylla-server', SYSCONFIG)
    assert sysconfig_setup.run('scylla_sysconfig_setup', arg) == 3


def test_sysconfig_setup_dpdk(sysconfig_setup):
    sysconfig_setup.file('/etc/sysconfig/scylla-server', SYSCONFIG)
    sysconfig_setup.cmd.set('dpdk-devbind.py --status',
                            '0000:00:03.0 \'Virtio network device\' if=eth0 drv=virtio-pci unused=\n')
    assert sysconfig_setup.run('scylla_sysconfig_setup', '--mode', 'dpdk') == 0
    assert sysconfig_setup.read('/etc/sysconfig/scylla-server') == sysconfig(
        NETWORK_MODE='dpdk', ETHDRV='virtio-pci', ETHPCIID='0000:00:03.0')


def test_sysconfig_setup_legacy_options(sysconfig_setup):
    """Pre SET_NIC_AND_DISKS sysconfig: SET_NIC, no SET_CLOCKSOURCE/DISABLE_WRITEBACK_CACHE."""
    legacy = sysconfig(SET_NIC_AND_DISKS=None, SET_CLOCKSOURCE=None, DISABLE_WRITEBACK_CACHE=None)
    sysconfig_setup.file('/etc/sysconfig/scylla-server', legacy + 'SET_NIC=no\n')
    sysconfig_setup.cmd.set('--get-cpu-mask', '0x00000001')
    assert sysconfig_setup.run('scylla_sysconfig_setup', '--setup-nic-and-disks') == 0
    assert '/opt/scylladb/scripts/scylla_cpuset_setup --cpuset 0' in sysconfig_setup.cmd
    assert sysconfig_setup.read('/etc/sysconfig/scylla-server') == legacy + 'SET_NIC=yes\n'


def test_sysconfig_setup_nr_hugepages(sysconfig_setup):
    sysconfig_setup.file('/etc/sysconfig/scylla-server', SYSCONFIG)
    assert sysconfig_setup.run('scylla_sysconfig_setup', '--nr-hugepages', '128') == 0
    assert sysconfig_setup.read('/etc/sysconfig/scylla-server') == sysconfig(NR_HUGEPAGES='128')
