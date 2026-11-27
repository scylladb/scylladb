#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from pathlib import Path

import pytest

import scylla_util

RELEASE = '7.0.0-1014-aws'
DROPIN = '/etc/default/grub.d/99-scylla-kho.cfg'
GRUBBY = 'grubby --update-kernel=ALL --args=kho=off'
KHO_DEFAULT_ON = 'CONFIG_KEXEC_HANDOVER=y\nCONFIG_KEXEC_HANDOVER_ENABLE_DEFAULT=y\n'
SYSCTL_CONF = '/etc/sysctl.d/99-scylla-lowmem-reserve.conf'
SYSCTL = f'sysctl -p {SYSCTL_CONF}'


def boot(scripts, monkeypatch, cmdline='ro', config=KHO_DEFAULT_ON):
    """The running kernel: its command line, its build config (None: no /boot/config file), and its memory zones."""
    monkeypatch.setattr('platform.release', lambda: RELEASE)
    scripts.file('/proc/cmdline', cmdline + '\n')
    scripts.file('/proc/sys/vm/lowmem_reserve_ratio', '256\t256\t32\t0\t0\n')
    scripts.fs.create_dir('/etc/sysctl.d')
    if config is not None:
        scripts.file(f'/boot/config-{RELEASE}', config)


@pytest.fixture
def kernel(scripts, monkeypatch):
    """A running kernel with KHO enabled by default and no kho= on the command line."""
    boot(scripts, monkeypatch)
    return scripts


def tools(monkeypatch, *names):
    monkeypatch.setattr('shutil.which', lambda x: f'/usr/sbin/{x}' if x in names else None)


def test_debian_grub_dropin(kernel, monkeypatch, capsys):
    """On Debian/Ubuntu kho=off goes into a GRUB drop-in that extends the command line, then grub.cfg is regenerated."""
    kernel.distro('ubuntu')
    tools(monkeypatch, 'update-grub')
    assert kernel.run('scylla_kernel_memory_setup') == 0
    assert 'GRUB_CMDLINE_LINUX="$GRUB_CMDLINE_LINUX kho=off"\n' in kernel.read(DROPIN)
    assert kernel.cmd.calls == ['update-grub', SYSCTL]
    assert 'Reboot the machine to apply kho=off' in capsys.readouterr().out


def test_debian_prefers_dropin_over_grubby(kernel, monkeypatch):
    """Debian/Ubuntu use the drop-in even with grubby installed, since update-grub would drop grubby's edit of grub.cfg."""
    kernel.distro('ubuntu')
    tools(monkeypatch, 'grubby', 'update-grub')
    assert kernel.run('scylla_kernel_memory_setup') == 0
    assert kernel.cmd.calls == ['update-grub', SYSCTL]
    assert 'kho=off' in kernel.read(DROPIN)


def test_debian_rerun_rewrites_dropin(kernel, monkeypatch):
    """Running setup again leaves a single kho=off in the drop-in rather than appending another one."""
    kernel.distro('ubuntu')
    tools(monkeypatch, 'update-grub')
    kernel.run('scylla_kernel_memory_setup')
    first = kernel.read(DROPIN)
    kernel.run('scylla_kernel_memory_setup')
    assert kernel.read(DROPIN) == first
    assert first.count('kho=off') == 1


def test_grubby(kernel, monkeypatch):
    """Elsewhere (the Red Hat family) grubby adds kho=off to every installed kernel's boot entry."""
    tools(monkeypatch, 'grubby')
    assert kernel.run('scylla_kernel_memory_setup') == 0
    assert kernel.cmd.calls == [GRUBBY, SYSCTL]


@pytest.mark.parametrize('config,message', [(KHO_DEFAULT_ON, True), ('CONFIG_KEXEC_HANDOVER=y\n', False)],
                         ids=['kho-enabled', 'kho-disabled'])
def test_unsupported_bootloader(scripts, monkeypatch, capsys, config, message):
    """Without a known bootloader tool setup goes on; the user is asked to add kho=off by hand only if KHO is on."""
    boot(scripts, monkeypatch, config=config)
    scripts.distro('ubuntu')
    tools(monkeypatch)
    assert scripts.run('scylla_kernel_memory_setup') == 0
    assert scripts.cmd.calls == [SYSCTL]
    out = capsys.readouterr().out
    assert ('add kho=off to the kernel command line manually' in out) == message
    assert 'Reboot' not in out


def test_bootloader_failure_does_not_fail_setup(kernel, monkeypatch, capsys):
    """A failing bootloader tool (e.g. in a container or an image chroot) is reported, and scylla_setup goes on."""
    tools(monkeypatch, 'grubby')
    kernel.cmd.set('grubby', returncode=1)
    assert kernel.run('scylla_kernel_memory_setup') == 0
    assert 'Disabling Kexec HandOver failed' in capsys.readouterr().out
    assert kernel.cmd.calls[-1] == SYSCTL


def test_sysctl_failure_does_not_skip_kho(kernel, monkeypatch, capsys):
    """A failing sysctl (e.g. a read-only /proc/sys in a container) neither skips kho=off nor fails scylla_setup."""
    tools(monkeypatch, 'grubby')
    kernel.cmd.set('sysctl', returncode=1)
    assert kernel.run('scylla_kernel_memory_setup') == 0
    assert GRUBBY in kernel.cmd
    assert 'Removing the low zone reserve failed' in capsys.readouterr().out


@pytest.mark.parametrize('cmdline,config,enabled', [
    ('ro', KHO_DEFAULT_ON, True),
    ('ro kho=off', KHO_DEFAULT_ON, False),
    ('ro kho=on kho=n', KHO_DEFAULT_ON, False),
    ('ro kho=off kho=bogus', KHO_DEFAULT_ON, False),
    ('ro kho=bogus', KHO_DEFAULT_ON, True),
    ('ro kho=disable', KHO_DEFAULT_ON, False),
    ('ro kho=on', 'CONFIG_KEXEC_HANDOVER=y\n', True),
    ('ro kho=enable', 'CONFIG_KEXEC_HANDOVER=y\n', True),
    ('ro kho=1', None, True),
    ('ro kho=on', '# CONFIG_KEXEC_HANDOVER is not set\n', False),
    ('ro kho_scratch=1G', KHO_DEFAULT_ON, True),
    ('ro', 'CONFIG_KEXEC_HANDOVER=y\n', False),
    ('ro', None, False),
], ids=['default-on', 'off', 'last-wins', 'invalid-ignored', 'invalid-keeps-default', 'disable', 'on', 'enable',
        'no-config', 'not-built', 'kho-scratch-not-kho', 'default-off', 'no-config-no-arg'])
def test_kho_enabled(scripts, monkeypatch, cmdline, config, enabled):
    """KHO detection follows the kernel: the build default, then each valid kho= in order (kstrtobool semantics)."""
    boot(scripts, monkeypatch, cmdline, config)
    assert scylla_util.kho_enabled() == enabled


@pytest.mark.parametrize('enabled', [True, False])
def test_reboot_message(scripts, monkeypatch, capsys, enabled):
    """kho=off is always written, so a later KHO kernel is covered, but a reboot is asked for only if KHO is on now."""
    boot(scripts, monkeypatch, cmdline='ro' if enabled else 'ro kho=off')
    tools(monkeypatch, 'grubby')
    assert scripts.run('scylla_kernel_memory_setup') == 0
    assert scripts.cmd.calls == [GRUBBY, SYSCTL]
    assert ('Reboot the machine' in capsys.readouterr().out) == enabled


@pytest.mark.parametrize('zones,expected', [('256\t256\t32\t0\t0\n', '0 0 0 0 0'), ('256\t32\t0\t0\n', '0 0 0 0')],
                         ids=['5-zones', '4-zones'])
def test_lowmem_reserve(kernel, monkeypatch, zones, expected):
    """The low zone reserve is removed with one ratio per zone of the running kernel, persisted and applied now."""
    Path('/proc/sys/vm/lowmem_reserve_ratio').write_text(zones)
    tools(monkeypatch, 'grubby')
    assert kernel.run('scylla_kernel_memory_setup') == 0
    assert f'vm.lowmem_reserve_ratio = {expected}\n' in kernel.read(SYSCTL_CONF)
    assert kernel.cmd.calls == [GRUBBY, SYSCTL]


@pytest.mark.parametrize('release', ['4.14.355-275.570.amzn2.x86_64', 'unknown'])
def test_lowmem_reserve_old_kernel(kernel, monkeypatch, capsys, release):
    """Before Linux 4.17 a ratio of 0 was raised to 1, reserving whole zones, so the sysctl is left alone there."""
    monkeypatch.setattr('platform.release', lambda: release)
    tools(monkeypatch, 'grubby')
    assert kernel.run('scylla_kernel_memory_setup') == 0
    assert kernel.cmd.calls == [GRUBBY]
    assert not Path(SYSCTL_CONF).exists()
    assert 'keeping vm.lowmem_reserve_ratio' in capsys.readouterr().out
