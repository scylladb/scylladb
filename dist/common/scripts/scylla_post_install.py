#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2019-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Machine-dependent systemd drop-ins for the units shipped with Scylla.

Run from the RPM %post, the Debian postinst and the offline installer, through
the scylla_post_install.sh wrapper. Must never fail the package transaction:
every step is best-effort and main() always returns 0.
"""

import argparse
import logging
import platform
import re
import subprocess
import sys
from pathlib import Path

from scylla_util import parse_cpu_list, cpu_list_to_str, parse_cpuset_conf

HELPER_SLICE = 'scylla-helper.slice'
JOURNALD_SERVICE = 'systemd-journald.service'

# Below this, Scylla's 7% reservation hits seastar's 1.5GB floor, so the helper
# slice uses fixed limits rather than percentages.
MEMORY_PERCENT_THRESHOLD_BYTES = 23008753371

# CAP_PERFMON is only available on linux-5.8+
PERFMON_KERNEL_VERSION = (5, 8)

# AllowedCPUs= requires systemd v244 and the unified cgroup hierarchy.
ALLOWED_CPUS_SYSTEMD_VERSION = 244

logger = logging.getLogger('scylla_post_install')


def parse_version(version):
    """Leading dotted numeric components of a version string, as a tuple."""
    match = re.match(r'(\d+(?:\.\d+)*)', version)
    if not match:
        return ()
    return tuple(int(x) for x in match.group(1).split('.'))


class PostInstall:
    """Post-install actions. Every path is rooted at `root`, so tests can drive
    this against a scratch directory."""

    def __init__(self, root='/', kernel_version=None):
        self.root = Path(root)
        self.systemd_dir = self.root / 'etc/systemd/system'
        self.cpuset_conf = self.root / 'etc/scylla.d/cpuset.conf'
        self.meminfo = self.root / 'proc/meminfo'
        self.cpu_online = self.root / 'sys/devices/system/cpu/online'
        self.cgroup_controllers = self.root / 'sys/fs/cgroup/cgroup.controllers'
        self.kernel_version = kernel_version if kernel_version is not None else platform.release()
        self.changed = set()   # drop-ins this run actually changed

    # ---- system probing ---------------------------------------------------

    def total_memory(self):
        """MemTotal in bytes, or None if it cannot be determined."""
        match = re.search(r'^MemTotal:\s*(\d+) kB$', self.meminfo.read_text(), re.MULTILINE)
        if not match:
            return None
        return int(match.group(1)) * 1024

    def online_cpus(self):
        """The CPUs present on this machine, per sysfs."""
        return parse_cpu_list(self.cpu_online.read_text().strip())

    def scylla_cpus(self):
        """The CPUs dedicated to Scylla, or None when it is not pinned."""
        return parse_cpuset_conf(self.cpuset_conf)['cpuset']

    def free_cpus(self):
        """CPUs the helpers may use. Empty when Scylla is unpinned (it may then
        use every CPU) or when its cpuset covers the whole machine."""
        scylla_cpus = self.scylla_cpus()
        if not scylla_cpus:
            return set()
        return self.online_cpus() - scylla_cpus

    def has_cgroup_cpuset(self):
        """True on a unified (v2) hierarchy that exposes the cpuset controller."""
        if not self.cgroup_controllers.exists():
            return False
        return 'cpuset' in self.cgroup_controllers.read_text().split()

    def systemd_version(self):
        """Version of the running systemd, or None if it cannot be determined."""
        res = self.run_systemctl(['--version'], capture=True)
        if res is None:
            return None
        match = re.search(r'^systemd\s+(\d+)', res, re.MULTILINE)
        return int(match.group(1)) if match else None

    def supports_allowed_cpus(self):
        if not self.has_cgroup_cpuset():
            logger.info('cgroup v2 cpuset controller not available, skipping AllowedCPUs')
            return False
        version = self.systemd_version()
        if version is None or version < ALLOWED_CPUS_SYSTEMD_VERSION:
            logger.info('systemd %s does not support AllowedCPUs (needs %s), skipping',
                        version, ALLOWED_CPUS_SYSTEMD_VERSION)
            return False
        return True

    # ---- systemctl --------------------------------------------------------

    def run_systemctl(self, args, capture=False):
        """Returns stdout when `capture`, True on success, None on failure -
        systemd problems must not fail the install."""
        cmd = ['systemctl'] + args
        try:
            res = subprocess.run(cmd, capture_output=True, encoding='utf-8', check=True)
        except (subprocess.CalledProcessError, OSError) as e:
            logger.warning('%s failed: %s', ' '.join(cmd), e)
            return None
        return res.stdout if capture else True

    # ---- drop-in files ----------------------------------------------------

    def write_dropin(self, unit, name, content):
        """Create a drop-in for `unit`. Returns True if its content changed."""
        path = self.systemd_dir / '{}.d'.format(unit) / name
        if path.exists() and path.read_text() == content:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        path.chmod(0o644)
        logger.info('wrote %s', path)
        self.changed.add(path)
        return True

    def remove_dropin(self, unit, name):
        """Drop one left behind by an earlier install. True if it was there."""
        path = self.systemd_dir / '{}.d'.format(unit) / name
        if not path.exists():
            return False
        path.unlink()
        logger.info('removed %s', path)
        self.changed.add(path)
        return True

    # ---- individual configuration steps -----------------------------------

    def setup_capabilities(self):
        """Grant CAP_PERFMON to scylla-server where the kernel supports it."""
        if parse_version(self.kernel_version) < PERFMON_KERNEL_VERSION:
            return
        self.write_dropin('scylla-server.service', 'capabilities.conf',
                          '[Service]\nAmbientCapabilities=CAP_PERFMON\n')

    def setup_helper_slice_memory(self):
        """Replace the slice's percentage-based limits with fixed ones where
        there is not a lot of memory to take a percentage of."""
        memtotal = self.total_memory()
        if memtotal is None:
            logger.warning('could not read MemTotal from %s, leaving slice memory limits alone',
                           self.meminfo)
            return
        if memtotal >= MEMORY_PERCENT_THRESHOLD_BYTES:
            return
        self.write_dropin(HELPER_SLICE, 'memory.conf',
                          '[Slice]\nMemoryHigh=1200M\nMemoryMax=1400M\n')

    def setup_helper_slice_cpuset(self):
        """Keep the helpers off the cores Scylla has to itself.

        With no spare core to give, no drop-in is installed - and a stale one is
        removed, so the slice is not left pinned to cores Scylla has since
        taken over.
        """
        free_cpus = self.free_cpus()
        if not free_cpus or not self.supports_allowed_cpus():
            self.remove_dropin(HELPER_SLICE, 'cpuset.conf')
            return
        self.write_dropin(HELPER_SLICE, 'cpuset.conf',
                          '[Slice]\nAllowedCPUs={}\n'.format(cpu_list_to_str(free_cpus)))

    def setup_journald_slice(self):
        """Run journald in the helper slice, so its memory, IO and CPU are
        capped alongside Scylla's other companions instead of competing with
        the database."""
        self.write_dropin(JOURNALD_SERVICE, 'scylla-helper-slice.conf',
                          '[Service]\nSlice={}\n'.format(HELPER_SLICE))

    def setup_limitnofile(self):
        """Retire a per-machine LimitNOFILE drop-in left by an older install.

        scylla-server.service now ships LimitNOFILE=infinity. An existing
        drop-in would keep overriding it, so rewrite rather than remove it: a
        downgrade then ends up with more file descriptors, not fewer.
        """
        path = self.systemd_dir / 'scylla-server.service.d/limitnofile.conf'
        if not path.exists():
            return
        self.write_dropin('scylla-server.service', 'limitnofile.conf',
                          '[Service]\nLimitNOFILE=infinity\n')

    def setup_coredump_timeout(self):
        """Let a coredump of a Scylla-sized process run to completion."""
        path = self.systemd_dir / 'systemd-coredump@.service.d/timeout.conf'
        if not path.exists() or 'RuntimeMaxSec' in path.read_text():
            return
        self.write_dropin('systemd-coredump@.service', 'timeout.conf',
                          '[Service]\nRuntimeMaxSec=infinity\nTimeoutSec=infinity\n')

    # ---- applying the configuration ---------------------------------------

    def apply(self):
        """Make the drop-ins written by this run take effect.

        daemon-reload re-realizes the cgroup attributes of active units, so the
        slice limits apply to the running helpers without restarting them. Only
        journald needs more than that: a running unit cannot change slice, so
        its new Slice= takes effect on restart.
        """
        # Unconditional, as in the shell version: the package manager has just
        # laid down the unit files themselves, and systemd has to hear about
        # those even when no drop-in of ours changed.
        self.run_systemctl(['daemon-reload'])
        if any(p.parent.name == '{}.d'.format(JOURNALD_SERVICE) for p in self.changed):
            logger.info('restarting %s', JOURNALD_SERVICE)
            self.run_systemctl(['try-restart', JOURNALD_SERVICE])

    def run(self):
        for step in (self.setup_capabilities,
                     self.setup_helper_slice_memory,
                     self.setup_helper_slice_cpuset,
                     self.setup_journald_slice,
                     self.setup_limitnofile,
                     self.setup_coredump_timeout):
            try:
                step()
            except Exception:
                logger.warning('%s failed', step.__name__, exc_info=True)
        self.apply()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Configure the systemd units shipped with Scylla.')
    parser.add_argument('--root', default='/',
                        help='prefix every configured path with this directory')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='scylla_post_install: %(levelname)s: %(message)s')
    try:
        PostInstall(root=args.root).run()
    except Exception:
        # A broken post-install must not break the package transaction.
        logger.warning('post-install configuration failed', exc_info=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
