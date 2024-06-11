#  Copyright (C) 2017-present ScyllaDB

# SPDX-License-Identifier: AGPL-3.0-or-later

import configparser
import glob
import io
import os
import re
import shlex
import shutil
import subprocess
import yaml
import sys
import time
from pathlib import Path, PurePath
from subprocess import run, DEVNULL, PIPE, CalledProcessError
from datetime import datetime, timedelta

import distro
from scylla_sysconfdir import SYSCONFDIR
from scylla_product import PRODUCT

from multiprocessing import cpu_count

import traceback
import traceback_with_variables
import logging

def scylla_excepthook(etype, value, tb):
    os.makedirs('/var/tmp/scylla', mode=0o755, exist_ok=True)
    traceback.print_exception(etype, value, tb)
    exc_logger = logging.getLogger(__name__)
    exc_logger.setLevel(logging.DEBUG)
    exc_logger_file = f'/var/tmp/scylla/{os.path.basename(sys.argv[0])}-{os.getpid()}-debug.log'
    exc_logger.addHandler(logging.FileHandler(exc_logger_file))
    traceback_with_variables.print_exc(e=value, file_=traceback_with_variables.LoggerAsFile(exc_logger))
    print(f'Debug log created: {exc_logger_file}')

sys.excepthook = scylla_excepthook


def out(cmd, shell=True, timeout=None, encoding='utf-8', ignore_error=False, user=None, group=None):
    res = subprocess.run(cmd, capture_output=True, shell=shell, timeout=timeout, check=False, encoding=encoding, user=user, group=group)
    if not ignore_error and res.returncode != 0:
        print(f'Command \'{cmd}\' returned non-zero exit status: {res.returncode}')
        print('----------  stdout  ----------')
        print(res.stdout, end='')
        print('------------------------------')
        print('----------  stderr  ----------')
        print(res.stderr, end='')
        print('------------------------------')
        res.check_returncode()
    return res.stdout.strip()


def scriptsdir_p():
    p = Path(sys.argv[0]).resolve()
    if p.parent.name == 'libexec':
        return p.parents[1]
    return p.parent

def scylladir_p():
    p = scriptsdir_p()
    return p.parent

def is_nonroot():
    return Path(scylladir_p() / 'SCYLLA-NONROOT-FILE').exists()

def is_offline():
    return Path(scylladir_p() / 'SCYLLA-OFFLINE-FILE').exists()

def bindir_p():
    if is_nonroot():
        return scylladir_p() / 'bin'
    else:
        return Path('/usr/bin')

def etcdir_p():
    if is_nonroot():
        return scylladir_p() / 'etc'
    else:
        return Path('/etc')

def datadir_p():
    if is_nonroot():
        return scylladir_p()
    else:
        return Path('/var/lib/scylla')

def scyllabindir_p():
    return scylladir_p() / 'bin'

def sysconfdir_p():
    return Path(SYSCONFDIR)

def scriptsdir():
    return str(scriptsdir_p())

def scylladir():
    return str(scylladir_p())

def bindir():
    return str(bindir_p())

def etcdir():
    return str(etcdir_p())

def datadir():
    return str(datadir_p())

def scyllabindir():
    return str(scyllabindir_p())

def sysconfdir():
    return str(sysconfdir_p())

def get_id_like():
    like = distro.like()
    if not like:
        return None
    return like.split(' ')

def is_debian_variant():
    d = get_id_like() if get_id_like() else distro.id()
    return ('debian' in d)

def is_redhat_variant():
    d = get_id_like() if get_id_like() else distro.id()
    return ('rhel' in d) or ('fedora' in d) or ('oracle') in d

def is_gentoo():
    return ('gentoo' in distro.id())

def is_arch():
    return ('arch' in distro.id())

def is_amzn2():
    return ('amzn' in distro.id()) and ('2' in distro.version())

def is_suse_variant():
    d = get_id_like() if get_id_like() else distro.id()
    return ('suse' in d)

def is_developer_mode():
    # non-advancing comment matcher
    _nocomment = r"^\s*(?!#)"
    # non-capturing grouping
    _scyllaeq = r"(?:\s*|=)"
    f = open(etcdir() + "/scylla.d/dev-mode.conf", "r")
    pattern = re.compile(_nocomment + r".*developer-mode" + _scyllaeq + "(1|true)")
    return len([x for x in f if pattern.match(x)]) >= 1

def get_text_from_path(fpath):
    board_vendor_path = Path(fpath)
    if board_vendor_path.exists():
        return board_vendor_path.read_text().strip()
    return ""

def match_patterns_in_files(list_of_patterns_files):
    for pattern, fpath in list_of_patterns_files:
        if re.match(pattern, get_text_from_path(fpath), flags=re.IGNORECASE):
            return True
    return False

def hex2list(hex_str):
    hex_str2 = hex_str.replace("0x", "").replace(",", "")
    hex_int = int(hex_str2, 16)
    bin_str = "{0:b}".format(hex_int)
    bin_len = len(bin_str)
    cpu_list = []
    i = 0
    while i < bin_len:
        if 1 << i & hex_int:
            j = i
            while j + 1 < bin_len and 1 << j + 1 & hex_int:
                j += 1
            if j == i:
                cpu_list.append(str(i))
            else:
                cpu_list.append("{0}-{1}".format(i, j))
                i = j
        i += 1
    return ",".join(cpu_list)


SYSTEM_PARTITION_UUIDS = [
        '21686148-6449-6e6f-744e-656564454649', # BIOS boot partition
        'c12a7328-f81f-11d2-ba4b-00a0c93ec93b', # EFI system partition
        '024dee41-33e7-11d3-9d69-0008c781f39f'  # MBR partition scheme
]

def get_partition_uuid(dev):
    return out(f'lsblk -n -oPARTTYPE {dev}')

def is_system_partition(dev):
    uuid = get_partition_uuid(dev)
    return (uuid in SYSTEM_PARTITION_UUIDS)

def is_unused_disk(dev):
    # resolve symlink to real path
    dev = os.path.realpath(dev)
    # dev is not in /sys/class/block/, like /dev/nvme[0-9]+
    if not os.path.isdir('/sys/class/block/{dev}'.format(dev=dev.replace('/dev/', ''))):
        return False
    try:
        fd = os.open(dev, os.O_EXCL)
        os.close(fd)
        # dev is not reserved for system
        return not is_system_partition(dev)
    except OSError:
        return False


CONCOLORS = {'green': '\033[1;32m', 'red': '\033[1;31m', 'nocolor': '\033[0m'}


def colorprint(msg, **kwargs):
    fmt = dict(CONCOLORS)
    fmt.update(kwargs)
    print(msg.format(**fmt))


def parse_scylla_dirs_with_default(conf='/etc/scylla/scylla.yaml'):
    y = yaml.safe_load(open(conf))
    if 'workdir' not in y or not y['workdir']:
        y['workdir'] = datadir()
    if 'data_file_directories' not in y or \
            not y['data_file_directories'] or \
            not len(y['data_file_directories']) or \
            not " ".join(y['data_file_directories']).strip():
        y['data_file_directories'] = [os.path.join(y['workdir'], 'data')]
    for t in [ "commitlog", "schema_commitlog", "hints", "view_hints", "saved_caches" ]:
        key = "%s_directory" % t
        if key not in y or not y[key]:
            y[key] = os.path.join(y['workdir'], t)
    return y


def get_scylla_dirs():
    """
    Returns a list of scylla directories configured in /etc/scylla/scylla.yaml.
    Verifies that mandatory parameters are set.
    """
    y = parse_scylla_dirs_with_default()

    dirs = []
    dirs.extend(y['data_file_directories'])
    dirs.append(y['commitlog_directory'])
    dirs.append(y['schema_commitlog_directory'])

    if 'hints_directory' in y and y['hints_directory']:
        dirs.append(y['hints_directory'])
    if 'view_hints_directory' in y and y['view_hints_directory']:
        dirs.append(y['view_hints_directory'])

    return [d for d in dirs if d is not None]


def perftune_base_command():
    disk_tune_param = "--tune disks " + " ".join("--dir {}".format(d) for d in get_scylla_dirs())
    return '/opt/scylladb/scripts/perftune.py {}'.format(disk_tune_param)


def is_valid_nic(nic):
    if len(nic) == 0:
        return False
    return os.path.exists('/sys/class/net/{}'.format(nic))


# Remove this when we do not support SET_NIC configuration value anymore
def get_set_nic_and_disks_config_value(cfg):
    """
    Get the SET_NIC_AND_DISKS configuration value.
    Return the SET_NIC configuration value if SET_NIC_AND_DISKS is not found (old releases case).
    :param cfg: sysconfig_parser object
    :return configuration value
    :except If the configuration value is not found
    """

    # Sanity check
    if cfg.has_option('SET_NIC_AND_DISKS') and cfg.has_option('SET_NIC'):
        raise Exception("Only one of 'SET_NIC_AND_DISKS' and 'SET_NIC' is allowed to be present")

    try:
        return cfg.get('SET_NIC_AND_DISKS')
    except Exception:
        # For backwards compatibility
        return cfg.get('SET_NIC')


def swap_exists():
    swaps = out('swapon --noheadings --raw')
    return True if swaps != '' else False

def pkg_error_exit(pkg):
    print(f'Package "{pkg}" required.')
    sys.exit(1)

def yum_install(pkg):
    if is_offline():
        pkg_error_exit(pkg)
    return run(f'yum install -y {pkg}', shell=True, check=True)

def apt_is_updated():
    if os.path.exists('/var/lib/apt/periodic/update-success-stamp'):
        cache_mtime = os.stat('/var/lib/apt/periodic/update-success-stamp').st_mtime
    elif os.path.exists('/var/lib/apt/lists'):
        cache_mtime = os.stat('/var/lib/apt/lists').st_mtime
    else:
        return False
    return datetime.now() - datetime.fromtimestamp(cache_mtime) <= timedelta(days=1)

APT_GET_UPDATE_NUM_RETRY = 30
APT_GET_UPDATE_RETRY_INTERVAL = 10
def apt_install(pkg):
    if is_offline():
        pkg_error_exit(pkg)

    # The lock for update and install/remove are different, and
    # DPkg::Lock::Timeout will only wait for install/remove lock.
    # So we need to manually retry apt-get update.
    for i in range(APT_GET_UPDATE_NUM_RETRY):
        if apt_is_updated():
            break
        try:
            res = run('apt-get update', shell=True, check=True, stderr=PIPE, encoding='utf-8')
            break
        except CalledProcessError as e:
            print(e.stderr, end='')
            # if error is "Could not get lock", wait a while and retry
            match = re.match('^E: Could not get lock ', e.stderr, re.MULTILINE)
            if match:
                print('Sleep 10 seconds to wait for apt lock...')
                time.sleep(APT_GET_UPDATE_RETRY_INTERVAL)
                # if this is last time to retry, re-raise exception
                if i == APT_GET_UPDATE_NUM_RETRY - 1:
                    raise
            # if error is not "Could not get lock", re-raise Exception
            else:
                raise

    apt_env = os.environ.copy()
    apt_env['DEBIAN_FRONTEND'] = 'noninteractive'
    return run(f'apt-get -o DPkg::Lock::Timeout=300 install -y {pkg}', shell=True, check=True, env=apt_env)

def emerge_install(pkg):
    if is_offline():
        pkg_error_exit(pkg)
    return run(f'emerge -uq {pkg}', shell=True, check=True)

def zypper_install(pkg):
    if is_offline():
        pkg_error_exit(pkg)
    return run(f'zypper install -y {pkg}', shell=True, check=True)

def pkg_distro():
    if is_debian_variant():
        return 'debian'
    if is_suse_variant():
        return 'suse'
    elif is_amzn2():
        return 'amzn2'
    else:
        return distro.id()

pkg_xlat = {'cpupowerutils': {'debian': 'linux-cpupower', 'gentoo':'sys-power/cpupower', 'arch':'cpupower', 'suse': 'cpupower'}}
def pkg_install(pkg):
    if pkg in pkg_xlat and pkg_distro() in pkg_xlat[pkg]:
        pkg = pkg_xlat[pkg][pkg_distro()]
    if is_redhat_variant():
        return yum_install(pkg)
    elif is_debian_variant():
        return apt_install(pkg)
    elif is_gentoo():
        return emerge_install(pkg)
    elif is_suse_variant():
        return zypper_install(pkg)
    else:
        pkg_error_exit(pkg)

def yum_uninstall(pkg):
    return run(f'yum remove -y {pkg}', shell=True, check=True)

def apt_uninstall(pkg):
    apt_env = os.environ.copy()
    apt_env['DEBIAN_FRONTEND'] = 'noninteractive'
    return run(f'apt-get -o DPkg::Lock::Timeout=300 remove -y {pkg}', shell=True, check=True, env=apt_env)

def emerge_uninstall(pkg):
    return run(f'emerge --deselect {pkg}', shell=True, check=True)

def pkg_uninstall(pkg):
    if is_redhat_variant():
        return yum_uninstall(pkg)
    elif is_debian_variant():
        return apt_uninstall(pkg)
    elif is_gentoo():
        return emerge_uninstall(pkg)
    else:
        print(f'WARNING: Package "{pkg}" should be removed.')

class SystemdException(Exception):
    pass


class systemd_unit:
    def __init__(self, unit):
        if is_nonroot():
            self.ctlparam = '--user'
        else:
            self.ctlparam = ''
        try:
            run('systemctl {} cat {}'.format(self.ctlparam, unit), shell=True, check=True, stdout=DEVNULL, stderr=DEVNULL)
        except subprocess.CalledProcessError:
            raise SystemdException('unit {} is not found or invalid'.format(unit))
        self._unit = unit

    def __str__(self):
        return self._unit

    def start(self):
        return run('systemctl {} start {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    def stop(self):
        return run('systemctl {} stop {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    def restart(self):
        return run('systemctl {} restart {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    def enable(self):
        return run('systemctl {} enable {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    def disable(self):
        return run('systemctl {} disable {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    def is_active(self):
        return out('systemctl {} is-active {}'.format(self.ctlparam, self._unit), ignore_error=True)

    def mask(self):
        return run('systemctl {} mask {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    def unmask(self):
        return run('systemctl {} unmask {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    @classmethod
    def reload(cls):
        run('systemctl daemon-reload', shell=True, check=True)

    @classmethod
    def available(cls, unit):
        res = run('systemctl cat {}'.format(unit), shell=True, check=False, stdout=DEVNULL, stderr=DEVNULL)
        return res.returncode == 0


class sysconfig_parser:
    def __load(self):
        f = io.StringIO('[global]\n{}'.format(self._data))
        self._cfg = configparser.ConfigParser()
        self._cfg.optionxform = str
        self._cfg.read_file(f)

    def __escape(self, val):
        return re.sub(r'"', r'\"', val)

    def __unescape(self, val):
        return re.sub(r'\\"', r'"', val)

    def __format_line(self, key, val):
        need_quotes = any([ch.isspace() for ch in val])
        esc_val = self.__escape(val)
        return f'{key}="{esc_val}"' if need_quotes else f'{key}={esc_val}'

    def __add(self, key, val):
        self._data += self.__format_line(key, val) + '\n'
        self.__load()

    def __init__(self, filename):
        if isinstance(filename, PurePath):
            self._filename = str(filename)
        else:
            self._filename = filename
        if not os.path.exists(filename):
            open(filename, 'a').close()
        with open(filename) as f:
            self._data = f.read()
        self.__load()

    def get(self, key):
        val = self._cfg.get('global', key).strip('"')
        return self.__unescape(val)

    def has_option(self, key):
        return self._cfg.has_option('global', key)

    def set(self, key, val):
        if not self.has_option(key):
            return self.__add(key, val)
        new_line = self.__format_line(key, val)
        self._data = re.sub(f'^{key}=[^\n]*$', new_line, self._data, flags=re.MULTILINE)
        self.__load()

    def commit(self):
        with open(self._filename, 'w') as f:
            f.write(self._data)
