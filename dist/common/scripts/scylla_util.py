#  Copyright (C) 2017 ScyllaDB

# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

import configparser
import io
import logging
import os
import platform
import re
import shlex
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import yaml


def curl(url, byte=False):
    max_retries = 5
    retries = 0
    while True:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as res:
                if byte:
                    return res.read()
                else:
                    return res.read().decode('utf-8')
        except urllib.error.HTTPError:
            logging.warn("Failed to grab %s..." % url)
            time.sleep(5)
            retries += 1
            if (retries >= max_retries):
                raise


class aws_instance:
    """Describe several aspects of the current AWS instance"""
    def __disk_name(self, dev):
        name = re.compile(r"(?:/dev/)?(?P<devname>[a-zA-Z]+)\d*")
        return name.search(dev).group("devname")

    def __instance_metadata(self, path):
        return curl("http://169.254.169.254/latest/meta-data/" + path)

    def __device_exists(self, dev):
        if dev[0:4] != "/dev":
            dev = "/dev/%s" % dev
        return os.path.exists(dev)

    def __xenify(self, devname):
        dev = self.__instance_metadata('block-device-mapping/' + devname)
        return dev.replace("sd", "xvd")

    def __populate_disks(self):
        devmap = self.__instance_metadata("block-device-mapping")
        self._disks = {}
        devname = re.compile("^\D+")
        nvme_re = re.compile(r"nvme\d+n\d+$")
        nvmes_present = list(filter(nvme_re.match, os.listdir("/dev")))
        if nvmes_present:
            self._disks["ephemeral"] = nvmes_present

        for dev in devmap.splitlines():
            t = devname.match(dev).group()
            if t == "ephemeral" and nvmes_present:
                continue
            if t not in self._disks:
                self._disks[t] = []
            if not self.__device_exists(self.__xenify(dev)):
                continue
            self._disks[t] += [self.__xenify(dev)]

    def __mac_address(self, nic='eth0'):
        with open('/sys/class/net/{}/address'.format(nic)) as f:
            return f.read().strip()

    def __init__(self):
        self._type = self.__instance_metadata("instance-type")
        self.__populate_disks()

    def instance(self):
        """Returns which instance we are running in. i.e.: i3.16xlarge"""
        return self._type

    def instance_size(self):
        """Returns the size of the instance we are running in. i.e.: 16xlarge"""
        return self._type.split(".")[1]

    def instance_class(self):
        """Returns the class of the instance we are running in. i.e.: i3"""
        return self._type.split(".")[0]

    def is_supported_instance_class(self):
        if self.instance_class() in ['i2', 'i3']:
            return True
        return False

    def get_en_interface_type(self):
        instance_class = self.instance_class()
        instance_size = self.instance_size()
        if instance_class in ['c3', 'c4', 'd2', 'i2', 'r3']:
            return 'ixgbevf'
        if instance_class in ['c5', 'c5d', 'f1', 'g3', 'h1', 'i3', 'm5', 'm5d', 'p2', 'p3', 'r4', 'x1']:
            return 'ena'
        if instance_class == 'm4':
            if instance_size == '16xlarge':
                return 'ena'
            else:
                return 'ixgbevf'
        return None

    def disks(self):
        """Returns all disks in the system, as visible from the AWS registry"""
        disks = set()
        for v in list(self._disks.values()):
            disks = disks.union([self.__disk_name(x) for x in v])
        return disks

    def root_device(self):
        """Returns the device being used for root data. Unlike root_disk(),
           which will return a device name (i.e. xvda), this function will return
           the full path to the root partition as returned by the AWS instance
           metadata registry"""
        return set(self._disks["root"])

    def root_disk(self):
        """Returns the disk used for the root partition"""
        return self.__disk_name(self._disks["root"][0])

    def non_root_disks(self):
        """Returns all attached disks but root. Include ephemeral and EBS devices"""
        return set(self._disks["ephemeral"] + self._disks["ebs"])

    def ephemeral_disks(self):
        """Returns all ephemeral disks. Include standard SSDs and NVMe"""
        return set(self._disks["ephemeral"])

    def ebs_disks(self):
        """Returns all EBS disks"""
        return set(self._disks["ephemeral"])

    def public_ipv4(self):
        """Returns the public IPv4 address of this instance"""
        return self.__instance_metadata("public-ipv4")

    def private_ipv4(self):
        """Returns the private IPv4 address of this instance"""
        return self.__instance_metadata("local-ipv4")

    def is_vpc_enabled(self, nic='eth0'):
        mac = self.__mac_address(nic)
        mac_stat = self.__instance_metadata('network/interfaces/macs/{}'.format(mac))
        return True if re.search(r'^vpc-id$', mac_stat, flags=re.MULTILINE) else False


# Regular expression helpers
# non-advancing comment matcher
_nocomment = r"^\s*(?!#)"
# non-capturing grouping
_scyllaeq = r"(?:\s*|=)"
_cpuset = r"(?:\s*--cpuset" + _scyllaeq + r"(?P<cpuset>\d+(?:[-,]\d+)*))"
_smp = r"(?:\s*--smp" + _scyllaeq + r"(?P<smp>\d+))"


def _reopt(s):
    return s + r"?"


def is_developer_mode():
    f = open("/etc/scylla.d/dev-mode.conf", "r")
    pattern = re.compile(_nocomment + r".*developer-mode" + _scyllaeq + "(1|true)")
    return len([x for x in f if pattern.match(x)]) >= 1


class scylla_cpuinfo:
    """Class containing information about how Scylla sees CPUs in this machine.
    Information that can be probed include in which hyperthreads Scylla is configured
    to run, how many total threads exist in the system, etc"""
    def __parse_cpuset(self):
        f = open("/etc/scylla.d/cpuset.conf", "r")
        pattern = re.compile(_nocomment + r"CPUSET=\s*\"" + _reopt(_cpuset) + _reopt(_smp) + "\s*\"")
        grp = [pattern.match(x) for x in f.readlines() if pattern.match(x)]
        if not grp:
            d = {"cpuset": None, "smp": None}
        else:
            # if more than one, use last
            d = grp[-1].groupdict()
        actual_set = set()
        if d["cpuset"]:
            groups = d["cpuset"].split(",")
            for g in groups:
                ends = [int(x) for x in g.split("-")]
                actual_set = actual_set.union(set(range(ends[0], ends[-1] + 1)))
            d["cpuset"] = actual_set
        if d["smp"]:
            d["smp"] = int(d["smp"])
        self._cpu_data = d

    def __system_cpus(self):
        cur_proc = -1
        f = open("/proc/cpuinfo", "r")
        results = {}
        for line in f:
            if line == '\n':
                continue
            key, value = [x.strip() for x in line.split(":")]
            if key == "processor":
                cur_proc = int(value)
                results[cur_proc] = {}
            results[cur_proc][key] = value
        return results

    def __init__(self):
        self.__parse_cpuset()
        self._cpu_data["system"] = self.__system_cpus()

    def system_cpuinfo(self):
        """Returns parsed information about CPUs in the system"""
        return self._cpu_data["system"]

    def system_nr_threads(self):
        """Returns the number of threads available in the system"""
        return len(self._cpu_data["system"])

    def system_nr_cores(self):
        """Returns the number of cores available in the system"""
        return len(set([x['core id'] for x in list(self._cpu_data["system"].values())]))

    def cpuset(self):
        """Returns the current cpuset Scylla is configured to use. Returns None if no constraints exist"""
        return self._cpu_data["cpuset"]

    def smp(self):
        """Returns the explicit smp configuration for Scylla, returns None if no constraints exist"""
        return self._cpu_data["smp"]

    def nr_shards(self):
        """How many shards will Scylla use in this machine"""
        if self._cpu_data["smp"]:
            return self._cpu_data["smp"]
        elif self._cpu_data["cpuset"]:
            return len(self._cpu_data["cpuset"])
        else:
            return len(self._cpu_data["system"])


def run(cmd, shell=False, silent=False, exception=True):
    stdout = subprocess.DEVNULL if silent else None
    stderr = subprocess.DEVNULL if silent else None
    if not shell:
        cmd = shlex.split(cmd)
    if exception:
        return subprocess.check_call(cmd, shell=shell, stdout=stdout, stderr=stderr)
    else:
        p = subprocess.Popen(cmd, shell=shell, stdout=stdout, stderr=stderr)
        return p.wait()


def out(cmd, shell=False, exception=True):
    if not shell:
        cmd = shlex.split(cmd)
    if exception:
        return subprocess.check_output(cmd, shell=shell).strip().decode('utf-8')
    else:
        p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE)
        return p.communicate()[0].strip().decode('utf-8')


def is_debian_variant():
    return os.path.exists('/etc/debian_version')


def is_redhat_variant():
    return os.path.exists('/etc/redhat-release')


def is_gentoo_variant():
    return os.path.exists('/etc/gentoo-release')


def is_ec2():
    if os.path.exists('/sys/hypervisor/uuid'):
        with open('/sys/hypervisor/uuid') as f:
            s = f.read()
        return True if re.match(r'^ec2.*', s, flags=re.IGNORECASE) else False
    elif os.path.exists('/sys/class/dmi/id/board_vendor'):
        with open('/sys/class/dmi/id/board_vendor') as f:
            s = f.read()
        return True if re.match(r'^Amazon EC2$', s) else False
    return False


def is_systemd():
    try:
        with open('/proc/1/comm') as f:
            s = f.read()
        return True if re.match(r'^systemd$', s, flags=re.MULTILINE) else False
    except Exception:
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


def makedirs(name):
    if not os.path.isdir(name):
        os.makedirs(name)


def rmtree(path):
    if not os.path.islink(path):
        shutil.rmtree(path)
    else:
        os.remove(path)


def dist_name():
    return platform.dist()[0]


def dist_ver():
    return platform.dist()[1]


def is_unused_disk(dev):
    # dev is not in /sys/class/block/, like /dev/nvme[0-9]+
    if not os.path.isdir('/sys/class/block/{dev}'.format(dev=dev.replace('/dev/', ''))):
        return False
    try:
        fd = os.open(dev, os.O_EXCL)
        os.close(fd)
        return True
    except OSError:
        return False


CONCOLORS = {'green': '\033[1;32m', 'red': '\033[1;31m', 'nocolor': '\033[0m'}


def colorprint(msg, **kwargs):
    fmt = dict(CONCOLORS)
    fmt.update(kwargs)
    print(msg.format(**fmt))


def get_mode_cpuset(nic, mode):
    try:
        mode_cpu_mask = out('/usr/lib/scylla/perftune.py --tune net --nic "{nic}" --mode "{mode}" --get-cpu-mask'.format(nic=nic, mode=mode))
        return hex2list(mode_cpu_mask)
    except subprocess.CalledProcessError:
        return '-1'

def get_scylla_dirs():
    """
    Returns a list of scylla directories configured in /etc/scylla/scylla.yaml.
    Verifies that mandatory parameters are set.
    """
    scylla_yaml_name = '/etc/scylla/scylla.yaml'
    y = yaml.load(open(scylla_yaml_name))

    # Check that mandatory fields are set
    if 'data_file_directories' not in y or \
            not y['data_file_directories'] or \
            not len(y['data_file_directories']) or \
            not " ".join(y['data_file_directories']).strip():
        raise Exception("{}: at least one directory has to be set in 'data_file_directory'".format(scylla_yaml_name))
    if 'commitlog_directory' not in y or not y['commitlog_directory']:
        raise Exception("{}: 'commitlog_directory' has to be set".format(scylla_yaml_name))

    dirs = []
    dirs.extend(y['data_file_directories'])
    dirs.append(y['commitlog_directory'])

    if 'hints_directory' in y and y['hints_directory']:
        dirs.append(y['hints_directory'])
    if 'view_hints_directory' in y and y['view_hints_directory']:
        dirs.append(y['view_hints_directory'])

    return [d for d in dirs if d is not None]

def perftune_base_command():
    disk_tune_param = "--tune disks " + " ".join("--dir {}".format(d) for d in get_scylla_dirs())
    return '/usr/lib/scylla/perftune.py {}'.format(disk_tune_param)

def get_cur_cpuset():
    cfg = sysconfig_parser('/etc/scylla.d/cpuset.conf')
    cpuset = cfg.get('CPUSET')
    return re.sub(r'^--cpuset (.+)$', r'\1', cpuset).strip()


def get_tune_mode(nic):
    if not os.path.exists('/etc/scylla.d/cpuset.conf'):
        return
    cur_cpuset = get_cur_cpuset()
    mq_cpuset = get_mode_cpuset(nic, 'mq')
    sq_cpuset = get_mode_cpuset(nic, 'sq')
    sq_split_cpuset = get_mode_cpuset(nic, 'sq_split')

    if cur_cpuset == mq_cpuset:
        return 'mq'
    elif cur_cpuset == sq_cpuset:
        return 'sq'
    elif cur_cpuset == sq_split_cpuset:
        return 'sq_split'


def create_perftune_conf(nic='eth0'):
    if os.path.exists('/etc/scylla.d/perftune.yaml'):
        return
    mode = get_tune_mode(nic)
    yaml = out('/usr/lib/scylla/perftune.py --tune net --nic "{nic}" --mode {mode} --dump-options-file'.format(nic=nic, mode=mode))
    with open('/etc/scylla.d/perftune.yaml', 'w') as f:
        f.write(yaml)


def is_valid_nic(nic):
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
    except:
        # For backwards compatibility
        return cfg.get('SET_NIC')

class SystemdException(Exception):
    pass


class systemd_unit:
    def __init__(self, unit):
        try:
            run('systemctl cat {}'.format(unit), silent=True)
        except subprocess.CalledProcessError:
            raise SystemdException('unit {} not found'.format(unit))
        self._unit = unit

    def start(self):
        return run('systemctl start {}'.format(self._unit))

    def stop(self):
        return run('systemctl stop {}'.format(self._unit))
        return subprocess.check_call(['systemctl', 'stop', self._unit])

    def restart(self):
        return run('systemctl restart {}'.format(self._unit))

    def enable(self):
        return run('systemctl enable {}'.format(self._unit))

    def disable(self):
        return run('systemctl disable {}'.format(self._unit))

    def is_active(self):
        return out('systemctl is-active {}'.format(self._unit), exception=False)

    def mask(self):
        return run('systemctl mask {}'.format(self._unit))

    def unmask(self):
        return run('systemctl unmask {}'.format(self._unit))


class sysconfig_parser:
    def __load(self):
        f = io.StringIO('[global]\n{}'.format(self._data))
        self._cfg = configparser.ConfigParser()
        self._cfg.optionxform = str
        self._cfg.readfp(f)

    def __escape(self, val):
        return re.sub(r'"', r'\"', val)

    def __add(self, key, val):
        self._data += '{}="{}"\n'.format(key, self.__escape(val))
        self.__load()

    def __init__(self, filename):
        self._filename = filename
        if not os.path.exists(filename):
            open(filename, 'a').close()
        with open(filename) as f:
            self._data = f.read()
        self.__load()

    def get(self, key):
        return self._cfg.get('global', key).strip('"')

    def has_option(self, key):
        return self._cfg.has_option('global', key)

    def set(self, key, val):
        if not self.has_option(key):
            return self.__add(key, val)
        self._data = re.sub('^{}=[^\n]*$'.format(key), '{}="{}"'.format(key, self.__escape(val)), self._data, flags=re.MULTILINE)
        self.__load()

    def commit(self):
        with open(self._filename, 'w') as f:
            f.write(self._data)
