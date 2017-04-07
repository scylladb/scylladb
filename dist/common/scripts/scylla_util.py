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

import urllib2
import urllib
import logging
import time
import re
import os
import string

def curl(url):
    max_retries = 5
    retries = 0
    while True:
        try:
            req = urllib2.Request(url)
            return urllib2.urlopen(req).read()
        except urllib2.HTTPError:
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
            dev = "/dev/%s" %dev
        return os.path.exists(dev)

    def __xenify(self, devname):
        dev = self.__instance_metadata('block-device-mapping/' + devname)
        return dev.replace("sd", "xvd")

    def __populate_disks(self):
        devmap = self.__instance_metadata("block-device-mapping")
        self._disks = {}
        devname = re.compile("^\D+")
        nvme_re = re.compile(r"nvme\d+n\d+$")
        nvmes_present = filter(nvme_re.match, os.listdir("/dev"))
        if nvmes_present:
            self._disks["ephemeral"] = nvmes_present;

        for dev in devmap.splitlines():
            t = devname.match(dev).group()
            if t == "ephemeral"  and nvmes_present:
                continue;
            if not self._disks.has_key(t):
                self._disks[t] = []
            if not self.__device_exists(self.__xenify(dev)):
                continue
            self._disks[t] += [ self.__xenify(dev) ]

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

    def disks(self):
        """Returns all disks in the system, as visible from the AWS registry"""
        disks = set()
        for v in self._disks.values():
            disks = disks.union([ self.__disk_name(x) for x in v ])
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


## Regular expression helpers
# non-advancing comment matcher
_nocomment=r"^\s*(?!#)"
# non-capturing grouping
_scyllaeq=r"(?:\s*|=)"
_cpuset = r"(?:\s*--cpuset" + _scyllaeq + r"(?P<cpuset>\d+(?:[-,]\d+)*))"
_smp = r"(?:\s*--smp" + _scyllaeq + r"(?P<smp>\d+))"

def _reopt(s):
    return s + r"?"

def is_developer_mode():
    f = file("/etc/scylla.d/dev-mode.conf", "ro")
    pattern = re.compile(_nocomment + r".*developer-mode" + _scyllaeq + "(1|true)")
    return len([ x for x in f.xreadlines() if pattern.match(x) ]) >= 1

class scylla_cpuinfo:
    """Class containing information about how Scylla sees CPUs in this machine.
    Information that can be probed include in which hyperthreads Scylla is configured
    to run, how many total threads exist in the system, etc"""
    def __parse_cpuset(self):
        f = file("/etc/scylla.d/cpuset.conf", "ro")
        pattern = re.compile(_nocomment + r"CPUSET=\s*\"" + _reopt(_cpuset) + _reopt(_smp) + "\s*\"")
        grp = [ pattern.match(x) for x in f.readlines() if pattern.match(x) ]
        if not grp:
            d = { "cpuset" : None, "smp" : None }
        else:
            # if more than one, use last
            d = grp[-1].groupdict()
        actual_set = set()
        if d["cpuset"]:
            groups = d["cpuset"].split(",")
            for g in groups:
                ends = [ int(x) for x in g.split("-") ]
                actual_set = actual_set.union(set(xrange(ends[0], ends[-1] +1)))
            d["cpuset"] = actual_set
        if d["smp"]:
            d["smp"] = int(d["smp"])
        self._cpu_data = d;

    def __system_cpus(self):
        cur_proc = -1
        f = file("/proc/cpuinfo", "ro")
        results = {}
        for line in f.xreadlines():
            if line == '\n':
                continue
            key, value = [ x.strip() for x in line.split(":") ]
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
        return len(set([ x['core id'] for x in self._cpu_data["system"].values() ]))

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
