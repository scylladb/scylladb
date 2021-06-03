#  Copyright (C) 2017-present ScyllaDB

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
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import yaml
import psutil
import socket
import sys
from pathlib import Path, PurePath
from subprocess import run, DEVNULL

import distro
from scylla_sysconfdir import SYSCONFDIR
from scylla_product import PRODUCT

from multiprocessing import cpu_count

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

# @param headers dict of k:v
def curl(url, headers=None, byte=False, timeout=3, max_retries=5, retry_interval=5):
    retries = 0
    while True:
        try:
            req = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout) as res:
                if byte:
                    return res.read()
                else:
                    return res.read().decode('utf-8')
        except urllib.error.URLError:
            time.sleep(retry_interval)
            retries += 1
            if retries >= max_retries:
                raise


class gcp_instance:
    """Describe several aspects of the current GCP instance"""

    EPHEMERAL = "ephemeral"
    ROOT = "root"
    GETTING_STARTED_URL = "http://www.scylladb.com/doc/getting-started-google/"
    META_DATA_BASE_URL = "http://metadata.google.internal/computeMetadata/v1/instance/"
    ENDPOINT_SNITCH = "GoogleCloudSnitch"

    def __init__(self):
        self.__type = None
        self.__cpu = None
        self.__memoryGB = None
        self.__nvmeDiskCount = None
        self.__firstNvmeSize = None
        self.__osDisks = None

    @staticmethod
    def is_gce_instance():
        """Check if it's GCE instance via DNS lookup to metadata server."""
        try:
            addrlist = socket.getaddrinfo('metadata.google.internal', 80)
        except socket.gaierror:
            return False
        for res in addrlist:
            af, socktype, proto, canonname, sa = res
            if af == socket.AF_INET:
                addr, port = sa
                if addr == "169.254.169.254":
                    return True
        return False

    def __instance_metadata(self, path, recursive=False):
        return curl(self.META_DATA_BASE_URL + path + "?recursive=%s" % str(recursive).lower(),
                    headers={"Metadata-Flavor": "Google"})

    def is_in_root_devs(self, x, root_devs):
        for root_dev in root_devs:
            if root_dev.startswith(os.path.join("/dev/", x)):
                return True
        return False

    def _non_root_nvmes(self):
        """get list of nvme disks from os, filter away if one of them is root"""
        nvme_re = re.compile(r"nvme\d+n\d+$")

        root_dev_candidates = [x for x in psutil.disk_partitions() if x.mountpoint == "/"]

        root_devs = [x.device for x in root_dev_candidates]

        nvmes_present = list(filter(nvme_re.match, os.listdir("/dev")))
        return {self.ROOT: root_devs, self.EPHEMERAL: [x for x in nvmes_present if not self.is_in_root_devs(x, root_devs)]}

    @property
    def os_disks(self):
        """populate disks from /dev/ and root mountpoint"""
        if self.__osDisks is None:
            __osDisks = {}
            nvmes_present = self._non_root_nvmes()
            for k, v in nvmes_present.items():
                __osDisks[k] = v
            self.__osDisks = __osDisks
        return self.__osDisks

    def getEphemeralOsDisks(self):
        """return just transient disks"""
        return self.os_disks[self.EPHEMERAL]

    @staticmethod
    def isNVME(gcpdiskobj):
        """check if disk from GCP metadata is a NVME disk"""
        if gcpdiskobj["interface"]=="NVME":
            return True
        return False

    def __get_nvme_disks_from_metadata(self):
        """get list of nvme disks from metadata server"""
        try:
            disksREST=self.__instance_metadata("disks", True)
            disksobj=json.loads(disksREST)
            nvmedisks=list(filter(self.isNVME, disksobj))
        except Exception as e:
            print ("Problem when parsing disks from metadata:")
            print (e)
            nvmedisks={}
        return nvmedisks

    @property
    def nvmeDiskCount(self):
        """get # of nvme disks available for scylla raid"""
        if self.__nvmeDiskCount is None:
            try:
                ephemeral_disks = self.getEphemeralOsDisks()
                count_os_disks=len(ephemeral_disks)
            except Exception as e:
                print ("Problem when parsing disks from OS:")
                print (e)
                count_os_disks=0
            nvme_metadata_disks = self.__get_nvme_disks_from_metadata()
            count_metadata_nvme_disks=len(nvme_metadata_disks)
            self.__nvmeDiskCount = count_os_disks if count_os_disks<count_metadata_nvme_disks else count_metadata_nvme_disks
        return self.__nvmeDiskCount

    @property
    def instancetype(self):
        """return the type of this instance, e.g. n2-standard-2"""
        if self.__type is None:
            self.__type = self.__instance_metadata("machine-type").split("/")[-1]
        return self.__type

    @property
    def cpu(self):
        """return the # of cpus of this instance"""
        if self.__cpu is None:
            self.__cpu = psutil.cpu_count()
        return self.__cpu

    @property
    def memoryGB(self):
        """return the size of memory in GB of this instance"""
        if self.__memoryGB is None:
            self.__memoryGB = psutil.virtual_memory().total/1024/1024/1024
        return self.__memoryGB

    def instance_size(self):
        """Returns the size of the instance we are running in. i.e.: 2"""
        instancetypesplit = self.instancetype.split("-")
        return instancetypesplit[2] if len(instancetypesplit)>2 else 0

    def instance_class(self):
        """Returns the class of the instance we are running in. i.e.: n2"""
        return self.instancetype.split("-")[0]

    def instance_purpose(self):
        """Returns the purpose of the instance we are running in. i.e.: standard"""
        return self.instancetype.split("-")[1]

    m1supported="m1-megamem-96" #this is the only exception of supported m1 as per https://cloud.google.com/compute/docs/machine-types#m1_machine_types

    def is_unsupported_instance_class(self):
        """Returns if this instance type belongs to unsupported ones for nvmes"""
        if self.instancetype == self.m1supported:
            return False
        if self.instance_class() in ['e2', 'f1', 'g1', 'm2', 'm1']:
            return True
        return False

    def is_supported_instance_class(self):
        """Returns if this instance type belongs to supported ones for nvmes"""
        if self.instancetype == self.m1supported:
            return True
        if self.instance_class() in ['n1', 'n2', 'n2d' ,'c2']:
            return True
        return False

    def is_recommended_instance_size(self):
        """if this instance has at least 2 cpus, it has a recommended size"""
        if int(self.instance_size()) > 1:
            return True
        return False

    @staticmethod
    def get_file_size_by_seek(filename):
        "Get the file size by seeking at end"
        fd= os.open(filename, os.O_RDONLY)
        try:
            return os.lseek(fd, 0, os.SEEK_END)
        finally:
            os.close(fd)

    # note that GCP has 3TB physical devices actually, which they break into smaller 375GB disks and share the same mem with multiple machines
    # this is a reference value, disk size shouldn't be lower than that
    GCP_NVME_DISK_SIZE_2020=375

    @property
    def firstNvmeSize(self):
        """return the size of first non root NVME disk in GB"""
        if self.__firstNvmeSize is None:
            ephemeral_disks = self.getEphemeralOsDisks()
            if len(ephemeral_disks) > 0:
                firstDisk = ephemeral_disks[0]
                firstDiskSize = self.get_file_size_by_seek(os.path.join("/dev/", firstDisk))
                firstDiskSizeGB = firstDiskSize/1024/1024/1024
                if firstDiskSizeGB >= self.GCP_NVME_DISK_SIZE_2020:
                    self.__firstNvmeSize = firstDiskSizeGB
                else:
                    self.__firstNvmeSize = 0
                    logging.warning("First nvme is smaller than lowest expected size. ".format(firstDisk))
            else:
                self.__firstNvmeSize = 0
        return self.__firstNvmeSize

    def is_recommended_instance(self):
        if not self.is_unsupported_instance_class() and self.is_supported_instance_class() and self.is_recommended_instance_size():
            # at least 1:2GB cpu:ram ratio , GCP is at 1:4, so this should be fine
            if self.cpu/self.memoryGB < 0.5:
                diskCount = self.nvmeDiskCount
                # to reach max performance for > 16 disks we mandate 32 or more vcpus
                # https://cloud.google.com/compute/docs/disks/local-ssd#performance
                if diskCount >= 16 and self.cpu < 32:
                    logging.warning(
                        "This machine doesn't have enough CPUs for allocated number of NVMEs (at least 32 cpus for >=16 disks). Performance will suffer.")
                    return False
                if diskCount < 1:
                    logging.warning("No ephemeral disks were found.")
                    return False
                diskSize = self.firstNvmeSize
                max_disktoramratio = 105
                # 30:1 Disk/RAM ratio must be kept at least(AWS), we relax this a little bit
                # on GCP we are OK with {max_disktoramratio}:1 , n1-standard-2 can cope with 1 disk, not more
                disktoramratio = (diskCount * diskSize) / self.memoryGB
                if (disktoramratio > max_disktoramratio):
                    logging.warning(
                        f"Instance disk-to-RAM ratio is {disktoramratio}, which is higher than the recommended ratio {max_disktoramratio}. Performance may suffer.")
                    return False
                return True
            else:
                logging.warning("At least 2G of RAM per CPU is needed. Performance will suffer.")
        return False

    def private_ipv4(self):
        return self.__instance_metadata("network-interfaces/0/ip")

    @staticmethod
    def check():
        pass

    @staticmethod
    def io_setup():
        return run('/opt/scylladb/scripts/scylla_io_setup', shell=True, check=True)

    @property
    def user_data(self):
        try:
            return self.__instance_metadata("attributes/user-data")
        except urllib.error.HTTPError:  # empty user-data
            return ""


class azure_instance:
    """Describe several aspects of the current Azure instance"""

    EPHEMERAL = "ephemeral"
    ROOT = "root"
    GETTING_STARTED_URL = "http://www.scylladb.com/doc/getting-started-azure/"
    ENDPOINT_SNITCH = "GossipingPropertyFileSnitch"
    META_DATA_BASE_URL = "http://169.254.169.254/metadata/instance"

    def __init__(self):
        self.__type = None
        self.__cpu = None
        self.__location = None
        self.__zone = None
        self.__memoryGB = None
        self.__nvmeDiskCount = None
        self.__firstNvmeSize = None
        self.__osDisks = None

    @staticmethod
    def is_azure_instance():
        """Check if it's Azure instance via DNS lookup to metadata server."""
        try:
            addrlist = socket.getaddrinfo('metadata.azure.internal', 80)
        except socket.gaierror:
            return False
        return True

# as per https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service?tabs=windows#supported-api-versions
    API_VERSION = "?api-version=2021-01-01"

    def __instance_metadata(self, path):
        """query Azure metadata server"""
        return curl(self.META_DATA_BASE_URL + path + self.API_VERSION + "&format=text", headers = { "Metadata": "True" })

    def is_in_root_devs(self, x, root_devs):
        for root_dev in root_devs:
            if root_dev.startswith(os.path.join("/dev/", x)):
                return True
        return False

    def _non_root_nvmes(self):
        """get list of nvme disks from os, filter away if one of them is root"""
        nvme_re = re.compile(r"nvme\d+n\d+$")

        root_dev_candidates = [x for x in psutil.disk_partitions() if x.mountpoint == "/"]
        if len(root_dev_candidates) != 1:
            raise Exception("found more than one disk mounted at root ".format(root_dev_candidates))

        root_devs = [x.device for x in root_dev_candidates]

        nvmes_present = list(filter(nvme_re.match, os.listdir("/dev")))
        return {self.ROOT: root_devs, self.EPHEMERAL: [x for x in nvmes_present if not self.is_in_root_devs(x, root_devs)]}

    @property
    def os_disks(self):
        """populate disks from /dev/ and root mountpoint"""
        if self.__osDisks is None:
            __osDisks = {}
            nvmes_present = self._non_root_nvmes()
            for k, v in nvmes_present.items():
                __osDisks[k] = v
            self.__osDisks = __osDisks
        return self.__osDisks

    def getEphemeralOsDisks(self):
        """return just transient disks"""
        return self.os_disks[self.EPHEMERAL]

    @property
    def nvmeDiskCount(self):
        """get # of nvme disks available for scylla raid"""
        if self.__nvmeDiskCount is None:
            try:
                ephemeral_disks = self.getEphemeralOsDisks()
                count_os_disks = len(ephemeral_disks)
            except Exception as e:
                print("Problem when parsing disks from OS:")
                print(e)
                count_os_disks = 0
            count_metadata_nvme_disks = self.__get_nvme_disks_count_from_metadata()
            self.__nvmeDiskCount = count_os_disks if count_os_disks < count_metadata_nvme_disks else count_metadata_nvme_disks
        return self.__nvmeDiskCount

    instanceToDiskCount = {
        "L8s": 1,
        "L16s": 2,
        "L32s": 4,
        "L48s": 6,
        "L64s": 8,
        "L80s": 10
    }

    def __get_nvme_disks_count_from_metadata(self):
        #storageProfile in VM metadata lacks the number of NVMEs, it's hardcoded based on VM type
        return self.instanceToDiskCount.get(self.instance_class(), 0)

    @property
    def instancelocation(self):
        """return the location of this instance, e.g. eastus"""
        if self.__location is None:
            self.__location = self.__instance_metadata("location")
        return self.__location

    @property
    def instancezone(self):
        """return the zone of this instance, e.g. 1"""
        if self.__zone is None:
            self.__zone = self.__instance_metadata("zone")
        return self.__zone

    @property
    def instancetype(self):
        """return the type of this instance, e.g. Standard_L8s_v2"""
        if self.__type is None:
            self.__type = self.__instance_metadata("/compute/vmSize")
        return self.__type

    @property
    def cpu(self):
        """return the # of cpus of this instance"""
        if self.__cpu is None:
            self.__cpu = psutil.cpu_count()
        return self.__cpu

    @property
    def memoryGB(self):
        """return the size of memory in GB of this instance"""
        if self.__memoryGB is None:
            self.__memoryGB = psutil.virtual_memory().total/1024/1024/1024
        return self.__memoryGB

    def instance_purpose(self):
        """Returns the class of the instance we are running in. i.e.: Standard"""
        return self.instancetype.split("_")[0]

    def instance_class(self):
        """Returns the purpose of the instance we are running in. i.e.: L8s"""
        return self.instancetype.split("_")[1]

    def is_unsupported_instance_class(self):
        """Returns if this instance type belongs to unsupported ones for nvmes"""
        return False

    def is_supported_instance_class(self):
        """Returns if this instance type belongs to supported ones for nvmes"""
        if self.instance_class() in list(self.instanceToDiskCount.keys()):
            return True
        return False

    def is_recommended_instance_size(self):
        """if this instance has at least 2 cpus, it has a recommended size"""
        if int(self.instance_size()) > 1:
            return True
        return False

    def is_recommended_instance(self):
        if self.is_unsupported_instance_class() and self.is_supported_instance_class():
            return True
        return False

    def private_ipv4(self):
        return self.__instance_metadata("/network/interface/0/ipv4/ipAddress/0/privateIpAddress")

    @staticmethod
    def check():
        pass

    @staticmethod
    def io_setup():
        return run('/opt/scylladb/scripts/scylla_io_setup', shell=True, check=True)

class aws_instance:
    """Describe several aspects of the current AWS instance"""
    GETTING_STARTED_URL = "http://www.scylladb.com/doc/getting-started-amazon/"
    META_DATA_BASE_URL = "http://169.254.169.254/latest/"
    ENDPOINT_SNITCH = "Ec2Snitch"

    def __disk_name(self, dev):
        name = re.compile(r"(?:/dev/)?(?P<devname>[a-zA-Z]+)\d*")
        return name.search(dev).group("devname")

    def __instance_metadata(self, path):
        return curl(self.META_DATA_BASE_URL + "meta-data/" + path)

    def __device_exists(self, dev):
        if dev[0:4] != "/dev":
            dev = "/dev/%s" % dev
        return os.path.exists(dev)

    def __xenify(self, devname):
        dev = self.__instance_metadata('block-device-mapping/' + devname)
        return dev.replace("sd", "xvd")

    def _non_root_nvmes(self):
        nvme_re = re.compile(r"nvme\d+n\d+$")

        root_dev_candidates = [ x for x in psutil.disk_partitions() if x.mountpoint == "/" ]
        if len(root_dev_candidates) != 1:
            raise Exception("found more than one disk mounted at root'".format(root_dev_candidates))

        root_dev = root_dev_candidates[0].device
        if root_dev == '/dev/root':
            root_dev = run('findmnt -n -o SOURCE /', shell=True, check=True, capture_output=True, encoding='utf-8').stdout.strip()
        nvmes_present = list(filter(nvme_re.match, os.listdir("/dev")))
        return {"root": [ root_dev ], "ephemeral": [ x for x in nvmes_present if not root_dev.startswith(os.path.join("/dev/", x)) ] }

    def __populate_disks(self):
        devmap = self.__instance_metadata("block-device-mapping")
        self._disks = {}
        devname = re.compile("^\D+")
        nvmes_present = self._non_root_nvmes()
        for k,v in nvmes_present.items():
            self._disks[k] = v

        for dev in devmap.splitlines():
            t = devname.match(dev).group()
            if t == "ephemeral" and nvmes_present:
                continue
            if t not in self._disks:
                self._disks[t] = []
            if not self.__device_exists(self.__xenify(dev)):
                continue
            self._disks[t] += [self.__xenify(dev)]
        if not 'ebs' in self._disks:
            self._disks['ebs'] = []

    def __mac_address(self, nic='eth0'):
        with open('/sys/class/net/{}/address'.format(nic)) as f:
            return f.read().strip()

    def __init__(self):
        self._type = self.__instance_metadata("instance-type")
        self.__populate_disks()

    @classmethod
    def is_aws_instance(cls):
        """Check if it's AWS instance via query to metadata server."""
        try:
            curl(cls.META_DATA_BASE_URL, max_retries=2, retry_interval=1)
            return True
        except (urllib.error.URLError, urllib.error.HTTPError):
            return False

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
        if self.instance_class() in ['i2', 'i3', 'i3en', 'c5d', 'm5d', 'm5ad', 'r5d', 'z1d']:
            return True
        return False

    def get_en_interface_type(self):
        instance_class = self.instance_class()
        instance_size = self.instance_size()
        if instance_class in ['c3', 'c4', 'd2', 'i2', 'r3']:
            return 'ixgbevf'
        if instance_class in ['a1', 'c5', 'c5a', 'c5d', 'c5n', 'c6g', 'c6gd', 'f1', 'g3', 'g4', 'h1', 'i3', 'i3en', 'inf1', 'm5', 'm5a', 'm5ad', 'm5d', 'm5dn', 'm5n', 'm6g', 'm6gd', 'p2', 'p3', 'r4', 'r5', 'r5a', 'r5ad', 'r5b', 'r5d', 'r5dn', 'r5n', 't3', 't3a', 'u-6tb1', 'u-9tb1', 'u-12tb1', 'u-18tn1', 'u-24tb1', 'x1', 'x1e', 'z1d']:
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
        return set(self._disks["ebs"])

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

    @staticmethod
    def check():
        return run('/opt/scylladb/scripts/scylla_ec2_check --nic eth0', shell=True)

    @staticmethod
    def io_setup():
        return run('/opt/scylladb/scripts/scylla_io_setup --ami', shell=True, check=True)

    @property
    def user_data(self):
        return curl(self.META_DATA_BASE_URL + "user-data")


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


def is_ec2():
    return aws_instance.is_aws_instance()

def is_gce():
    return gcp_instance.is_gce_instance()

def is_azure():
    return azure_instance.is_azure_instance()

def get_cloud_instance():
    if is_ec2():
        return aws_instance()
    elif is_gce():
        return gcp_instance()
    elif is_azure():
        return azure_instance()
    else:
        raise Exception("Unknown cloud provider! Only AWS/GCP/Azure supported.")

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
    return run(f'lsblk -n -oPARTTYPE {dev}', shell=True, check=True, capture_output=True, encoding='utf-8').stdout.strip()

def is_system_partition(dev):
    uuid = get_partition_uuid(dev)
    return (uuid in SYSTEM_PARTITION_UUIDS)

def is_unused_disk(dev):
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
    for t in [ "commitlog", "hints", "view_hints", "saved_caches" ]:
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
    swaps = run('swapon --noheadings --raw', shell=True, check=True, capture_output=True, encoding='utf-8').stdout.strip()
    return True if swaps != '' else False

def pkg_error_exit(pkg):
    print(f'Package "{pkg}" required.')
    sys.exit(1)

def yum_install(pkg):
    if is_offline():
        pkg_error_exit(pkg)
    return run(f'yum install -y {pkg}', shell=True, check=True)

def apt_install(pkg):
    if is_offline():
        pkg_error_exit(pkg)
    apt_env = os.environ.copy()
    apt_env['DEBIAN_FRONTEND'] = 'noninteractive'
    return run(f'apt-get install -y {pkg}', shell=True, check=True, env=apt_env)

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
    return run(f'apt-get remove -y {pkg}', shell=True, check=True, env=apt_env)

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
        return True if run('systemctl {} is-active {}'.format(self.ctlparam, self._unit), shell=True, capture_output=True, encoding='utf-8').stdout.strip() == 'active' else False

    def mask(self):
        return run('systemctl {} mask {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    def unmask(self):
        return run('systemctl {} unmask {}'.format(self.ctlparam, self._unit), shell=True, check=True)

    @classmethod
    def reload(cls):
        run('systemctl daemon-reload', shell=True, check=True)

    @classmethod
    def available(cls, unit):
        res = run('systemctl cat {}'.format(unit), shell=True, check=True, stdout=DEVNULL, stderr=DEVNULL)
        return True if res.returncode == 0 else False


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
