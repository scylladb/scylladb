#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2018-present ScyllaDB
#

#
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
#

import argparse
import io
import os
import subprocess
import tarfile
import pathlib


RELOC_PREFIX='scylla'
def reloc_add(self, name, arcname=None, recursive=True, *, filter=None):
    if arcname:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, arcname),
                        filter=filter)
    else:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, name),
                        filter=filter)

tarfile.TarFile.reloc_add = reloc_add

def ldd(executable):
    '''Given an executable file, return a dictionary with the keys
    containing its shared library dependencies and the values pointing
    at the files they resolve to. A fake key ld.so points at the
    dynamic loader.'''
    libraries = {}
    for ldd_line in subprocess.check_output(
            ['ldd', executable],
            universal_newlines=True).splitlines():
        elements = ldd_line.split()
        if ldd_line.endswith('not found'):
            raise Exception('ldd {}: could not resolve {}'.format(executable, elements[0]))
        if elements[1] != '=>':
            if elements[0].startswith('linux-vdso.so'):
                # provided by kernel
                continue
            libraries['ld.so'] = os.path.realpath(elements[0])
        elif '//' in elements[0]:
            # We know that the only DSO with a // in the path is the
            # dynamic linker used by scylla, which is the same ld.so
            # above.
            pass
        else:
            libraries[elements[0]] = os.path.realpath(elements[2])
    return libraries

def filter_dist(info):
    for x in ['dist/ami/files/', 'dist/ami/packer', 'dist/ami/variables.json']:
        if info.name.startswith(x):
            return None
    return info

SCYLLA_DIR='scylla-package'
def reloc_add(ar, name, arcname=None):
    ar.add(name, arcname="{}/{}".format(SCYLLA_DIR, arcname if arcname else name))

ap = argparse.ArgumentParser(description='Create a relocatable scylla package.')
ap.add_argument('dest',
                help='Destination file (tar format)')
ap.add_argument('--mode', dest='mode', default='release',
                help='Build mode (debug/release) to use')

args = ap.parse_args()

executables = ['build/{}/scylla'.format(args.mode),
               'build/{}/iotune'.format(args.mode),
               '/usr/bin/patchelf',
               '/usr/bin/lscpu',
               '/usr/bin/gawk',
               '/usr/bin/gzip',
               '/usr/sbin/ifconfig',
               '/usr/sbin/ethtool',
               '/usr/bin/netstat',
               '/usr/bin/hwloc-distrib',
               '/usr/bin/hwloc-calc',
               '/usr/bin/lsblk']

output = args.dest

libs = {}
for exe in executables:
    libs.update(ldd(exe))

# manually add libthread_db for debugging thread
libs.update({'libthread_db.so.1': '/lib64/libthread_db-1.0.so'})

ld_so = libs['ld.so']

have_gnutls = any([lib.startswith('libgnutls.so')
                   for lib in libs.keys()])

# Although tarfile.open() can write directly to a compressed tar by using
# the "w|gz" mode, it does so using a slow Python implementation. It is as
# much as 3 times faster (!) to output to a pipe running the external gzip
# command. We can complete the compression even faster by using the pigz
# command - a parallel implementation of gzip utilizing all processors
# instead of just one.
gzip_process = subprocess.Popen("pigz > "+output, shell=True, stdin=subprocess.PIPE)

ar = tarfile.open(fileobj=gzip_process.stdin, mode='w|')
# relocatable package format version = 2.1
with open('build/.relocatable_package_version', 'w') as f:
    f.write('2.1\n')
ar.add('build/.relocatable_package_version', arcname='.relocatable_package_version')

for exe in executables:
    basename = os.path.basename(exe)
    ar.reloc_add(exe, arcname='libexec/' + basename)
for lib, libfile in libs.items():
    ar.reloc_add(libfile, arcname='libreloc/' + lib)
if have_gnutls:
    gnutls_config_nolink = os.path.realpath('/etc/crypto-policies/back-ends/gnutls.config')
    ar.reloc_add(gnutls_config_nolink, arcname='libreloc/gnutls.config')
    ar.reloc_add('conf')
ar.reloc_add('dist', filter=filter_dist)
pathlib.Path('build/SCYLLA-RELOCATABLE-FILE').touch()
ar.reloc_add('build/SCYLLA-RELOCATABLE-FILE', arcname='SCYLLA-RELOCATABLE-FILE')
ar.reloc_add('build/SCYLLA-RELEASE-FILE', arcname='SCYLLA-RELEASE-FILE')
ar.reloc_add('build/SCYLLA-VERSION-FILE', arcname='SCYLLA-VERSION-FILE')
ar.reloc_add('build/SCYLLA-PRODUCT-FILE', arcname='SCYLLA-PRODUCT-FILE')
ar.reloc_add('seastar/scripts')
ar.reloc_add('seastar/dpdk/usertools')
ar.reloc_add('install.sh')
# scylla_post_install.sh lives at the top level together with install.sh in the src tree, but while install.sh is
# not distributed in the .rpm and .deb packages, scylla_post_install is, so we'll add it in the package
# together with the other scripts that will end up in /usr/lib/scylla
ar.reloc_add('scylla_post_install.sh', arcname="dist/common/scripts/scylla_post_install.sh")
ar.reloc_add('README.md')
ar.reloc_add('NOTICE.txt')
ar.reloc_add('ORIGIN')
ar.reloc_add('licenses')
ar.reloc_add('swagger-ui')
ar.reloc_add('api')
def exclude_submodules(tarinfo):
    if tarinfo.name in ('scylla/tools/jmx',
                        'scylla/tools/java',
                        'scylla/tools/python3'):
        return None
    return tarinfo
ar.reloc_add('tools', filter=exclude_submodules)
ar.reloc_add('scylla-gdb.py')
ar.reloc_add('build/debian/debian', arcname='debian')
ar.reloc_add('build/node_exporter', arcname='node_exporter')
ar.reloc_add('ubsan-suppressions.supp')

# Complete the tar output, and wait for the gzip process to complete
ar.close()
gzip_process.communicate()
