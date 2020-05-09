#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 ScyllaDB
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
               '/usr/bin/hwloc-calc']

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
pathlib.Path('build/SCYLLA-RELOCATABLE-FILE').touch()
ar.add('build/SCYLLA-RELOCATABLE-FILE', arcname='SCYLLA-RELOCATABLE-FILE')

for exe in executables:
    basename = os.path.basename(exe)
    ar.add(exe, arcname='libexec/' + basename)
for lib, libfile in libs.items():
    ar.add(libfile, arcname='libreloc/' + lib)
if have_gnutls:
    gnutls_config_nolink = os.path.realpath('/etc/crypto-policies/back-ends/gnutls.config')
    ar.add(gnutls_config_nolink, arcname='libreloc/gnutls.config')
ar.add('conf')
ar.add('dist', filter=filter_dist)
ar.add('build/SCYLLA-RELEASE-FILE', arcname='SCYLLA-RELEASE-FILE')
ar.add('build/SCYLLA-VERSION-FILE', arcname='SCYLLA-VERSION-FILE')
ar.add('build/SCYLLA-PRODUCT-FILE', arcname='SCYLLA-PRODUCT-FILE')
ar.add('seastar/scripts')
ar.add('seastar/dpdk/usertools')
ar.add('install.sh')
# scylla_post_install.sh lives at the top level together with install.sh in the src tree, but while install.sh is
# not distributed in the .rpm and .deb packages, scylla_post_install is, so we'll add it in the package
# together with the other scripts that will end up in /usr/lib/scylla
ar.add('scylla_post_install.sh', arcname="dist/common/scripts/scylla_post_install.sh")
ar.add('README.md')
ar.add('NOTICE.txt')
ar.add('ORIGIN')
ar.add('licenses')
ar.add('swagger-ui')
ar.add('api')
ar.add('tools')
ar.add('scylla-gdb.py')
ar.add('build/debian/debian', arcname='debian')

# Complete the tar output, and wait for the gzip process to complete
ar.close()
gzip_process.communicate()
