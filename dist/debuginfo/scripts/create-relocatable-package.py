#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import argparse
import io
import os
import subprocess
import tarfile
import pathlib
import shutil


RELOC_PREFIX='scylla-debuginfo'
def reloc_add(self, name, arcname=None, recursive=True, *, filter=None):
    if arcname:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, arcname),
                        filter=filter)
    else:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, name),
                        filter=filter)

tarfile.TarFile.reloc_add = reloc_add

SCYLLA_DIR='scylla-debuginfo-package'
def reloc_add(ar, name, arcname=None):
    ar.add(name, arcname="{}/{}".format(SCYLLA_DIR, arcname if arcname else name))

ap = argparse.ArgumentParser(description='Create a relocatable scylla-debuginfo package.')
ap.add_argument('dest',
                help='Destination file (tar format)')
ap.add_argument('--mode', dest='mode', default='release',
                help='Build mode (debug/release) to use')

args = ap.parse_args()

executables_scylla = [
                'build/{}/scylla'.format(args.mode),
                'build/{}/iotune'.format(args.mode)]

output = args.dest

# Although tarfile.open() can write directly to a compressed tar by using
# the "w|gz" mode, it does so using a slow Python implementation. It is as
# much as 3 times faster (!) to output to a pipe running the external gzip
# command. We can complete the compression even faster by using the pigz
# command - a parallel implementation of gzip utilizing all processors
# instead of just one.
gzip_process = subprocess.Popen("pigz > "+output, shell=True, stdin=subprocess.PIPE)

ar = tarfile.open(fileobj=gzip_process.stdin, mode='w|')
# relocatable package format version = 2.1
try:
    shutil.rmtree(f'build/{SCYLLA_DIR}')
except FileNotFoundError:
    pass
os.makedirs(f'build/{SCYLLA_DIR}')
with open(f'build/{SCYLLA_DIR}/.relocatable_package_version', 'w') as f:
    f.write('2.1\n')
ar.add(f'build/{SCYLLA_DIR}/.relocatable_package_version', arcname='.relocatable_package_version')

for exe in executables_scylla:
    basename = os.path.basename(exe)
    ar.reloc_add(f'{exe}.debug', arcname=f'libexec/.debug/{basename}.debug')
ar.reloc_add('build/node_exporter/node_exporter.debug', arcname='node_exporter/.debug/node_exporter.debug')
ar.reloc_add('dist/debuginfo/install.sh', arcname='install.sh')

# Complete the tar output, and wait for the gzip process to complete
ar.close()
gzip_process.communicate()
