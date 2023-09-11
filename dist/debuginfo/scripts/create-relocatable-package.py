#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import argparse
import os
import subprocess
import tarfile
import tempfile

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
ap.add_argument('--build-dir', default='build/release',
                help='Build dir ("build/debug" or "build/release") to use')
ap.add_argument('--node-exporter-dir', default='build/node_exporter',
                help='the directory where node_exporter is located')

args = ap.parse_args()

executables_scylla = [
                '{}/scylla'.format(args.build_dir),
                '{}/iotune'.format(args.build_dir)]

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
with tempfile.NamedTemporaryFile('w+t') as version_file:
    version_file.write('2.1\n')
    version_file.flush()
    ar.add(version_file.name, arcname='.relocatable_package_version')

for exe in executables_scylla:
    basename = os.path.basename(exe)
    ar.reloc_add(f'{exe}.debug', arcname=f'libexec/.debug/{basename}.debug')
ar.reloc_add(f'{args.node_exporter_dir}/node_exporter.debug', arcname='node_exporter/.debug/node_exporter.debug')
ar.reloc_add('dist/debuginfo/install.sh', arcname='install.sh')

# Complete the tar output, and wait for the gzip process to complete
ar.close()
gzip_process.communicate()
