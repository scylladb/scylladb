#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import argparse
import string
import os
import shutil
import re
import subprocess
from pathlib import Path

class DebianFilesTemplate(string.Template):
    delimiter = '%'


def get_version(builddir):
    with open(os.path.join(builddir, 'SCYLLA-VERSION-FILE')) as f:
        return f.read().strip().replace('-', '~')


def get_release(builddir):
    with open(os.path.join(builddir, 'SCYLLA-RELEASE-FILE')) as f:
        return f.read().strip()


def get_product(builddir):
    with open(os.path.join(builddir, 'SCYLLA-PRODUCT-FILE')) as f:
        return f.read().strip()


def generate_control(scriptdir, outputdir, product):
    with open(os.path.join(scriptdir, 'control.template')) as f:
        control_template = f.read()
    s = DebianFilesTemplate(control_template)
    control_applied = s.substitute(product=product)
    with open(os.path.join(outputdir, 'control'), 'w', encoding='utf-8') as f:
        f.write(control_applied)


def generate_changelog(scriptdir, outputdir, product, version, release):
    with open(os.path.join(scriptdir, 'changelog.template')) as f:
        changelog_template = f.read()
    s = DebianFilesTemplate(changelog_template)
    changelog_applied = s.substitute(product=product,
                                     version=version,
                                     release=release,
                                     revision='1',
                                     codename='stable')
    with open(os.path.join(outputdir, 'changelog'), 'w', encoding='utf-8') as f:
        f.write(changelog_applied)


def generate_include_binaries(outputdir):
    include_binaries = subprocess.run(
        "./scripts/create-relocatable-package.py --print-libexec -",
        shell=True,
        check=True,
        capture_output=True,
        encoding='utf-8').stdout
    with open(os.path.join(outputdir, 'source', 'include-binaries'), 'w', encoding='utf-8') as f:
        f.write(include_binaries)


def rename_to(outputdir, from_product, to_product):
    for p in Path(outputdir).glob('scylla-*'):
        # pat1: scylla-server.service
        #    -> scylla-enterprise-server.scylla-server.service
        #       or
        #       scylla-server.default
        #    -> scylla-enterprise-server.scylla-server.default
        # pat2: scylla-server.scylla-fstrim.service
        #    -> scylla-enterprise-server.scylla-fstrim.service
        # pat3: scylla-conf.install
        #    -> scylla-enterprise-conf.install

        if m := re.match(r'^scylla(-[^.]+)\.(service|default)$', p.name):
            p.rename(p.parent / f'{to_product}{m.group(1)}.{p.name}')
        elif m := re.match(r'^scylla(-[^.]+\.scylla-[^.]+\.[^.]+)$', p.name):
            p.rename(p.parent / f'{to_product}{m.group(1)}')
        else:
            p.rename(p.parent / p.name.replace(from_product, to_product, 1))


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--build-dir', action='store', default='build',
                            help='build directory')
    arg_parser.add_argument('--output-dir', action='store', default='debian/debian',
                            help='output directory')
    args = arg_parser.parse_args()
    builddir = args.build_dir
    if os.path.isabs(args.output_dir):
        outputdir = args.output_dir
    else:
        outputdir = os.path.join(builddir, args.output_dir)

    if os.path.exists(outputdir):
        shutil.rmtree(outputdir)
    shutil.copytree('dist/debian/debian', outputdir)

    product = get_product(builddir)
    if product != 'scylla':
        rename_to(outputdir, 'scylla', product)
    scriptdir = os.path.dirname(__file__)
    generate_changelog(scriptdir, outputdir, product, get_version(builddir), get_release(builddir))
    generate_control(scriptdir, outputdir, product)
    generate_include_binaries(outputdir)


if __name__ == '__main__':
    main()
