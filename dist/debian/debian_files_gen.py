#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either args.version 3 of the License, or
# (at your option) any later args.version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

import string
import os
import shutil
import re
from pathlib import Path
import argparse

arg_parser = argparse.ArgumentParser('Genrate files for debian packager')
arg_parser.add_argument('-p', '--product')
arg_parser.add_argument('-v', '--version')
arg_parser.add_argument('-r', '--release')
args = arg_parser.parse_args()

class DebianFilesTemplate(string.Template):
    delimiter = '%'

scriptdir = os.path.dirname(__file__)

with open(os.path.join(scriptdir, 'changelog.template')) as f:
    changelog_template = f.read()

with open(os.path.join(scriptdir, 'control.template')) as f:
    control_template = f.read()

if os.path.exists('build/debian/debian'):
    shutil.rmtree('build/debian/debian')
shutil.copytree('dist/debian/debian', 'build/debian/debian')

if args.product != 'scylla':
    for p in Path('build/debian/debian').glob('scylla-*'):
        # pat1: scylla-server.service
        #    -> scylla-enterprise-server.scylla-server.service
        # pat2: scylla-server.scylla-fstrim.service
        #    -> scylla-enterprise-server.scylla-fstrim.service
        # pat3: scylla-conf.install
        #    -> scylla-enterprise-conf.install

        if m := re.match(r'^scylla(-[^.]+)\.service$', p.name):
            p.rename(p.parent / f'{args.product}{m.group(1)}.{p.name}')
        elif m := re.match(r'^scylla(-[^.]+\.scylla-[^.]+\.[^.]+)$', p.name):
            p.rename(p.parent / f'{args.product}{m.group(1)}')
        else:
            p.rename(p.parent / p.name.replace('scylla', args.product, 1))

s = DebianFilesTemplate(changelog_template)
changelog_applied = s.substitute(product=args.product, version=args.version, release=args.release, revision='1', codename='stable')

s = DebianFilesTemplate(control_template)
control_applied = s.substitute(product=args.product)

with open('build/debian/debian/changelog', 'w') as f:
    f.write(changelog_applied)

with open('build/debian/debian/control', 'w') as f:
    f.write(control_applied)

