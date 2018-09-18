#!/usr/bin/python3

#
# Copyright (C) 2016 ScyllaDB
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

import os


# try to write data to a sysfs path, expect problems
def try_write(path, data):
    try:
        open(path, 'w').write(data)
    except Exception:
        print("warning: unable to tune {} to {}".format(path, data))


# update a sysfs path if it does not satisfy a check
# function (default = check that the data is already there)
def tune_path(path, data, check=None):
    def default_check(current):
        return current == data
    if check is None:
        check = default_check
    if not os.path.exists(path):
        return
    if check(open(path).read().strip()):
        print('already tuned: {}'.format(path))
        return
    print('tuning: {} {}'.format(path, data))
    try_write(path, data + '\n')


tuned_blockdevs = set()


# tune a blockdevice (sysfs node); updates I/O scheduler
# and merge behavior.  Tunes dependent devices
def tune_blockdev(path, nomerges):
    from os.path import join, exists, dirname, realpath
    path = realpath(path)
    print('tuning {}'.format(path))
    if path in tuned_blockdevs:
        return
    tuned_blockdevs.add(path)

    def check_sched(current):
        return current == 'none' or '[noop]' in current

    if not nomerges:
        tune_path(join(path, 'queue', 'scheduler'), 'noop', check_sched)
        tune_path(join(path, 'queue', 'nomerges'), '2')
    else:
        tune_path(join(path, 'queue', 'nomerges'), nomerges)
    slaves = join(path, 'slaves')
    if exists(slaves):
        for slave in os.listdir(slaves):
            tune_blockdev(join(slaves, slave), nomerges)
    if exists(join(path, 'partition')):
        tune_blockdev(dirname(path), nomerges)


# tunes a /dev/foo blockdev
def tune_dev(path, nomerges):
    dev = os.stat(path).st_rdev
    devfile = '/sys/dev/block/{}:{}'.format(dev // 256, dev % 256)
    tune_blockdev(devfile, nomerges)


# tunes a filesystem
# FIXME: btrfs
def tune_fs(path, nomerges):
    dev = os.stat(path).st_dev
    devfile = '/sys/dev/block/{}:{}'.format(dev // 256, dev % 256)
    tune_blockdev(devfile, nomerges)


# tunes all filesystems referenced from a scylla.yaml
def tune_yaml(path, nomerges):
    import yaml
    y = yaml.load(open(path))
    for fs in y['data_file_directories']:
        tune_fs(fs, nomerges)
    tune_fs(y['commitlog_directory'], nomerges)
