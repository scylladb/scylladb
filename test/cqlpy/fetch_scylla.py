#!/usr/bin/python
# Copyright (C) 2024-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
# Fetch from ScyllaDB's S3 bucket (downloads.scylladb.com) a pre-compiled
# version of Scylla for a desired release, ready to be run by tests (run.py).
# What we need to fetch is a "relocatable" package containing just the Scylla
# executable and the shared libraries it needs - we don't need a full OS and
# not even stuff like JMX or Python that cqlpy tests don't need.
#
# Can fetch released versions with names like:
#   * 5.4.7
#   * 5.4 (latest in the 5.4 branch, e.g., 5.4.7)
#   * 5.4.0~rc2 (prerelease)
#   * 2021.1.9 (Enterprise release)
#   * 2023.1 (latest in this branch, Enterprise release)

import boto3
import sys
import os
import subprocess

# Parse version 5.6.2 into the array [5, 6, 2]. Ignore rc numbers.
def parse_version(version):
    return version.split('~')[0].split('.')

def s3_download(s3, bucket, object_key, out):
    print(f'Downloading {object_key}... ', end='')
    if not sys.stdout.isatty():
        sys.stdout.flush() # show the "Downloading" message at least
        bucket.download_file(object_key, out)
    else:
        # When interactive, show download progress:
        print('')
        objlen = s3.Object(bucket.name, object_key).content_length
        downloaded = 0
        def progress(chunk):
            nonlocal downloaded
            downloaded += chunk
            print('\33[2K\r%02d%%  %d MB' % (int(downloaded*100/objlen), int(downloaded/1024/1024)), end='')
        bucket.download_file(object_key, out, Callback=progress)
        print('')
    print('done.')

# Download Scylla release "release" (e.g., 5.4, 5.4.7, 2023.1, 2023.1.7)
# into a subdirectory of directory "dir" named by the specific release
# downloaded (e.g., 5.4.7). Returns the ready-to-use scylla executable
# (actually, a shell-script wrapper) in that subdirectory - or raise exception
# if this release cannot be found.
# If the same download was already done in the past, discover this as quickly
# as possible and don't download again. A 3-component release like 5.4.7
# doesn't even require a network lookup, a 2-component release like 5.4 does
# require a lookup to see if a newer 5.4 release appeared - but may not
# require a full download if the same release remains the latest.
def download_scylla(release, dir):
    scylla_arch = 'x86_64'

    v = parse_version(release)
    # If release has 3 components (e.g., 5.4.7) we know exactly which version
    # we are looking for, and can check if we already downloaded it without
    # even fetching the list of versions from s3:
    if len(v) == 3 and os.path.exists(dir + '/' + release + '/scylla_wrapper'):
        print(f'Already downloaded, in {dir}/{release}')
        return dir + '/' + release + '/scylla_wrapper'
    major = f'{v[0]}.{v[1]}'
    bucket = f'downloads.scylladb.com'
    scylla_arch_string = '.' + scylla_arch + '.'
    if int(v[0]) >= 2023: # Enterprise release (new organization)
        prefix = f'downloads/scylla-enterprise/relocatable/scylladb-{major}/scylla-enterprise-{release}'
    elif int(v[0]) > 2000: # Enterprise release (old organization)
        prefix = f'downloads/scylla-enterprise/relocatable/scylladb-{major}/scylla-enterprise-{scylla_arch}-package-{release}'
        scylla_arch_string = '' # arch already restricted by prefix
    elif [int(v[0]),int(v[1])] <= [4,5]: # Open source (very old organization)
        prefix = f'downloads/scylla/relocatable/scylladb-{major}/scylla-package-{release}'
        scylla_arch_string = '' # no arch (only x86)
    elif [int(v[0]),int(v[1])] <= [5,1]: # Open source (old organization)
        prefix = f'downloads/scylla/relocatable/scylladb-{major}/scylla-{scylla_arch}-package-{release}'
        scylla_arch_string = '' # arch already restricted by prefix
    else: # Open source (new organization)
        prefix = f'downloads/scylla/relocatable/scylladb-{major}/scylla-{release}'
    # This prefix has many different packages belonging to all releases in
    # the same major version. We need to look only for those matching the
    # minor version, and take the highest minor number.
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    matches = bucket.objects.filter(Prefix=prefix)
    candidates = [o.key.removeprefix(prefix) for o in matches if scylla_arch_string in o.key]
    # candidates for release 5.4 look like ".2-0.20240117.6c625e8cd3c6.x86_64.tar.gz"
    # refering to 5.4.2. Remove the first character (can be . or -) and take
    # the one with highest number (assume it is up to two digits).
    # Some old Enterprise releases used the convension of rc7 instead of
    # 0~rc7, so the rcs come first in this sorting instead of last, so we
    # need to fix that too.
    def comparison_key(x):
        x = x[1:].replace('.', '-').split('-')[0]
        if x.startswith('rc'):
            return '00~'+x
        elif x.startswith('0~rc'):
            return '0'+x
        else:
            return '%02d' % (int(x))
    candidates = sorted(candidates, key=comparison_key, reverse=True)
    if not candidates:
        raise Exception(f"Can't find release {release}")
    chosen = prefix + candidates[0]
    # different versions used different delimeters
    candidates[0] = candidates[0].replace('.', '-')
    minor = candidates[0].split('-')[1]
    chosen_release = release if len(v)==3 else major + '.' + minor
    print(f'Chosen download for ScyllaDB {release}: {chosen_release} ({chosen})')
    
    out_dir = dir + '/' + chosen_release
    out_wrapper = out_dir + '/scylla_wrapper'
    if os.path.exists(out_wrapper):
        # We already downloaded this version, nothing left to do.
        print(f'Already downloaded, in {out_dir}')
        return out_wrapper
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    out_tar = out_dir + '/download.tgz'
    s3_download(s3, bucket, chosen, out_tar)
    subprocess.run(['tar', '--extract', '--file', out_tar,
        '--strip-components=1', '-C', out_dir])
    os.unlink(out_tar)
    with open(out_wrapper, 'w') as f:
        f.write(f'#!/bin/sh\nLD_LIBRARY_PATH={out_dir}/libreloc {out_dir}/libreloc/ld.so {out_dir}/libexec/scylla "$@"')
    os.chmod(out_wrapper, 0o755)
    print(f'Downloaded to {out_dir}')
    return out_wrapper
