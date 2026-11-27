#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""GH Archive, 2024-01-15 15:00 UTC hour -> bench.github_events.

Usage: python3 -I conv_github_events.py <data_root>

payload is the event's payload object re-serialised as compact JSON.
"""
import gzip
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'github_events'
URL = 'https://data.gharchive.org/2024-01-15-15.json.gz'


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME),
                      ['repo_id', 'created_at', 'event_id'], 1)
    with gzip.open(os.path.join(root, 'raw', NAME, os.path.basename(URL)), 'rb') as g:
        for line in g:
            try:
                e = json.loads(line)
            except ValueError:
                w.drop('unparseable json')
                continue
            repo = e.get('repo') or {}
            actor = e.get('actor') or {}
            org = e.get('org') or {}
            payload = e.get('payload')
            w.add({'repo_id': repo.get('id'),
                   'created_at': common.ts_from_iso(e['created_at']) if e.get('created_at') else None,
                   'event_id': int(e['id']) if e.get('id') else None,
                   'event_type': e.get('type'),
                   'repo_name': repo.get('name'),
                   'actor_id': actor.get('id'),
                   'actor_login': actor.get('login'),
                   'org_login': org.get('login'),
                   'is_public': e.get('public'),
                   'payload': None if payload is None else
                   json.dumps(payload, ensure_ascii=False, separators=(',', ':'))})
            if w.full:
                break
    w.close(sources=[URL],
            licence='GH Archive metadata: CC0-like public data (gharchive.org); event contents '
                    'subject to GitHub terms',
            download_bytes=common.raw_bytes(root, NAME),
            description='Semi-structured events with a large JSON-text payload column; partition = '
                        'repository, mostly 1-5 events per repo in one hour, some very hot repos.')


if __name__ == '__main__':
    main(sys.argv[1])
