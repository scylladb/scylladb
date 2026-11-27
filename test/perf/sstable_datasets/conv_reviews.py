#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Amazon Reviews 2023, Software category -> bench.reviews.

Usage: python3 -I conv_reviews.py <data_root>

Streams a prefix of the file.  Duplicate (parent_asin, timestamp, user_id)
keys keep the first review.
"""
import gzip
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'reviews'
URL = 'https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Software.jsonl.gz'


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME),
                      ['parent_asin', 'review_time', 'user_id'], 1)
    with gzip.open(os.path.join(root, 'raw', NAME, os.path.basename(URL)), 'rb') as g:
        for line in g:
            try:
                e = json.loads(line)
            except ValueError:
                w.drop('unparseable json')
                continue
            images = e.get('images')
            hv = e.get('helpful_vote')
            w.add({'parent_asin': e.get('parent_asin'),
                   'review_time': common.ts_from_ms(e['timestamp']) if e.get('timestamp') is not None else None,
                   'user_id': e.get('user_id'),
                   'asin': e.get('asin'),
                   'rating': None if e.get('rating') is None else float(e['rating']),
                   'title': e.get('title'),
                   'body': e.get('text'),
                   'images_json': None if images is None else
                   json.dumps(images, ensure_ascii=False, separators=(',', ':')),
                   'helpful_vote': None if hv is None else int(hv),
                   'verified_purchase': e.get('verified_purchase')})
            if w.full:
                break
    w.close(sources=[URL],
            licence='Amazon Reviews 2023 (McAuley Lab, UCSD): research use; no explicit open licence',
            download_bytes=common.raw_bytes(root, NAME),
            description='Text-heavy rows (title + body up to tens of KB); partition = product, '
                        'clustered newest-first; heavily skewed partition sizes.')


if __name__ == '__main__':
    main(sys.argv[1])
