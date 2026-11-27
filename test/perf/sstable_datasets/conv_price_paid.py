#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""HM Land Registry Price Paid Data 2023 (+ 2022 prefix) -> bench.price_paid.

Usage: python3 -I conv_price_paid.py <data_root>

CSV without header: transaction_id ({GUID}), price, transfer date
('YYYY-MM-DD 00:00'), postcode, property_type, old_new, duration, paon,
saon, street, locality, town, district, county, ppd_category, record_status.
Rows with an empty postcode are dropped.
"""
import csv
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'price_paid'
FILES = ['pp-2023.csv', 'pp-2022.csv']
URL = 'https://price-paid-data.publicdata.landregistry.gov.uk/%s'
COLS = ['transaction_id', 'price', 'transfer_date', 'postcode', 'property_type', 'old_new',
        'duration', 'paon', 'saon', 'street', 'locality', 'town', 'district', 'county',
        'ppd_category', 'record_status']


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME),
                      ['postcode', 'transfer_date', 'transaction_id'], 1)
    used = []
    for fn in FILES:
        if w.full:
            break
        used.append(fn)
        with open(os.path.join(root, 'raw', NAME, fn), encoding='utf-8', newline='') as f:
            for rec in csv.reader(f):
                if len(rec) != len(COLS):
                    w.drop('malformed line')
                    continue
                d = dict(zip(COLS, rec))
                row = {c: common.opt_text(v) for c, v in d.items()}
                row['transaction_id'] = str(uuid.UUID(d['transaction_id'].strip('{}')))
                row['price'] = int(d['price'])
                row['transfer_date'] = common.date_str(d['transfer_date'])
                if not d['postcode']:
                    w.drop('empty postcode')
                    continue
                w.add(row)
                if w.full:
                    break
    w.close(sources=[URL % fn for fn in used],
            licence='Open Government Licence v3.0 (contains HM Land Registry data, Crown copyright)',
            download_bytes=common.raw_bytes(root, NAME, used),
            description='Address-like text columns with low-cardinality codes and a uuid clustering '
                        'column; partition = postcode, mostly 1-10 sales per postcode.',
            extra={'files_used': used})


if __name__ == '__main__':
    main(sys.argv[1])
