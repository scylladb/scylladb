#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Binance spot BTCUSDT trades, daily files from 2024-01-15 -> bench.trades.

Usage: python3 -I conv_trades.py <data_root>

CSV columns (no header): id, price, qty, quote_qty, time, is_buyer_maker,
is_best_match.  Time is in milliseconds for 2024 files (checked: 13 digits).
Whole hours only are kept: the run stops at an hour boundary once the
size target is reached.
"""
import csv
import io
import os
import sys
import zipfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'trades'
SYMBOL = 'BTCUSDT'
DAYS = ['2024-01-15', '2024-01-16', '2024-01-17']
URL = 'https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-%s.zip'
HOUR_MS = 3600 * 1000


def rows(root):
    for d in DAYS:
        z = zipfile.ZipFile(os.path.join(root, 'raw', NAME, os.path.basename(URL % d)))
        with z.open('%s-trades-%s.csv' % (SYMBOL, d)) as f:
            for rec in csv.reader(io.TextIOWrapper(f, encoding='utf-8', newline='')):
                yield d, rec
        z.close()


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME), ['symbol', 'hour_bucket', 'trade_id'], 2)
    used = []
    cur_hour = None
    for d, rec in rows(root):
        if len(rec) != 7 or not rec[0].isdigit():
            w.drop('malformed or header line')
            continue
        t = int(rec[4])
        if t >= 10 ** 14:
            raise SystemExit('timestamp looks like microseconds: %d' % t)
        hour = t - t % HOUR_MS
        if hour != cur_hour:
            if w.full:
                break
            cur_hour = hour
        if d not in used:
            used.append(d)
        w.add({'symbol': SYMBOL,
               'hour_bucket': common.ts_from_ms(hour),
               'trade_id': int(rec[0]),
               'price': float(rec[1]),
               'qty': float(rec[2]),
               'quote_qty': float(rec[3]),
               'trade_time': common.ts_from_ms(t),
               'is_buyer_maker': {'True': True, 'False': False}[rec[5]],
               'is_best_match': {'True': True, 'False': False}[rec[6]]})
    w.close(sources=[URL % d for d in used],
            licence='Binance public market data (data.binance.vision); terms of use, no explicit open licence',
            download_bytes=common.raw_bytes(root, NAME, [os.path.basename(URL % d) for d in used]),
            description='Narrow fixed-width numeric ticks; partition = (symbol, hour) with tens of '
                        'thousands of rows clustered by monotonically increasing trade_id.',
            extra={'time_unit_in_source': 'milliseconds', 'days_used': used})


if __name__ == '__main__':
    main(sys.argv[1])
