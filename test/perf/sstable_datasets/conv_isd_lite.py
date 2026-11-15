#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""NOAA ISD-Lite 2023, US stations -> bench.isd_lite.

Usage: python3 -I conv_isd_lite.py <data_root>
       python3 -I conv_isd_lite.py --candidates <data_root>

ISD-Lite has one gzip file of hourly observations per station and year.
The candidate stations are those of isd-history.csv with country US whose
period of record covers all of 2023, ordered by (crc32(station_id),
station_id).  --candidates prints the first N_CANDIDATES of them, which
download.sh fetches.  The conversion takes the candidates in that order,
keeps the stations with at least MIN_ROWS hourly rows in 2023, and stops
after the station that reaches the size target.  Only US stations are used
because the non-US data in ISD are subject to WMO Resolution 40, which
restricts their redistribution.

Values stay in the units and scaling of ISD-Lite: temperatures, pressure,
wind speed and precipitation are tenths (degrees C, hPa, m/s, mm), wind
direction is degrees (0 = calm), sky condition is a code, and trace
precipitation is -1.  The missing value -9999 becomes an absent cell.
"""
import csv
import datetime
import gzip
import os
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'isd_lite'
YEAR = 2023
BASE = 'https://noaa-isd-pds.s3.amazonaws.com'
HISTORY_URL = BASE + '/isd-history.csv'
FILE_URL = BASE + '/isd-lite/data/%d/<station_id>-%d.gz' % (YEAR, YEAR)
N_CANDIDATES = 600
MIN_ROWS = 8322            # 95% of the 8760 hours of 2023
MISSING = -9999
# (column, start offset) of the eight 6-character value fields
FIELDS = [('air_temp', 13), ('dew_point', 19), ('sea_level_pressure', 25),
          ('wind_direction', 31), ('wind_speed', 37), ('sky_condition', 43),
          ('precip_1h', 49), ('precip_6h', 55)]
LINE_LEN = 61


def candidates(root):
    """Station ids 'USAF-WBAN' in selection order."""
    ids = set()
    lo, hi = '%d0101' % YEAR, '%d1231' % YEAR
    with open(os.path.join(root, 'raw', NAME, 'isd-history.csv'), newline='', encoding='latin-1') as f:
        for r in csv.DictReader(f):
            if r['CTRY'] == 'US' and r['BEGIN'] <= lo and r['END'] >= hi:
                ids.add(r['USAF'] + '-' + r['WBAN'])
    return sorted(ids, key=lambda s: (zlib.crc32(s.encode()), s))[:N_CANDIDATES]


def file_name(sid):
    return '%s-%d.gz' % (sid, YEAR)


def read_station(w, path):
    """Parse one station file -> list of (obs_time, values); count bad lines."""
    rows = []
    with gzip.open(path, 'rt', encoding='ascii') as g:
        for line in g:
            line = line.rstrip('\n')
            try:
                if len(line) != LINE_LEN:
                    raise ValueError
                t = datetime.datetime(int(line[0:4]), int(line[5:7]), int(line[8:10]), int(line[11:13]))
                vals = [(c, int(line[o:o + 6])) for c, o in FIELDS]
            except ValueError:
                w.drop('malformed line')
                continue
            if t.year != YEAR:
                w.drop('observation outside %d' % YEAR)
                continue
            rows.append((t, vals))
    return rows


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME), ['station_id', 'obs_time'], 1)
    raw = os.path.join(root, 'raw', NAME)
    cands = candidates(root)
    stations = []
    absent = 0
    short = 0
    for sid in cands:
        if w.full:
            break
        path = os.path.join(raw, file_name(sid))
        if not os.path.exists(path):
            absent += 1
            continue
        rows = read_station(w, path)
        if len(rows) < MIN_ROWS:
            short += 1
            continue
        stations.append(sid)
        for t, vals in rows:
            row = {'station_id': sid, 'obs_time': common.ts_from_dt(t)}
            present = 0
            for c, v in vals:
                if v != MISSING:
                    row[c] = v
                    present += 1
            if present == 0:
                w.note('rows with every value missing')
            w.add(row)
    if not w.full:
        raise SystemExit('size target not reached; raise N_CANDIDATES')
    w.close(sources=[HISTORY_URL, FILE_URL],
            licence='NOAA open data (NODD): open to the public, attribution requested, no implied '
                    'endorsement (registry.opendata.aws/noaa-isd); cite NOAA NCEI (2001), Global Surface '
                    'Hourly. US stations only: the non-US data in ISD are subject to WMO Resolution 40.',
            download_bytes=common.raw_bytes(root, NAME),
            description='Numeric hourly time series; partition = US weather station (USAF-WBAN) with up '
                        'to 8760 hourly rows of 2023, clustered by obs_time; 8 int columns in ISD-Lite '
                        'scaling, -9999 omitted.',
            extra={'candidate_rule': 'isd-history.csv CTRY=US, BEGIN<=%d0101, END>=%d1231, ordered by '
                                     '(crc32(station_id), station_id), first %d' % (YEAR, YEAR, N_CANDIDATES),
                   'station_rule': 'at least %d rows in %d; stop at the size target' % (MIN_ROWS, YEAR),
                   'candidates_examined': absent + short + len(stations),
                   'candidates_without_file': absent,
                   'stations_below_min_rows': short,
                   'stations': stations})


if __name__ == '__main__':
    if sys.argv[1] == '--candidates':
        print('\n'.join(candidates(sys.argv[2])))
    else:
        main(sys.argv[1])
