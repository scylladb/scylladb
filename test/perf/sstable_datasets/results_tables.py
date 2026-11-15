#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Turn the results of scylla perf-sstable-datasets into markdown tables.

Usage: python3 results_tables.py <results_dir> [--datasets <data_root>] [--table NAME]...

<results_dir>/<name>/results.json is the output of run_dataset.sh for the
dataset <name>. With --datasets, the dataset table also uses
<data_root>/prepared/<name>/meta.json. Every value is the median over the
runs, as results.json gives it. "Net" write costs subtract the median cost
of scanning the source memtable. Ratios are aa_zstd / mt_zstd_dict.

Without --table, all tables are printed, each under a heading with its name.
Standard library only.
"""

import argparse
import json
import os
import sys

CONFIGS = ['mt_zstd', 'mt_zstd_dict', 'aa_zstd']
BASE = 'mt_zstd_dict'
PQ = 'aa_zstd'
ORDER = ['drive_stats', 'ghcn_daily', 'fhvhv_trips', 'github_events', 'reviews', 'trades', 'pageviews', 'price_paid', 'isd_lite']
POINT = ['partition', 'slice', 'row']
KiB = 1024
MiB = 1024 * 1024


def load(results_dir, data_root):
    names = [n for n in os.listdir(results_dir) if os.path.isfile(os.path.join(results_dir, n, 'results.json'))]
    names.sort(key=lambda n: (ORDER.index(n) if n in ORDER else len(ORDER), n))
    ds = []
    for n in names:
        with open(os.path.join(results_dir, n, 'results.json')) as f:
            r = json.load(f)
        meta = None
        if data_root:
            p = os.path.join(data_root, 'prepared', n, 'meta.json')
            if os.path.isfile(p):
                with open(p) as f:
                    meta = json.load(f)
        ds.append((n, r, meta))
    return ds


def med(r, cfg, bench, field):
    c = r['configs'][cfg]
    o = c['write'] if bench == 'write' else c['open'] if bench == 'open' else c['read'][bench]
    return o['median'][field]


def src(r, field):
    return r['source_memtable_scan']['median'][field]


def ratio(a, b):
    return '%.2f' % (a / b) if b else 'n/a'


def table(header, rows):
    out = ['| ' + ' | '.join(header) + ' |', '|' + '|'.join('---' for _ in header) + '|']
    for row in rows:
        out.append('| ' + ' | '.join(str(x) for x in row) + ' |')
    return '\n'.join(out)


def f0(x):
    return '{:,.0f}'.format(x)


def f1(x):
    return '{:,.1f}'.format(x)


def f2(x):
    return '{:,.2f}'.format(x)


def has(r, bench):
    return bench in r['configs'][BASE]['read']


def queries(r, bench):
    return r['keys']


def write_cpu_net(r, cfg):
    return med(r, cfg, 'write', 'thread_cpu_s') - src(r, 'thread_cpu_s')


# ---------------------------------------------------------------- tables

def t_summary(ds):
    rows = []
    for n, r, _ in ds:
        s = r['sizes']
        row = [n, ratio(s[PQ]['total_bytes'], s[BASE]['total_bytes']),
               ratio(med(r, PQ, 'write', 'thread_cpu_s'), med(r, BASE, 'write', 'thread_cpu_s')),
               ratio(write_cpu_net(r, PQ), write_cpu_net(r, BASE)),
               ratio(med(r, PQ, 'full_scan', 'thread_cpu_s'), med(r, BASE, 'full_scan', 'thread_cpu_s'))]
        for b in POINT:
            row.append(ratio(med(r, PQ, b, 'thread_cpu_s'), med(r, BASE, b, 'thread_cpu_s')) if has(r, b) else 'n/a')
        row.append(ratio(med(r, PQ, 'row', 'read_bytes'), med(r, BASE, 'row', 'read_bytes')))
        row.append(ratio(med(r, PQ, 'row', 'read_ops'), med(r, BASE, 'row', 'read_ops')))
        rows.append(row)
    return table(['Dataset', 'Size', 'Write CPU', 'Write CPU, net', 'Full scan CPU',
                  'Partition CPU', 'Slice CPU', 'Row CPU', 'Row bytes read', 'Row I/O ops'], rows)


def t_datasets(ds):
    rows = []
    for n, r, m in ds:
        rpp = m['rows_per_partition'] if m else {}
        rows.append([n, f0(r['rows']), f0(r['partitions']),
                     f0(rpp.get('median', 0)) if m else 'n/a', f0(rpp.get('max', 0)) if m else 'n/a',
                     f1(r['ingestion']['input_bytes'] / MiB), f0(r['ingestion']['input_bytes'] / r['rows']),
                     f0(r['cells']), f1(r['mt_uncompressed_data_bytes'] / MiB)])
    return table(['Dataset', 'Rows', 'Partitions', 'Rows per partition, median', 'Rows per partition, max',
                  'Input JSONL (MiB)', 'Input bytes per row', 'Cells', 'mt uncompressed Data (MiB)'], rows)


def t_sources(ds):
    rows = []
    for n, r, m in ds:
        if not m:
            continue
        rows.append([n, '<br>'.join(m['source_urls']), m['licence']])
    return table(['Dataset', 'Source', 'Licence (as recorded by the converter)'], rows)


def t_dictionary(ds):
    rows = []
    for n, r, _ in ds:
        d = r['dictionary']
        rows.append([n, f0(d['size']), f0(d['samples']), f0(d['sample_size']), f0(d['population_chunks']),
                     f2(d['samples'] / d['population_chunks']),
                     'yes' if r['sizes'][BASE].get('dictionary_matches_trained') else 'NO',
                     f0(r['sizes'][BASE]['components'].get('CompressionInfo.db', 0) - r['sizes']['mt_zstd']['components'].get('CompressionInfo.db', 0))])
    return table(['Dataset', 'Dictionary (B)', 'Samples', 'Sample size (B)', 'Chunks in mt_zstd Data',
                  'Samples / chunks', 'Same dictionary in mt_zstd_dict', 'CompressionInfo.db growth (B)'], rows)


def comp_groups(comps):
    data = comps.get('Data.db', 0)
    index = sum(comps.get(c, 0) for c in ('Partitions.db', 'Rows.db', 'Index.db', 'Summary.db'))
    filt = comps.get('Filter.db', 0)
    ci = comps.get('CompressionInfo.db', 0)
    other = sum(comps.values()) - data - index - filt - ci
    return data, index, filt, ci, other


def t_size(ds):
    rows = []
    for n, r, _ in ds:
        for cfg in CONFIGS:
            s = r['sizes'][cfg]
            data, index, filt, ci, other = comp_groups(s['components'])
            rows.append([n, cfg, f2(s['total_bytes'] / MiB), f2(data / MiB), f0(index / KiB), f0(filt / KiB),
                         f0(ci / KiB), f0(other / KiB),
                         '%.3f' % s['ratio_total_to_mt_uncompressed'], '%.3f' % s['ratio_total_to_input'],
                         ratio(s['total_bytes'], r['sizes'][BASE]['total_bytes'])])
    return table(['Dataset', 'Config', 'Total (MiB)', 'Data.db (MiB)', 'Index (KiB)', 'Filter (KiB)',
                  'CompressionInfo (KiB)', 'Other (KiB)', 'Total / mt uncompressed', 'Total / input',
                  'Total / mt_zstd_dict'], rows)


def t_write(ds):
    rows = []
    for n, r, _ in ds:
        for cfg in CONFIGS:
            cpu = med(r, cfg, 'write', 'thread_cpu_s')
            ins = med(r, cfg, 'write', 'instructions')
            net = write_cpu_net(r, cfg)
            ins_net = ins - src(r, 'instructions') if ins is not None else None
            rows.append([n, cfg, f2(med(r, cfg, 'write', 'wall_s')), f2(cpu), f2(net),
                         f0(net * 1e9 / r['rows']),
                         f1(ins / 1e9) if ins is not None else 'n/a',
                         f1(ins_net / 1e9) if ins_net is not None else 'n/a',
                         f1(med(r, cfg, 'write', 'write_bytes') / MiB), f0(med(r, cfg, 'write', 'write_ops')),
                         ratio(cpu, med(r, BASE, 'write', 'thread_cpu_s')), ratio(net, write_cpu_net(r, BASE))])
        rows.append([n, 'source scan', f2(src(r, 'wall_s')), f2(src(r, 'thread_cpu_s')), '', '',
                     f1(src(r, 'instructions') / 1e9) if src(r, 'instructions') is not None else 'n/a', '', '', '', '', ''])
    return table(['Dataset', 'Config', 'Wall (s)', 'CPU (s)', 'CPU net (s)', 'CPU net per row (ns)',
                  'Instructions (G)', 'Instructions net (G)', 'Written (MiB)', 'Write ops',
                  'CPU / dict', 'CPU net / dict'], rows)


def t_open(ds):
    rows = []
    for n, r, _ in ds:
        row = [n]
        for cfg in CONFIGS:
            row += [f0(med(r, cfg, 'open', 'thread_cpu_s') * 1e6), f0(med(r, cfg, 'open', 'read_bytes') / KiB),
                    f0(med(r, cfg, 'open', 'read_ops'))]
        rows.append(row)
    hdr = ['Dataset']
    for cfg in CONFIGS:
        hdr += [cfg + ' CPU (us)', cfg + ' KiB', cfg + ' ops']
    return table(hdr, rows)


def t_full_scan(ds):
    rows = []
    for n, r, _ in ds:
        for cfg in CONFIGS:
            cpu = med(r, cfg, 'full_scan', 'thread_cpu_s')
            ins = med(r, cfg, 'full_scan', 'instructions')
            rows.append([n, cfg, f2(cpu), f0(cpu * 1e9 / r['rows']), f0(ins / r['rows']) if ins is not None else 'n/a',
                         f1(med(r, cfg, 'full_scan', 'read_bytes') / MiB), f0(med(r, cfg, 'full_scan', 'read_ops')),
                         ratio(cpu, med(r, BASE, 'full_scan', 'thread_cpu_s')),
                         ratio(med(r, cfg, 'full_scan', 'read_bytes'), med(r, BASE, 'full_scan', 'read_bytes'))])
    return table(['Dataset', 'Config', 'CPU (s)', 'CPU per row (ns)', 'Instructions per row', 'Read (MiB)',
                  'Read ops', 'CPU / dict', 'Bytes / dict'], rows)


def t_point(bench):
    def t(ds):
        rows = []
        for n, r, _ in ds:
            if not has(r, bench):
                rows.append([n, 'n/a'] + [''] * 9)
                continue
            q = queries(r, bench)
            for cfg in CONFIGS:
                cpu = med(r, cfg, bench, 'thread_cpu_s')
                ins = med(r, cfg, bench, 'instructions')
                rb = med(r, cfg, bench, 'read_bytes')
                ro = med(r, cfg, bench, 'read_ops')
                rows.append([n, cfg, f0(med(r, cfg, bench, 'rows') / q), f1(cpu * 1e6 / q),
                             f2(ins / q / 1e6) if ins is not None else 'n/a', f1(rb / q / KiB), f1(ro / q),
                             ratio(cpu, med(r, BASE, bench, 'thread_cpu_s')),
                             ratio(rb, med(r, BASE, bench, 'read_bytes')),
                             ratio(ro, med(r, BASE, bench, 'read_ops'))])
        return table(['Dataset', 'Config', 'Rows per query', 'CPU per query (us)', 'Instructions per query (M)',
                      'KiB read per query', 'Read ops per query', 'CPU / dict', 'Bytes / dict', 'Ops / dict'], rows)
    return t


def spread(runs, field):
    v = sorted(x[field] for x in runs if x[field] is not None)
    if not v or v[len(v) // 2] == 0:
        return 0.0
    return (v[-1] - v[0]) / v[len(v) // 2]


def t_spread(ds):
    """The largest run-to-run spread, (max - min) / median of the thread CPU
    time, of each benchmark over the datasets and configurations."""
    rows = []
    benches = ['write', 'open', 'full_scan'] + POINT
    for b in benches:
        worst = (0.0, '')
        for n, r, _ in ds:
            for cfg in CONFIGS:
                c = r['configs'][cfg]
                o = c['write'] if b == 'write' else c['open'] if b == 'open' else c['read'].get(b)
                if not o:
                    continue
                s = spread(o['runs'], 'thread_cpu_s')
                if s > worst[0]:
                    worst = (s, '%s %s' % (n, cfg))
        rows.append([b, '%.1f%%' % (worst[0] * 100), worst[1]])
    return table(['Benchmark', 'Largest CPU spread', 'Where'], rows)


TABLES = {
    'summary': t_summary,
    'datasets': t_datasets,
    'sources': t_sources,
    'dictionary': t_dictionary,
    'size': t_size,
    'write': t_write,
    'open': t_open,
    'full_scan': t_full_scan,
    'partition': t_point('partition'),
    'slice': t_point('slice'),
    'row': t_point('row'),
    'spread': t_spread,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('results_dir')
    ap.add_argument('--datasets', help='data root of prepare_all.sh, for meta.json')
    ap.add_argument('--table', action='append', choices=sorted(TABLES), help='print only this table (repeatable)')
    a = ap.parse_args()
    ds = load(a.results_dir, a.datasets)
    if not ds:
        sys.exit('no results.json under %s' % a.results_dir)
    names = a.table or list(TABLES)
    for i, name in enumerate(names):
        if not a.table:
            print('### %s\n' % name)
        print(TABLES[name](ds))
        if i + 1 < len(names):
            print()


if __name__ == '__main__':
    main()
