#!/usr/bin/env python3

import os, sys, re, subprocess

def parse_log_output_1(input_file, output_file):
    import re

    reactor_stalled_re = re.compile(r'Reactor stalled for (\d+) ms on shard \d+, in scheduling group [^.]*\. Backtrace: (.*)')


    print(f'processing {input_file}')

    with open(input_file, 'r', encoding='utf8') as inp:
        data = inp.readlines()

    for x in range(len(data)):
        l = data[x]
        if 'api - /system/log: test/alternator: Starting test_batch.py::' in l:
            data2 = data[x + 1:]
            break
    else:
        assert False, input

    for x in range(len(data2)):
        l = data2[x]
        if 'api - /system/log: test/alternator: Ended test_batch.py::' in l:
            data3 = data2[:x]
            break
    else:
        assert False, input

    with open(output_file, 'w', encoding='utf8') as output:
        for x in range(len(data3)):
            l = data3[x].rstrip()
            if not l: continue
            if l.startswith(' '):
                continue
            if 'sleeping ' in l and ' seconds until next period' in l: continue
            if ' - Setting reconcile time to' in l: continue
            if l.startswith('kernel callstack: '): continue
            if 'sstable - Rebuilding bloom filter' in l: continue
            if '] schema_tables - ' in l: continue
            if 'seastar_memory - oversized allocation: ' in l: continue
            if 'alternator-executor - Creating keyspace' in l: continue
            if '] query_processor' in l: continue
            m = reactor_stalled_re.match(l)
            if m:
                print(l, file=output)
                if x + 1 < len(data3):
                    l2 = data3[x + 1]
                    if l2.startswith('kernel callstack: '):
                        print(l2.rstrip(), file=output)
            else:
                print(f'UNPARSED               -> {l[:120]}')

def parse_log_output_2(input_file, output_file):
    with open(input_file, 'r', encoding='utf8') as input, open(output_file, 'w', encoding='utf8') as output:
        for line in input:
            x = line.rfind(' ')
            ms = None
            if x >= 0:
                try:
                    ms = int(line[x + 1:])
                    line = line[:x]
                except:
                    pass
            assert ms, line
            line = line.replace('std::basic_string_view<char, std::char_traits<char> >', 'std::string_view')
            line = line.replace('std::basic_string<char, std::char_traits<char> >', 'std::string')

            total_line = line
            prev = ''
            count = 0
            def update(nxt):
                nonlocal prev, count
                nxt = nxt.strip()
                if prev == nxt:
                    count += 1
                else:
                    if prev:
                        if count > 1:
                            print(f'      {prev} x{count}', file=output)
                        else:
                            print(f'      {prev}', file=output)
                    prev = nxt
                    count = 1

            print(f'stalled for {ms} ms', file=output)
            for line in total_line.split(';'):
                x = line.find('<')
                if x > 0:
                    y = line.rfind('>')
                    if y > x:
                        line = line[:x] + ' ' + line[y + 1:]
                update(line)
            update('')
            print('', file=output)
            print('', file=output)

if 1:
    for x in (
            'test_performance_batch_write_item_1',
            'test_performance_batch_write_item_2',
            'test_performance_batch_write_item_3',
            'test_performance_create_table_1',
            'test_performance_put_item_1',
            'test_performance_put_item_2',
            'test_performance_update_item_1',
            'test_performance_update_item_2',
            'test_performance_scan_1',
            'test_performance_query_1',
            'test_performance_tag_resource_1',
            'test_performance_update_time_to_live_1',
            'test_performance_update_time_to_live_2',
            ):
        subprocess.check_call(['rm', '-rf', '/tmp/scylla-*' ])
        subprocess.check_call(['/home/y/work/scylladb/test/alternator/run', 'test_batch.py', '-k', x])
        for y in os.listdir('/tmp'):
            if y.startswith('scylla-'):
                p = os.path.join('/tmp', y, 'log')
                os.rename(p, f'/tmp/test_o/log_{x}.txt')
                break
        else:
            assert False

for f in os.listdir('.'):
    if f.startswith('log_'):
        o = 'p1_' + f[4:]
        o2 = 'p2_' + f[4:]
        o3 = 'res_' + f[4:]
        parse_log_output_1(f, o)

        if 1:
            with open(o2, 'w', encoding='utf8') as output:
                subprocess.check_call([
                    '/home/y/work/scylladb/seastar/scripts/stall-analyser.py',
                    os.path.join(os.getcwd(), o),
                    '--format', 'trace',
                    '-e', os.environ['SCYLLA'] ],
                    stdout=output)
        
        parse_log_output_2(o2, o3)

