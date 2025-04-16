#!/usr/bin/env python3

import os, sys, re, subprocess

def parse_log_output_1(input_file, output_file):
    import re

    reactor_stalled_re = re.compile(r'Reactor stalled for (\d+) ms on shard \d+, in scheduling group [^.]*\. Backtrace: (.*)')


    print(f'processing {input_file}')

    with open(input_file, 'r', encoding='utf8') as inp:
        data = inp.readlines()
    
    emited = False
    with open(output_file, 'w', encoding='utf8') as output:
        for x in range(len(data)):
            l = data[x].rstrip()
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
                if x + 1 < len(data):
                    l2 = data[x + 1]
                    if l2.startswith('kernel callstack: '):
                        print(l2.rstrip(), file=output)
                emited = True
            else:
                print(f'UNPARSED               -> {l[:120]}')
    return emited

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
    os.makedirs('/tmp/test_o', exist_ok=True)
    tries = 10
    for test_name in (
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
        dest_path = f'/tmp/test_o/log_{test_name}.txt'
        if os.path.exists(dest_path):
            os.remove(dest_path)
        for attempt in range(tries):
            subprocess.check_call(['rm', '-rf', '/tmp/scylla-*' ])
            subprocess.check_call(['/home/y/work/scylladb/test/alternator/run', 'test_batch.py', '-k', test_name])
            for y in os.listdir('/tmp'):
                if y.startswith('scylla-'):
                    s = os.path.join('/tmp', y, 'log')
                    with open(dest_path, "a", encoding='utf8') as dest, open(s, 'r', encoding='utf8') as src:
                        data = src.readlines()

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

                        for d in data3:
                            dest.write(d)
                    break
            else:
                assert False

for f in os.listdir('/tmp/test_o'):
    if f.startswith('log_'):
        o = os.path.join('/tmp/test_o', 'p1_' + f[4:])
        o2 = os.path.join('/tmp/test_o', 'p2_' + f[4:])
        o3 = os.path.join('/tmp/test_o', 'res_' + f[4:])
        f = os.path.join('/tmp/test_o', f)
        if parse_log_output_1(f, o):
            with open(o2, 'w', encoding='utf8') as output:
                subprocess.check_call([
                    '/home/y/work/scylla-testtest/seastar/scripts/stall-analyser.py',
                    os.path.join(os.getcwd(), o),
                    '--format', 'graph',
                    '-e', os.environ['SCYLLA'] ],
                    stdout=output)
        
        #parse_log_output_2(o2, o3)

