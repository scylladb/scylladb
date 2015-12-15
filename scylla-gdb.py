import gdb, gdb.printing, uuid

def uint64_t(val):
    val = int(val)
    if val < 0:
        val += 1 << 64
    return val

class sstring_printer(gdb.printing.PrettyPrinter):
    'print an sstring'
    def __init__(self, val):
        self.val = val
    def to_string(self):
        if self.val['u']['internal']['size'] >= 0:
            array = self.val['u']['internal']['str']
            len = int(self.val['u']['internal']['size'])
            return ''.join([chr(array[x]) for x in range(len)])
        else:
            return self.val['u']['external']['str']
    def display_hint(self):
        return 'string'

class uuid_printer(gdb.printing.PrettyPrinter):
    'print a uuid'
    def __init__(self, val):
        self.val = val
    def to_string(self):
        msb = uint64_t(self.val['most_sig_bits'])
        lsb = uint64_t(self.val['least_sig_bits'])
        return str(uuid.UUID(int=(msb << 64) | lsb))
    def display_hint(self):
        return 'string'


def build_pretty_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter('scylla')
    pp.add_printer('sstring', r'^basic_sstring<char,.*>$', sstring_printer)
    pp.add_printer('uuid', r'^utils::UUID$', uuid_printer)
    return pp

gdb.printing.register_pretty_printer(gdb.current_objfile(), build_pretty_printer(), replace=True)

def cpus():
    return int(gdb.parse_and_eval('smp::count'))

def find_db(shard):
    return gdb.parse_and_eval('debug::db')['_instances']['_M_impl']['_M_start'][shard]['service']['_p']

def find_dbs():
    return [find_db(shard) for shard in range(cpus())]

def list_unordered_map(map):
    kt = map.type.template_argument(0)
    vt = map.type.template_argument(1)
    value_type = gdb.lookup_type('std::pair<{} const, {}>'.format(kt.name, vt.name))
    hashnode_ptr_type = gdb.lookup_type('std::__detail::_Hash_node<' + value_type.name + ', true>').pointer()
    h = map['_M_h']
    p = h['_M_before_begin']['_M_nxt']
    while p:
        pc = p.cast(hashnode_ptr_type)['_M_storage']['_M_storage']['__data'].cast(value_type.pointer())
        yield (pc['first'], pc['second'].address)
        p = p['_M_nxt']
    raise StopIteration()

class scylla(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND, True)

class scylla_databases(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla databases', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        for shard in range(cpus()):
            db = find_db(shard)
            gdb.write('{:5} (database*){}\n'.format(shard, db))

class scylla_keyspaces(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla keyspaces', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        for shard in range(cpus()):
            db = find_db(shard)
            keyspaces = db['_keyspaces']
            for (key, value) in list_unordered_map(keyspaces):
                gdb.write('{:5} {:20} (keyspace*){}\n'.format(shard, key, value))

class scylla_column_families(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla column_families', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        for shard in range(cpus()):
            db = find_db(shard)
            cfs = db['_column_families']
            for (key, value) in list_unordered_map(cfs):
                value = value['_p']['_value']  # it's a lw_shared_ptr
                schema = value['_schema']['_p']['_value']
                name = str(schema['_raw']['_ks_name']) + '/' + str(schema['_raw']['_cf_name'])
                schema_version = str(schema['_raw']['_version'])
                gdb.write('{:5} {} v={} {:45} (column_family*){}\n'.format(shard, key, schema_version, name, value.address))

class scylla_memory(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla memory', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        cpu_mem = gdb.parse_and_eval('memory::cpu_mem')
        page_size = int(gdb.parse_and_eval('memory::page_size'))
        free_mem = int(cpu_mem['nr_free_pages']) * page_size
        total_mem = int(cpu_mem['nr_pages']) * page_size
        gdb.write('Used memory: {used_mem:>13}\nFree memory: {free_mem:>13}\nTotal memory: {total_mem:>12}\n\n'
            .format(used_mem=total_mem-free_mem, free_mem=free_mem, total_mem=total_mem))

        gdb.write('Small pools:\n')
        small_pools = cpu_mem['small_pools']
        nr = small_pools['nr_small_pools']
        gdb.write('{objsize:>5} {span_size:>6} {use_count:>10} {memory:>12} {wasted_percent:>5}\n'
              .format(objsize='objsz', span_size='spansz', use_count='usedobj', memory='memory', wasted_percent='wst%'))
        for i in range(int(nr)):
            sp = small_pools['_u']['a'][i]
            object_size = int(sp['_object_size'])
            span_size = int(sp['_span_size']) * page_size
            free_count = int(sp['_free_count'])
            spans_in_use = int(sp['_spans_in_use'])
            memory = spans_in_use * span_size
            use_count = spans_in_use * int(span_size / object_size) - free_count
            wasted = free_count * object_size
            wasted_percent = wasted * 100.0 / memory if memory else 0
            gdb.write('{objsize:5} {span_size:6} {use_count:10} {memory:12} {wasted_percent:5.1f}\n'
                  .format(objsize=object_size, span_size=span_size, use_count=use_count, memory=memory, wasted_percent=wasted_percent))

        gdb.write('Page spans:\n')
        gdb.write('{index:5} {size:>13} {total}\n'.format(index="index", size="size [B]", total="free [B]"))
        for index in range(int(cpu_mem['nr_span_lists'])):
            span_list = cpu_mem['fsu']['free_spans'][index]
            front = int(span_list['_front'])
            pages = cpu_mem['pages']
            total = 0
            while front:
                span = pages[front]
                total += int(span['span_size'])
                front = int(span['link']['_next'])
            gdb.write('{index:5} {size:13} {total}\n'.format(index=index, size=(1<<index)*page_size, total=total*page_size))

class scylla_lsa(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla lsa', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        lsa = gdb.parse_and_eval('logalloc::shard_segment_pool')
        segment_size = int(gdb.parse_and_eval('logalloc::segment::size'))

        lsa_mem = int(lsa['_segments_in_use']) * segment_size
        non_lsa_mem = int(lsa['_non_lsa_memory_in_use'])
        total_mem = lsa_mem + non_lsa_mem
        gdb.write('Log Structured Allocator\n\nLSA memory in use: {lsa_mem:>16}\n'
            'Non-LSA memory in use: {non_lsa_mem:>12}\nTotal memory in use: {total_mem:>14}\n\n'
            .format(lsa_mem=lsa_mem, non_lsa_mem = non_lsa_mem, total_mem = total_mem))

        er_goal = int(lsa['_current_emergency_reserve_goal'])
        er_max = int(lsa['_emergency_reserve_max'])
        er_current = int(lsa['_emergency_reserve']['_stack']['data_']['root_plus_size_']['size_'])
        gdb.write('Emergency reserve goal: {er_goal:>11}\nEmergency reserve max: {er_max:>12}\n'
            'Emergency reserve current: {er_current:>8}\n\n'
            .format(er_goal=er_goal, er_max=er_max, er_current=er_current))

        lsa_tracker = gdb.parse_and_eval('logalloc::tracker_instance._impl')['_M_t']['_M_head_impl']
        regions = lsa_tracker['_regions']
        region = regions['_M_impl']['_M_start']
        gdb.write('LSA regions:\n')
        while region != regions['_M_impl']['_M_finish']:
            gdb.write('    Region #{r_id}\n      - reclaimable: {r_en:>14}\n'
                '      - evictable: {r_ev:16}\n      - non-LSA memory: {r_non_lsa:>11}\n'
                '      - closed LSA memory: {r_lsa:>8}\n      - unused memory: {r_unused:>12}\n'
                .format(r_id=int(region['_id']), r_en=bool(region['_reclaiming_enabled']),
                    r_ev=bool(region['_evictable']),
                    r_non_lsa=int(region['_non_lsa_occupancy']['_total_space']),
                    r_lsa=int(region['_closed_occupancy']['_total_space']),
                    r_unused=int(region['_closed_occupancy']['_free_space'])))
            region = region + 1

def lsa_zone_tree(node):
    if node:
        zone = node.cast(gdb.lookup_type('logalloc::segment_zone').pointer())

        for x in lsa_zone_tree(node['left_']):
            yield x

        yield zone

        for x in lsa_zone_tree(node['right_']):
            yield x

class scylla_lsa_zones(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla lsa_zones', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        gdb.write('LSA zones:\n')
        all_zones = gdb.parse_and_eval('logalloc::shard_segment_pool._all_zones')
        for zone in lsa_zone_tree(all_zones['holder']['root']['parent_']):
            gdb.write('    Zone:\n      - base: {z_base:08X}\n      - size: {z_size:>12}\n'
                '      - used: {z_used:>12}\n'
                .format(z_base=int(zone['_base']), z_size=int(zone['_segments']['_bits_count']),
                    z_used=int(zone['_used_segment_count'])));

scylla()
scylla_databases()
scylla_keyspaces()
scylla_column_families()
scylla_memory()
scylla_lsa()
scylla_lsa_zones()
