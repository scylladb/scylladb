import gdb, gdb.printing, uuid

def template_arguments(gdb_type):
    n = 0
    while True:
        try:
            yield gdb_type.template_argument(n)
            n += 1
        except RuntimeError:
            return

def get_template_arg_with_prefix(gdb_type, prefix):
    for arg in template_arguments(gdb_type):
        if str(arg).startswith(prefix):
            return arg

def get_base_class_offset(gdb_type, base_class_name):
    name_pattern = re.escape(base_class_name) + "(<.*>)?$"
    for field in gdb_type.fields():
        if field.is_base_class and re.match(name_pattern, field.name):
            return field.bitpos / 8

class intrusive_list:
    size_t = gdb.lookup_type('size_t')

    def __init__(self, list_ref):
        list_type = list_ref.type.strip_typedefs()
        self.node_type = list_type.template_argument(0)
        rps = list_ref['data_']['root_plus_size_']
        try:
            self.root = rps['root_']
        except:
            # Some boost versions have this instead
            self.root = rps['m_header']
        member_hook = get_template_arg_with_prefix(list_type, "boost::intrusive::member_hook")
        if member_hook:
            self.link_offset = member_hook.template_argument(2).cast(self.size_t)
        else:
            self.link_offset = get_base_class_offset(self.node_type, "boost::intrusive::list_base_hook")
            if self.link_offset == None:
                raise Exception("Class does not extend list_base_hook: " + str(self.node_type))

    def __iter__(self):
        hook = self.root['next_']
        while hook != self.root.address:
            node_ptr = hook.cast(self.size_t) - self.link_offset
            yield node_ptr.cast(self.node_type.pointer()).dereference()
            hook = hook['next_']

    def __nonzero__(self):
        return self.root['next_'] != self.root.address

    def __bool__(self):
        return self.__nonzero__()

class std_array:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        elems = self.ref['_M_elems']
        return elems.type.sizeof / elems[0].type.sizeof

    def __iter__(self):
        elems = self.ref['_M_elems']
        count = self.__len__()
        i = 0
        while i < count:
            yield elems[i]
            i += 1

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()

class std_vector:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        return self.ref['_M_impl']['_M_finish'] - self.ref['_M_impl']['_M_start']

    def __iter__(self):
        i = self.ref['_M_impl']['_M_start']
        end = self.ref['_M_impl']['_M_finish']
        while i != end:
            yield i.dereference()
            i += 1

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()

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
    return int(gdb.parse_and_eval('::smp::count'))

def find_db(shard):
    return gdb.parse_and_eval('::debug::db')['_instances']['_M_impl']['_M_start'][shard]['service']['_p']

def find_dbs():
    return [find_db(shard) for shard in range(cpus())]

def list_unordered_map(map):
    kt = map.type.template_argument(0)
    vt = map.type.template_argument(1)
    value_type = gdb.lookup_type('::std::pair<{} const, {}>'.format(kt.name, vt.name))
    hashnode_ptr_type = gdb.lookup_type('::std::__detail::_Hash_node<' + value_type.name + ', true>').pointer()
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
                schema = value['_schema']['_p'].reinterpret_cast(gdb.lookup_type('schema').pointer())
                name = str(schema['_raw']['_ks_name']) + '/' + str(schema['_raw']['_cf_name'])
                schema_version = str(schema['_raw']['_version'])
                gdb.write('{:5} {} v={} {:45} (column_family*){}\n'.format(shard, key, schema_version, name, value.address))

class scylla_memory(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla memory', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        cpu_mem = gdb.parse_and_eval('\'memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'memory::page_size\''))
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

def get_seastar_memory_start_and_size():
    cpu_mem = gdb.parse_and_eval('\'memory::cpu_mem\'')
    page_size = int(gdb.parse_and_eval('\'memory::page_size\''))
    total_mem = int(cpu_mem['nr_pages']) * page_size
    start = int(cpu_mem['memory'])
    return start, total_mem

def seastar_memory_layout():
    results = []
    for t in reactor_threads():
        start, total_mem = get_seastar_memory_start_and_size()
        results.append((t, start, total_mem))
    return results

def get_thread_owning_memory(ptr):
    for t in reactor_threads():
        start, size = get_seastar_memory_start_and_size()
        if start <= ptr < start + size:
            return t

class scylla_ptr(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla ptr', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        ptr = int(arg, 0)

        owning_thread = None
        for t, start, size in seastar_memory_layout():
            if ptr >= start and ptr < start + size:
                owning_thread = t
                break

        if not owning_thread:
            gdb.write("Not managed by seastar\n")
            return

        msg = "thread %d" % t.num

        owning_thread.switch()

        cpu_mem = gdb.parse_and_eval('\'memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'memory::page_size\''))
        offset = ptr - int(cpu_mem['memory'])

        page = cpu_mem['pages'][offset / page_size];
        if page['free']:
            msg += ', page is free'
            gdb.write(msg + '\n')
            return

        pool = page['pool']
        offset_in_span = page['offset_in_span'] * page_size + ptr % page_size
        first_page_in_span = cpu_mem['pages'][offset / page_size - page['offset_in_span']];
        if pool:
            object_size = int(pool['_object_size'])
            msg += ', small (size <= %d)' % object_size
            offset_in_object = offset_in_span % object_size
            free_object_ptr = gdb.lookup_type('void').pointer().pointer()
            char_ptr = gdb.lookup_type('char').pointer()
            # pool's free list
            next_free = pool['_free']
            free = False
            while next_free:
                if ptr >= next_free and ptr < next_free.reinterpret_cast(char_ptr) + object_size:
                    free = True
                    break
                next_free = next_free.reinterpret_cast(free_object_ptr).dereference()
            if not free:
                # span's free list
                next_free = first_page_in_span['freelist']
                while next_free:
                    if ptr >= next_free and ptr < next_free.reinterpret_cast(char_ptr) + object_size:
                        free = True
                        break
                    next_free = next_free.reinterpret_cast(free_object_ptr).dereference()
            if free:
                msg += ', free'
            else:
                msg += ', live (0x%x +%d)' % (ptr - offset_in_object, offset_in_object)
        else:
            msg += ', large'

        # FIXME: handle debug-mode build
        segment_size = int(gdb.parse_and_eval('\'logalloc\'::segment::size'))
        index = gdb.parse_and_eval('(%d - \'logalloc\'::shard_segment_pool._segments_base) / \'logalloc\'::segment::size' % (ptr))
        desc = gdb.parse_and_eval('\'logalloc\'::shard_segment_pool._segments[%d]' % (index))
        if desc['_lsa_managed']:
            msg += ', LSA-managed'

        gdb.write(msg + '\n')

class scylla_segment_descs(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla segment-descs', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        # FIXME: handle debug-mode build
        base = int(gdb.parse_and_eval('\'logalloc\'::shard_segment_pool._segments_base'))
        segment_size = int(gdb.parse_and_eval('\'logalloc\'::segment::size'))
        addr = base
        for desc in std_vector(gdb.parse_and_eval('\'logalloc\'::shard_segment_pool._segments')):
            if desc['_lsa_managed']:
                gdb.write('0x%x: lsa free=%d region=0x%x zone=0x%x\n' % (addr, desc['_free_space'], desc['_region'], desc['_zone']))
            else:
                gdb.write('0x%x: std\n' % (addr))
            addr += segment_size

class scylla_lsa(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla lsa', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        lsa = gdb.parse_and_eval('\'logalloc::shard_segment_pool\'')
        segment_size = int(gdb.parse_and_eval('\'logalloc::segment::size\''))

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

        lsa_tracker = gdb.parse_and_eval('\'logalloc::tracker_instance\'._impl')['_M_t']['_M_head_impl']
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
        zone = node.cast(gdb.lookup_type('::logalloc::segment_zone').pointer())

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
        all_zones = gdb.parse_and_eval('\'logalloc::shard_segment_pool\'._all_zones')
        for zone in lsa_zone_tree(all_zones['holder']['root']['parent_']):
            gdb.write('    Zone:\n      - base: {z_base:08X}\n      - size: {z_size:>12}\n'
                '      - used: {z_used:>12}\n'
                .format(z_base=int(zone['_base']), z_size=int(zone['_segments']['_bits_count']),
                    z_used=int(zone['_used_segment_count'])));

names = {} # addr (int) -> name (str)
def resolve(addr):
    if addr in names:
        return names[addr]

    infosym = gdb.execute('info symbol 0x%x' % (addr), False, True)
    if infosym.startswith('No symbol'):
        name = None
    else:
        name = infosym[:infosym.find('in section')]
    names[addr] = name
    return name

class scylla_lsa_segment(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla lsa-segment', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        # See logalloc::region_impl::for_each_live()
        ptr = int(arg, 0)
        seg = gdb.parse_and_eval('(char*)(%d & ~(\'logalloc\'::segment::size - 1))' % (ptr))
        segment_size = int(gdb.parse_and_eval('\'logalloc\'::segment::size'))
        obj_desc_size = int(gdb.parse_and_eval('sizeof(\'logalloc\'::region_impl::object_descriptor)'))
        obj_desc_ptr = gdb.lookup_type('logalloc::region_impl::object_descriptor').pointer()
        offset = 0
        while offset < segment_size:
            padding = seg[offset] >> 2
            offset += padding
            if seg[offset] & 2:
                break;
            flags = seg[offset]
            desc = (seg + offset).reinterpret_cast(obj_desc_ptr)
            offset += obj_desc_size;
            addr = seg + offset
            if flags & 1:
                migrator_name = resolve(int(desc['_migrator'])) or ('0x%x' % (addr))
                gdb.write('0x%x: live size=%d migrator=%s\n' % (addr, desc['_size'], migrator_name))
            else:
                gdb.write('0x%x: free size=%d\n' % (addr, desc['_size']))
            offset += desc['_size'];

class scylla_timers(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla timers', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        gdb.write('Timers:\n')
        timer_set = gdb.parse_and_eval('local_engine->_timers')
        for timer_list in std_array(timer_set['_buckets']):
            for t in intrusive_list(timer_list):
                gdb.write('(%s*) %s = %s\n' % (t.type, t.address, t))
        timer_set = gdb.parse_and_eval('local_engine->_lowres_timers')
        for timer_list in std_array(timer_set['_buckets']):
            for t in intrusive_list(timer_list):
                gdb.write('(%s*) %s = %s\n' % (t.type, t.address, t))

def has_reactor():
    if gdb.parse_and_eval('local_engine'):
        return True
    return False

def reactor_threads():
    orig = gdb.selected_thread()
    for t in gdb.selected_inferior().threads():
        t.switch()
        if has_reactor():
            yield t
    orig.switch()

def reactors():
    orig = gdb.selected_thread()
    for t in gdb.selected_inferior().threads():
        t.switch()
        reactor = gdb.parse_and_eval('local_engine')
        if reactor:
            yield reactor.dereference()
    orig.switch()

class scylla_apply(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla apply', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        for r in reactors():
            gdb.write("\nShard %d: \n\n" % (r['_id']))
            gdb.execute(arg)

class scylla_shard(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla shard', gdb.COMMAND_USER, gdb.COMPLETE_NONE)
    def invoke(self, arg, from_tty):
        id = int(arg)
        orig = gdb.selected_thread()
        for t in gdb.selected_inferior().threads():
            t.switch()
            reactor = gdb.parse_and_eval('local_engine')
            if reactor and reactor['_id'] == id:
                gdb.write('Switched to thread %d\n' % t.num)
                return
        orig.switch()
        gdb.write('Error: Shard %d not found\n' % (id))

class scylla_mem_ranges(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla mem-ranges', gdb.COMMAND_USER, gdb.COMPLETE_NONE)
    def invoke(self, arg, from_tty):
        for t, start, total_mem in seastar_memory_layout():
            gdb.write('0x%x +%d\n' % (start, total_mem))

class scylla_mem_range(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla mem-range', gdb.COMMAND_USER, gdb.COMPLETE_NONE)
    def invoke(self, arg, from_tty):
        if not has_reactor():
            gdb.write('Not a reactor thread')
            return
        gdb.write('0x%x +%d\n' % get_seastar_memory_start_and_size())

class thread_switched_in(object):
    def __init__(self, gdb_thread):
        self.new = gdb_thread
    def __enter__(self):
        self.old = gdb.selected_thread()
        self.new.switch()
    def __exit__(self, *_):
        self.old.switch()

class seastar_thread_context(object):
    ulong_type = gdb.lookup_type('unsigned long')

    # FIXME: The jmpbuf interpreting code targets x86_64 and glibc 2.19
    # Offsets taken from sysdeps/x86_64/jmpbuf-offsets.h.
    jmpbuf_offsets = {
        'rbx':  0,
        'rbp':  1,
        'r12':  2,
        'r13':  3,
        'r14':  4,
        'r15':  5,
        'rsp':  6,
        'rip':  7,
    }
    mangled_registers = ['rip', 'rsp', 'rbp']

    def save_regs(self):
        result = {}
        for reg in self.jmpbuf_offsets.keys():
            result[reg] = gdb.parse_and_eval('$%s' % reg).cast(self.ulong_type)
        return result

    def restore_regs(self, values):
        gdb.newest_frame().select()
        for reg, value in values.items():
            gdb.execute('set $%s = %s' % (reg, value))

    def get_fs_base(self):
        holder_addr  = get_seastar_memory_start_and_size()[0]
        holder = gdb.Value(holder_addr).reinterpret_cast(self.ulong_type.pointer())
        saved = holder.dereference()
        gdb.execute('set *(void**)%s = 0' % holder_addr)
        if gdb.parse_and_eval('arch_prctl(0x1003, %d)' % holder_addr) != 0:
            raise Exception('arch_prctl() failed')
        fs_base = holder.dereference()
        gdb.execute('set *(void**)%s = %s' % (holder_addr, saved))
        return fs_base

    def regs_from_jmpbuf(self, jmpbuf):
        canary = gdb.Value(self.get_fs_base()).reinterpret_cast(self.ulong_type.pointer())[6]
        result = {}
        for reg, offset in self.jmpbuf_offsets.items():
            value = jmpbuf['__jmpbuf'][offset].cast(self.ulong_type)
            if reg in self.mangled_registers:
                # glibc mangles by doing:
                #   xor %reg, %fs:0x30
                #   rol %reg, $0x11
                bits = 64
                shift = 0x11
                value = (value << (bits-shift)) & (2**bits-1) | (value >> shift)
                value = value ^ canary
            result[reg] = value
        return result

    def is_switched_in(self):
        jmpbuf_link_ptr = gdb.parse_and_eval('seastar::g_current_context')
        if jmpbuf_link_ptr['thread'] == self.thread_ctx.address:
            return True
        return False

    def __init__(self, thread_ctx):
        self.thread_ctx = thread_ctx
        self.old_frame = gdb.selected_frame()
        self.old_regs = self.save_regs()
        self.old_gdb_thread = gdb.selected_thread()
        self.gdb_thread = get_thread_owning_memory(thread_ctx.address)
        self.new_regs = None

    def __enter__(self):
        gdb.write('Switched to thread %d, (seastar::thread_context*) 0x%x\n' % (self.gdb_thread.num, self.thread_ctx.address))
        self.gdb_thread.switch()
        if not self.is_switched_in():
            self.new_regs = self.regs_from_jmpbuf(self.thread_ctx['_context']['jmpbuf'])
            self.restore_regs(self.new_regs)

    def __exit__(self, *_):
        if self.new_regs:
            self.gdb_thread.switch()
            self.restore_regs(self.old_regs)
        self.old_gdb_thread.switch()
        self.old_frame.select()
        gdb.write('Switched to thread %d\n' % self.old_gdb_thread.num)

active_thread_context = None

def exit_thread_context():
    global active_thread_context
    if active_thread_context:
        active_thread_context.__exit__()
        active_thread_context = None

def seastar_threads_on_current_shard():
    return intrusive_list(gdb.parse_and_eval('\'seastar::thread_context::_all_threads\''))

class scylla_thread(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla thread', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND, True)
    def invoke_apply_all(self, args):
        for r in reactors():
            for t in seastar_threads_on_current_shard():
                gdb.write('\n[shard %2d] (seastar::thread_context*) 0x%x:\n\n' % (r['_id'], t.address))
                with seastar_thread_context(t):
                    gdb.execute(' '.join(args))

    def print_usage(self):
        gdb.write("""Missing argument. Usage:

 scylla thread <seastar::thread_context pointer> - switches to given seastar thread
 scylla thread apply all <cmd>                   - executes cmd in the context of each seastar thread

""")

    def invoke(self, arg, for_tty):
        args = arg.split()

        if len(args) < 1:
            self.print_usage()
            return

        if args[0] == 'apply':
            args.pop(0)
            if len(args) < 2 or args[0] != 'all':
                self.print_usage()
                return
            args.pop(0)
            self.invoke_apply_all(args)
            return

        addr = gdb.parse_and_eval(args[0])
        ctx = addr.reinterpret_cast(gdb.lookup_type('seastar::thread_context').pointer()).dereference()
        exit_thread_context()
        global active_thread_context
        active_thread_context = seastar_thread_context(ctx)
        active_thread_context.__enter__()

class scylla_unthread(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla unthread', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
    def invoke(self, arg, for_tty):
        exit_thread_context()

class scylla_threads(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla threads', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
    def invoke(self, arg, for_tty):
        for r in reactors():
            shard = r['_id']
            for t in seastar_threads_on_current_shard():
                gdb.write('[shard %2d] (seastar::thread_context*) 0x%x\n' % (shard, t.address))

scylla()
scylla_databases()
scylla_keyspaces()
scylla_column_families()
scylla_memory()
scylla_ptr()
scylla_mem_ranges()
scylla_mem_range()
scylla_lsa()
scylla_lsa_zones()
scylla_lsa_segment()
scylla_segment_descs()
scylla_timers()
scylla_apply()
scylla_shard()
scylla_thread()
scylla_unthread()
scylla_threads()
