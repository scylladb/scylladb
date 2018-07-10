import gdb, gdb.printing, uuid, argparse
import re
from operator import attrgetter
from collections import defaultdict
import re
import random

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

class intrusive_set:
    size_t = gdb.lookup_type('size_t')

    def __init__(self, ref):
        container_type = ref.type.strip_typedefs()
        self.node_type = container_type.template_argument(0)
        member_hook = get_template_arg_with_prefix(container_type, "boost::intrusive::member_hook")
        if not member_hook:
            raise Exception('Expected member_hook<> option not found in container\'s template parameters')
        self.link_offset = member_hook.template_argument(2).cast(self.size_t)
        self.root = ref['holder']['root']['parent_']

    def __visit(self, node):
        if node:
            for n in self.__visit(node['left_']):
                yield n

            node_ptr = node.cast(self.size_t) - self.link_offset
            yield node_ptr.cast(self.node_type.pointer()).dereference()

            for n in self.__visit(node['right_']):
                yield n

    def __iter__(self):
        for n in self.__visit(self.root):
            yield n

class intrusive_set_external_comparator:
    size_t = gdb.lookup_type('size_t')

    def __init__(self, ref):
        container_type = ref.type.strip_typedefs()
        self.node_type = container_type.template_argument(0)
        self.link_offset = container_type.template_argument(1).cast(self.size_t)
        self.root = ref['_header']['parent_']

    def __visit(self, node):
        if node:
            for n in self.__visit(node['left_']):
                yield n

            node_ptr = node.cast(self.size_t) - self.link_offset
            yield node_ptr.cast(self.node_type.pointer()).dereference()

            for n in self.__visit(node['right_']):
                yield n

    def __iter__(self):
        for n in self.__visit(self.root):
            yield n

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
        return int(self.ref['_M_impl']['_M_finish'] - self.ref['_M_impl']['_M_start'])

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

class static_vector:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        return int(self.ref['m_holder']['m_size'])

    def __iter__(self):
        t = self.ref.type.strip_typedefs()
        value_type = t.template_argument(0)
        data = self.ref['m_holder']['storage']['dummy'].cast(value_type.pointer())
        for i in range(self.__len__()):
            yield data[i]

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

class managed_bytes_printer(gdb.printing.PrettyPrinter):
    'print a managed_bytes'
    def __init__(self, val):
        self.val = val

    def bytes(self):
        def signed_chr(c):
            return int(c).to_bytes(1, byteorder='little', signed=True)
        if self.val['_u']['small']['size'] >= 0:
            array = self.val['_u']['small']['data']
            len = int(self.val['_u']['small']['size'])
            return b''.join([signed_chr(array[x]) for x in range(len)])
        else:
            ref = self.val['_u']['ptr']
            chunks = list()
            while ref['ptr']:
                array = ref['ptr']['data']
                len = int(ref['ptr']['frag_size'])
                ref = ref['ptr']['next']
                chunks.append(b''.join([signed_chr(array[x]) for x in range(len)]))
            return b''.join(chunks)

    def to_string(self):
        return str(self.bytes())

    def display_hint(self):
        return 'managed_bytes'

class partition_entry_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = val

    def to_string(self):
        versions = list()
        v = self.val['_version']['_version']
        while v:
            versions.append('@%s: %s' % (v, v.dereference()))
            v = v['_next']
        return '{_snapshot=%s, _version=%s, versions=[\n%s\n]}' % (self.val['_snapshot'], self.val['_version'], ',\n'.join(versions))

    def display_hint(self):
        return 'partition_entry'

class mutation_partition_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = val

    def to_string(self):
        rows = list(str(r) for r in intrusive_set_external_comparator(self.val['_rows']))
        range_tombstones = list(str(r) for r in intrusive_set(self.val['_row_tombstones']['_tombstones']))
        return '{_tombstone=%s, _static_row=%s (cont=%s), _row_tombstones=[%s], _rows=[%s]}' % (
            self.val['_tombstone'],
            self.val['_static_row'],
            ('no', 'yes')[self.val['_static_row_continuous']],
            '\n' + ',\n'.join(range_tombstones) + '\n' if range_tombstones else '',
            '\n' + ',\n'.join(rows) + '\n' if rows else '')

    def display_hint(self):
        return 'mutation_partition'

class row_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = val

    def to_string(self):
        if self.val['_type'] == gdb.parse_and_eval('row::storage_type::vector'):
            cells = str(self.val['_storage']['vector'])
        elif self.val['_type'] == gdb.parse_and_eval('row::storage_type::set'):
            cells = '[%s]' % (', '.join(str(cell) for cell in intrusive_set(self.val['_storage']['set'])))
        else:
            raise Exception('Unsupported storage type: ' + self.val['_type'])
        return '{type=%s, cells=%s}' % (self.val['_type'], cells)

    def display_hint(self):
        return 'row'

class managed_vector_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = val

    def to_string(self):
        size = int(self.val['_size'])
        items = list()
        for i in range(size):
            items.append(str(self.val['_data'][i]))
        return '{size=%d, items=[%s]}' % (size, ', '.join(items))

    def display_hint(self):
        return 'managed_vector'

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
    pp.add_printer('sstring', r'^seastar::basic_sstring<char,.*>$', sstring_printer)
    pp.add_printer('managed_bytes', r'^managed_bytes$', managed_bytes_printer)
    pp.add_printer('partition_entry', r'^partition_entry$', partition_entry_printer)
    pp.add_printer('mutation_partition', r'^mutation_partition$', mutation_partition_printer)
    pp.add_printer('row', r'^row$', row_printer)
    pp.add_printer('managed_vector', r'^managed_vector<.*>$', managed_vector_printer)
    pp.add_printer('uuid', r'^utils::UUID$', uuid_printer)
    return pp

gdb.printing.register_pretty_printer(gdb.current_objfile(), build_pretty_printer(), replace=True)

def cpus():
    return int(gdb.parse_and_eval('::seastar::smp::count'))

def find_db(shard):
    return gdb.parse_and_eval('::debug::db')['_instances']['_M_impl']['_M_start'][shard]['service']['_p']

def find_dbs():
    return [find_db(shard) for shard in range(cpus())]

def list_unordered_map(map, cache=True):
    kt = map.type.template_argument(0)
    vt = map.type.template_argument(1)
    value_type = gdb.lookup_type('::std::pair<{} const, {} >'.format(str(kt), str(vt)))
    hashnode_ptr_type = gdb.lookup_type('::std::__detail::_Hash_node<' + value_type.name + ', ' + ('false', 'true')[cache] + '>' ).pointer()
    h = map['_M_h']
    p = h['_M_before_begin']['_M_nxt']
    while p:
        pc = p.cast(hashnode_ptr_type)['_M_storage']['_M_storage']['__data'].cast(value_type.pointer())
        yield (pc['first'], pc['second'])
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
                gdb.write('{:5} {:20} (keyspace*){}\n'.format(shard, str(key), value.address))

class scylla_column_families(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla column_families', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        for shard in range(cpus()):
            db = find_db(shard)
            cfs = db['_column_families']
            for (key, value) in list_unordered_map(cfs):
                value = value['_p'].reinterpret_cast(gdb.lookup_type('column_family').pointer()).dereference()  # it's a lw_shared_ptr
                schema = value['_schema']['_p'].reinterpret_cast(gdb.lookup_type('schema').pointer())
                name = str(schema['_raw']['_ks_name']) + '/' + str(schema['_raw']['_cf_name'])
                schema_version = str(schema['_raw']['_version'])
                gdb.write('{:5} {} v={} {:45} (column_family*){}\n'.format(shard, key, schema_version, name, value.address))

class scylla_task_histogram(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla task_histogram', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        args = arg.split(' ')
        def print_usage():
            gdb.write("Usage: scylla task_histogram [object size]\n")

        if len(args) > 1:
            print_usage()
            return

        size = int(args[0]) if args[0] != '' else 0
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        mem_start = cpu_mem['memory']

        vptr_type = gdb.lookup_type('uintptr_t').pointer()

        pages = cpu_mem['pages']
        nr_pages = int(cpu_mem['nr_pages'])

        sections = gdb.execute('info files', False, True).split('\n')
        for line in sections:
            # vptrs are in .rodata section
            if line.find("is .rodata") > -1:
                items = line.split()
                text_start = int(items[0], 16)
                text_end = int(items[2], 16)
                break

        vptr_count = defaultdict(int)
        scanned_pages = 0;
        limit = 20000
        for idx in random.sample(range(0, nr_pages), nr_pages):
            pool = pages[idx]['pool']
            if not pool or pages[idx]['offset_in_span'] != 0:
                continue
            if int(pool.dereference()['_object_size']) != size and size != 0:
                continue
            scanned_pages += 1
            objsize = size if size != 0 else int(pool.dereference()['_object_size'])
            span_size = pages[idx]['span_size'] * page_size
            for idx2 in range(0, int(span_size / objsize)):
                addr = (mem_start + idx * page_size + idx2 * objsize).reinterpret_cast(vptr_type).dereference()
                if addr >= text_start and addr <= text_end:
                    vptr_count[int(addr)] += 1
            if scanned_pages >= limit or len(vptr_count) >= limit:
                break

        for vptr, count in sorted(vptr_count.items(), key=lambda e: -e[1])[:30]:
            sym = resolve(vptr)
            if sym:
                gdb.write('%10d: 0x%x %s\n' % (count, vptr, sym))

def find_vptrs():
    cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
    page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
    mem_start = cpu_mem['memory']
    vptr_type = gdb.lookup_type('uintptr_t').pointer()
    pages = cpu_mem['pages']
    nr_pages = int(cpu_mem['nr_pages'])

    sections = gdb.execute('info files', False, True).split('\n')
    for line in sections:
        # vptrs are in .rodata section
        if line.find("is .rodata") > -1:
            items = line.split()
            text_start = int(items[0], 16)
            text_end = int(items[2], 16)
            break

    def is_vptr(addr):
        return addr >= text_start and addr <= text_end

    idx = 0
    while idx < nr_pages:
        if pages[idx]['free']:
            idx += pages[idx]['span_size']
            continue
        pool = pages[idx]['pool']
        if not pool or pages[idx]['offset_in_span'] != 0:
            idx += 1
            continue
        objsize = int(pool.dereference()['_object_size'])
        span_size = pages[idx]['span_size'] * page_size
        for idx2 in range(0, int(span_size / objsize) + 1):
            obj_addr = mem_start + idx * page_size + idx2 * objsize
            vptr = obj_addr.reinterpret_cast(vptr_type).dereference()
            if is_vptr(vptr):
                yield obj_addr, vptr
        idx += pages[idx]['span_size']

def find_single_sstable_readers():
    try:
        # For Scylla < 2.1
        # FIXME: this only finds range readers
        ptr_type = gdb.lookup_type('sstable_range_wrapping_reader').pointer()
        vtable_name = 'vtable for sstable_range_wrapping_reader'
    except:
        ptr_type = gdb.lookup_type('sstables::sstable_mutation_reader').pointer()
        vtable_name = 'vtable for sstables::sstable_mutation_reader'

    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr)
        if name and name.startswith(vtable_name):
            yield obj_addr.reinterpret_cast(ptr_type)

# Yields sstable* once for each active sstable reader
def find_active_sstables():
    sstable_ptr_type = gdb.lookup_type('sstables::sstable').pointer()
    for reader in  find_single_sstable_readers():
        sstable_ptr = reader['_sst']['_p']
        yield sstable_ptr.reinterpret_cast(sstable_ptr_type)

class schema_ptr:
    def __init__(self, ptr):
        schema_ptr_type = gdb.lookup_type('schema').pointer()
        self.ptr = ptr['_p'].reinterpret_cast(schema_ptr_type)

    def table_name(self):
        return '%s.%s' % (self.ptr['_raw']['_ks_name'], self.ptr['_raw']['_cf_name'])

class scylla_active_sstables(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla active-sstables', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        try:
            sizeof_index_entry = int(gdb.parse_and_eval('sizeof(sstables::index_entry)'))
            sizeof_entry = int(gdb.parse_and_eval('sizeof(sstables::shared_index_lists::entry)'))
            def count_index_lists(sst):
                index_lists_size = 0
                for key, entry in list_unordered_map(sst['_index_lists']['_lists'], cache=False):
                    index_entries = std_vector(entry['list'])
                    index_lists_size += sizeof_entry
                    for e in index_entries:
                        index_lists_size += sizeof_index_entry
                        index_lists_size += e['_key']['_size']
                        index_lists_size += e['_promoted_index_bytes']['_size']
                return index_lists_size
        except:
            count_index_lists = None

        sstables = dict() # name -> sstable*
        for sst in find_active_sstables():
            schema = schema_ptr(sst['_schema'])
            id = '%s#%d' % (schema.table_name(), sst['_generation'])
            if id in sstables:
                sst, count = sstables[id]
                sstables[id] = (sst, count + 1)
                continue
            sstables[id] = (sst, 1)

        total_index_lists_size = 0
        for id, (sst, count) in sstables.items():
            if count_index_lists:
                total_index_lists_size += count_index_lists(sst)
            gdb.write('sstable %s, readers=%d data_file_size=%d\n' % (id, count, sst['_data_file_size']))

        gdb.write('sstable_count=%d, total_index_lists_size=%d\n' % (len(sstables), total_index_lists_size))

class scylla_memory(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla memory', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
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
            span_size = int(sp['_span_sizes']['preferred']) * page_size
            free_count = int(sp['_free_count'])
            pages_in_use = int(sp['_pages_in_use'])
            memory = pages_in_use * page_size
            # use_count can be off if we used fallback spans rather than preferred spans
            use_count = int(memory / span_size) * int(span_size / object_size) - free_count
            wasted = free_count * object_size
            wasted_percent = wasted * 100.0 / memory if memory else 0
            gdb.write('{objsize:5} {span_size:6} {use_count:10} {memory:12} {wasted_percent:5.1f}\n'
                  .format(objsize=object_size, span_size=span_size, use_count=use_count, memory=memory, wasted_percent=wasted_percent))

        gdb.write('Page spans:\n')
        gdb.write('{index:5} {size:>13} {total}\n'.format(index="index", size="size [B]", total="free [B]"))
        for index in range(int(cpu_mem['nr_span_lists'])):
            span_list = cpu_mem['free_spans'][index]
            front = int(span_list['_front'])
            pages = cpu_mem['pages']
            total = 0
            while front:
                span = pages[front]
                total += int(span['span_size'])
                front = int(span['link']['_next'])
            gdb.write('{index:5} {size:13} {total}\n'.format(index=index, size=(1<<index)*page_size, total=total*page_size))



class TreeNode(object):
    def __init__(self, key):
        self.key = key
        self.children_by_key = {}

    def get_or_add(self, key):
        node = self.children_by_key.get(key, None)
        if not node:
            node = self.__class__(key)
            self.add(node)
        return node

    def add(self, node):
        self.children_by_key[node.key] = node

    def squash_child(self):
        assert self.has_only_one_child()
        self.children_by_key = next(iter(self.children)).children_by_key

    @property
    def children(self):
        return self.children_by_key.values()

    def has_only_one_child(self):
        return len(self.children_by_key) == 1

    def has_children(self):
        return bool(self.children_by_key)

    def remove_all(self):
        self.children_by_key.clear()

class ProfNode(TreeNode):
    def __init__(self, key):
        super(ProfNode, self).__init__(key)
        self.size = 0
        self.count = 0
        self.tail = []

    @property
    def attributes(self):
        return {
            'size': self.size,
            'count': self.count
        }

def collapse_similar(node):
    while node.has_only_one_child():
        child = next(iter(node.children))
        if node.attributes == child.attributes:
            node.squash_child()
            node.tail.append(child.key)
        else:
            break

    for child in node.children:
        collapse_similar(child)

def strip_level(node, level):
    if level <= 0:
        node.remove_all()
    else:
        for child in node.children:
            strip_level(child, level - 1)

def print_tree(root_node,
        formatter=attrgetter('key'),
        order_by=attrgetter('key'),
        printer=sys.stdout.write,
        node_filter=None):

    def print_node(node, is_last_history):
        stems = (" |   ", "     ")
        branches = (" |-- ", " \-- ")

        label_lines = formatter(node).rstrip('\n').split('\n')
        prefix_without_branch = ''.join(map(stems.__getitem__, is_last_history[:-1]))

        if is_last_history:
            printer(prefix_without_branch)
            printer(branches[is_last_history[-1]])
        printer("%s\n" % label_lines[0])

        for line in label_lines[1:]:
            printer(''.join(map(stems.__getitem__, is_last_history)))
            printer("%s\n" % line)

        children = sorted(filter(node_filter, node.children), key=order_by)
        if children:
            for child in children[:-1]:
                print_node(child, is_last_history + [False])
            print_node(children[-1], is_last_history + [True])

        is_last = not is_last_history or is_last_history[-1]
        if not is_last:
           printer("%s%s\n" % (prefix_without_branch, stems[False]))

    if not node_filter or node_filter(root_node):
        print_node(root_node, [])


class scylla_heapprof(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla heapprof', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="scylla heapprof")
        parser.add_argument("-G", "--inverted", action="store_true",
            help="Compute caller-first profile instead of callee-first")
        parser.add_argument("-a", "--addresses", action="store_true",
            help="Show raw addresses before resolved symbol names")
        parser.add_argument("--no-symbols", action="store_true",
            help="Show only raw addresses")
        parser.add_argument("--flame", action="store_true",
            help="Write flamegraph data to heapprof.stacks instead of showing the profile")
        parser.add_argument("--min", action="store", type=int, default=0,
            help="Drop branches allocating less than given amount")
        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        root = ProfNode(None)
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        site = cpu_mem['alloc_site_list_head']

        while site:
            size = int(site['size'])
            count = int(site['count'])
            if size:
                n = root
                n.size += size
                n.count += count
                bt = site['backtrace']
                addresses = list(int(f['addr']) for f in static_vector(bt['_frames']))
                addresses.pop(0) # drop memory::get_backtrace()
                if args.inverted:
                    seq = reversed(addresses)
                else:
                    seq = addresses
                for addr in seq:
                    n = n.get_or_add(addr)
                    n.size += size
                    n.count += count
            site = site['next']

        def resolver(addr):
            if args.no_symbols:
                return '0x%x' % addr
            if args.addresses:
                return '0x%x %s' % (addr, resolve(addr) or '')
            return resolve(addr) or ('0x%x' % addr)

        if args.flame:
            file_name = 'heapprof.stacks'
            with open(file_name, 'w') as out:
                trace = list()
                def print_node(n):
                    if n.key:
                        trace.append(n.key)
                        trace.extend(n.tail)
                    for c in n.children:
                        print_node(c)
                    if not n.has_children():
                        out.write("%s %d\n" % (';'.join(map(lambda x: '%s (#%d)' % (x, n.count), map(resolver, trace))), n.size))
                    if n.key:
                        del trace[-1 - len(n.tail):]
                print_node(root)
            gdb.write('Wrote %s\n' % (file_name))
        else:
            def node_formatter(n):
                if n.key is None:
                    name = "All"
                else:
                    name = resolver(n.key)
                return "%s (%d, #%d)\n%s" % (name, n.size, n.count, '\n'.join(map(resolver, n.tail)))

            def node_filter(n):
                return n.size >= args.min

            collapse_similar(root)
            print_tree(root,
                formatter=node_formatter,
                order_by=lambda n: -n.size,
                node_filter=node_filter,
                printer=gdb.write)

def get_seastar_memory_start_and_size():
    cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
    page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
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

        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        offset = ptr - int(cpu_mem['memory'])
        ptr_page_idx = offset / page_size
        pages = cpu_mem['pages']
        page = pages[ptr_page_idx];

        def is_page_free(page_index):
            for index in range(int(cpu_mem['nr_span_lists'])):
                span_list = cpu_mem['free_spans'][index]
                span_page_idx = span_list['_front']
                while span_page_idx:
                    span_page = pages[span_page_idx]
                    if span_page_idx <= page_index < span_page_idx + span_page['span_size']:
                        return True
                    span_page_idx = span_page['link']['_next']
            return False

        if is_page_free(ptr_page_idx):
            msg += ', page is free'
            gdb.write(msg + '\n')
            return

        pool = page['pool']
        offset_in_span = int(page['offset_in_span']) * page_size + ptr % page_size
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
        segment_size = int(gdb.parse_and_eval('\'logalloc::segment\'::size'))
        index = gdb.parse_and_eval('(%d - \'logalloc::shard_segment_pool\'._segments_base) / \'logalloc::segment\'::size' % (ptr))
        desc = gdb.parse_and_eval('\'logalloc::shard_segment_pool\'._segments._M_impl._M_start[%d]' % (index))
        if desc['_region']:
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
            if desc['_region']:
                gdb.write('0x%x: lsa free=%-6d used=%-6d %6.2f%% region=0x%x\n' % (addr, desc['_free_space'],
                                                                        segment_size - int(desc['_free_space']),
                                                                        float(segment_size - int(desc['_free_space'])) * 100 / segment_size,
                                                                        int(desc['_region'])))
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

class lsa_object_descriptor(object):
    @staticmethod
    def decode(pos):
        start_pos = pos
        b = pos.dereference() & 0xff
        pos += 1
        if not (b & 0x40):
            raise Exception('object descriptor at 0x%x does not start with 0x40: 0x%x' % (int(start_pos), int(b)))
        value = b & 0x3f
        shift = 0
        while not (b & 0x80):
            shift += 6
            b = pos.dereference() & 0xff
            pos += 1
            value |= (b & 0x3f) << shift
        return lsa_object_descriptor(value, start_pos, pos)
    mig_re = re.compile(r'.* standard_migrator<(.*)>\+16>,')
    vec_ext_re = re.compile(r'managed_vector<(.*), (.*u), (.*)>::external')
    def __init__(self, value, desc_pos, obj_pos):
        self.value = value
        self.desc_pos = desc_pos
        self.obj_pos = obj_pos
    def is_live(self):
        return (self.value & 1) == 1
    def dead_size(self):
        return self.value / 2
    def migrator(self):
        static_migrators = gdb.parse_and_eval("'::debug::static_migrators'")
        migrator = static_migrators['_migrators']['_M_impl']['_M_start'][self.value >> 1]
        return migrator.dereference()
    def migrator_str(self):
        mig = str(self.migrator())
        m = re.match(self.mig_re, mig)
        return m.group(1)
    def live_size(self):
        mig = str(self.migrator())
        m = re.match(self.mig_re, mig)
        if m:
            type = m.group(1)
            external = self.vec_ext_re.match(type)
            if type == 'blob_storage':
                t = gdb.lookup_type('blob_storage')
                blob = self.obj_pos.cast(t.pointer())
                return t.sizeof + blob['frag_size']
            elif external:
                element_type = external.group(1)
                count = external.group(2)
                size_type = external.group(3)
                vec_type = gdb.lookup_type('managed_vector<%s, %s, %s>' % (element_type, count, size_type))
                # gdb doesn't see 'external' for some reason
                backref_ptr = self.obj_pos.cast(vec_type.pointer().pointer())
                vec = backref_ptr.dereference()
                element_count = vec['_capacity']
                element_type = gdb.lookup_type(element_type)
                return backref_ptr.type.sizeof + element_count * element_type.sizeof
            else:
                return gdb.lookup_type(type).sizeof
        return 0
    def end_pos(self):
        if self.is_live():
            return self.obj_pos + self.live_size()
        else:
            return self.desc_pos + self.dead_size()
    def __str__(self):
        if self.is_live():
            return '0x%x: live %s @ 0x%x size=%d' % (int(self.desc_pos), self.migrator(),
                                             int(self.obj_pos), self.live_size())
        else:
            return '0x%x: dead size=%d' % (int(self.desc_pos), self.dead_size())


class scylla_lsa_segment(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla lsa-segment', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        # See logalloc::region_impl::for_each_live()
        ptr = int(arg, 0)
        seg = gdb.parse_and_eval('(char*)(%d & ~(\'logalloc\'::segment::size - 1))' % (ptr))
        segment_size = int(gdb.parse_and_eval('\'logalloc\'::segment::size'))
        seg_end = seg + segment_size
        while seg < seg_end:
            desc = lsa_object_descriptor.decode(seg)
            print(desc)
            seg = desc.end_pos()

class scylla_timers(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla timers', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
    def invoke(self, arg, from_tty):
        gdb.write('Timers:\n')
        timer_set = gdb.parse_and_eval('\'seastar\'::local_engine->_timers')
        for timer_list in std_array(timer_set['_buckets']):
            for t in intrusive_list(timer_list):
                gdb.write('(%s*) %s = %s\n' % (t.type, t.address, t))
        timer_set = gdb.parse_and_eval('\'seastar\'::local_engine->_lowres_timers')
        for timer_list in std_array(timer_set['_buckets']):
            for t in intrusive_list(timer_list):
                gdb.write('(%s*) %s = %s\n' % (t.type, t.address, t))

def has_reactor():
    if gdb.parse_and_eval('\'seastar\'::local_engine'):
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
        reactor = gdb.parse_and_eval('\'seastar\'::local_engine')
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
            reactor = gdb.parse_and_eval('\'seastar\'::local_engine')
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
        gdb.write('Switched to thread %d, (seastar::thread_context*) 0x%x\n' % (self.gdb_thread.num, int(self.thread_ctx.address)))
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
                gdb.write('\n[shard %2d] (seastar::thread_context*) 0x%x:\n\n' % (r['_id'], int(t.address)))
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
                gdb.write('[shard %2d] (seastar::thread_context*) 0x%x\n' % (shard, int(t.address)))

class circular_buffer(object):
    def __init__(self, ref):
        self.ref = ref
    def __iter__(self):
        impl = self.ref['_impl']
        st = impl['storage']
        cap = impl['capacity']
        i = impl['begin']
        end = impl['end']
        while i < end:
            yield st[i % cap]
            i += 1

# Prints histogram of task types in reactor's pending task queue.
#
# Example:
# (gdb) scylla task-stats
#    16243: 0x18904f0 vtable for lambda_task<later()::{lambda()#1}> + 16
#    16091: 0x197fc60 _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_exception...
#    16090: 0x19bab50 _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZN7s...
#    14280: 0x1b36940 _ZTV12continuationIZN6futureIJEE12then_wrappedIZN17smp_message_queue15...
#
#    ^      ^         ^
#    |      |         '-- symbol name for vtable pointer
#    |      '------------ vtable pointer for the object pointed to by task*
#    '------------------- task count
#
class scylla_task_stats(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla task-stats', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
    def invoke(self, arg, for_tty):
        vptr_count = defaultdict(int)
        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        for t in circular_buffer(gdb.parse_and_eval('\'seastar\'::local_engine._pending_tasks')):
            vptr = int(t['_M_t']['_M_head_impl'].reinterpret_cast(vptr_type).dereference())
            vptr_count[vptr] += 1
        for vptr, count in sorted(vptr_count.items(), key=lambda e: -e[1]):
            gdb.write('%10d: 0x%x %s\n' % (count, vptr, resolve(vptr)))


# Prints contents of reactor pending tasks queue.
#
# Example:
# (gdb) scylla tasks
# (task*) 0x60017d8c7f88  _ZTV12continuationIZN6futureIJEE12then_wrappedIZN17smp_message_queu...
# (task*) 0x60019a391730  _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_except...
# (task*) 0x60018fac2208  vtable for lambda_task<later()::{lambda()#1}> + 16
# (task*) 0x60016e8b7428  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
# (task*) 0x60017e5bece8  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
# (task*) 0x60017e7f8aa0  _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_except...
# (task*) 0x60018fac21e0  vtable for lambda_task<later()::{lambda()#1}> + 16
# (task*) 0x60016e8b7540  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
# (task*) 0x600174c34d58  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
#
#         ^               ^
#         |               |
#         |               '------------ symbol name for task's vtable pointer
#         '---------------------------- task pointer
#
class scylla_tasks(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla tasks', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
    def invoke(self, arg, for_tty):
        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        for t in circular_buffer(gdb.parse_and_eval('\'seastar\'::local_engine._pending_tasks')):
            ptr = t['_M_t']['_M_head_impl']
            vptr = int(ptr.reinterpret_cast(vptr_type).dereference())
            gdb.write('(task*) 0x%x  %s\n' % (ptr, resolve(vptr)))

def find_in_live(mem_start, mem_size, value, size_selector='g'):
    for line in gdb.execute("find/%s 0x%x, +0x%x, 0x%x" % (size_selector, mem_start, mem_size, value), to_string=True).split('\n'):
        if line.startswith('0x'):
            ptr_info = gdb.execute("scylla ptr %s" % line, to_string=True)
            if 'live' in ptr_info:
                m = re.search('live \((0x[0-9a-f]+)', ptr_info)
                if m:
                    obj_start = int(m.group(1), 0)
                    addr = int(line, 0)
                    offset = addr - obj_start
                    yield obj_start, offset

# Finds live objects on seastar heap of current shard which contain given value.
# Prints results in 'scylla ptr' format.
#
# Example:
#
#   (gdb) scylla find 0x600005321900
#   thread 1, small (size <= 512), live (0x6000000f3800 +48)
#   thread 1, small (size <= 56), live (0x6000008a1230 +32)
#
class scylla_find(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla find', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
    def invoke(self, arg, for_tty):
        args = arg.split(' ')
        def print_usage():
            gdb.write("Usage: scylla find [ -w | -g ] <value>\n")

        if len(args) < 1 or not args[0]:
            print_usage()
            return

        selector = 'g'
        if args[0] in ['-w', '-g']:
            selector = args[0][1:]
            args = args[1:]

        if len(args) != 1:
            print_usage()
            return
        value = int(args[0], 0)

        mem_start, mem_size = get_seastar_memory_start_and_size()
        for obj, off in find_in_live(mem_start, mem_size, value, 'g'):
            gdb.execute("scylla ptr 0x%x" % (obj + off))

scylla()
scylla_databases()
scylla_keyspaces()
scylla_column_families()
scylla_memory()
scylla_ptr()
scylla_mem_ranges()
scylla_mem_range()
scylla_heapprof()
scylla_lsa()
scylla_lsa_segment()
scylla_segment_descs()
scylla_timers()
scylla_apply()
scylla_shard()
scylla_thread()
scylla_unthread()
scylla_threads()
scylla_task_stats()
scylla_tasks()
scylla_find()
scylla_task_histogram()
scylla_active_sstables()
