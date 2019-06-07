#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gdb
import gdb.printing
import uuid
import argparse
import re
from operator import attrgetter
from collections import defaultdict
import sys
import struct
import random
import bisect


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
        except Exception:
            # Some boost versions have this instead
            self.root = rps['m_header']
        member_hook = get_template_arg_with_prefix(list_type, "boost::intrusive::member_hook")
        if member_hook:
            self.link_offset = member_hook.template_argument(2).cast(self.size_t)
        else:
            self.link_offset = get_base_class_offset(self.node_type, "boost::intrusive::list_base_hook")
            if self.link_offset is None:
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


class std_optional:
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        return self.ref['_M_payload']['_M_payload']

    def __bool__(self):
        return self.__nonzero__()

    def __nonzero__(self):
        return bool(self.ref['_M_payload']['_M_engaged'])


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


class boost_variant:
    def __init__(self, ref):
        self.ref = ref

    def which(self):
        return self.ref['which_']

    def type(self):
        return self.ref.type.template_argument(self.ref['which_'])

    def get(self):
        return self.ref['storage_'].address.cast(self.type().pointer())


class std_map:
    size_t = gdb.lookup_type('size_t')

    def __init__(self, ref):
        container_type = ref.type.strip_typedefs()
        kt = container_type.template_argument(0)
        vt = container_type.template_argument(1)
        self.value_type = gdb.lookup_type('::std::pair<{} const, {} >'.format(str(kt), str(vt)))
        self.root = ref['_M_t']['_M_impl']['_M_header']['_M_parent']

    def __visit(self, node):
        if node:
            for n in self.__visit(node['_M_left']):
                yield n

            value = (node + 1).cast(self.value_type.pointer()).dereference()
            yield value['first'], value['second']

            for n in self.__visit(node['_M_right']):
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

    def __getitem__(self, item):
        return (self.ref['_M_impl']['_M_start'] + item).dereference()

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()

    def external_memory_footprint(self):
        return int(self.ref['_M_impl']['_M_end_of_storage']) - int(self.ref['_M_impl']['_M_start'])


class static_vector:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        return int(self.ref['m_holder']['m_size'])

    def __iter__(self):
        t = self.ref.type.strip_typedefs()
        value_type = t.template_argument(0)
        data = self.ref['m_holder']['storage']['dummy']['dummy'].cast(value_type.pointer())
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


def current_shard():
    return int(gdb.parse_and_eval('\'seastar\'::local_engine->_id'))


def find_db(shard=None):
    if not shard:
        shard = current_shard()
    return gdb.parse_and_eval('::debug::db')['_instances']['_M_impl']['_M_start'][shard]['service']['_p']


def find_dbs():
    return [find_db(shard) for shard in range(cpus())]


def for_each_table(db=None):
    if not db:
        db = find_db()
    cfs = db['_column_families']
    for (key, value) in list_unordered_map(cfs):
        yield value['_p'].reinterpret_cast(gdb.lookup_type('column_family').pointer()).dereference()  # it's a lw_shared_ptr


def list_unordered_map(map, cache=True):
    kt = map.type.template_argument(0)
    vt = map.type.template_argument(1)
    value_type = gdb.lookup_type('::std::pair<{} const, {} >'.format(str(kt), str(vt)))
    hashnode_ptr_type = gdb.lookup_type('::std::__detail::_Hash_node<' + value_type.name + ', ' + ('false', 'true')[cache] + '>').pointer()
    h = map['_M_h']
    p = h['_M_before_begin']['_M_nxt']
    while p:
        pc = p.cast(hashnode_ptr_type)['_M_storage']['_M_storage']['__data'].cast(value_type.pointer())
        yield (pc['first'], pc['second'])
        p = p['_M_nxt']


def list_unordered_set(map, cache=True):
    value_type = map.type.template_argument(0)
    hashnode_ptr_type = gdb.lookup_type('::std::__detail::_Hash_node<' + value_type.name + ', ' + ('false', 'true')[cache] + '>').pointer()
    h = map['_M_h']
    p = h['_M_before_begin']['_M_nxt']
    while p:
        pc = p.cast(hashnode_ptr_type)['_M_storage']['_M_storage']['__data'].cast(value_type.pointer())
        yield pc.dereference()
        p = p['_M_nxt']


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

        sc = span_checker()
        vptr_count = defaultdict(int)
        scanned_pages = 0
        limit = 20000
        for idx in random.sample(range(0, nr_pages), nr_pages):
            span = sc.get_span(mem_start + idx * page_size)
            if not span or span.index != idx or not span.is_small():
                continue
            pool = span.pool()
            if int(pool.dereference()['_object_size']) != size and size != 0:
                continue
            scanned_pages += 1
            objsize = size if size != 0 else int(pool.dereference()['_object_size'])
            span_size = span.used_span_size() * page_size
            for idx2 in range(0, int(span_size / objsize)):
                obj_addr = span.start + idx2 * objsize
                addr = gdb.Value(obj_addr).reinterpret_cast(vptr_type).dereference()
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
    except Exception:
        ptr_type = gdb.lookup_type('sstables::sstable_mutation_reader').pointer()
        vtable_name = 'vtable for sstables::sstable_mutation_reader'

    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr)
        if name and name.startswith(vtable_name):
            yield obj_addr.reinterpret_cast(ptr_type)

def find_active_sstables():
    """ Yields sstable* once for each active sstable reader. """
    sstable_ptr_type = gdb.lookup_type('sstables::sstable').pointer()
    for reader in find_single_sstable_readers():
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
        except Exception:
            count_index_lists = None

        sstables = dict()  # name -> sstable*
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


class seastar_shared_ptr():
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        return self.ref['_p']


def has_enable_lw_shared_from_this(type):
    for f in type.fields():
        if f.is_base_class and 'enable_lw_shared_from_this' in f.name:
            return True
    return False


class seastar_lw_shared_ptr():
    def __init__(self, ref):
        self.ref = ref
        self.elem_type = ref.type.template_argument(0)

    def get(self):
        if has_enable_lw_shared_from_this(self.elem_type):
            return self.ref['_p'].cast(self.elem_type.pointer())
        else:
            type = gdb.lookup_type('seastar::shared_ptr_no_esft<%s>' % str(self.elem_type.unqualified())).pointer()
            return self.ref['_p'].cast(type)['_value'].address


class lsa_region():
    def __init__(self, region):
        impl_ptr_type = gdb.lookup_type('logalloc::region_impl').pointer()
        self.region = seastar_shared_ptr(region['_impl']).get().cast(impl_ptr_type)
        self.segment_size = int(gdb.parse_and_eval('\'logalloc::segment::size\''))

    def total(self):
        size = int(self.region['_closed_occupancy']['_total_space'])
        if int(self.region['_active_offset']) > 0:
            size += self.segment_size
        return size

    def free(self):
        return int(self.region['_closed_occupancy']['_free_space'])

    def used(self):
        return self.total() - self.free()


class dirty_mem_mgr():
    def __init__(self, ref):
        self.ref = ref

    def real_dirty(self):
        return int(self.ref['_real_region_group']['_total_memory'])

    def virt_dirty(self):
        return int(self.ref['_virtual_region_group']['_total_memory'])


def find_instances(type_name):
    """
    A generator for pointers to live objects of virtual type 'type_name'.
    Only objects located at the beginning of allocation block are returned.
    This is true, for instance, for all objects allocated using std::make_unique().
    """
    ptr_type = gdb.lookup_type(type_name).pointer()
    vtable_name = 'vtable for %s ' % type_name
    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr)
        if name and name.startswith(vtable_name):
            yield gdb.Value(obj_addr).cast(ptr_type)


class span(object):
    """
    Represents seastar allocator's memory span
    """

    def __init__(self, index, start, page):
        """
        :param index: index into cpu_mem.pages of the first page of the span
        :param start: memory address of the first page of the span
        :param page: seastar::memory::page* for the first page of the span
        """
        self.index = index
        self.start = start
        self.page = page

    def is_free(self):
        return self.page['free']

    def pool(self):
        """
        Returns seastar::memory::small_pool* of this span.
        Valid only when is_small().
        """
        return self.page['pool']

    def is_small(self):
        return not self.is_free() and self.page['pool']

    def is_large(self):
        return not self.is_free() and not self.page['pool']

    def size(self):
        return int(self.page['span_size'])

    def used_span_size(self):
        """
        Returns the number of pages at the front of the span which are used by the allocator.

        Due to https://github.com/scylladb/seastar/issues/625 there may be some
        pages at the end of the span which are not used by the small pool.
        We try to detect this. It's not 100% accurrate but should work in most cases.

        Returns 0 for free spans.
        """
        n_pages = 0
        pool = self.page['pool']
        if self.page['free']:
            return 0
        if not pool:
            return self.page['span_size']
        for idx in range(int(self.page['span_size'])):
            page = self.page.address + idx
            if not page['pool'] or page['pool'] != pool or page['offset_in_span'] != idx:
                break
            n_pages += 1
        return n_pages


def spans():
    cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
    page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
    nr_pages = int(cpu_mem['nr_pages'])
    pages = cpu_mem['pages']
    mem_start = int(cpu_mem['memory'])
    idx = 1
    while idx < nr_pages:
        page = pages[idx]
        span_size = int(page['span_size'])
        if span_size == 0:
            idx += 1
            continue
        last_page = pages[idx + span_size - 1]
        addr = mem_start + idx * page_size
        yield span(idx, addr, page)
        idx += span_size


class span_checker(object):
    def __init__(self):
        self._page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        span_list = list(spans())
        self._start_to_span = dict((s.start, s) for s in span_list)
        self._starts = list(s.start for s in span_list)

    def spans(self):
        return self._start_to_span.values()

    def get_span(self, ptr):
        idx = bisect.bisect_right(self._starts, ptr)
        if idx == 0:
            return None
        span_start = self._starts[idx - 1]
        s = self._start_to_span[span_start]
        if span_start + s.page['span_size'] * self._page_size <= ptr:
            return None
        return s


class scylla_memory(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla memory', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        free_mem = int(cpu_mem['nr_free_pages']) * page_size
        total_mem = int(cpu_mem['nr_pages']) * page_size
        gdb.write('Used memory: {used_mem:>13}\nFree memory: {free_mem:>13}\nTotal memory: {total_mem:>12}\n\n'
                  .format(used_mem=total_mem - free_mem, free_mem=free_mem, total_mem=total_mem))

        lsa = gdb.parse_and_eval('\'logalloc::shard_segment_pool\'')
        segment_size = int(gdb.parse_and_eval('\'logalloc::segment::size\''))
        lsa_free = int(lsa['_free_segments']) * segment_size
        non_lsa_mem = int(lsa['_non_lsa_memory_in_use'])
        lsa_used = int(lsa['_segments_in_use']) * segment_size + non_lsa_mem
        lsa_allocated = lsa_used + lsa_free

        gdb.write('LSA:\n'
                  '  allocated: {lsa:>13}\n'
                  '  used:      {lsa_used:>13}\n'
                  '  free:      {lsa_free:>13}\n\n'
                  .format(lsa=lsa_allocated, lsa_used=lsa_used, lsa_free=lsa_free))

        db = find_db()
        cache_region = lsa_region(db['_row_cache_tracker']['_region'])

        gdb.write('Cache:\n'
                  '  total:     {cache_total:>13}\n'
                  '  used:      {cache_used:>13}\n'
                  '  free:      {cache_free:>13}\n\n'
                  .format(cache_total=cache_region.total(), cache_used=cache_region.used(), cache_free=cache_region.free()))

        gdb.write('Memtables:\n'
                  ' total:       {total:>13}\n'
                  ' Regular:\n'
                  '  real dirty: {reg_real_dirty:>13}\n'
                  '  virt dirty: {reg_virt_dirty:>13}\n'
                  ' System:\n'
                  '  real dirty: {sys_real_dirty:>13}\n'
                  '  virt dirty: {sys_virt_dirty:>13}\n'
                  ' Streaming:\n'
                  '  real dirty: {str_real_dirty:>13}\n'
                  '  virt dirty: {str_virt_dirty:>13}\n\n'
                  .format(total=(lsa_allocated-cache_region.total()),
                          reg_real_dirty=dirty_mem_mgr(db['_dirty_memory_manager']).real_dirty(),
                          reg_virt_dirty=dirty_mem_mgr(db['_dirty_memory_manager']).virt_dirty(),
                          sys_real_dirty=dirty_mem_mgr(db['_system_dirty_memory_manager']).real_dirty(),
                          sys_virt_dirty=dirty_mem_mgr(db['_system_dirty_memory_manager']).virt_dirty(),
                          str_real_dirty=dirty_mem_mgr(db['_streaming_dirty_memory_manager']).real_dirty(),
                          str_virt_dirty=dirty_mem_mgr(db['_streaming_dirty_memory_manager']).virt_dirty()))

        gdb.write('Small pools:\n')
        small_pools = cpu_mem['small_pools']
        nr = small_pools['nr_small_pools']
        gdb.write('{objsize:>5} {span_size:>6} {use_count:>10} {memory:>12} {unused:>12} {wasted_percent:>5}\n'
                  .format(objsize='objsz', span_size='spansz', use_count='usedobj', memory='memory',
                          unused='unused', wasted_percent='wst%'))
        total_small_bytes = 0
        sc = span_checker()
        for i in range(int(nr)):
            sp = small_pools['_u']['a'][i]
            object_size = int(sp['_object_size'])
            span_size = int(sp['_span_sizes']['preferred']) * page_size
            free_count = int(sp['_free_count'])
            pages_in_use = 0
            use_count = 0
            for s in sc.spans():
                if s.pool() == sp.address:
                    pages_in_use += s.size()
                    use_count += int(s.used_span_size() * page_size / object_size)
            memory = pages_in_use * page_size
            total_small_bytes += memory
            use_count -= free_count
            wasted = free_count * object_size
            unused = memory - use_count * object_size
            wasted_percent = wasted * 100.0 / memory if memory else 0
            gdb.write('{objsize:5} {span_size:6} {use_count:10} {memory:12} {unused:12} {wasted_percent:5.1f}\n'
                      .format(objsize=object_size, span_size=span_size, use_count=use_count, memory=memory, unused=unused,
                              wasted_percent=wasted_percent))
        gdb.write('Small allocations: %d [B]\n' % total_small_bytes)

        large_allocs = defaultdict(int) # key: span size [B], value: span count
        for s in sc.spans():
            span_size = s.size()
            if s.is_large():
                large_allocs[span_size * page_size] += 1

        gdb.write('Page spans:\n')
        gdb.write('{index:5} {size:>13} {total:>13} {allocated_size:>13} {allocated_count:>7}\n'.format(
            index="index", size="size [B]", total="free [B]", allocated_size="large [B]", allocated_count="[spans]"))
        total_large_bytes = 0
        for index in range(int(cpu_mem['nr_span_lists'])):
            span_list = cpu_mem['free_spans'][index]
            front = int(span_list['_front'])
            pages = cpu_mem['pages']
            total = 0
            while front:
                span = pages[front]
                total += int(span['span_size'])
                front = int(span['link']['_next'])
            span_size = (1 << index) * page_size
            allocated_size = large_allocs[span_size] * span_size
            total_large_bytes += allocated_size
            gdb.write('{index:5} {size:13} {total:13} {allocated_size:13} {allocated_count:7}\n'.format(index=index, size=span_size, total=total * page_size,
                                                                allocated_count=large_allocs[span_size],
                                                                allocated_size=allocated_size))
        gdb.write('Large allocations: %d [B]\n' % total_large_bytes)


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
                addresses.pop(0)  # drop memory::get_backtrace()
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
                        out.write("%s %d\n" % (';'.join(map(lambda x: '%s' % (x), map(resolver, trace))), n.size))
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


class pointer_metadata(object):
    def __init__(self, ptr, *args):
        if isinstance(args[0], gdb.InferiorThread):
            self._init_seastar_ptr(ptr, *args)
        else:
            self._init_generic_ptr(ptr, *args)

    def _init_seastar_ptr(self, ptr, thread):
        self.ptr = ptr
        self.thread = thread
        self._is_containing_page_free = False
        self.is_small = False
        self.is_live = False
        self.is_lsa = False
        self.size = 0
        self.offset_in_object = 0

    def _init_generic_ptr(self, ptr, speculative_size):
        self.ptr = ptr
        self.thread = None
        self._is_containing_page_free = None
        self.is_small = None
        self.is_live = None
        self.is_lsa = None
        self.size = speculative_size
        self.offset_in_object = 0

    def is_managed_by_seastar(self):
        return not self.thread is None

    @property
    def is_containing_page_free(self):
        return self._is_containing_page_free

    def mark_free(self):
        self._is_containing_page_free = True
        self._is_live = False

    def __str__(self):
        if not self.is_managed_by_seastar():
            return "0x{:x} (default allocator)".format(self.ptr)

        msg = "thread %d" % self.thread.num

        if self.is_containing_page_free:
            msg += ', page is free'
            return msg

        if self.is_small:
            msg += ', small (size <= %d)' % self.size
        else:
            msg += ', large (size=%d)' % self.size

        if self.is_live:
            msg += ', live (0x%x +%d)' % (self.ptr - self.offset_in_object, self.offset_in_object)
        else:
            msg += ', free'

        if self.is_lsa:
            msg += ', LSA-managed'

        return msg


class scylla_ptr(gdb.Command):
    _is_seastar_allocator_used = None

    def __init__(self):
        gdb.Command.__init__(self, 'scylla ptr', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    @staticmethod
    def is_seastar_allocator_used():
        if not scylla_ptr._is_seastar_allocator_used is None:
            return scylla_ptr._is_seastar_allocator_used

        try:
            gdb.parse_and_eval('&\'seastar::memory::cpu_mem\'')
            scylla_ptr._is_seastar_allocator_used = True
            return True
        except:
            scylla_ptr._is_seastar_allocator_used = False
            return False

    @staticmethod
    def analyze(ptr):
        owning_thread = None
        for t, start, size in seastar_memory_layout():
            if ptr >= start and ptr < start + size:
                owning_thread = t
                break

        ptr_meta = pointer_metadata(ptr, owning_thread)

        if not owning_thread:
            return ptr_meta

        owning_thread.switch()

        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        offset = ptr - int(cpu_mem['memory'])
        ptr_page_idx = offset / page_size
        pages = cpu_mem['pages']
        page = pages[ptr_page_idx]

        span = span_checker().get_span(ptr)
        offset_in_span = ptr - span.start
        if offset_in_span >= span.used_span_size() * page_size:
            ptr_meta.mark_free()
        elif span.is_small():
            pool = span.pool()
            object_size = int(pool['_object_size'])
            ptr_meta.size = object_size
            ptr_meta.is_small = True
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
                first_page_in_span = span.page
                next_free = first_page_in_span['freelist']
                while next_free:
                    if ptr >= next_free and ptr < next_free.reinterpret_cast(char_ptr) + object_size:
                        free = True
                        break
                    next_free = next_free.reinterpret_cast(free_object_ptr).dereference()
            if free:
                ptr_meta.is_live = False
            else:
                ptr_meta.is_live = True
                ptr_meta.offset_in_object = offset_in_object
        else:
            ptr_meta.is_small = False
            ptr_meta.is_live = not span.is_free()
            ptr_meta.size = span.size() * page_size
            ptr_meta.offset_in_object = ptr - span.start

        # FIXME: handle debug-mode build
        index = gdb.parse_and_eval('(%d - \'logalloc::shard_segment_pool\'._segments_base) / \'logalloc::segment\'::size' % (ptr))
        desc = gdb.parse_and_eval('\'logalloc::shard_segment_pool\'._segments._M_impl._M_start[%d]' % (index))
        ptr_meta.is_lsa = bool(desc['_region'])

        return ptr_meta

    def invoke(self, arg, from_tty):
        ptr = int(gdb.parse_and_eval(arg))

        ptr_meta = self.analyze(ptr)

        gdb.write("{}\n".format(str(ptr_meta)))

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
                  .format(lsa_mem=lsa_mem, non_lsa_mem=non_lsa_mem, total_mem=total_mem))

        er_goal = int(lsa['_current_emergency_reserve_goal'])
        er_max = int(lsa['_emergency_reserve_max'])
        free_segments = int(lsa['_free_segments'])
        gdb.write('Emergency reserve goal: {er_goal:>11}\n'
                  'Emergency reserve max: {er_max:>12}\n'
                  'Free segments:         {free_segments:>12}\n\n'
                  .format(er_goal=er_goal, er_max=er_max, free_segments=free_segments))

        lsa_tracker = std_unique_ptr(gdb.parse_and_eval('\'logalloc::tracker_instance\'._impl'))
        regions = lsa_tracker['_regions']
        region = regions['_M_impl']['_M_start']
        gdb.write('LSA regions:\n')
        while region != regions['_M_impl']['_M_finish']:
            gdb.write('    Region #{r_id} (logalloc::region_impl*) 0x{r_addr}\n      - reclaimable: {r_en:>14}\n'
                      '      - evictable: {r_ev:16}\n      - non-LSA memory: {r_non_lsa:>11}\n'
                      '      - closed LSA memory: {r_lsa:>8}\n      - unused memory: {r_unused:>12}\n'
                      .format(r_addr=str(region.dereference()), r_id=int(region['_id']), r_en=bool(region['_reclaiming_enabled']),
                              r_ev=bool(region['_evictable']),
                              r_non_lsa=int(region['_non_lsa_occupancy']['_total_space']),
                              r_lsa=int(region['_closed_occupancy']['_total_space']),
                              r_unused=int(region['_closed_occupancy']['_free_space'])))
            region = region + 1


names = {}  # addr (int) -> name (str)


def resolve(addr, cache=True):
    if addr in names:
        return names[addr]

    infosym = gdb.execute('info symbol 0x%x' % (addr), False, True)
    if infosym.startswith('No symbol'):
        name = None
    else:
        name = infosym[:infosym.find('in section')]
    if cache:
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
        'rbx': 0,
        'rbp': 1,
        'r12': 2,
        'r13': 3,
        'r14': 4,
        'r15': 5,
        'rsp': 6,
        'rip': 7,
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
        holder_addr = get_seastar_memory_start_and_size()[0]
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
                value = (value << (bits - shift)) & (2**bits - 1) | (value >> shift)
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

    def size(self):
        impl = self.ref['_impl']
        return int(impl['end']) - int(impl['begin'])

    def external_memory_footprint(self):
        impl = self.ref['_impl']
        return int(impl['capacity']) * self.ref.type.template_argument(0).sizeof


class small_vector(object):
    def __init__(self, ref):
        self.ref = ref

    def external_memory_footprint(self):
        if self.ref['_begin'] == self.ref['_internal']['storage'].address:
            return 0
        return int(self.ref['_capacity_end']) - int(self.ref['_begin'])


class chunked_vector(object):
    def __init__(self, ref):
        self.ref = ref

    def external_memory_footprint(self):
        return int(self.ref['_capacity']) * self.ref.type.template_argument(0).sizeof \
               + small_vector(self.ref['_chunks']).external_memory_footprint()


def get_local_tasks():
    """ Return a list of task pointers for the local reactor. """
    for tq_ptr in static_vector(gdb.parse_and_eval('\'seastar\'::local_engine._task_queues')):
        tq = std_unique_ptr(tq_ptr).dereference()
        for t in circular_buffer(tq['_q']):
            yield std_unique_ptr(t).get()


class scylla_task_stats(gdb.Command):
    """ Prints histogram of task types in reactor's pending task queue.

    Example:
    (gdb) scylla task-stats
       16243: 0x18904f0 vtable for lambda_task<later()::{lambda()#1}> + 16
       16091: 0x197fc60 _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_exception...
       16090: 0x19bab50 _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZN7s...
       14280: 0x1b36940 _ZTV12continuationIZN6futureIJEE12then_wrappedIZN17smp_message_queue15...

       ^      ^         ^
       |      |         '-- symbol name for vtable pointer
       |      '------------ vtable pointer for the object pointed to by task*
       '------------------- task count
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla task-stats', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        vptr_count = defaultdict(int)
        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        for ptr in get_local_tasks():
            vptr = int(ptr.reinterpret_cast(vptr_type).dereference())
            vptr_count[vptr] += 1
        for vptr, count in sorted(vptr_count.items(), key=lambda e: -e[1]):
            gdb.write('%10d: 0x%x %s\n' % (count, vptr, resolve(vptr)))


class scylla_tasks(gdb.Command):
    """ Prints contents of reactor pending tasks queue.

    Example:
    (gdb) scylla tasks
    (task*) 0x60017d8c7f88  _ZTV12continuationIZN6futureIJEE12then_wrappedIZN17smp_message_queu...
    (task*) 0x60019a391730  _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_except...
    (task*) 0x60018fac2208  vtable for lambda_task<later()::{lambda()#1}> + 16
    (task*) 0x60016e8b7428  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x60017e5bece8  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x60017e7f8aa0  _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_except...
    (task*) 0x60018fac21e0  vtable for lambda_task<later()::{lambda()#1}> + 16
    (task*) 0x60016e8b7540  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x600174c34d58  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...

            ^               ^
            |               |
            |               '------------ symbol name for task's vtable pointer
            '---------------------------- task pointer
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla tasks', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        for ptr in get_local_tasks():
            vptr = int(ptr.reinterpret_cast(vptr_type).dereference())
            gdb.write('(task*) 0x%x  %s\n' % (ptr, resolve(vptr)))



class scylla_fiber(gdb.Command):
    """ Walk the continuation chain starting from the given task

    Example (cropped for brevity):
    (gdb) scylla fiber this
    #0  (task*) 0x0000600000550360 0x000000000468ac40 vtable for seastar::continuation<seastar::future<>::then_impl<...>(...)::{lambda(auto:1)#1}> + 16
    #1  (task*) 0x0000600000550300 0x00000000046c3778 vtable for seastar::continuation<seastar::future<seastar::optimized_optional<mutation_fragment> >::then_impl<>()::{lambda(auto:2)#1}, seastar::optimized_optional<mutation_fragment> > + 16
    #2  (task*) 0x00006000018af600 0x00000000046c37a0 vtable for seastar::continuation<seastar::future<>::then_impl<...>(...)::{lambda(auto:1)#1}> + 16
    #3  (task*) 0x00006000005502a0 0x00000000046c37f0 vtable for seastar::continuation<seastar::future<>::then_impl<...>(...)::{lambda(auto:1)#1}> + 16
    #4  (task*) 0x0000600001a65e10 0x00000000046c6b10 vtable for seastar::continuation<seastar::future<boost::iterator_range<mutation_fragment*> >::then_impl<...>(...)::{lambda(auto:1)#1}, boost::iterator_range<mutation_fragment*> > + 16
     ^          ^                  ^                  ^
    (1)        (2)                (3)                (4)

    1) Task index (0 is the task passed to the command).
    2) Pointer to the task object.
    3) Pointer to the task's vtable.
    4) Symbol name of the task's vtable.

    Invoke `scylla fiber --help` for more information on usage.
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla fiber', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
        self._vptr_type = gdb.lookup_type('uintptr_t').pointer()
        # List of whitelisted symbol names. Each symbol is a tuple, where each
        # element is a component of the name, the last element being the class
        # name itself.
        # We can't just merge them as `info symbol` might return mangled names too.
        self._whitelist = scylla_fiber._make_symbol_matchers([
                ("seastar", "continuation"),
                ("seastar", "future", "thread_wake_task"),
                ("seastar", "internal", "do_until_state"),
                ("seastar", "internal", "do_with_state"),
                ("seastar", "internal", "repeat_until_value_state"),
                ("seastar", "internal", "repeater"),
                ("seastar", "internal", "when_all_state_component"),
                ("seastar", "lambda_task"),
                ("seastar", "smp_message_queue", "async_work_item"),
        ])


    @staticmethod
    def _make_symbol_matchers(symbol_specs):
        return list(map(scylla_fiber._make_symbol_matcher, symbol_specs))

    @staticmethod
    def _make_symbol_matcher(symbol_spec):
        unmangled_prefix = 'vtable for {}'.format('::'.join(symbol_spec))
        def matches_symbol(name):
            if name.startswith(unmangled_prefix):
                return True

            try:
                positions = [name.index(part) for part in symbol_spec]
                return sorted(positions) == positions
            except ValueError:
                return False

        return matches_symbol

    def _name_is_on_whitelist(self, name):
        for matcher in self._whitelist:
            if matcher(name):
                return True
        return False

    def _maybe_log(self, msg, verbose):
        if verbose:
            gdb.write(msg)

    def _probe_pointer(self, ptr, scanned_region_size, using_seastar_allocator, verbose):
        """ Check if the pointer is a task pointer

        The pattern we are looking for is:
        ptr -> vtable ptr for a symbol that matches our whitelist

        In addition, ptr has to point to a the beginning of an allocation
        block, managed by seastar, that contains a live object.
        """
        try:
            maybe_vptr = int(gdb.Value(ptr).reinterpret_cast(self._vptr_type).dereference())
            self._maybe_log(" -> 0x{:016x}".format(maybe_vptr), verbose)
        except gdb.MemoryError:
            self._maybe_log(" Not a pointer\n", verbose)
            return

        resolved_symbol = resolve(maybe_vptr, False)
        if resolved_symbol is None:
            self._maybe_log(" Not a vtable ptr\n", verbose)
            return

        self._maybe_log(" => {}".format(resolved_symbol), verbose)

        if not self._name_is_on_whitelist(resolved_symbol):
            self._maybe_log(" Symbol name doesn't match whitelisted symbols\n", verbose)
            return

        if using_seastar_allocator:
            ptr_meta = scylla_ptr.analyze(ptr)
            if not ptr_meta.is_managed_by_seastar() or not ptr_meta.is_live or ptr_meta.offset_in_object != 0:
                self._maybe_log(" Not the start of an allocation block or not a live object\n", verbose)
                return
        else:
            ptr_meta = pointer_metadata(ptr, scanned_region_size)

        self._maybe_log(" Task found\n", verbose)

        return ptr_meta, maybe_vptr, resolved_symbol

    def _do_walk(self, ptr_meta, i, max_depth, scanned_region_size, using_seastar_allocator, verbose):
        if max_depth > -1 and i >= max_depth:
            return []

        ptr = ptr_meta.ptr
        region_start = ptr + self._vptr_type.sizeof # ignore our own vtable
        region_end = region_start + (ptr_meta.size - ptr_meta.size % self._vptr_type.sizeof)
        self._maybe_log("Scanning task #{} @ 0x{:016x}: {}\n".format(i, ptr, str(ptr_meta)), verbose)

        for it in range(region_start, region_end, self._vptr_type.sizeof):
            maybe_tptr = int(gdb.Value(it).reinterpret_cast(self._vptr_type).dereference())
            self._maybe_log("0x{:016x}+0x{:04x} -> 0x{:016x}".format(ptr, it - ptr, maybe_tptr), verbose)

            res = self._probe_pointer(maybe_tptr, scanned_region_size, using_seastar_allocator, verbose)

            if res is None:
                continue

            tptr_meta, vptr, name = res

            fiber = self._do_walk(tptr_meta, i + 1, max_depth, scanned_region_size, using_seastar_allocator, verbose)
            fiber.append((maybe_tptr, vptr, name))
            return fiber

        return []

    def _walk(self, ptr, max_depth, scanned_region_size, force_fallback_mode, verbose):
        using_seastar_allocator = not force_fallback_mode and scylla_ptr.is_seastar_allocator_used()
        if using_seastar_allocator:
            ptr_meta = scylla_ptr.analyze(int(ptr))
            if not ptr_meta.is_managed_by_seastar():
                gdb.write("Task pointer 0x{:016x} is not an object managed by seastar\n")
                return []
        else:
            gdb.write("Not using the seastar allocator, falling back to scanning a fixed-size region of memory\n")
            ptr_meta = pointer_metadata(int(ptr), scanned_region_size)

        return reversed(self._do_walk(ptr_meta, 0, max_depth, scanned_region_size, using_seastar_allocator, verbose))

    def invoke(self, arg, for_tty):
        parser = argparse.ArgumentParser(description="scylla fiber")
        parser.add_argument("-v", "--verbose", action="store_true", default=False,
                help="Make the command more verbose about what it is doing")
        parser.add_argument("-d", "--max-depth", action="store", type=int, default=-1,
                help="Maximum depth to traverse on the continuation chain")
        parser.add_argument("-s", "--scanned-region-size", action="store", type=int, default=512,
                help="The size of the memory region to be scanned when examining a task object."
                " Only used in fallback-mode. Fallback mode is used either when the default allocator is used by the application"
                " (and hence pointer-metadata is not available) or when `scylla fiber` was invoked with `--force-fallback-mode`.")
        parser.add_argument("--force-fallback-mode", action="store_true", default=False,
                help="Force fallback mode to be used, that is, scan a fixed-size region of memory"
                " (configurable via --scanned-region-size), instead of relying on `scylla ptr` for determining the size of the task objects.")
        parser.add_argument("task", action="store", help="An expression that evaluates to a valid `seastar::task*` value. Cannot contain white-space.")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        try:
            fiber = self._walk(gdb.parse_and_eval(args.task), args.max_depth, args.scanned_region_size, args.force_fallback_mode, args.verbose)

            for i, (tptr, vptr, name) in enumerate(fiber):
                gdb.write("#{:<2d} (task*) 0x{:016x} 0x{:016x} {}\n".format(i, int(tptr), int(vptr), name))
        except KeyboardInterrupt:
            return


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

class scylla_find(gdb.Command):
    """ Finds live objects on seastar heap of current shard which contain given value.
    Prints results in 'scylla ptr' format.

    Example:

      (gdb) scylla find 0x600005321900
      thread 1, small (size <= 512), live (0x6000000f3800 +48)
      thread 1, small (size <= 56), live (0x6000008a1230 +32)
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla find', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        args = arg.split(' ')

        def print_usage():
            gdb.write("Usage: scylla find [ -w | -g ] <value>\n")

        if len(args) < 1 or not args[0]:
            print_usage()
            return

        if args[0] in ['-w', '-g']:
            args = args[1:]

        if len(args) != 1:
            print_usage()
            return
        value = int(args[0], 0)

        mem_start, mem_size = get_seastar_memory_start_and_size()
        for obj, off in find_in_live(mem_start, mem_size, value, 'g'):
            gdb.execute("scylla ptr 0x%x" % (obj + off))


class std_unique_ptr:
    def __init__(self, obj):
        self.obj = obj

    def get(self):
        return self.obj['_M_t']['_M_t']['_M_head_impl']

    def dereference(self):
        return self.get().dereference()

    def __getitem__(self, item):
        return self.dereference()[item]

    def address(self):
        return self.get()

    def __nonzero__(self):
        return bool(self.get())

    def __bool__(self):
        return self.__nonzero__()


class sharded:
    def __init__(self, val):
        self.val = val

    def local(self):
        shard = int(gdb.parse_and_eval('\'seastar\'::local_engine->_id'))
        return std_vector(self.val['_instances'])[shard]['service']['_p']


def ip_to_str(val, byteorder):
    return '%d.%d.%d.%d' % (struct.unpack('BBBB', val.to_bytes(4, byteorder=byteorder))[::-1])


class scylla_netw(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla netw', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        ms = sharded(gdb.parse_and_eval('netw::_the_messaging_service')).local()
        gdb.write('Dropped messages: %s\n' % ms['_dropped_messages'])
        gdb.write('Outgoing connections:\n')
        for (addr, shard_info) in list_unordered_map(ms['_clients']['_M_elems'][0]):
            ip = ip_to_str(int(addr['addr']['_addr']['ip']['raw']), byteorder=sys.byteorder)
            client = shard_info['rpc_client']['_p']
            rpc_client = std_unique_ptr(client['_p'])
            gdb.write('IP: %s, (netw::messaging_service::rpc_protocol_client_wrapper*) %s:\n' % (ip, client))
            gdb.write('  stats: %s\n' % rpc_client['_stats'])
            gdb.write('  outstanding: %d\n' % int(rpc_client['_outstanding']['_M_h']['_M_element_count']))

        servers = [
            std_unique_ptr(ms['_server']['_M_elems'][0]),
            std_unique_ptr(ms['_server']['_M_elems'][1]),
        ]
        for srv in servers:
            if srv:
                gdb.write('Server: resources=%s\n' % srv['_resources_available'])
                gdb.write('Incoming connections:\n')
                for clnt in list_unordered_set(srv['_conns']):
                    conn = clnt['_p'].cast(clnt.type.template_argument(0).pointer())
                    ip = ip_to_str(int(conn['_info']['addr']['u']['in']['sin_addr']['s_addr']), byteorder='big')
                    port = int(conn['_info']['addr']['u']['in']['sin_port'])
                    gdb.write('%s:%d: \n' % (ip, port))
                    gdb.write('   %s\n' % (conn['_stats']))


class scylla_gms(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla gms', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        gossiper = sharded(gdb.parse_and_eval('gms::_the_gossiper')).local()
        for (endpoint, state) in list_unordered_map(gossiper['endpoint_state_map']):
            ip = ip_to_str(int(endpoint['_addr']['ip']['raw']), byteorder=sys.byteorder)
            gdb.write('%s: (gms::endpoint_state*) %s (%s)\n' % (ip, state.address, state['_heart_beat_state']))
            for app_state, value in std_map(state['_application_state']):
                gdb.write('  %s: {version=%d, value=%s}\n' % (app_state, value['version'], value['value']))


class scylla_cache(gdb.Command):
    """Prints contents of the cache on current shard"""

    def __init__(self):
        gdb.Command.__init__(self, 'scylla cache', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        schema_ptr_type = gdb.lookup_type('schema').pointer()
        for table in for_each_table():
            schema = table['_schema']['_p'].reinterpret_cast(schema_ptr_type)
            name = '%s.%s' % (schema['_raw']['_ks_name'], schema['_raw']['_cf_name'])
            gdb.write("%s:\n" % (name))
            for e in intrusive_set(table['_cache']['_partitions']):
                gdb.write('  (cache_entry*) 0x%x {_key=%s, _flags=%s, _pe=%s}\n' % (
                    int(e.address), e['_key'], e['_flags'], e['_pe']))
            gdb.write("\n")


def find_sstables():
    """A generator which yields pointers to all live sstable objects on current shard."""
    visited = set()
    # FIXME: Add support for other sstable sets. Also, we should change Scylla to make this easier
    for sst_set in find_instances('sstables::bag_sstable_set'):
        sstables = std_vector(sst_set['_sstables'])
        for sst_ptr in sstables:
            sst = seastar_lw_shared_ptr(sst_ptr).get()
            if not int(sst) in visited:
                visited.add(int(sst))
                yield sst


class scylla_sstables(gdb.Command):
    """Lists all sstable objects on currents shard together with useful information like on-disk and in-memory size."""

    def __init__(self):
        gdb.Command.__init__(self, 'scylla sstables', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        filter_type = gdb.lookup_type('utils::filter::murmur3_bloom_filter')
        cpu_id = current_shard()
        total_size = 0 # in memory
        total_on_disk_size = 0
        count = 0

        for sst in find_sstables():
            count += 1
            size = 0

            sc = seastar_lw_shared_ptr(sst['_components']['_value']).get()
            local = sst['_components']['_cpu'] == cpu_id
            size += sc.dereference().type.sizeof

            bf = std_unique_ptr(sc['filter']).get().cast(filter_type.pointer())
            bf_size = bf.dereference().type.sizeof + chunked_vector(bf['_bitset']['_storage']).external_memory_footprint()
            size += bf_size

            summary_size = std_vector(sc['summary']['_summary_data']).external_memory_footprint()
            summary_size += chunked_vector(sc['summary']['entries']).external_memory_footprint()
            summary_size += chunked_vector(sc['summary']['positions']).external_memory_footprint()
            for e in std_vector(sc['summary']['_summary_data']):
                summary_size += e['_size'] + e.type.sizeof
            # FIXME: include external memory footprint of summary entries
            size += summary_size

            sm_size = 0
            sm = std_optional(sc['scylla_metadata'])
            if sm:
                for tag, value in list_unordered_map(sm.get()['data']['data']):
                    bv = boost_variant(value)
                    # FIXME: only gdb.Type.template_argument(0) works for boost::variant<>
                    if bv.which() != 0:
                        continue
                    val = bv.get()['value']
                    if str(val.type) == 'sstables::sharding_metadata':
                        sm_size += chunked_vector(val['token_ranges']['elements']).external_memory_footprint()
            size += sm_size

            # FIXME: Include compression info

            data_file_size = sst['_data_file_size']
            gdb.write('(sstables::sstable*) 0x%x: local=%d data_file=%d, in_memory=%d (bf=%d, summary=%d, sm=%d)\n'
                      % (int(sst), local, data_file_size, size, bf_size, summary_size, sm_size))

            if local:
                total_size += size
                total_on_disk_size += data_file_size

        gdb.write('total (shard-local): count=%d, data_file=%d, in_memory=%d\n' % (count, total_on_disk_size, total_size))


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
scylla_fiber()
scylla_find()
scylla_task_histogram()
scylla_active_sstables()
scylla_netw()
scylla_gms()
scylla_cache()
scylla_sstables()
