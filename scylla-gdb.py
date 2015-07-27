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
            len = self.val['u']['internal']['size']
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

gdb.printing.register_pretty_printer(gdb.current_objfile(), build_pretty_printer())

def cpus():
    return gdb.parse_and_eval('smp::count')

def find_db(shard):
    return gdb.parse_and_eval('debug::db')['_instances']['_M_impl']['_M_start'][shard]

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
                gdb.write('{:5} {} {:45} (column_family*){}\n'.format(shard, key, name, value.address))

scylla()
scylla_databases()
scylla_keyspaces()
scylla_column_families()
