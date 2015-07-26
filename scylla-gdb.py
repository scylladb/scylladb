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
