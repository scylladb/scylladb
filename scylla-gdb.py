#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gdb
import gdb.printing
import uuid
import argparse
import datetime
import functools
import re
from operator import attrgetter
from collections import defaultdict
import sys
import struct
import random
import bisect
import os
import subprocess
import time
import socket
import string
import math


def align_up(ptr, alignment):
    res = ptr % alignment
    if not res:
        return ptr
    return ptr + alignment - res


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
        if field.is_base_class:
            field_offset = int(field.bitpos / 8)
            if re.match(name_pattern, field.type.strip_typedefs().name):
                return field_offset
            offset = get_base_class_offset(field.type, base_class_name)
            if offset is not None:
                return field_offset + offset


def get_field_offset(gdb_type, name):
    for field in gdb_type.fields():
        if field.name == name:
            return int(field.bitpos / 8)


# Defer initialization to first use
# Don't prevent loading `scylla-gdb.py` due to any problem in the init code.
vtable_symbol_pattern = None
vptr_type = None


def _check_vptr(ptr):
    global vtable_symbol_pattern
    global vptr_type
    symbol_name = resolve(ptr.reinterpret_cast(vptr_type).dereference(), startswith='vtable for ')
    if symbol_name is None:
        raise ValueError("Failed to resolve first word of virtual object @ {} as a vtable symbol".format(int(ptr)))

    m = re.match(vtable_symbol_pattern, symbol_name)
    if m is None:
        raise ValueError("Failed to extract type name from symbol name `{}'".format(symbol_name))

    return m


def downcast_vptr(ptr):
    global vtable_symbol_pattern
    global vptr_type
    if vtable_symbol_pattern is None:
        vtable_symbol_pattern = re.compile(r'vtable for (.*) \+ ([0-9]+).*')
        vptr_type = gdb.lookup_type('uintptr_t').pointer()

    if not isinstance(ptr, gdb.Value):
        ptr = gdb.parse_and_eval(ptr)

    m = _check_vptr(ptr)

    actual_type = gdb.lookup_type(m.group(1))
    actual_type_ptr = actual_type.pointer()

    if int(m.group(2)) == 16:
        return ptr.reinterpret_cast(actual_type_ptr)

    # We are most likely dealing with multiple inheritance and a pointer to a
    # non-first base-class type.
    base_class_field = actual_type.fields()[0]
    assert base_class_field.is_base_class
    base_classes = list(base_class_field.type.fields())

    # The pointer is surely not to the first base-class, we would have found
    # the expected m.group(2) == 16 offset otherwise.
    for bc in base_classes[1:]:
        speculative_ptr = gdb.Value(int(ptr) - int(bc.bitpos / 8))
        m = _check_vptr(speculative_ptr)
        if int(m.group(2)) == 16:
            return speculative_ptr.reinterpret_cast(actual_type_ptr)

    return None


class intrusive_list:
    @classmethod
    @property
    @functools.cache
    def size_t(cls):
        return gdb.lookup_type('size_t')

    def __init__(self, list_ref, link=None):
        list_type = list_ref.type.strip_typedefs()
        self.node_type = list_type.template_argument(0)
        rps = list_ref['data_']['root_plus_size_']
        try:
            self.root = rps['root_']
        except gdb.error:
            # Some boost versions have this instead
            self.root = rps['m_header']
        if link is not None:
            self.link_offset = get_field_offset(self.node_type, link)
        else:
            member_hook = get_template_arg_with_prefix(list_type, "boost::intrusive::member_hook")
            if not member_hook:
                member_hook = get_template_arg_with_prefix(list_type, "struct boost::intrusive::member_hook")
            if member_hook:
                self.link_offset = member_hook.template_argument(2).cast(self.size_t)
            else:
                self.link_offset = get_base_class_offset(self.node_type, "boost::intrusive::list_base_hook")
                if self.link_offset is None:
                    raise Exception("Class does not extend list_base_hook: " + str(self.node_type))

    def __iter__(self):
        hook = self.root['next_']
        while hook and hook != self.root.address:
            node_ptr = hook.cast(self.size_t) - self.link_offset
            yield node_ptr.cast(self.node_type.pointer()).dereference()
            hook = hook['next_']

    def __nonzero__(self):
        return self.root['next_'] != self.root.address

    def __bool__(self):
        return self.__nonzero__()

    def __len__(self):
        return len(list(iter(self)))


class intrusive_slist:
    @classmethod
    @property
    @functools.cache
    def size_t(cls):
        return gdb.lookup_type('size_t')

    def __init__(self, list_ref, link=None):
        list_type = list_ref.type.strip_typedefs()
        self.node_type = list_type.template_argument(0)
        rps = list_ref['data_']['root_plus_size_']
        self.root = rps['header_holder_']

        if link is not None:
            self.link_offset = get_field_offset(self.node_type, link)
        else:
            member_hook = get_template_arg_with_prefix(list_type, "struct boost::intrusive::member_hook")
            if member_hook:
                self.link_offset = member_hook.template_argument(2).cast(self.size_t)
            else:
                self.link_offset = get_base_class_offset(self.node_type, "boost::intrusive::slist_base_hook")
                if self.link_offset is None:
                    raise Exception("Class does not extend slist_base_hook: " + str(self.node_type))

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

    def __len__(self):
        return len(list(iter(self)))


class std_optional:
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        try:
            return self.ref['_M_payload']['_M_payload']['_M_value']
        except gdb.error:
            return self.ref['_M_payload'] # Scylla 3.0 compatibility

    def __bool__(self):
        return self.__nonzero__()

    def __nonzero__(self):
        try:
            return bool(self.ref['_M_payload']['_M_engaged'])
        except gdb.error:
            return bool(self.ref['_M_engaged']) # Scylla 3.0 compatibility


class std_tuple:
    def __init__(self, ref):
        self.ref = ref
        self.members = []

        t = self.ref[self.ref.type.fields()[0]]
        while len(t.type.fields()) == 2:
            tail, head = t.type.fields()
            self.members.append(t[head]['_M_head_impl'])
            t = t[tail]

        self.members.append(t[t.type.fields()[0]]['_M_head_impl'])

    def __len__(self):
        return len(self.members)

    def __iter__(self):
        return iter(self.members)

    def __getitem__(self, item):
        return self.members[item]


class intrusive_set:
    @classmethod
    @property
    @functools.cache
    def size_t(cls):
        return gdb.lookup_type('size_t')

    def __init__(self, ref, link=None):
        container_type = ref.type.strip_typedefs()
        self.node_type = container_type.template_argument(0)
        if link is not None:
            self.link_offset = get_field_offset(self.node_type, link)
        else:
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


class compact_radix_tree:
    def __init__(self, ref):
        self.root = ref['_root']['_v']

    def to_string(self):
        if self.root['_base_layout'] == 0:
            return '<empty>'

        # Compiler optimizes-away lots of critical stuff, so
        # for now just show where the tree is
        return 'compact radix tree @ 0x%x' % self.root


class intrusive_btree:
    def __init__(self, ref):
        container_type = ref.type.strip_typedefs()
        self.tree = ref
        self.leaf_node_flag = gdb.parse_and_eval('intrusive_b::node_base::NODE_LEAF')
        self.key_type = container_type.template_argument(0)

    def __visit_node_base(self, base, kids):
        for i in range(0, base['num_keys']):
            if kids:
                for r in self.__visit_node(kids[i]):
                    yield r

            yield base['keys'][i].cast(self.key_type.pointer()).dereference()

        if kids:
            for r in self.__visit_node(kids[base['num_keys']]):
                yield r

    def __visit_node(self, node):
        base = node['_base']
        kids = node['_kids'] if not base['flags'] & self.leaf_node_flag else None

        for r in self.__visit_node_base(base, kids):
            yield r

    def __iter__(self):
        if self.tree['_root']:
            for r in self.__visit_node(self.tree['_root']):
                yield r
        else:
            for r in self.__visit_node_base(self.tree['_inline'], None):
                yield r


class bplus_tree:
    def __init__(self, ref):
        self.tree = ref
        self.leaf_node_flag = int(gdb.parse_and_eval(self.tree.type.name + "::node::NODE_LEAF"))
        self.rightmost_leaf_flag = int(gdb.parse_and_eval(self.tree.type.name + "::node::NODE_RIGHTMOST"))

    def __len__(self):
        i = 0
        for _ in self:
            i += 1
        return i

    def __iter__(self):
        node_p = self.tree['_left']
        while node_p:
            node = node_p.dereference()
            if not node['_flags'] & self.leaf_node_flag:
                raise ValueError("Expected B+ leaf node")

            for i in range(0, node['_num_keys']):
                yield node['_kids'][i+1]['d'].dereference()['value']

            if node['_flags'] & self.rightmost_leaf_flag:
                node_p = None
            else:
                node_p = node['__next']


class double_decker:
    def __init__(self, ref):
        self.tree = ref['_tree']
        self.leaf_node_flag = int(gdb.parse_and_eval(self.tree.type.name + "::node::NODE_LEAF"))
        self.rightmost_leaf_flag = int(gdb.parse_and_eval(self.tree.type.name + "::node::NODE_RIGHTMOST"))
        self.max_conflicting_partitions = 128

    def __iter__(self):
        node_p = self.tree['_left']
        while node_p:
            node = node_p.dereference()
            if not node['_flags'] & self.leaf_node_flag:
                raise ValueError("Expected B+ leaf node")

            for i in range(0, node['_num_keys']):
                parts = node['_kids'][i+1]['d'].dereference()['value']
                p = 0
                while True:
                    ce = parts['_data'][p]['object']
                    if p == 0 and not ce['_flags']['_head']:
                        raise ValueError("Expected head cache_entry")
                    yield ce
                    if ce['_flags']['_tail']:
                        break
                    if p >= self.max_conflicting_partitions:
                        raise ValueError("Too many conflicting partitions")
                    p += 1

            if node['_flags'] & self.rightmost_leaf_flag:
                node_p = None
            else:
                node_p = node['__next']


class boost_variant:
    def __init__(self, ref):
        self.ref = ref

    def which(self):
        return self.ref['which_']

    def type(self):
        return self.ref.type.template_argument(self.ref['which_'])

    def get(self):
        return self.ref['storage_'].address.cast(self.type().pointer())


class std_variant:
    """Wrapper around and std::variant.

    Call get() to access the current value.
    """
    def __init__(self, ref):
        self.ref = ref
        self.member_types = list(template_arguments(self.ref.type))

    def index(self):
        return int(self.ref['_M_index'])

    # workaround for when template arguments refuses to work
    def get_with_type(self, current_type):
        index = self.index()
        variadic_union = self.ref['_M_u']
        for i in range(index):
            variadic_union = variadic_union['_M_rest']

        wrapper = variadic_union['_M_first']['_M_storage']
        # literal types are stored directly in `_M_storage`.
        if wrapper.type.strip_typedefs() == current_type:
            return wrapper

        # non-literal types are stored via a __gnu_cxx::__aligned_membuf
        return wrapper['_M_storage'].reinterpret_cast(current_type.pointer()).dereference()

    def get(self):
        index = self.index()
        current_type = self.member_types[index].strip_typedefs()
        return self.get_with_type(index, current_type)


class std_map:
    @classmethod
    @property
    @functools.cache
    def size_t(cls):
        return gdb.lookup_type('size_t')

    def __init__(self, ref):
        container_type = ref.type.strip_typedefs()
        kt = container_type.template_argument(0)
        vt = container_type.template_argument(1)
        self.value_type = gdb.lookup_type('::std::pair<{} const, {} >'.format(str(kt), str(vt)))
        self.root = ref['_M_t']['_M_impl']['_M_header']['_M_parent']
        self.size = int(ref['_M_t']['_M_impl']['_M_node_count'])

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

    def __len__(self):
        return self.size


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

    def __getitem__(self, i):
        return self.ref['_M_elems'][int(i)]


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


class std_unordered_set:
    def __init__(self, ref):
        self.ht = ref['_M_h']
        value_type = ref.type.template_argument(0)
        _, node_type = lookup_type(['::std::__detail::_Hash_node<{}, {}>'.format(value_type.name, cache)
                                    for cache in ('false', 'true')])
        self.node_ptr_type = node_type.pointer()
        self.value_ptr_type = value_type.pointer()

    def __len__(self):
        return self.ht['_M_element_count']

    def __iter__(self):
        p = self.ht['_M_before_begin']['_M_nxt']
        while p:
            pc = p.cast(self.node_ptr_type)['_M_storage']['_M_storage']['__data'].cast(self.value_ptr_type)
            yield pc.dereference()
            p = p['_M_nxt']

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()


class std_unordered_map:
    def __init__(self, ref):
        self.ht = ref['_M_h']
        kt = ref.type.template_argument(0)
        vt = ref.type.template_argument(1)
        value_type = gdb.lookup_type('::std::pair<{} const, {} >'.format(str(kt), str(vt)))
        _, node_type = lookup_type(['::std::__detail::_Hash_node<{}, {}>'.format(value_type.name, cache)
                                    for cache in ('false', 'true')])
        self.node_ptr_type = node_type.pointer()
        self.value_ptr_type = value_type.pointer()

    def __len__(self):
        return self.ht['_M_element_count']

    def __iter__(self):
        p = self.ht['_M_before_begin']['_M_nxt']
        while p:
            pc = p.cast(self.node_ptr_type)['_M_storage']['_M_storage']['__data'].cast(self.value_ptr_type)
            yield (pc['first'], pc['second'])
            p = p['_M_nxt']

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()


class flat_hash_map:
    def __init__(self, ref):
        kt = ref.type.template_argument(0)
        vt = ref.type.template_argument(1)
        slot_ptr_type = gdb.lookup_type('::std::pair<const {}, {} >'.format(str(kt), str(vt))).pointer()
        self.slots = ref['slots_'].cast(slot_ptr_type)
        self.size = ref['size_']

    def __len__(self):
        return self.size

    def __iter__(self):
        size = self.size
        slot = self.slots
        while size > 0:
            yield (slot['first'], slot['second'])
            slot += 1
            size -= 1

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()


class absl_container:
    # absl_container is the underlying type for flat_hash_map and flat_hash_set
    # if we need to print flat_hash_set, we should yield the element of set
    # instead of <key, value> tuple in `__iter__()`
    def __init__(self, ref):
        self.val = ref
        HasInfozShift = 1
        self.size = ref["settings_"]["value"]["size_"] >> HasInfozShift

    def __len__(self):
        return self.size

    def __iter__(self):
        if self.size == 0:
            return
        capacity = int(self.val["settings_"]["value"]["capacity_"])
        control = self.val["settings_"]["value"]["control_"]
        # for the map the slot_type is std::pair<K, V>
        slot_type = gdb.lookup_type(str(self.val.type.strip_typedefs()) + "::slot_type")
        slots = self.val["settings_"]["value"]["slots_"].cast(slot_type.pointer())
        for i in range(capacity):
            ctrl_t = int(control[i])
            # if the control is empty or deleted, its value is less than -1, see
            # https://github.com/abseil/abseil-cpp/blob/c1e1b47d989978cde8c5a2a219df425b785a0c47/absl/container/internal/raw_hash_set.h#L487-L503
            if ctrl_t == -1:
                break
            if ctrl_t >= 0:
                # NOTE: this only works for flat_hash_map
                if slots[i]['value'].type.name.find("::map_slot_type") != -1:
                    yield slots[i]['key'], slots[i]['value']['second']
                else:
                    yield slots[i]['key'], slots[i]['value']

    def __nonzero__(self):
        return self.size > 0

    def __bool__(self):
        return self.size > 0


def unordered_map(ref):
    if ref.type.name.startswith('flat_hash_map'):
        try:
            return flat_hash_map(ref)
        except gdb.error:
            # newer absl container uses a different memory layout
            return absl_container(ref)
    else:
        return std_unordered_map(ref)


def std_priority_queue(ref):
    return std_vector(ref['c'])


class std_deque:
    # should reflect the value of _GLIBCXX_DEQUE_BUF_SIZE
    DEQUE_BUF_SIZE = 512

    def __init__(self, ref):
        self.ref = ref
        self.value_type = self.ref.type.strip_typedefs().template_argument(0)
        if self.value_type.sizeof < std_deque.DEQUE_BUF_SIZE:
            self.buf_size = int(std_deque.DEQUE_BUF_SIZE / self.value_type.sizeof)
        else:
            self.buf_size = 1

    def _foreach_node(self):
        node_it = self.ref['_M_impl']['_M_start']
        node_end = self.ref['_M_impl']['_M_finish']
        node = node_it['_M_node']
        it = node_it['_M_first']
        end = node_it['_M_last']

        if not bool(node):
            return

        while node != node_end['_M_node']:
            yield it, end
            node = node + 1
            it = node.dereference()
            end = it + self.buf_size

        yield node_end['_M_first'], node_end['_M_cur']

    def __len__(self):
        l = 0
        for start, end in self._foreach_node():
            l += (end - start)
        return l

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__len__() > 0

    def __iter__(self):
        for it, end in self._foreach_node():
            while it != end:
                yield it.dereference()
                it += 1

    def __str__(self):
        items = [str(item) for item in self]
        return "{{size={}, [{}]}}".format(len(self), ", ".join(items))


class static_vector:
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        return int(self.ref['m_holder']['m_size'])

    def __iter__(self):
        t = self.ref.type.strip_typedefs()
        value_type = t.template_argument(0)
        try:
            data = self.ref['m_holder']['storage']['data'].cast(value_type.pointer())
        except:
            try:
                data = self.ref['m_holder']['storage']['dummy']['dummy'].cast(value_type.pointer()) # Scylla 3.1 compatibility
            except gdb.error:
                data = self.ref['m_holder']['storage']['dummy'].cast(value_type.pointer()) # Scylla 3.0 compatibility
        for i in range(self.__len__()):
            yield data[i]

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()


class std_list:
    """Make `std::list` usable in python as a read-only container."""

    @staticmethod
    def _make_dereference_func(value_type):
        list_node_type = gdb.lookup_type('std::_List_node<{}>'.format(str(value_type))).pointer()

        def deref(node):
            list_node = node.cast(list_node_type)
            return list_node['_M_storage']['_M_storage'].cast(value_type.pointer()).dereference()

        return deref

    def __init__(self, ref):
        self.ref = ref
        self._dereference_node = std_list._make_dereference_func(self.ref.type.strip_typedefs().template_argument(0))

    def __len__(self):
        try:
            return int(self.ref['_M_impl']['_M_node']['_M_size'])
        except gdb.error:
            i = 0
            for _ in self:
                i += 1
            return i

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__nonzero__()

    def __getitem__(self, item):
        if not isinstance(item, int):
            raise ValueError("Invalid index: expected `{}`, got: `{}`".format(int, type(item)))

        if item >= len(self):
            raise ValueError("Index out of range: expected < {}, got {}".format(len(self), item))

        i = 0
        it = iter(self)
        val = next(it)
        while i != item:
            i += 1
            val = next(it)

        return val

    def __iter__(self):
        class std_list_iterator:
            def __init__(self, lst):
                self._list = lst
                node_header = self._list.ref['_M_impl']['_M_node']
                self._node = node_header['_M_next']
                self._end = node_header['_M_next']['_M_prev']

            def __next__(self):
                if self._node == self._end:
                    raise StopIteration()

                val = self._list._dereference_node(self._node)
                self._node = self._node['_M_next']
                return val

            # python2 compatibility
            def next(self):
                return self.__next__()

        return std_list_iterator(self)

    @staticmethod
    def dereference_iterator(it):
        deref = std_list._make_dereference_func(it.type.strip_typedefs().template_argument(0))
        return deref(it['_M_node'])


class managed_vector:
    def __init__(self, ref):
        self._ref = ref

    def __len__(self):
        return int(self._ref['_size'])

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__len__() > 0

    def __iter__(self):
        for i in range(len(self)):
            yield self._ref['_data'][i]


class chunked_managed_vector:
    def __init__(self, ref):
        self._ref = ref

    def __len__(self):
        return int(self._ref['_size'])

    def __nonzero__(self):
        return self.__len__() > 0

    def __bool__(self):
        return self.__len__() > 0

    def __iter__(self):
        for chunk in managed_vector(self._ref['_chunks']):
            for e in managed_vector(chunk):
                yield e


class chunked_fifo:
    class chunk:
        def __init__(self, ref):
            self.items = ref['items']
            self.begin = ref['begin']
            self.end = ref['end']
            self.next_one = ref['next']

        def __len__(self):
            return self.end - self.begin

    class iterator:
        def __init__(self, fifo, chunk):
            self.fifo = fifo
            self.chunk = chunk
            self.index = self.chunk.begin if chunk else 0

        def __next__(self):
            if self.index == 0:
                raise StopIteration
            if self.index == self.chunk.end:
                if not self.chunk.next_one:
                    raise StopIteration
                self.chunk = chunked_fifo.chunk(self.chunk.next_one)
                self.index = self.chunk.begin
            index = self.index
            self.index += 1
            return self.chunk.items[self.fifo.mask(index)]['data']

    def __init__(self, ref):
        self._ref = ref
        # try to access the member variable, so the constructor throws if the
        # inspected variable is of the wrong type
        _ = self.front_chunk

    def mask(self, index):
        return index & (self.items_per_chunk - 1)

    @property
    def items_per_chunk(self):
        return self._ref.type.template_argument(1)

    @property
    def front_chunk(self):
        return self._ref['_front_chunk']

    @property
    def back_chunk(self):
        return self._ref['_back_chunk']

    def __len__(self):
        if not self.front_chunk:
            return 0
        if self.back_chunk == self.front_chunk:
            front_chunk = self.chunk(self.front_chunk.dereference())
            return len(front_chunk)
        else:
            front_chunk = self.chunk(self.front_chunk.dereference())
            back_chunk = self.chunk(self.back_chunk.dereference())
            num_chunks = self._ref['_nchunks'] - 2
            return (len(front_chunk) +
                    len(back_chunk) +
                    num_chunks * self.items_per_chunk)

    def __iter__(self):
        return self.iterator(self, self.front_chunk)


class sstring:
    def __init__(self, ref):
        self.ref = ref

    @staticmethod
    def to_hex(data, size):
        inf = gdb.selected_inferior()
        return bytes(inf.read_memory(data, size)).hex()

    def as_hex(self):
        return self.to_hex(self.data(), len(self))

    def is_internal(self):
        return self.ref['u']['internal']['size'] >= 0

    def __len__(self):
        if self.is_internal():
            return self.ref['u']['internal']['size']
        else:
            return self.ref['u']['external']['size']

    def data(self):
        if self.is_internal():
            return self.ref['u']['internal']['str']
        else:
            return self.ref['u']['external']['str']

    def __str__(self):
        return self.as_hex()


def uint64_t(val):
    val = int(val)
    if val < 0:
        val += 1 << 64
    return val


class inet_address_printer(gdb.printing.PrettyPrinter):
    'print a gms::inet_address'

    def __init__(self, val):
        self.val = val

    def to_string(self):
        family = str(self.val['_addr']['_in_family'])
        if family == "seastar::net::inet_address::family::INET":
            raw = self.val['_addr']['_in']['s_addr']
            ipv4 = socket.inet_ntop(socket.AF_INET, struct.pack('=L', raw))
            return str(ipv4)
        else:
            raw = self.val['_addr']['_in6']['__in6_u']['__u6_addr32']
            ipv6 = socket.inet_ntop(socket.AF_INET6, struct.pack('=LLLL', raw[0], raw[1], raw[2], raw[3]))
            return str(ipv6)

    def display_hint(self):
        return 'string'


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


class string_view_printer(gdb.printing.PrettyPrinter):
    'print an std::string_view'

    def __init__(self, val):
        self.val = val

    def to_string(self):
        inf = gdb.selected_inferior()
        return str(inf.read_memory(self.val['_M_str'], self.val['_M_len']), encoding='utf-8')

    def display_hint(self):
        return 'string'


class managed_bytes_printer(gdb.printing.PrettyPrinter):
    'print a managed_bytes'

    def __init__(self, val):
        self.val = val

    def pure_bytes(self):
        inf = gdb.selected_inferior()

        def to_bytes(data, size):
            return bytes(inf.read_memory(data, size))

        if self.val['_inline_size'] >= 0:
            return to_bytes(self.val['_u']['inline_data'], int(self.val['_inline_size']))
        elif self.val['_inline_size'] == -1:
            return to_bytes(self.val['_u']['single_chunk_ref']['ptr']['data'], int(self.val['_u']['single_chunk_ref']['size']))
        else:
            ref = self.val['_u']['multi_chunk_ref']['ptr']
            chunks = list()
            while ref['ptr']:
                chunks.append(to_bytes(ref['ptr']['data'], int(ref['ptr']['frag_size'])))
                ref = ref['ptr']['next']
            return b''.join(chunks)

    def bytes_as_hex(self):
        return self.pure_bytes().hex()

    def bytes(self):
        return self.bytes_as_hex()

    def to_string(self):
        return str(self.bytes())

    def display_hint(self):
        return 'managed_bytes'


class optional_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = std_optional(val)

    def to_string(self):
        if not self.val:
            return 'std::nullopt'
        return str(self.val.get())

    def display_hint(self):
        return 'std::optional'


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

    def __rows(self):
        return intrusive_btree(self.val['_rows'])

    def to_string(self):
        rows = list(str(r) for r in self.__rows())
        range_tombstones = list(str(r) for r in intrusive_set(self.val['_row_tombstones']['_tombstones'], link='_link'))
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

    def __to_string_legacy(self):
        if self.val['_type'] == gdb.parse_and_eval('row::storage_type::vector'):
            cells = str(self.val['_storage']['vector'])
        elif self.val['_type'] == gdb.parse_and_eval('row::storage_type::set'):
            cells = '[%s]' % (', '.join(str(cell) for cell in intrusive_set(self.val['_storage']['set'])))
        else:
            raise Exception('Unsupported storage type: ' + self.val['_type'])
        return '{type=%s, cells=%s}' % (self.val['_type'], cells)

    def to_string(self):
        try:
            return '{cells=[%s]}' % compact_radix_tree(self.val['_cells']).to_string()
        except gdb.error:
            return self.__to_string_legacy()

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


class sstable_generation_printer(gdb.printing.PrettyPrinter):
    'print an sstables::generation_type'
    BASE36_ALPHABET = string.digits + string.ascii_lowercase
    DECIMICRO_RATIO = 10_000_000

    def __init__(self, val):
        self.val = val['_value']

    def _to_uuid(self):
        msb = uint64_t(self.val['most_sig_bits'])
        lsb = uint64_t(self.val['least_sig_bits'])
        bytes = msb.to_bytes(8) + lsb.to_bytes(8)
        return uuid.UUID(bytes=bytes)

    @classmethod
    def _encode_n_with_base36(cls, n):
        assert n >= 0
        output = ''
        alphabet_len = len(cls.BASE36_ALPHABET)
        while n:
            n, index = divmod(n, alphabet_len)
            output += cls.BASE36_ALPHABET[index]
        return output[::-1]

    @classmethod
    def _encode_uuid_with_base36(cls, timeuuid):
        # see also scripts/base36-uuid.py for more context on the encoding
        # of the sstable (generation) identifiers.
        seconds, decimicro = divmod(timeuuid.time, cls.DECIMICRO_RATIO)
        delta = datetime.timedelta(seconds=seconds)
        encoded_days = cls._encode_n_with_base36(delta.days)
        encoded_seconds = cls._encode_n_with_base36(delta.seconds)
        encoded_decimicro = cls._encode_n_with_base36(decimicro)
        lsb = int.from_bytes(timeuuid.bytes[8:])
        encoded_lsb = cls._encode_n_with_base36(lsb)
        return (f'{encoded_days:0>4}_'
                f'{encoded_seconds:0>4}_'
                f'{encoded_decimicro:0>5}'
                f'{encoded_lsb:0>13}')

    def to_string(self):
        if self.val.type == gdb.lookup_type('int64_t'):
            # before the uuid-generation change
            return str(self.val)

        # after the uuid-generation change
        assert self.val.type == gdb.lookup_type('utils::UUID')
        timeuuid = self._to_uuid()
        # a uuid encoded generation can present one of the following types:
        # 1. null: the generation is empty.
        # 2. an integer: if the sstable is created with
        #    "uuid_sstable_identifiers_enabled" option disabled.
        # 3. a uuid: if the option above is enabled as it is by default.
        if timeuuid.int == 0:
            return "<null>"
        elif timeuuid.time == 0:
            # encodes an integer
            # just for the sake of correctness, as always use int64_t for
            # representing sstable generation even the generations are
            # always positive.
            lsb = int.from_bytes(timeuuid.bytes[8:], signed=True)
            return str(lsb)
        else:
            # encodes a uuid
            return self._encode_uuid_with_base36(timeuuid)


class boost_intrusive_list_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = intrusive_list(val)

    def to_string(self):
        items = ['@' + str(v.address) + '=' + str(v) for v in self.val]
        ptrs = [str(v.address) for v in self.val]
        return 'boost::intrusive::list of size {} = [{}] = [{}]'.format(len(items), ', '.join(ptrs), ', '.join(items))


class interval_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        try:
            self.val = val['_interval']
        except gdb.error: # 4.1 compatibility
            self.val = val['_range']

    def inspect_bound(self, bound_opt):
        bound = std_optional(bound_opt)
        if not bound:
            return False, False, None

        bound = bound.get()

        return True, bool(bound['_inclusive']), bound['_value']

    def to_string(self):
        has_start, start_inclusive, start_value = self.inspect_bound(self.val['_start'])
        has_end, end_inclusive, end_value = self.inspect_bound(self.val['_end'])

        return '{}{}, {}{}'.format(
            '[' if start_inclusive else '(',
            str(start_value) if has_start else '-inf',
            str(end_value) if has_end else '+inf',
            ']' if end_inclusive else ')',
        )


class ring_position_printer(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = val

    def to_string(self):
        pkey = std_optional(self.val['_key'])
        if pkey:
            # we can assume token_kind == token_kind::key
            return '{{{}, {}}}'.format(str(self.val['_token']['_data']), str(pkey.get()['_bytes']))

        token_bound = int(self.val['_token_bound'])
        token_kind = str(self.val['_token']['_kind'])[17:] # ignore the dht::token_kind:: prefix
        if token_kind == 'key':
            token = str(self.val['_token']['_data'])
        else:
            token = token_kind

        return '{{{}, {}}}'.format(token, token_bound)


def build_pretty_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter('scylla')
    pp.add_printer('sstring', r'^seastar::basic_sstring<char,.*>$', sstring_printer)
    pp.add_printer('std::string_view', r'^std::basic_string_view<char,.*>$', string_view_printer)
    pp.add_printer('bytes', r'^seastar::basic_sstring<signed char, unsigned int, 31, false>$', sstring_printer)
    pp.add_printer('managed_bytes', r'^managed_bytes$', managed_bytes_printer)
    pp.add_printer('partition_entry', r'^partition_entry$', partition_entry_printer)
    pp.add_printer('mutation_partition', r'^mutation_partition$', mutation_partition_printer)
    pp.add_printer('row', r'^row$', row_printer)
    pp.add_printer('managed_vector', r'^managed_vector<.*>$', managed_vector_printer)
    pp.add_printer('uuid', r'^utils::UUID$', uuid_printer)
    pp.add_printer('sstable_generation', r'^sstables::generation_type$', sstable_generation_printer)
    pp.add_printer('boost_intrusive_list', r'^boost::intrusive::list<.*>$', boost_intrusive_list_printer)
    pp.add_printer('inet_address_printer', r'^gms::inet_address$', inet_address_printer)
    pp.add_printer('interval', r'^interval<.*$', interval_printer)
    pp.add_printer('nonwrapping_range', r'^nonwrapping_range<.*$', interval_printer) # scylla < 4.3 calls it nonwrapping_range
    pp.add_printer('ring_position', r'^dht::ring_position$', ring_position_printer)
    pp.add_printer('optional', r'^std::_Optional_base.*', optional_printer)
    return pp


gdb.printing.register_pretty_printer(gdb.current_objfile(), build_pretty_printer(), replace=True)


def cpus():
    return int(gdb.parse_and_eval('::seastar::smp::count'))


def current_shard():
    return int(gdb.parse_and_eval('\'seastar\'::local_engine->_id'))


class sharded:
    def __init__(self, val):
        self.val = val
        self.instances = std_vector(self.val['_instances'])

    def instance(self, shard=None):
        idx = shard or current_shard()
        if idx >= len(self.instances):
            return None
        return self.instances[idx]['service']['_p']

    def local(self):
        return self.instance()


def get_lsa_segment_pool():
    try:
        tracker = gdb.parse_and_eval('\'logalloc::tracker_instance\'')
        tracker_impl = std_unique_ptr(tracker["_impl"]).get().dereference()
        return std_unique_ptr(tracker_impl["_segment_pool"]).get().dereference()
    except gdb.error:
        return gdb.parse_and_eval('\'logalloc::shard_segment_pool\'')


def find_db(shard=None):
    try:
        db = gdb.parse_and_eval('::debug::the_database')
    except Exception as e:
        try:
            db = gdb.parse_and_eval('::debug::db')
        except:
            return None

    return sharded(db).instance(shard)


def find_dbs():
    return [find_db(shard) for shard in range(cpus())]


def for_each_table(db=None):
    """Returns pointers to table objects which exist on current shard"""
    if not db:
        db = find_db()

    try:
        tables = unordered_map(db['_tables_metadata']['_column_families'])
    except gdb.error:
        tables = unordered_map(db['_column_families'])

    for (key, value) in tables:
        yield seastar_lw_shared_ptr(value).get().dereference()


def lookup_type(type_names):
    for type_name in type_names:
        try:
            return (type_name, gdb.lookup_type(type_name))
        except gdb.error:
            continue
    raise gdb.error('none of the types found')


def get_text_ranges():
    try:
        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        reactor_backend = gdb.parse_and_eval('seastar::local_engine->_backend')
        # 2019.1 has value member, >=3.0 has std::unique_ptr<>
        if reactor_backend.type.strip_typedefs().name.startswith('std::unique_ptr<'):
            reactor_backend = std_unique_ptr(reactor_backend).get()
        else:
            reactor_backend = gdb.parse_and_eval('&seastar::local_engine->_backend')
        known_vptr = int(reactor_backend.reinterpret_cast(vptr_type).dereference())
    except Exception as e:
        gdb.write("get_text_ranges(): Falling back to locating .rodata section because lookup to reactor backend to use as known vptr failed: {}\n".format(e))
        known_vptr = None

    def section_bounds(line):
        items = line.split()
        start = int(items[0], 16)
        end = int(items[2], 16)
        return (start, end)

    ret = []
    sections = gdb.execute('info files', False, True).split('\n')
    for line in sections:
        # add .text for coroutines
        if line.endswith("is .text"):
            ret.append(section_bounds(line))
        elif known_vptr and " is ." in line:
            bounds = section_bounds(line)
            if bounds[0] <= known_vptr and known_vptr <= bounds[1]:
                ret.append(bounds)
        # otherwise vptrs are in .rodata section
        elif line.endswith("is .rodata"):
            ret.append(section_bounds(line))

    if len(ret) == 0:
        raise Exception("Failed to find plausible text sections")
    return ret


def addr_in_ranges(ranges, addr):
    for r in ranges:
        if addr >= r[0] and addr <= r[1]:
            return True
    return False


class histogram:
    """Simple histogram.

    Aggregate items by their count and present them in a histogram format.
    Example:

        h = histogram()
        h['item1'] = 20 # Set an absolute value
        h.add('item2') # Equivalent to h['item2'] += 1
        h.add('item2')
        h.add('item3')
        h.print_to_console()

    Would print:
        4 item1 ++++++++++++++++++++++++++++++++++++++++
        2 item2 ++++
        1 item1 ++

    Note that the number of indicators ('+') is does not correspond to the
    actual number of items, rather it is supposed to illustrate their relative
    counts.
    """
    _column_count = 40

    def __init__(self, counts = None, print_indicators = True, formatter=None, limit=None):
        """Constructor.

        Params:
        * counts: initial counts (default to empty).
        * print_indicators: print the '+' characters to illustrate relative
            count. Can be turned off when the item names are very long and would
            thus make indicators unreadable.
        * formatter: a callable that receives the item as its argument and is
            expected to return the string to be printed in the second column.
            By default, items are printed verbatim.
        * limit: limit the number of printed items to the top limit ones.
        """
        if counts is None:
            self._counts = defaultdict(int)
        else:
            self._counts = counts
        self._print_indicators = print_indicators

        def default_formatter(value):
            return str(value)
        if formatter is None:
            self._formatter = default_formatter
        else:
            self._formatter = formatter

        if limit is None:
            self._limit = -1
        else:
            self._limit = limit

    def __len__(self):
        return len(self._counts)

    def __nonzero__(self):
        return bool(len(self))

    def __getitem__(self, item):
        return self._counts[item]

    def __setitem__(self, item, value):
        self._counts[item] = value

    def add(self, item):
        self._counts[item] += 1

    def __str__(self):
        if not self._counts:
            return ''

        by_counts = defaultdict(list)
        for k, v in self._counts.items():
            by_counts[v].append(k)

        counts_sorted = list(reversed(sorted(by_counts.keys())))
        max_count = counts_sorted[0]

        if max_count == 0:
            count_per_column = 0
        else:
            count_per_column = self._column_count / max_count

        lines = []
        limit = self._limit

        for count in counts_sorted:
            items = by_counts[count]
            if self._print_indicators:
                indicator = '+' * max(1, int(count * count_per_column))
            else:
                indicator = ''
            for item in items:
                try:
                    lines.append('{:9d} {} {}'.format(count, self._formatter(item), indicator))
                except:
                    gdb.write("error: failed to format item `{}': {}\n".format(item, sys.exc_info()[1]))
            limit -= 1
            if not limit:
                break

        return '\n'.join(lines)

    def __repr__(self):
        return 'histogram({})'.format(self._counts)

    def print_to_console(self):
        gdb.write(str(self) + '\n')


class task_symbol_matcher:
    def __init__(self):
        self._coro_pattern = re.compile(r'\)( \[clone \.\w+\])?$')

        # List of whitelisted symbol names. Each symbol is a tuple, where each
        # element is a component of the name, the last element being the class
        # name itself.
        # We can't just merge them as `info symbol` might return mangled names too.
        self._whitelist = task_symbol_matcher._make_symbol_matchers([
                ("seastar", "continuation"),
                ("seastar", "future", "thread_wake_task"), # backward compatibility with older versions
                ("seastar", "(anonymous namespace)", "thread_wake_task"),
                ("seastar", "thread_context"),
                ("seastar", "internal", "do_until_state"),
                ("seastar", "internal", "do_with_state"),
                ("seastar", "internal", "do_for_each_state"),
                ("seastar", "parallel_for_each_state"),
                ("seastar", "internal", "repeat_until_value_state"),
                ("seastar", "internal", "repeater"),
                ("seastar", "internal", "when_all_state"),
                ("seastar", "internal", "when_all_state_component"),
                ("seastar", "internal", "coroutine_traits_base", "promise_type"),
                ("seastar", "lambda_task"),
                ("seastar", "smp_message_queue", "async_work_item"),
        ])

    @staticmethod
    def _make_symbol_matchers(symbol_specs):
        return list(map(task_symbol_matcher._make_symbol_matcher, symbol_specs))

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

    def __call__(self, name):
        name = name.strip()
        if re.search(self._coro_pattern, name) is not None:
            return True

        for matcher in self._whitelist:
            if matcher(name):
                return True
        return False


class scylla(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND, True)


class scylla_databases(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla databases', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        for shard in range(cpus()):
            db = find_db(shard)
            gdb.write('{:5} (replica::database*){}\n'.format(shard, db))


class scylla_commitlog(gdb.Command):
    """Prints info about commitlog segment manager and managed segments
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla commitlog', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        db = find_db()
        cmtlog = std_unique_ptr(db['_commitlog']).get()
        gdb.write('(db::commitlog*){}\n'.format(cmtlog))
        segmgr = seastar_shared_ptr(cmtlog['_segment_manager']).get()
        gdb.write('(db::commitlog::segment_manager*){}\n'.format(segmgr))
        segs = std_vector(segmgr['_segments'])
        gdb.write('segments ({}):\n'.format(len(segs)))
        for seg_p in segs:
            seg = seastar_shared_ptr(seg_p).get()
            gdb.write('(db::commitlog::segment*){}\n'.format(seg))


class scylla_keyspaces(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla keyspaces', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        for shard in range(cpus()):
            db = find_db(shard)
            keyspaces = db['_keyspaces']
            for (key, value) in unordered_map(keyspaces):
                gdb.write('{:5} {:20} (replica::keyspace*){}\n'.format(shard, str(key), value.address))


class scylla_tables(gdb.Command):
    """Lists tables (column-families)"""
    def __init__(self):
        gdb.Command.__init__(self, 'scylla tables', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="scylla tables")
        parser.add_argument("-a", "--all", action="store_true", default=False,
                help="List tables from all shards")
        parser.add_argument("-u", "--user", action="store_true",
                help="List only user tables")
        parser.add_argument("-k", "--keyspace", action="store",
                help="List tables only for provided keyspace")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        if args.all:
            shards = list(range(cpus()))
        else:
            shards = [current_shard()]

        for shard in shards:
            for value in for_each_table():
                schema = schema_ptr(value['_schema'])
                if args.user and schema.is_system():
                    continue
                if args.keyspace and schema.ks_name != args.keyspace:
                    continue
                schema_id = str(schema['_raw']['_id'])
                schema_version = str(schema['_raw']['_version'])
                gdb.write('{:5} {} v={} {:45} (replica::table*){}\n'.format(shard, schema_id, schema_version, schema.table_name(), value.address))


class scylla_table(gdb.Command):
    """Prints various info about individual table
        Example:
          scylla table ks.cf
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla table', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def _find_table(self, ks, cf):
        for value in for_each_table():
            schema = schema_ptr(value['_schema'])
            if schema.ks_name == ks and schema.cf_name == cf:
                return value, schema

        return None, None

    @staticmethod
    def phased_barrier_count(table, barrier_name):
        g = seastar_lw_shared_ptr(table[barrier_name]['_gate']).get()
        return int(g['_count'])

    def invoke(self, arg, from_tty):
        if arg is None or arg == '':
            gdb.write("Specify keyspace.table argument\n")
            return

        [ ks, cf ] = arg.split('.', 2)
        [ table, schema ] = self._find_table(ks, cf)
        if table is None:
            gdb.write("No such table\n")
            return

        gdb.write(f'(replica::table*){table.address}\n')
        gdb.write(f'schema version: {schema["_raw"]["_version"]}\n')
        gdb.write('pending ops:')
        for barrier_name in ['_pending_flushes_phaser', '_pending_writes_phaser', '_pending_reads_phaser', '_pending_streams_phaser']:
            gdb.write(f' {barrier_name}: {scylla_table.phased_barrier_count(table, barrier_name)}')
        gdb.write('\n')


class scylla_task_histogram(gdb.Command):
    """Print a histogram of the virtual objects found in memory.

    Sample the virtual objects in memory and create a histogram with the results.
    By default up to 20000 samples will be collected and the top 30 items will
    be shown. The number of collected samples, as well as number of items shown
    can be customized by command line arguments. The sampling can also be
    constrained to objects of a certain size. For more details invoke:

        scylla task_histogram --help

    Example:
     12280: 0x4bc5878 vtable for seastar::file_data_source_impl + 16
      9352: 0x4be2cf0 vtable for seastar::continuation<seastar::future<seasta...
      9352: 0x4bc59a0 vtable for seastar::continuation<seastar::future<seasta...
     (1)    (2)       (3)

     Where:
     (1): Number of objects of this type.
     (2): The address of the class's vtable.
     (3): The name of the class's vtable symbol.
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla task_histogram', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="scylla task_histogram")
        parser.add_argument("-m", "--samples", action="store", type=int, default=20000,
                help="The number of samples to collect. Defaults to 20000. Set to 0 to sample all objects. Ignored when `--all` is used."
                " Note that due to this limit being checked only after scanning an entire page, in practice it will always be overshot.")
        parser.add_argument("-c", "--count", action="store", type=int, default=30,
                help="Show only the top COUNT elements of the histogram. Defaults to 30. Set to 0 to show all items. Ignored when `--all` is used.")
        parser.add_argument("-a", "--all", action="store_true", default=False,
                help="Sample all pages and show all results. Equivalent to -m=0 -c=0.")
        parser.add_argument("-s", "--size", type=int, action="store", default=0,
                help="The size of objects to sample. When set, only objects of this size will be sampled. A size of 0 (the default value) means no size restrictions.")
        parser.add_argument("-f", "--filter-tasks", action="store_true",
                help="Include only task objects in the histogram, reduces noise but might exclude items due to inexact filtering.")
        parser.add_argument("-g", "--scheduling-groups", action="store_true",
                help="Histogram is made from the scheduling groups of the sampled task objects. Implies -f.")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        size = args.size
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        mem_start = cpu_mem['memory']

        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        task_type = gdb.lookup_type('seastar::task')
        task_fields = {f.name: f for f in task_type.fields()}
        sg_offset = int(task_fields['_sg'].bitpos / 8)
        sg_ptr = gdb.lookup_type('unsigned').pointer()

        pages = cpu_mem['pages']
        nr_pages = int(cpu_mem['nr_pages'])
        page_samples = range(0, nr_pages) if args.all else random.sample(range(0, nr_pages), nr_pages)

        text_ranges = get_text_ranges()

        scheduling_group_names = {int(tq['_id']): str(tq['_name']) for tq in get_local_task_queues()}

        def formatter(o):
            if args.scheduling_groups:
                return "{:02} {}".format(o, scheduling_group_names[o])
            else:
                return "0x{:x} {}".format(o, resolve(o))

        limit = None if args.all or args.count == 0 else args.count
        h = histogram(print_indicators=False, formatter=formatter, limit=limit)
        symbol_matcher = task_symbol_matcher()

        sc = span_checker()
        vptr_count = defaultdict(int)
        scanned_pages = 0
        for idx in page_samples:
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
                addr = int(gdb.Value(obj_addr).reinterpret_cast(vptr_type).dereference())
                if not addr_in_ranges(text_ranges, addr):
                    continue
                if args.filter_tasks:
                    sym = resolve(addr)
                    if not sym or not symbol_matcher(sym):
                        continue # we only want tasks
                if args.scheduling_groups:
                    # This is a dirty trick to make this reasonably fast even with 100K+ or 1M+ objects
                    # We have two good choices here:
                    # 1) gdb.Value(obj_addr).reinterpret_cast(task_type).dereference()['_sg']['_id']
                    #    This prints tons of warning about missing RTTI symbols
                    #    and is quite slow.
                    # 2) gdb.parse_and_eval("(seastar::task*)0x{:x}".format(obj_addr))
                    #    Works well but is horrendously slow: one type lookup per
                    #    obj + parsing is too much apparently.
                    #
                    # So we bypass casting to seastar::task* and use the known
                    # offset of the _id field instead directly.
                    key = int(gdb.Value(obj_addr + sg_offset).reinterpret_cast(sg_ptr).dereference())
                    # Task matching is not exact, we'll have some non-task
                    # objects here, with invalid sg derived, ignore these.
                    if key not in scheduling_group_names:
                        continue
                else:
                    key = addr
                h[key] += 1
            if args.all or args.samples == 0:
                continue
            if scanned_pages >= args.samples or len(vptr_count) >= args.samples:
                break

        h.print_to_console()


def find_vptrs():
    cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
    page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
    mem_start = cpu_mem['memory']
    vptr_type = gdb.lookup_type('uintptr_t').pointer()
    pages = cpu_mem['pages']
    nr_pages = int(cpu_mem['nr_pages'])

    text_ranges = get_text_ranges()

    def is_vptr(addr):
        return addr_in_ranges(text_ranges, addr)

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


def find_vptrs_of_type(vptr=None, typename=None):
    """
    Return virtual objects whose vtable pointer equals vptr and/or matches typename.
    typename has to be a prefix of the fully qualified name of the type
    """
    for obj_addr, vtable_addr in find_vptrs():
        if vptr is not None and vtable_addr != vptr:
            continue
        symbol_name = resolve(vtable_addr, startswith=typename)
        if symbol_name is not None:
            yield obj_addr, vtable_addr, symbol_name


def find_single_sstable_readers():
    def _lookup_type(type_names):
        n, t = lookup_type(type_names)
        return (n, t.pointer())

    types = []
    try:
        return intrusive_list(gdb.parse_and_eval('sstables::_reader_tracker._readers'), link='_tracker_link')
    except gdb.error:
        try:
            # for Scylla <= 4.5
            types = [_lookup_type(['sstables::sstable_mutation_reader<sstables::data_consume_rows_context_m, sstables::mp_row_consumer_m>',
                                   'sstables::sstable_mutation_reader<sstables::mx::data_consume_rows_context_m, sstables::mx::mp_row_consumer_m>']),
                     _lookup_type(['sstables::sstable_mutation_reader<sstables::data_consume_rows_context, sstables::mp_row_consumer_k_l>',
                                   'sstables::sstable_mutation_reader<sstables::kl::data_consume_rows_context, sstables::kl::mp_row_consumer_k_l>'])]
        except gdb.error:
            types = [_lookup_type(['sstables::mx::mx_sstable_mutation_reader']),
                     _lookup_type(['sstables::kl::sstable_mutation_reader'])]

    def _lookup_obj(obj_addr, vtable_addr):
        vtable_pfx = 'vtable for '
        name = resolve(vtable_addr, startswith=vtable_pfx)
        if not name:
            return None
        name = name[len(vtable_pfx):]
        for type_name, ptr_type in types:
            if name.startswith(type_name):
                return obj_addr.reinterpret_cast(ptr_type)

    for obj_addr, vtable_addr in find_vptrs():
        obj = _lookup_obj(obj_addr, vtable_addr)
        if obj:
            yield obj


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

    @property
    def ks_name(self):
        return str(self.ptr['_raw']['_ks_name'])[1:-1]

    @property
    def cf_name(self):
        return str(self.ptr['_raw']['_cf_name'])[1:-1]

    def table_name(self):
        return '%s.%s' % (self.ptr['_raw']['_ks_name'], self.ptr['_raw']['_cf_name'])

    def __getitem__(self, item):
        return self.ptr[item]

    def is_system(self):
        return self.ks_name in ["system", "system_schema", "system_distributed", "system_traces", "system_auth", "audit"]


class scylla_active_sstables(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla active-sstables', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        try:
            sizeof_index_entry = int(gdb.parse_and_eval('sizeof(sstables::index_entry)'))
            sizeof_entry = int(gdb.parse_and_eval('sizeof(sstables::partition_index_cache::entry)'))

            def count_index_lists(sst):
                index_lists_size = 0
                for key, entry in intrusive_btree(std_unique_ptr(sst['_index_cache']).get()['_entries']):
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


class std_shared_ptr():
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        return self.ref['_M_ptr']


class std_atomic():
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        return self.ref['_M_i']


def has_enable_lw_shared_from_this(type):
    for f in type.fields():
        if f.is_base_class and 'enable_lw_shared_from_this' in f.name:
            return True
    return False


def remove_prefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


class seastar_lw_shared_ptr():
    def __init__(self, ref):
        self.ref = ref
        self.elem_type = ref.type.template_argument(0)

    def _no_esft_type(self):
        try:
            return gdb.lookup_type('seastar::lw_shared_ptr_no_esft<%s>' % remove_prefix(str(self.elem_type.unqualified()), 'class ')).pointer()
        except:
            return gdb.lookup_type('seastar::shared_ptr_no_esft<%s>' % remove_prefix(str(self.elem_type.unqualified()), 'class ')).pointer()

    def get(self):
        if has_enable_lw_shared_from_this(self.elem_type):
            return self.ref['_p'].cast(self.elem_type.pointer())
        else:
            return self.ref['_p'].cast(self._no_esft_type())['_value'].address


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

    def impl(self):
        return self.region


class dirty_mem_mgr():
    def __init__(self, ref):
        self.ref = ref

    def real_dirty(self):
        try:
            return int(self.ref['_region_group']['_real_total_memory'])
        except gdb.error: # backward compatibility with <= 5.1
            return int(self.ref['_real_region_group']['_total_memory'])

    def unspooled(self):
        try:
            return int(self.ref['_region_group']['_unspooled_total_memory'])
        except gdb.error: # backward compatibility with <= 5.1
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
        name = resolve(vtable_addr, startswith=vtable_name)
        if name:
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
        We try to detect this. It's not 100% accurate but should work in most cases.

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
    """Summarize the state of the shard's memory.

    The goal of this summary is to provide a starting point when investigating
    memory issues.

    The summary consists of two parts:
    * A high level overview.
    * A per size-class population statistics.

    In an OOM situation the latter usually shows the immediate symptoms, one
    or more heavily populated size classes eating up all memory. The overview
    can be used to identify the subsystem that owns these problematic objects.
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla memory', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    @staticmethod
    def summarize_inheriting_execution_stage(ies):
        scheduling_group_names = {int(tq['_id']): str(tq['_name']) for tq in get_local_task_queues()}
        per_sg_stages = []
        i = 0
        for es_opt in std_vector(ies['_stage_for_group']):
            es_opt = std_optional(es_opt)
            if not es_opt:
                continue
            es = es_opt.get()
            enqueued = int(es['_stats']['function_calls_enqueued'])
            executed = int(es['_stats']['function_calls_executed'])
            size = enqueued - executed
            if size > 0:
                per_sg_stages.append((i, scheduling_group_names[i], size))
            i += 1

        return per_sg_stages

    @staticmethod
    def summarize_table_phased_barrier_users(db, barrier_name):
        tables_by_count = defaultdict(list)
        for table in for_each_table():
            schema = schema_ptr(table['_schema'])
            count = scylla_table.phased_barrier_count(table, barrier_name)
            if count > 0:
                tables_by_count[count].append(str(schema.table_name()).replace('"', ''))

        return [(c, tables_by_count[c]) for c in reversed(sorted(tables_by_count.keys()))]

    @staticmethod
    def summarize_storage_proxy_coordinator_stats(sp):
        try:
            return sp['_stats'], {None: sp['_stats']}
        except gdb.error: # > 3.3
            pass

        stats_ptr_type = gdb.lookup_type('service::storage_proxy::stats').pointer()
        key_id = int(sp['_stats_key']['_id'])
        per_sg_stats = {}
        reactor = gdb.parse_and_eval('seastar::local_engine')
        for tq in get_local_task_queues():
            try:
                sched_group_specific = std_array(reactor['_scheduling_group_specific_data']['per_scheduling_group_data'])[int(tq['_id'])]['specific_vals']
            except gdb.error: # 4.0 backwards compatibility
                sched_group_specific = tq['_scheduling_group_specific_vals']
            stats = std_vector(sched_group_specific)[key_id].reinterpret_cast(stats_ptr_type).dereference()
            if int(stats['writes']) == 0 and int(stats['background_writes']) == 0 and int(stats['foreground_reads']) == 0 and int(stats['reads']) == 0:
                continue
            per_sg_stats[tq] = stats

        return sp['_global_stats'], per_sg_stats

    @staticmethod
    def print_coordinator_stats():
        try:
            sp = sharded(gdb.parse_and_eval('debug::the_storage_proxy')).local()
        except gdb.error:
            sp = sharded(gdb.parse_and_eval('service::_the_storage_proxy')).local()
        if not sp:
            return
        global_sp_stats, per_sg_sp_stats = scylla_memory.summarize_storage_proxy_coordinator_stats(sp)

        try:
            # 4.4 compatibility
            hm = std_optional(sp['_hints_manager']).get()
        except gdb.error:
            hm = sp['_hints_manager']
        view_hm = sp['_hints_for_views_manager']

        gdb.write('Coordinator:\n'
                '  bg write bytes: {bg_wr_bytes:>13} B\n'
                '  hints:          {regular:>13} B\n'
                '  view hints:     {views:>13} B\n'
                .format(
                        bg_wr_bytes=int(global_sp_stats['background_write_bytes']),
                        regular=int(hm['_stats']['size_of_hints_in_progress']),
                        views=int(view_hm['_stats']['size_of_hints_in_progress'])))

        for sg_tq, stats in per_sg_sp_stats.items():
            if sg_tq is None:
                sg_header = ''
            else:
                sg_header = '  {sg_id:02} {sg_name}\n'.format(sg_id=int(sg_tq['_id']), sg_name=str(sg_tq['_name']))

            gdb.write(
                    '{sg_header}'
                    '    fg writes:  {fg_wr:>13}\n'
                    '    bg writes:  {bg_wr:>13}\n'
                    '    fg reads:   {fg_rd:>13}\n'
                    '    bg reads:   {bg_rd:>13}\n'
                    .format(
                        sg_header=sg_header,
                        fg_wr=int(stats['writes']) - int(stats['background_writes']),
                        bg_wr=int(stats['background_writes']),
                        fg_rd=int(stats['foreground_reads']),
                        bg_rd=int(stats['reads']) - int(stats['foreground_reads'])))

        gdb.write('\n')

    @staticmethod
    def format_semaphore_stats(semaphore):
        # older versions had names like "_user_concurrency_semaphore"
        semaphore_name = "{}:".format(str(semaphore['_name']).removeprefix('_').removesuffix('_concurrency_semaphore'))
        initial_count = int(semaphore["_initial_resources"]["count"])
        initial_memory = int(semaphore["_initial_resources"]["memory"])
        used_count = initial_count - int(semaphore["_resources"]["count"])
        used_memory = initial_memory - int(semaphore["_resources"]["memory"])
        try:
            waiters = int(semaphore["_stats"]["waiters"])
        except gdb.error: # 5.1 compatibility
            waiters = int(semaphore["_wait_list"]["_size"])
        return f'{semaphore_name:<16} {used_count:>3}/{initial_count:>3}, {used_memory:>13}/{initial_memory:>13}, queued: {waiters}'

    @staticmethod
    def print_replica_stats():
        db = find_db()
        if not db:
            return

        database_typename = lookup_type(['replica::database', 'database'])[1].name
        gdb.write('Replica:\n')
        gdb.write('  Read Concurrency Semaphores:\n    {}\n    {}\n    {}\n'.format(
                scylla_memory.format_semaphore_stats(db['_read_concurrency_sem']),
                scylla_memory.format_semaphore_stats(db['_streaming_concurrency_sem']),
                scylla_memory.format_semaphore_stats(db['_system_read_concurrency_sem'])))

        gdb.write('  Execution Stages:\n')
        for es_path in [('_apply_stage',)]:
            machine_name = es_path[0]
            human_name = machine_name.replace('_', ' ').strip()
            total = 0

            gdb.write('    {}:\n'.format(human_name))
            es = db
            for path_component in es_path:
                try:
                    es = es[path_component]
                except gdb.error:
                    break

            for sg_id, sg_name, count in scylla_memory.summarize_inheriting_execution_stage(es):
                total += count
                gdb.write('      {:02} {:32} {}\n'.format(sg_id, sg_name, count))
            gdb.write('         {:32} {}\n'.format('Total', total))

        gdb.write('  Tables - Ongoing Operations:\n')
        for machine_name in ['_pending_writes_phaser', '_pending_reads_phaser', '_pending_streams_phaser']:
            human_name = machine_name.replace('_', ' ').strip()
            gdb.write('    {} (top 10):\n'.format(human_name))
            total = 0
            i = 0
            for count, tables in scylla_memory.summarize_table_phased_barrier_users(db, machine_name):
                total += count
                if i < 10:
                    gdb.write('      {:9} {}\n'.format(count, ', '.join(tables)))
                i += 1
            gdb.write('      {:9} Total (all)\n'.format(total))
        gdb.write('\n')

    def invoke(self, arg, from_tty):
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
        free_mem = int(cpu_mem['nr_free_pages']) * page_size
        total_mem = int(cpu_mem['nr_pages']) * page_size
        gdb.write('Used memory: {used_mem:>13}\nFree memory: {free_mem:>13}\nTotal memory: {total_mem:>12}\n\n'
                  .format(used_mem=total_mem - free_mem, free_mem=free_mem, total_mem=total_mem))

        lsa = get_lsa_segment_pool()
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
                  '  unspooled:  {reg_unspooled:>13}\n'
                  ' System:\n'
                  '  real dirty: {sys_real_dirty:>13}\n'
                  '  unspooled:  {sys_unspooled:>13}\n\n'
                  .format(total=(lsa_allocated-cache_region.total()),
                          reg_real_dirty=dirty_mem_mgr(db['_dirty_memory_manager']).real_dirty(),
                          reg_unspooled=dirty_mem_mgr(db['_dirty_memory_manager']).unspooled(),
                          sys_real_dirty=dirty_mem_mgr(db['_system_dirty_memory_manager']).real_dirty(),
                          sys_unspooled=dirty_mem_mgr(db['_system_dirty_memory_manager']).unspooled()))

        scylla_memory.print_coordinator_stats()
        scylla_memory.print_replica_stats()

        gdb.write('Small pools:\n')
        small_pools = cpu_mem['small_pools']
        nr = small_pools['nr_small_pools']
        gdb.write('{objsize:>5} {span_size:>6} {use_count:>10} {memory:>12} {unused:>12} {wasted_percent:>5}\n'
                  .format(objsize='objsz', span_size='spansz', use_count='usedobj', memory='memory',
                          unused='unused', wasted_percent='wst%'))
        total_small_bytes = 0
        sc = span_checker()
        free_object_size = gdb.parse_and_eval('sizeof(\'seastar::memory::free_object\')')
        for i in range(int(nr)):
            sp = small_pools['_u']['a'][i]
            object_size = int(sp['_object_size'])
            # Skip pools that are smaller than sizeof(free_object), they won't have any content
            if object_size < free_object_size:
                continue
            span_size = int(sp['_span_sizes']['preferred']) * page_size
            free_count = int(sp['_free_count'])
            pages_in_use = 0
            use_count = 0
            for s in sc.spans():
                if not s.is_free() and s.pool() == sp.address:
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
        branches = (" |-- ", r" \-- ")

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
                bt = site['backtrace']['_main']
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
        return self.thread is not None

    @property
    def is_containing_page_free(self):
        return self._is_containing_page_free

    def mark_free(self):
        self._is_containing_page_free = True
        self._is_live = False

    @property
    def obj_ptr(self):
        return self.ptr - self.offset_in_object

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
            msg += ', live (0x%x +%d)' % (self.obj_ptr, self.offset_in_object)
        else:
            msg += ', free (0x%x +%d)' % (self.obj_ptr, self.offset_in_object)

        if self.is_lsa:
            msg += ', LSA-managed'

        return msg


def get_segment_base(segment_pool):
    try:
        segment_store = segment_pool["_store"]
        try:
            return int(std_unique_ptr(segment_store["_backend"]).get()["_segments_base"])
        except gdb.error:
            return int(segment_store["_segments_base"])
    except gdb.error:
        return int(segment_pool["_segments_base"])


class scylla_ptr(gdb.Command):
    _is_seastar_allocator_used = None

    def __init__(self):
        gdb.Command.__init__(self, 'scylla ptr', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    @staticmethod
    def is_seastar_allocator_used():
        if scylla_ptr._is_seastar_allocator_used is not None:
            return scylla_ptr._is_seastar_allocator_used

        try:
            gdb.parse_and_eval('&\'seastar::memory::cpu_mem\'')
            scylla_ptr._is_seastar_allocator_used = True
            return True
        except:
            scylla_ptr._is_seastar_allocator_used = False
            return False

    @staticmethod
    def _do_analyze(ptr):
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
            ptr_meta.offset_in_object = offset_in_object
            ptr_meta.is_live = not free
        else:
            ptr_meta.is_small = False
            ptr_meta.is_live = not span.is_free()
            ptr_meta.size = span.size() * page_size
            ptr_meta.offset_in_object = ptr - span.start

        # FIXME: handle debug-mode build
        segment_pool = get_lsa_segment_pool()
        segments_base = get_segment_base(segment_pool)
        segment_size = int(gdb.parse_and_eval('\'logalloc::segment\'::size'))
        index = int((int(ptr) - segments_base) / segment_size)
        desc = std_vector(segment_pool["_segments"])[index]
        ptr_meta.is_lsa = bool(desc['_region'])

        return ptr_meta

    @staticmethod
    def analyze(ptr):
        orig = gdb.selected_thread()
        try:
            return scylla_ptr._do_analyze(ptr)
        finally:
            orig.switch()

    def invoke(self, arg, from_tty):
        ptr = int(gdb.parse_and_eval(arg))

        ptr_meta = self.analyze(ptr)

        gdb.write("{}\n".format(str(ptr_meta)))


class segment_descriptor:
    def __init__(self, ref):
        self.ref = ref

    def region(self):
        return self.ref['_region']

    def is_lsa(self):
        return bool(self.ref['_region'])

    def free_space(self):
        return int(gdb.parse_and_eval('%d & (\'logalloc\'::segment::size_mask)'
                                      % (self.ref['_free_space'])))

    @property
    def address(self):
        return self.ref.address


class scylla_segment_descs(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla segment-descs', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        segment_size = int(gdb.parse_and_eval('\'logalloc\'::segment::size'))
        segment_pool = get_lsa_segment_pool()
        base = get_segment_base(segment_pool)

        def print_desc(seg_addr, desc):
            if desc.is_lsa():
                gdb.write('0x%x: lsa free=%-6d used=%-6d %6.2f%% region=0x%x\n' % (seg_addr, desc.free_space(),
                    segment_size - int(desc.free_space()),
                    float(segment_size - int(desc.free_space())) * 100 / segment_size,
                    int(desc.region())))
            else:
                gdb.write('0x%x: std\n' % (seg_addr))

        if base is None: # debug mode build
            segs = std_vector(segment_pool["_store"]["_segments"])
            descs = std_vector(segment_pool["_segments"])

            for seg, desc_ref in zip(segs, descs):
                desc = segment_descriptor(desc_ref)
                print_desc(int(seg), desc)
        else:
            addr = base
            for desc in std_vector(segment_pool["_segments"]):
                desc = segment_descriptor(desc)
                print_desc(addr, desc)
                addr += segment_size


def shard_of(ptr):
    return (int(ptr) >> 36) & 0xff


class scylla_lsa_check(gdb.Command):
    """
    Runs consistency checks on the LSA state on current shard.
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla lsa-check', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        segment_size = int(gdb.parse_and_eval('\'logalloc\'::segment::size'))

        db = find_db()

        # Collect segment* which are present in _segment_descs
        in_buckets = set()
        cache_region = lsa_region(db['_row_cache_tracker']['_region'])
        shard = shard_of(cache_region.impl())
        for bucket in std_array(cache_region.impl()['_segment_descs']['_buckets']):
            for desc in intrusive_list(bucket):
                if shard_of(int(desc.address)) != shard:
                    gdb.write('ERROR: Out-of-shard segment: (logalloc::segment_descriptor*)0x%x, free_space=%d\n'
                              % (int(desc.address), desc.free_space()))
                in_buckets.add(int(desc.address))

        # Scan shard's segment_descriptor:s for anomalies:
        #  - detect segments owned by cache which are not in cache region's _segment_descs
        #  - compute segment occupancy statistics for comparison with region's stored ones
        segment_pool = get_lsa_segment_pool()
        base = get_segment_base(segment_pool)
        desc_free_space = 0
        desc_total_space = 0
        for desc in std_vector(segment_pool["_segments"]):
            desc = segment_descriptor(desc)
            if desc.is_lsa() and desc.region() == cache_region.impl():
                if not int(desc.address) in in_buckets:
                    if cache_region.impl()['_active'] != base and cache_region.impl()['_buf_active'] != base:
                        # stray = not in _closed_segments
                        gdb.write('ERROR: Stray segment: (logalloc::segment*)0x%x, (logalloc::segment_descriptor*)0x%x, free_space=%d\n'
                              % (base, int(desc.address), desc.free_space()))
                else:
                    desc_free_space += desc.free_space()
                    desc_total_space += segment_size
            base += segment_size

        region_free_space = int(cache_region.impl()['_closed_occupancy']['_free_space'])
        if region_free_space != desc_free_space:
            gdb.write("ERROR: free space in closed segments according to region: %d\n" % region_free_space)
            gdb.write("ERROR: free space in closed segments according to segment descriptors: %d (diff: %d)\n"
                      % (desc_free_space, desc_free_space - region_free_space))

        region_total_space = int(cache_region.impl()['_closed_occupancy']['_total_space'])
        if region_total_space != desc_total_space:
            gdb.write("ERROR: total space in closed segments according to region: %d\n" % region_total_space)
            gdb.write("ERROR: total space in closed segments according to segment descriptors: %d (diff: %d)\n"
                      % (desc_total_space, desc_total_space - region_total_space))


class scylla_lsa(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla lsa', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        lsa = get_lsa_segment_pool()
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

        for region in lsa_regions():
            gdb.write('    Region #{r_id} (logalloc::region_impl*) {r_addr}\n      - reclaimable: {r_en:>14}\n'
                      '      - evictable: {r_ev:16}\n      - non-LSA memory: {r_non_lsa:>11}\n'
                      '      - closed LSA memory: {r_lsa:>8}\n      - unused memory: {r_unused:>12}\n'
                      .format(r_addr=str(region.dereference()), r_id=int(region['_id']), r_en=bool(region['_reclaiming_enabled']),
                              r_ev=bool(region['_evictable']),
                              r_non_lsa=int(region['_non_lsa_occupancy']['_total_space']),
                              r_lsa=int(region['_closed_occupancy']['_total_space']),
                              r_unused=int(region['_closed_occupancy']['_free_space'])))


names = {}  # addr (int) -> name (str)


def resolve(addr, cache=True, startswith=None):
    if addr in names:
        return names[addr]

    infosym = gdb.execute('info symbol 0x%x' % (addr), False, True)
    if infosym.startswith('No symbol'):
        return None

    name = infosym[:infosym.find('in section')]
    if startswith and not name.startswith(startswith):
        return None
    if cache:
        names[addr] = name
    return name


class lsa_regions(object):
    def __init__(self):
        lsa_tracker = std_unique_ptr(gdb.parse_and_eval('\'logalloc::tracker_instance\'._impl'))
        self._regions = lsa_tracker['_regions']
        self._region = self._regions['_M_impl']['_M_start']

    def __iter__(self):
        while self._region != self._regions['_M_impl']['_M_finish']:
            yield self._region
            self._region = self._region + 1


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

    def __init__(self, value, desc_pos, obj_pos):
        self.value = value
        self.desc_pos = desc_pos
        self.obj_pos = obj_pos

    def is_live(self):
        return (self.value & 1) == 1

    def dead_size(self):
        return self.value / 2

    def migrator_ptr(self):
        static_migrators = gdb.parse_and_eval("'::debug::static_migrators'")
        return static_migrators['_migrators']['_M_impl']['_M_start'][self.value >> 1]

    def migrator(self):
        return self.migrator_ptr().dereference()

    def migrator_str(self):
        mig = str(self.migrator())
        m = re.match(self.mig_re, mig)
        return m.group(1)

    def live_size(self):
        mig = self.migrator_ptr()
        obj = int(self.obj_pos)
        cmd = f'((migrate_fn_type*){mig})->size((const void*){obj})'
        res = gdb.parse_and_eval(cmd)
        return int(res)

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

        try:
            logalloc_alignment = gdb.parse_and_eval("'::debug::logalloc_alignment'")
        except gdb.error:
            # optimized-out and/or garbage-collected by ld, which
            # _probably_ means the mode is not "sanitize", so:
            logalloc_alignment = 1

        logalloc_alignment_mask = logalloc_alignment - 1

        ptr = int(arg, 0)
        seg = gdb.parse_and_eval('(char*)(%d & ~(\'logalloc\'::segment::size - 1))' % (ptr))
        segment_size = int(gdb.parse_and_eval('\'logalloc\'::segment::size'))
        seg_end = seg + segment_size
        while seg < seg_end:
            desc = lsa_object_descriptor.decode(seg)
            print(desc)
            seg = desc.end_pos()
            seg = gdb.parse_and_eval('(char*)((%d + %d) & ~%d)' % (seg, logalloc_alignment_mask, logalloc_alignment_mask))


class scylla_timers(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla timers', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        gdb.write('Timers:\n')
        timer_set = gdb.parse_and_eval('\'seastar\'::local_engine->_timers')
        for timer_list in std_array(timer_set['_buckets']):
            for t in intrusive_list(timer_list, link='_link'):
                gdb.write('(%s*) %s = %s\n' % (t.type, t.address, t))
        timer_set = gdb.parse_and_eval('\'seastar\'::local_engine->_lowres_timers')
        for timer_list in std_array(timer_set['_buckets']):
            for t in intrusive_list(timer_list, link='_link'):
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


def switch_to_shard(shard):
    for r in reactors():
        if int(r['_id']) == shard:
            return


class scylla_apply(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla apply', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        orig = gdb.selected_thread()
        try:
            for r in reactors():
                gdb.write("\nShard %d: \n\n" % (r['_id']))
                gdb.execute(arg)
        finally:
            orig.switch()


class scylla_shard(gdb.Command):
    """Retrieves information on the current shard and allows switching shards.

    Run without any parameters to print the current thread's shard number.
    Run with a numeric parameter <N> to switch to the thread which runs shard <N>.

    Example:
      scylla shard 0
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla shard', gdb.COMMAND_USER, gdb.COMPLETE_NONE)

    def invoke(self, arg, from_tty):
        if arg is None or arg == '':
            gdb.write('Current shard is %d\n' % current_shard())
            return
        id = 0
        try:
            id = int(arg)
        except:
            gdb.write('Error: %s is not a valid shard number\n' % arg)
            return
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
        return gdb.parse_and_eval('$fs_base')

    def regs(self):
        return self.regs_from_jmpbuf(self.thread_ctx['_context']['jmpbuf'])

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
    return intrusive_list(gdb.parse_and_eval('seastar::thread_context::_all_threads'), link='_all_link')


class scylla_thread(gdb.Command):
    """Operations with seastar::threads.

    The following operations are supported:
    * Switch to seastar thread (see also `scylla unthread`);
    * Execute command in the context of all existing seastar::threads.
    * Print saved registers for given thread

    Run `scylla thread --help` for more information on usage.

    DISCLAIMER: This is a dangerous command with the potential to crash the
    process if anything goes wrong!
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla thread', gdb.COMMAND_USER,
                             gdb.COMPLETE_COMMAND, True)

    def invoke_apply_all(self, args):
        for r in reactors():
            for t in seastar_threads_on_current_shard():
                gdb.write('\n[shard %2d] (seastar::thread_context*) 0x%x:\n\n' % (r['_id'], int(t.address)))
                with seastar_thread_context(t):
                    gdb.execute(' '.join(args))

    def invoke(self, arg, for_tty):
        parser = argparse.ArgumentParser(description="scylla thread")
        parser.add_argument("--iamsure", action="store_true", default=False,
                help="I know what I'm doing and I want to run this command knowing that it might *crash* this process.")
        parser.add_argument("-s", "--switch", action="store_true", default=False,
                help="Switch to the given thread."
                " The corresponding `seastar::thread_context*` is expected to be provided as the positional argument."
                " Any valid gdb expression that evaluates to such a pointer is accepted."
                " To leave the thread later, use the `scylla unthread` command.")
        parser.add_argument("-a", "--apply-all", action="store_true", default=False,
                help="Execute the command in the context of each seastar::thread."
                " The command (and its arguments) to execute is expected to be provided as the positional argument.")
        parser.add_argument("-p", "--print-regs", action="store_true", default=False,
                help="Print unmangled registers saved in the jump buffer")
        parser.add_argument("arg", nargs='+')

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        if not args.print_regs and not args.iamsure:
            gdb.write("DISCLAIMER: This is a dangerous command with the potential to crash the process if anything goes wrong!"
                    " Please pass the `--iamsure` flag to acknowledge being fine with this risk.\n")
            return

        if sum([arg for arg in [args.apply_all, args.switch, args.print_regs]]) > 1:
            gdb.write("Only one of `--apply-all`, `--switch` or `--print-regs` can be used.\n")
            return

        if not args.apply_all and not args.switch and not args.print_regs:
            gdb.write("No command specified, need one of `--apply-all`, `--switch` or `--print_regs`.\n")
            return

        if args.apply_all:
            self.invoke_apply_all(args.arg)
            return

        if len(args.arg) > 1:
            gdb.write("Expected only one argument for the `--switch` command.\n")
            return

        addr = gdb.parse_and_eval(args.arg[0])
        ctx = addr.reinterpret_cast(gdb.lookup_type('seastar::thread_context').pointer()).dereference()
        if args.print_regs:
            for reg, val in seastar_thread_context(ctx).regs().items():
                gdb.write("%s: 0x%x\n" % (reg, val))
            return
        exit_thread_context()
        global active_thread_context
        active_thread_context = seastar_thread_context(ctx)
        active_thread_context.__enter__()


class scylla_unthread(gdb.Command):
    """Leave the current seastar::thread context.

    Does nothing when there is no active seastar::thread context.
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla unthread', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        exit_thread_context()


class scylla_threads(gdb.Command):
    """Find and list seastar::threads

    Without any arguments, lists threads on the current shard. You can also list
    threads on a given shard with `-s $shard` or on all shards with `-a` (old
    behaviour).
    Pass `-v` to also add the functor vtable symbol to the listing, this might
    or might not tell you what the main function of the thread is.
    Invoke with `--help` to see all available options.
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla threads', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def _do_list_threads(self, shard, include_vtable):
        for t in seastar_threads_on_current_shard():
            stack = std_unique_ptr(t['_stack']).get()
            if include_vtable:
                maybe_vtable = ' vtable: {}'.format(t['_func']['_vtable'])
            else:
                maybe_vtable = ''
            gdb.write('[shard {}] (seastar::thread_context*) 0x{:x}, stack: 0x{:x}{}\n'.format(
                shard, int(t.address), int(stack), maybe_vtable))

    def invoke(self, arg, for_tty):
        parser = argparse.ArgumentParser(description="scylla threads")
        parser.add_argument("-a", "--all", action="store_true",
                help="list threads on all shards")
        parser.add_argument("-s", "--shard", action="store", type=int, default=-1,
                help="list threads on the specified shard")
        parser.add_argument("-v", "--vtable", action="store_true",
                help="include functor vtable in the printout; could be useful for identifying the thread, but makes the output very verbose")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        if args.all:
            shards = list(range(cpus()))
        elif args.shard != -1:
            shards = [args.shard]
        else:
            shards = [int(gdb.parse_and_eval('seastar::local_engine._id'))]

        for r in reactors():
            shard = int(r['_id'])
            if shard in shards:
                self._do_list_threads(shard, args.vtable)


class circular_buffer(object):
    def __init__(self, ref):
        self.ref = ref

    def _mask(self, i):
        return i & (int(self.ref['_impl']['capacity']) - 1)

    def __iter__(self):
        impl = self.ref['_impl']
        st = impl['storage']
        cap = impl['capacity']
        i = impl['begin']
        end = impl['end']
        while i < end:
            yield st[self._mask(i)]
            i += 1

    def size(self):
        impl = self.ref['_impl']
        return int(impl['end']) - int(impl['begin'])

    def __len__(self):
        return self.size()

    def __getitem__(self, item):
        impl = self.ref['_impl']
        return (impl['storage'] + self._mask(int(impl['begin']) + item)).dereference()

    def external_memory_footprint(self):
        impl = self.ref['_impl']
        return int(impl['capacity']) * self.ref.type.template_argument(0).sizeof


class small_vector(object):
    def __init__(self, ref):
        self.ref = ref

    def __len__(self):
        return self.ref['_begin'] - self.ref['_end']

    def __iter__(self):
        e = self.ref['_begin']
        while e != self.ref['_end']:
            yield e.dereference()
            e += 1

    def external_memory_footprint(self):
        if self.ref['_begin'] == self.ref['_internal']['storage'].address:
            return 0
        return int(self.ref['_capacity_end']) - int(self.ref['_begin'])


class boost_small_vector(object):
    def __init__(self, ref):
        self.ref = ref

    def __iter__(self):
        e = self.ref['m_holder']['m_start']
        for i in range(0, self.ref['m_holder']['m_size']):
            yield e.dereference()
            e += 1

    def __len__(self):
        return self.ref['m_holder']['m_size']


class chunked_vector(object):
    def __init__(self, ref):
        self.ref = ref
        self._max_contiguous_allocation = int(list(template_arguments(self.ref.type))[1])

    def max_chunk_capacity(self):
        return max(self._max_contiguous_allocation / self.ref.type.sizeof, 1);

    def __len__(self):
        return int(self.ref['_size'])

    def __iter__(self):
        sz = len(self)
        for chunk in small_vector(self.ref['_chunks']):
            chunk = std_unique_ptr(chunk).get()
            for i in range(min(sz, self.max_chunk_capacity())):
                yield chunk[i]
                sz -= 1

    def external_memory_footprint(self):
        return int(self.ref['_capacity']) * self.ref.type.template_argument(0).sizeof \
               + small_vector(self.ref['_chunks']).external_memory_footprint()


def get_local_task_queues():
    """ Return a list of task pointers for the local reactor. """
    for tq_ptr in static_vector(gdb.parse_and_eval('\'seastar\'::local_engine._task_queues')):
        yield std_unique_ptr(tq_ptr).dereference()

def get_local_io_queues():
    """ Return a list of io queues for the local reactor. """
    for dev, ioq in unordered_map(gdb.parse_and_eval('\'seastar\'::local_engine._io_queues')):
        yield dev, std_unique_ptr(ioq).dereference()


def get_local_tasks(tq_id = None):
    """ Return a list of task pointers for the local reactor. """
    if tq_id is not None:
        tqs = filter(lambda x: x['_id'] == tq_id, get_local_task_queues())
    else:
        tqs = get_local_task_queues()

    for tq in tqs:
        for t in circular_buffer(tq['_q']):
            yield t


class scylla_task_stats(gdb.Command):
    """ Prints histogram of task types in reactor's pending task queue.

    Example:
    (gdb) scylla task-stats
       16243: 0x18904f0 vtable for lambda_task<yield()::{lambda()#1}> + 16
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
    (task*) 0x60018fac2208  vtable for lambda_task<yield()::{lambda()#1}> + 16
    (task*) 0x60016e8b7428  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x60017e5bece8  _ZTV12continuationIZN6futureIJEE12then_wrappedINS1_12finally_bodyIZ...
    (task*) 0x60017e7f8aa0  _ZTV12continuationIZN6futureIJEE12then_wrappedIZNS1_16handle_except...
    (task*) 0x60018fac21e0  vtable for lambda_task<yield()::{lambda()#1}> + 16
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


class scylla_task_queues(gdb.Command):
    """ Print a summary of the reactor's task queues.

    Example:
       id name                             shares  tasks
     A 00 "main"                           1000.00 4
       01 "atexit"                         1000.00 0
       02 "streaming"                       200.00 0
     A 03 "compaction"                      171.51 1
       04 "mem_compaction"                 1000.00 0
    *A 05 "statement"                      1000.00 2
       06 "memtable"                          8.02 0
       07 "memtable_to_cache"               200.00 0

    Where:
        * id: seastar::reactor::task_queue::_id
        * name: seastar::reactor::task_queue::_name
        * shares: seastar::reactor::task_queue::_shares
        * tasks: seastar::reactor::task_queue::_q.size()
        * A: seastar::reactor::task_queue::_active == true
        * *: seastar::reactor::task_queue::_current == true
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla task-queues', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    @staticmethod
    def _active(a):
        if a:
            return 'A'
        return ' '

    @staticmethod
    def _current(c):
        if c:
            return '*'
        return ' '

    def invoke(self, arg, for_tty):
        current_sg = gdb.parse_and_eval('(seastar::scheduling_group) \'seastar::internal::current_scheduling_group_ptr()::sg\'')
        gdb.write('   {:2} {:32} {:7} {}\n'.format("id", "name", "shares", "tasks"))
        for tq in get_local_task_queues():
            gdb.write('{}{} {:02} {:32} {:>7.2f} {}\n'.format(
                    self._current(current_sg['_id'] == tq['_id']),
                    self._active(bool(tq['_active'])),
                    int(tq['_id']),
                    str(tq['_name']),
                    float(tq['_shares']),
                    len(circular_buffer(tq['_q']))))


class scylla_io_queues(gdb.Command):
    """ Print a summary of the reactor's IO queues.

    Example:
    Dev 0:
        Class:                  |shares:         |ptr:            
        --------------------------------------------------------
        "default"               |1               |0x6000002c6500  
        "commitlog"             |1000            |0x6000003ad940  
        "memtable_flush"        |1000            |0x6000005cb300  
        "streaming"             |200             |0x0             
        "query"                 |1000            |0x600000718580  
        "compaction"            |1000            |0x6000030ef0c0  

        Max request size:    2147483647
        Max capacity:        Ticket(weight: 4194303, size: 4194303)
        Capacity tail:       Ticket(weight: 73168384, size: 100561888)
        Capacity head:       Ticket(weight: 77360511, size: 104242143)

        Resources executing: Ticket(weight: 2176, size: 514048)
        Resources queued:    Ticket(weight: 384, size: 98304)
        Handles: (1)
            Class 0x6000005d7278:
                Ticket(weight: 128, size: 32768)
                Ticket(weight: 128, size: 32768)
                Ticket(weight: 128, size: 32768)
        Pending in sink: (0)


    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla io-queues', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    class ticket:
        def __init__(self, ref):
            self.ref = ref

        def __str__(self):
            return f"Ticket(weight: {self.ref['_weight']}, size: {self.ref['_size']})"

    @staticmethod
    def _print_io_priority_class(pclass, names_from_ptrs, indent = '\t\t'):
        gdb.write("{}Class {}:\n".format(indent, names_from_ptrs.get(pclass.address, pclass.address)))
        slist = intrusive_slist(pclass['_queue'], link='_hook')
        for entry in slist:
            gdb.write("{}\t{}\n".format(indent, scylla_io_queues.ticket(entry['_ticket'])))

    def _get_classes_infos(self, ioq):
        # Starting from 5.3 priority classes are removed and IO inherits its name and
        # shares from the respective sched group, so should the infos.  Not to rely on
        # gdb.parse_and_eval() to fail searching for _infos, use the secret knowledge:
        # commitlog gained its sched group at the same time, so if not present, we're
        # on some older version and should fallback to io-prio _infos (or worse)
        infos = {}
        commitlog_met = False
        for tq in get_local_task_queues():
            name = str(tq['_name'])
            if name == '"commitlog"':
                commitlog_met = True
            infos[int(tq['_id'])] = { 'name': name, 'shares': int(tq['_shares']) }
        if commitlog_met:
            return infos

        try:
            return std_array(gdb.parse_and_eval('seastar::io_priority_class::_infos'))
        except gdb.error:
            # Compatibility: io_queue::_registered_... stuff moved onto io_priority_class in version 4.6
            return [ { 'name': x[0], 'shares': x[1] } for x in zip(std_array(ioq['_registered_names']), std_array(ioq['_registered_shares'])) ]

    def invoke(self, arg, for_tty):
        for dev, ioq in get_local_io_queues():
            gdb.write("Dev {}:\n".format(dev))

            infos = self._get_classes_infos(ioq)
            pclasses = std_vector(ioq['_priority_classes'])

            names_from_ptrs = {}

            gdb.write("\t{:24}|{:16}|{:46}\n".format("Class:", "shares:", "ptr:"))
            gdb.write("\t" + '-'*64 + "\n")
            for i, pclass in enumerate(pclasses):
                pclass_ptr = std_unique_ptr(pclass).get()
                names_from_ptrs[pclass_ptr] = infos[i]['name']
                gdb.write("\t{:24}|{:16}|({:30}){:16}\n".format(str(infos[i]['name']), str(infos[i]['shares']), str(pclass_ptr.type), str(pclass_ptr)))
            gdb.write("\n")

            group = std_shared_ptr(ioq['_group']).get().dereference()
            while True:
                # in order to avoid nested try-catch block, use a single while block
                try:
                    f_groups = static_vector(group['_fgs'])
                    _ = len(f_groups)
                    f_queues = static_vector(ioq['_streams'])
                    _ = len(f_queues)
                    fq_pclass = lambda x: x.dereference()
                    break
                except gdb.error:
                    # try harder
                    pass
                try:
                    f_groups = [std_unique_ptr(x) for x in std_vector(group['_fgs'])]
                    f_queues = boost_small_vector(ioq['_streams'])
                    fq_pclass = lambda x: x.dereference()
                    break
                except gdb.error:
                    # try harder
                    pass
                try:
                    f_groups = [group['_fg']]
                    f_queues = [ioq['_fq']]
                    fq_pclass = lambda x: seastar_lw_shared_ptr(x).get().dereference()
                    break
                except gdb.error:
                    # give up
                    raise

            gdb.write("\t{} streams\n".format(len(f_groups)))
            gdb.write("\n")

            for fg, fq in zip(f_groups, f_queues):
                try:
                    try:
                        gdb.write("\tCapacity tail:       {}\n".format(std_atomic(fg['_token_bucket']['_rovers']['tail']).get()))
                        gdb.write("\tCapacity head:       {}\n".format(std_atomic(fg['_token_bucket']['_rovers']['head']).get()))
                        try:
                            gdb.write("\tCapacity ceil:       {}\n".format(std_atomic(fg['_token_bucket']['_rovers']['ceil']).get()))
                        except gdb.error:
                            pass
                    except gdb.error:
                        gdb.write("\tCapacity tail:       {}\n".format(std_atomic(fg['_capacity_tail']).get()))
                        gdb.write("\tCapacity head:       {}\n".format(std_atomic(fg['_capacity_head']).get()))
                        gdb.write("\tCapacity ceil:       {}\n".format(std_atomic(fg['_capacity_ceil']).get()))
                    gdb.write("\n")
                except gdb.error:
                    gdb.write("\tMax capacity:        {}\n".format(self.ticket(fg['_maximum_capacity'])))
                    gdb.write("\tCapacity tail:       {}\n".format(self.ticket(std_atomic(fg['_capacity_tail']).get())))
                    gdb.write("\tCapacity head:       {}\n".format(self.ticket(std_atomic(fg['_capacity_head']).get())))
                    gdb.write("\n")

                gdb.write("\tResources executing: {}\n".format(self.ticket(fq['_resources_executing'])))
                gdb.write("\tResources queued:    {}\n".format(self.ticket(fq['_resources_queued'])))
                handles = std_priority_queue(fq['_handles'])
                gdb.write("\tHandles: ({})\n".format(len(handles)))
                for pclass_ptr in handles:
                    self._print_io_priority_class(fq_pclass(pclass_ptr), names_from_ptrs)

            try:
                pending = chunked_fifo(ioq['_sink']['_pending_io'])
            except gdb.error:
                pending = circular_buffer(ioq['_sink']['_pending_io'])
            gdb.write("\tPending in sink: ({})\n".format(len(pending)))
            for op in pending:
                gdb.write("Completion {}\n".format(op['_completion']))


class scylla_fiber(gdb.Command):
    """ Walk the continuation chain starting from the given task

    Example (cropped for brevity):
    (gdb) scylla fiber 0x600016217c80
    [shard  0] #-1 (task*) 0x000060001a305910 0x0000000004aa5260 vtable for seastar::continuation<...> + 16
    [shard  0] #0  (task*) 0x0000600016217c80 0x0000000004aa5288 vtable for seastar::continuation<...> + 16
    [shard  0] #1  (task*) 0x000060000ac42940 0x0000000004aa2aa0 vtable for seastar::continuation<...> + 16
    [shard  0] #2  (task*) 0x0000600023f59a50 0x0000000004ac1b30 vtable for seastar::continuation<...> + 16
     ^          ^          ^                  ^                  ^
    (1)        (2)        (3)                (4)                (5)

    1) Shard the task lives on (continuation chains can spawn multiple shards)
    2) Task index:
        - 0 is the task passed to the command
        - < 0 are tasks this task waits on
        - > 0 are tasks waiting on this task
    3) Pointer to the task object.
    4) Pointer to the task's vtable.
    5) Symbol name of the task's vtable.

    Invoke `scylla fiber --help` for more information on usage.
    """

    @classmethod
    @property
    @functools.cache
    def _vptr_type(cls):
        return gdb.lookup_type('uintptr_t').pointer()

    def __init__(self):
        gdb.Command.__init__(self, 'scylla fiber', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
        self._task_symbol_matcher = task_symbol_matcher()
        self._thread_map = None

    def _name_is_on_whitelist(self, name):
        return self._task_symbol_matcher(name)

    def _maybe_log(self, msg, verbose):
        if verbose:
            gdb.write(msg)

    def _probe_pointer(self, ptr, scanned_region_size, using_seastar_allocator, verbose):
        """ Check if the pointer is a task pointer

        The pattern we are looking for is:
        ptr -> vtable ptr for a symbol that matches our whitelist

        In addition, ptr has to point to an allocation block, managed by
        seastar, that contains a live object.
        """
        # Save work if caller already analyzed the pointer
        if isinstance(ptr, pointer_metadata):
            ptr_meta = ptr
            ptr = ptr.ptr
        else:
            ptr_meta = None

        try:
            maybe_vptr = int(gdb.Value(ptr).reinterpret_cast(self._vptr_type).dereference())
            self._maybe_log("\t-> 0x{:016x}\n".format(maybe_vptr), verbose)
        except gdb.MemoryError:
            self._maybe_log("\tNot a pointer\n", verbose)
            return

        resolved_symbol = resolve(maybe_vptr, False)
        if resolved_symbol is None:
            self._maybe_log("\t\tNot a vtable ptr\n", verbose)
            return

        self._maybe_log("\t\t=> {}\n".format(resolved_symbol), verbose)

        if not self._name_is_on_whitelist(resolved_symbol):
            self._maybe_log("\t\t\tSymbol name doesn't match whitelisted symbols\n", verbose)
            return

        # The promise object starts on the third `uintptr_t` in the frame.
        # The resume_fn pointer is the first `uintptr_t`.
        # So if the task is a coroutine, we should be able to find the resume function via offsetting by -2.
        # AFAIK both major compilers respect this convention.
        if resolved_symbol.startswith('vtable for seastar::internal::coroutine_traits_base'):
            if block := gdb.block_for_pc((gdb.Value(ptr).cast(self._vptr_type) - 2).dereference()):
                resume = block.function
                resolved_symbol += f" ({resume.print_name} at {resume.symtab.filename}:{resume.line})"
            else:
                resolved_symbol += f" (unknown coroutine)"

        if using_seastar_allocator:
            if ptr_meta is None:
                ptr_meta = scylla_ptr.analyze(ptr)
            if not ptr_meta.is_managed_by_seastar() or not ptr_meta.is_live:
                self._maybe_log("\t\t\tNot a live object\n", verbose)
                return
        else:
            ptr_meta = pointer_metadata(ptr, scanned_region_size)

        self._maybe_log("\t\t\tTask found\n", verbose)

        return ptr_meta, maybe_vptr, resolved_symbol

    # Find futures waiting on this task
    def _walk_forward(self, ptr_meta, name, i, max_depth, scanned_region_size, using_seastar_allocator, verbose):
        ptr = ptr_meta.ptr

        # thread_context has a self reference in its `_func` field which will be
        # found before the ref to the next task if the whole object is scanned.
        # Exploit that thread_context is a type we can cast to and scan the
        # `_done` field explicitly to avoid that.
        if 'thread_context' in name:
            self._maybe_log("Current task is a thread, using its _done field to continue\n", verbose)
            thread_context_ptr_type = gdb.lookup_type('seastar::thread_context').pointer()
            ctxt = gdb.Value(int(ptr_meta.ptr)).reinterpret_cast(thread_context_ptr_type).dereference()
            pr = ctxt['_done']
            region_start = pr.address
            region_end = region_start + pr.type.sizeof
        else:
            region_start = ptr + self._vptr_type.sizeof # ignore our own vtable
            region_end = region_start + (ptr_meta.size - ptr_meta.size % self._vptr_type.sizeof)

        self._maybe_log("Scanning task #{} @ 0x{:016x}: {}\n".format(i, ptr, str(ptr_meta)), verbose)

        for it in range(region_start, region_end, self._vptr_type.sizeof):
            maybe_tptr = int(gdb.Value(it).reinterpret_cast(self._vptr_type).dereference())
            self._maybe_log("0x{:016x}+0x{:04x} -> 0x{:016x}\n".format(ptr, it - ptr, maybe_tptr), verbose)

            res = self._probe_pointer(maybe_tptr, scanned_region_size, using_seastar_allocator, verbose)

            if res is None:
                continue

            if int(res[0].ptr) == int(ptr):
                self._maybe_log("Rejecting self reference\n", verbose)
                continue

            return res

        return None

    # Find futures waited-on by this task
    def _walk_backward(self, ptr_meta, name, i, max_depth, scanned_region_size, using_seastar_allocator, verbose):
        orig = gdb.selected_thread()
        res = None

        # Threads need special handling as they allocate the thread_wait_task object on their stack.
        if 'thread_context' in name:
            context = gdb.parse_and_eval('(seastar::thread_context*)0x{:x}'.format(ptr_meta.ptr))
            stack_ptr = int(std_unique_ptr(context['_stack']).get())
            self._maybe_log("Current task is a thread, trying to find the thread_wake_task on its stack: 0x{:x}\n".format(stack_ptr), verbose)
            stack_meta = scylla_ptr.analyze(stack_ptr)
            # stack grows downwards, so walk from end of buffer towards the beginning
            for maybe_tptr in range(stack_ptr + stack_meta.size - self._vptr_type.sizeof, stack_ptr - self._vptr_type.sizeof, -self._vptr_type.sizeof):
                res = self._probe_pointer(maybe_tptr, scanned_region_size, using_seastar_allocator, verbose)
                if res is not None and 'thread_wake_task' in res[2]:
                    return res
            return None

        if name.startswith('vtable for seastar::internal::when_all_state'):
            when_all_state_base_ptr_type = gdb.lookup_type('seastar::internal::when_all_state_base').pointer()
            when_all_state_base = gdb.Value(int(ptr_meta.ptr)).reinterpret_cast(when_all_state_base_ptr_type)
            ptr = int(when_all_state_base['_continuation'])
            self._maybe_log("Current task is a when_all_state, looking for references to its continuation field 0x{:x}\n".format(ptr), verbose)
            return self._probe_pointer(ptr, scanned_region_size, using_seastar_allocator, verbose)

        # Async work items will have references on the remote shard, so we first
        # have to find out which is the remote shard and then switch to it.
        # Have to match the start of the name, as async_work_item kicks off
        # continuation and so it will be found in the name of those lambdas.
        if name.startswith('vtable for seastar::smp_message_queue::async_work_item'):
            self._maybe_log("Current task is a async work item, trying to deduce the remote shard\n", verbose)
            smp_mmessage_queue_ptr_type = gdb.lookup_type('seastar::smp_message_queue').pointer()
            work_item_type = gdb.lookup_type('seastar::smp_message_queue::work_item')
            # Casts to templates with lambda template arguments just don't work.
            # We know the offset of the message queue reference in
            # async_work_item (first field) so we calculate and cast a pointer to it.
            q_ptr = align_up(int(ptr_meta.ptr) + work_item_type.sizeof, self._vptr_type.sizeof)
            q = gdb.Value(q_ptr).reinterpret_cast(smp_mmessage_queue_ptr_type.pointer()).dereference()
            shard = int(q['_pending']['remote']['_id'])
            self._maybe_log("Deduced shard is {} (message queue 0x{:x} @ 0x{:x} + {})\n".format(shard, int(q), int(ptr_meta.ptr), q_ptr - int(ptr_meta.ptr)), verbose)
            # Sanity check.
            if shard < 0 or shard >= cpus():
                return None
            switch_to_shard(shard)
        else:
            ptr_meta.thread.switch()

        try:
            for maybe_tptr_meta, _ in scylla_find.find(ptr_meta.ptr):
                maybe_tptr_meta.ptr -= maybe_tptr_meta.offset_in_object
                res = self._probe_pointer(maybe_tptr_meta.ptr, scanned_region_size, using_seastar_allocator, verbose)
                if res is None:
                    continue

                if int(res[0].ptr) == int(ptr_meta.ptr):
                    self._maybe_log("Rejecting self reference\n", verbose)
                    continue

                return res
        finally:
            orig.switch()

        return res

    def _walk(self, walk_method, tptr_meta, name, max_depth, scanned_region_size, using_seastar_allocator, verbose):
        i = 0
        fiber = []
        known_tasks = set([tptr_meta.ptr])
        while True:
            if max_depth > -1 and i >= max_depth:
                break

            self._maybe_log("_walk() 0x{:x} {}\n".format(int(tptr_meta.ptr), name), verbose)
            res = walk_method(tptr_meta, name, i + 1, max_depth, scanned_region_size, using_seastar_allocator, verbose)
            if res is None:
                break

            tptr_meta, vptr, name = res

            if tptr_meta.ptr in known_tasks:
                gdb.write("Stopping because loop is detected: task 0x{:016x} was seen before.\n".format(tptr_meta.ptr))
                break

            known_tasks.add(tptr_meta.ptr)

            fiber.append((tptr_meta, vptr, name))

            i += 1

        return fiber

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

        if self._thread_map is None:
            self._thread_map = {}
            for r in reactors():
                self._thread_map[gdb.selected_thread().num] = int(r['_id'])

        def format_task_line(i, task_info):
            tptr_meta, vptr, name = task_info
            tptr = tptr_meta.ptr
            shard = self._thread_map[tptr_meta.thread.num]
            gdb.write("[shard {:2}] #{:<2d} (task*) 0x{:016x} 0x{:016x} {}\n".format(shard, i, int(tptr), int(vptr), name))

        try:
            using_seastar_allocator = not args.force_fallback_mode and scylla_ptr.is_seastar_allocator_used()
            if not using_seastar_allocator:
                gdb.write("Not using the seastar allocator, falling back to scanning a fixed-size region of memory\n")

            initial_task_ptr = int(gdb.parse_and_eval(args.task))
            this_task = self._probe_pointer(initial_task_ptr, args.scanned_region_size, using_seastar_allocator, args.verbose)
            if this_task is None:
                gdb.write("Provided pointer 0x{:016x} is not an object managed by seastar or not a task pointer\n".format(initial_task_ptr))
                return

            backwards_fiber = self._walk(self._walk_backward, this_task[0], this_task[2], args.max_depth, args.scanned_region_size, using_seastar_allocator, args.verbose)

            for i, task_info in enumerate(reversed(backwards_fiber)):
                format_task_line(i - len(backwards_fiber), task_info)

            format_task_line(0, this_task)

            forward_fiber = self._walk(self._walk_forward, this_task[0], this_task[2], args.max_depth, args.scanned_region_size, using_seastar_allocator, args.verbose)

            for i, task_info in enumerate(forward_fiber):
                format_task_line(i + 1, task_info)

            gdb.write("\nFound no further pointers to task objects.\n")
            if not backwards_fiber and not forward_fiber:
                gdb.write("If this is unexpected, run `scylla fiber 0x{:016x} --verbose` to learn more.\n".format(initial_task_ptr))
            else:
                gdb.write("If you think there should be more, run `scylla fiber 0x{:016x} --verbose` to learn more.\n"
                          "Note that continuation across user-created seastar::promise<> objects are not detected by scylla-fiber.\n".format(int(this_task[0].ptr)))
        except KeyboardInterrupt:
            return


def find_objects(mem_start, mem_size, value, size_selector='g', only_live=True):
    for line in gdb.execute("find/%s 0x%x, +0x%x, 0x%x" % (size_selector, mem_start, mem_size, value), to_string=True).split('\n'):
        if line.startswith('0x'):
            ptr_meta = scylla_ptr.analyze(int(line, base=16))
            if not only_live or ptr_meta.is_live:
                yield ptr_meta


class scylla_find(gdb.Command):
    """ Finds live objects on seastar heap of current shard which contain given value.
    Prints results in 'scylla ptr' format.

    See `scylla find --help` for more details on usage.

    Example:

      (gdb) scylla find 0x600005321900
      thread 1, small (size <= 512), live (0x6000000f3800 +48)
      thread 1, small (size <= 56), live (0x6000008a1230 +32)
    """
    @classmethod
    @property
    @functools.cache
    def _vptr_type(cls):
        return gdb.lookup_type('uintptr_t').pointer()

    _size_char_to_size = {
        'b': 8,
        'h': 16,
        'w': 32,
        'g': 64,
    }

    def __init__(self):
        gdb.Command.__init__(self, 'scylla find', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    @staticmethod
    def find(value, size_selector='g', value_range=0, find_all=False, only_live=True):
        step = int(scylla_find._size_char_to_size[size_selector] / 8)
        offset = 0
        mem_start, mem_size = get_seastar_memory_start_and_size()
        it = iter(find_objects(mem_start, mem_size, value, size_selector, only_live))

        # Find the first value in the range for which the search has results.
        while offset < value_range:
            try:
                yield next(it), offset
                if not find_all:
                    break
            except StopIteration:
                offset += step
                it = iter(find_objects(mem_start, mem_size, value + offset, size_selector, only_live))

        # List the rest of the results for value.
        try:
            while True:
                yield next(it), offset
        except StopIteration:
            pass


    def invoke(self, arg, for_tty):
        parser = argparse.ArgumentParser(description="scylla find")
        parser.add_argument("-s", "--size", action="store", choices=['b', 'h', 'w', 'g', '8', '16', '32', '64'],
                default='g',
                help="Size of the searched value."
                    " Accepted values are the size expressed in number of bits: 8, 16, 32 and 64."
                    " GDB's size classes are also accepted: b(byte), h(half-word), w(word) and g(giant-word)."
                    " Defaults to g (64 bits).")
        parser.add_argument("-r", "--resolve", action="store_true",
                help="Attempt to resolve the first pointer in the found objects as vtable pointer. "
                " If the resolve is successful the vtable pointer as well as the vtable symbol name will be printed in the listing.")
        parser.add_argument("--value-range", action="store", type=int, default=0,
                help="Find a range of values of the specified size."
                " Will start from VALUE, then use SIZE increments until VALUE + VALUE_RANGE is reached, or at least one usage is found."
                " By default only VALUE is searched.")
        parser.add_argument("-a", "--find-all", action="store_true",
                help="Find all references, don't stop at the first offset which has usages. See --value-range.")
        parser.add_argument("-f", "--include-free", action="store_true", help="Include freed object in the result.")
        parser.add_argument("value", action="store", help="The value to be searched.")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        size_arg_to_size_char = {
            'b': 'b',
            '8': 'b',
            'h': 'h',
            '16': 'h',
            'w': 'w',
            '32': 'w',
            'g': 'g',
            '64': 'g',
        }

        size_char = size_arg_to_size_char[args.size]

        for ptr_meta, offset in scylla_find.find(int(gdb.parse_and_eval(args.value)), size_char, args.value_range, find_all=args.find_all,
                only_live=(not args.include_free)):
            if args.value_range and offset:
                formatted_offset = "+{}; ".format(offset)
            else:
                formatted_offset = ""
            if args.resolve:
                maybe_vptr = int(gdb.Value(ptr_meta.obj_ptr).reinterpret_cast(scylla_find._vptr_type).dereference())
                symbol = resolve(maybe_vptr, cache=False)
                if symbol is None:
                    gdb.write('{}{}\n'.format(formatted_offset, ptr_meta))
                else:
                    gdb.write('{}{} 0x{:016x} {}\n'.format(formatted_offset, ptr_meta, maybe_vptr, symbol))
            else:
                gdb.write('{}{}\n'.format(formatted_offset, ptr_meta))


class std_unique_ptr:
    def __init__(self, obj):
        self.obj = obj

    def get(self):
        try:
            std_tuple(self.obj['_M_t']['_M_t'])[0].dereference()
            return std_tuple(self.obj['_M_t']['_M_t'])[0]
        except:
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


def ip_to_str(val, byteorder):
    return '%d.%d.%d.%d' % (struct.unpack('BBBB', val.to_bytes(4, byteorder=byteorder))[::-1])


def get_ip(ep):
    try:
        return ep['_addr']['ip']['raw']
    except gdb.error:
        return ep['_addr']['_in']['s_addr']


class scylla_netw(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla netw', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        mss = sharded(gdb.parse_and_eval('debug::the_messaging_service'))
        if not mss.instances:
            gdb.write('debug::the_messaging_service does not exist (yet?)\n')
            return

        ms = mss.local()
        gdb.write('Dropped messages: %s\n' % ms['_dropped_messages'])
        gdb.write('Outgoing connections:\n')
        idx = 0
        for clients in std_vector(ms['_clients']):
            for (addr, shard_info) in unordered_map(clients):
                ip = ip_to_str(int(get_ip(addr['addr'])), byteorder='big')
                client = shard_info['rpc_client']['_p']
                rpc_client = std_unique_ptr(client['_p'])
                gdb.write('[%d] IP: %s, (netw::messaging_service::rpc_protocol_client_wrapper*) %s:\n' % (idx, ip, client))
                gdb.write('  stats: %s\n' % rpc_client['_stats'])
                gdb.write('  outstanding: %d\n' % int(rpc_client['_outstanding']['_M_h']['_M_element_count']))
            idx += 1

        servers = [
            std_unique_ptr(ms['_server']['_M_elems'][0]),
            std_unique_ptr(ms['_server']['_M_elems'][1]),
            std_unique_ptr(ms['_server_tls']['_M_elems'][0]),
            std_unique_ptr(ms['_server_tls']['_M_elems'][1]),
        ]
        for srv in servers:
            if srv:
                gdb.write('Server: resources=%s\n' % srv['_resources_available'])
                gdb.write('Incoming connections:\n')
                for k, clnt in unordered_map(srv['_conns']):
                    conn = clnt['_p'].cast(clnt.type.template_argument(0).pointer())
                    ip = ip_to_str(int(conn['_info']['addr']['u']['in']['sin_addr']['s_addr']), byteorder='big')
                    port = int(conn['_info']['addr']['u']['in']['sin_port'])
                    gdb.write('%s:%d: \n' % (ip, port))
                    gdb.write('   %s\n' % (conn['_stats']))


def get_tagged_integer_type(i):
    try:
        return i['_value']
    except gdb.error:
        return i


def get_gms_generation_or_version(i):
    return get_tagged_integer_type(i)


def get_gms_versioned_value(vv):
    try:
        return {
            'version': get_gms_generation_or_version(vv['_version']),
            'value': vv['_value'],
        }
    except gdb.error:
        return {
            'version': vv['version'],
            'value': vv['value'],
        }


class scylla_gms(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, 'scylla gms', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        try:
            gossiper = sharded(gdb.parse_and_eval('debug::the_gossiper')).local()
            state_map = gossiper['_endpoint_state_map']
        except Exception as e:
            gossiper = sharded(gdb.parse_and_eval('gms::_the_gossiper')).local()
            state_map = gossiper['endpoint_state_map']
        for (endpoint, state) in unordered_map(state_map):
            try:
                state_ptr = seastar_lw_shared_ptr(state)
                state = state_ptr.get().dereference()
            except Exception:
                pass
            ip = ip_to_str(int(get_ip(endpoint)), byteorder=sys.byteorder)
            gdb.write('%s: (gms::endpoint_state*) %s (%s)\n' % (ip, state.address, state['_heart_beat_state']))
            try:
                app_states_map = std_unordered_map(state['_application_state'])
            except:
                app_states_map = std_map(state['_application_state'])
            for app_state, vv in app_states_map:
                value = get_gms_versioned_value(vv)
                gdb.write('  %s: {version=%d, value=%s}\n' % (app_state, value['version'], value['value']))


class scylla_cache(gdb.Command):
    """Prints contents of the cache on current shard"""

    def __init__(self):
        gdb.Command.__init__(self, 'scylla cache', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def __partitions(self, table):
        try:
            return double_decker(table['_cache']['_partitions'])
        except gdb.error:
            # Compatibility, the row-cache was switched to B+ tree at some point
            return intrusive_set(table['_cache']['_partitions'])

    def invoke(self, arg, from_tty):
        schema_ptr_type = gdb.lookup_type('schema').pointer()
        for table in for_each_table():
            schema = table['_schema']['_p'].reinterpret_cast(schema_ptr_type)
            name = '%s.%s' % (schema['_raw']['_ks_name'], schema['_raw']['_cf_name'])
            gdb.write("%s:\n" % (name))
            for e in self.__partitions(table):
                gdb.write('  (cache_entry*) 0x%x {_key=%s, _flags=%s, _pe=%s}\n' % (
                    int(e.address), e['_key'], e['_flags'], e['_pe']))
            gdb.write("\n")


def find_sstables_attached_to_tables():
    for table in for_each_table():
        for sst_ptr in std_unordered_set(seastar_lw_shared_ptr(seastar_lw_shared_ptr(table['_sstables']).get()['_all']).get().dereference()):
            yield seastar_lw_shared_ptr(sst_ptr).get()


def find_sstables():
    """A generator which yields pointers to all live sstable objects on current shard."""
    try:
        db = find_db(current_shard())
        user_sstables_manager = std_unique_ptr(db["_user_sstables_manager"]).get()
        system_sstables_manager = std_unique_ptr(db["_system_sstables_manager"]).get()
        for manager in (user_sstables_manager, system_sstables_manager):
            for sst_list_name in ("_active", "_undergoing_close"):
                for sst in intrusive_list(manager[sst_list_name], link="_manager_list_link"):
                    yield sst.address
    except gdb.error:
        # Scylla Enterprise 2020.1 compatibility
        for sst in intrusive_list(gdb.parse_and_eval('sstables::tracker._sstables'), link='_tracker_link'):
            yield sst.address


class scylla_sstables(gdb.Command):
    """Lists all sstable objects on currents shard together with useful information like on-disk and in-memory size."""

    def __init__(self):
        gdb.Command.__init__(self, 'scylla sstables', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    @staticmethod
    def filename(sst):
        """The name of the sstable.

        Should mirror `sstables::sstable::component_basename()`.
        """
        old_format = '{keyspace}-{table}-{version}-{generation}-Data.db'
        new_format = '{version}-{generation}-{format}-Data.db'
        version_to_format = {
            'ka': old_format,
            'la': new_format,
            'mc': new_format,
            'md': new_format,
            'me': new_format
        }
        format_to_str = ['big']
        schema = schema_ptr(sst['_schema'])
        int_type = gdb.lookup_type('int')
        version_number = int(sst['_version'])
        version_name = list(version_to_format)[version_number]
        generation = sst['_generation']
        return version_to_format[version_name].format(
                keyspace=str(schema.ks_name)[1:-1],
                table=str(schema.cf_name)[1:-1],
                version=version_name,
                generation=generation,
                format=format_to_str[int(sst['_format'].cast(int_type))],
            )

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="scylla sstables")
        parser.add_argument("-t", "--tables", action="store_true", help="Only consider sstables attached to tables")
        parser.add_argument("--histogram", action="store_true", help="Instead of printing all sstables, print a histogram of the number of sstables per table")
        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        filter_type = gdb.lookup_type('utils::filter::murmur3_bloom_filter')
        cpu_id = current_shard()
        total_size = 0 # in memory
        total_on_disk_size = 0
        count = 0

        sstable_generator = find_sstables_attached_to_tables if args.tables else find_sstables
        sstable_histogram = histogram(print_indicators=False)

        for sst in sstable_generator():
            try:
                is_open = sst['_open']
            except gdb.error:
                is_open = bool(std_optional(sst['_open_mode']))
            if not is_open:
                continue

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
                for tag, value in unordered_map(sm.get()['data']['data']):
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
            schema = schema_ptr(sst['_schema'])
            if args.histogram:
                sstable_histogram.add(schema.table_name())
            else:
                gdb.write('(sstables::sstable*) 0x%x: local=%d data_file=%d, in_memory=%d (bf=%d, summary=%d, sm=%d) %s filename=%s\n'
                          % (int(sst), local, data_file_size, size, bf_size, summary_size, sm_size, schema.table_name(), scylla_sstables.filename(sst)))

            if local:
                total_size += size
                total_on_disk_size += data_file_size

        if args.histogram:
           sstable_histogram.print_to_console()

        gdb.write('total (shard-local): count=%d, data_file=%d, in_memory=%d\n' % (count, total_on_disk_size, total_size))


class scylla_memtables(gdb.Command):
    """Lists basic information about all memtable objects on current shard."""

    def __init__(self):
        gdb.Command.__init__(self, 'scylla memtables', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    @staticmethod
    def dump_memtable_list(memtable_list):
        region_ptr_type = gdb.lookup_type('logalloc::region').pointer()
        for mt_ptr in std_vector(memtable_list['_memtables']):
            mt = seastar_lw_shared_ptr(mt_ptr).get()
            reg = lsa_region(mt.cast(region_ptr_type))
            gdb.write('  (memtable*) 0x%x: total=%d, used=%d, free=%d, flushed=%d\n' % (mt, reg.total(), reg.used(), reg.free(), mt['_flushed_memory']))

    @staticmethod
    def dump_compaction_group_memtables(compaction_group):
        scylla_memtables.dump_memtable_list(seastar_lw_shared_ptr(compaction_group['_memtables']).get())

    def invoke(self, arg, from_tty):
        for table in for_each_table():
            gdb.write('table %s:\n' % schema_ptr(table['_schema']).table_name())
            try:
                sg_manager = std_unique_ptr(table["_sg_manager"]).get().dereference()
                for (sg_id, sg_ptr) in absl_container(sg_manager["_storage_groups"]):
                    sg = seastar_lw_shared_ptr(sg_ptr).get()
                    scylla_memtables.dump_compaction_group_memtables(seastar_lw_shared_ptr(sg["_main_cg"]).get())
                    for cg_ptr in std_vector(sg["_split_ready_groups"]):
                        scylla_memtables.dump_compaction_group_memtables(seastar_lw_shared_ptr(cg_ptr).get())
                return
            except gdb.error:
                pass

            try:
                sg_manager = std_unique_ptr(table["_sg_manager"]).get().dereference()
                for cg in intrusive_list(sg_manager["_compaction_groups"], link='_list_hook'):
                    scylla_memtables.dump_compaction_group_memtables(cg)
                return
            except gdb.error:
                pass

            try:
                for cg in intrusive_list(table["_compaction_groups"], link='_list_hook'):
                    scylla_memtables.dump_compaction_group_memtables(cg)
                return
            except gdb.error:
                pass

            try:
                for cg_ptr in chunked_vector(table["_compaction_groups"]):
                    scylla_memtables.dump_compaction_group_memtables(std_unique_ptr(cg_ptr).get())
                return
            except gdb.error:
                pass

            try:
                for cg_ptr in std_vector(table["_compaction_groups"]):
                    scylla_memtables.dump_compaction_group_memtables(std_unique_ptr(cg_ptr).get())
                return
            except gdb.error:
                pass

            try:
                scylla_memtables.dump_compaction_group_memtables(std_unique_ptr(table["_compaction_group"]).get())
            except gdb.error:
                scylla_memtables.dump_memtable_list(seastar_lw_shared_ptr(table['_memtables']).get()) # Scylla 5.1 compatibility

def escape_html(s):
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


class scylla_generate_object_graph(gdb.Command):
    """Generate an object graph for an object.

    The object graph is a directed graph, where vertices are objects and edges
    are references between them, going from referrers to the referee. The
    vertices contain information, like the address of the object, its size,
    whether it is a live or not and if applies, the address and symbol name of
    its vtable. The edges contain the list of offsets the referrer has references
    at. The generated graph is an image, which allows the visual inspection of the
    object graph.

    The graph is generated with the help of `graphviz`. The command
    generates `.dot` files which can be converted to images with the help of
    the `dot` utility. The command can do this if the output file is one of
    the supported image formats (e.g. `png`), otherwise only the `.dot` file
    is generated, leaving the actual image generation to the user. When that is
    the case, the generated `.dot` file can be converted to an image with the
    following command:

        dot -Tpng graph.dot -o graph.png

    The `.dot` file is always generated, regardless of the specified output. This
    file will contain the full name of vtable symbols. The graph will only contain
    cropped versions of those to keep the size reasonable.

    See `scylla generate_object_graph --help` for more details on usage.
    Also see `man dot` for more information on supported output formats.

    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla generate-object-graph', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    @staticmethod
    def _traverse_object_graph_breadth_first(address, max_depth, max_vertices, timeout_seconds, value_range_override):
        vertices = dict() # addr -> obj info (ptr metadata, vtable symbol)
        edges = defaultdict(set) # (referrer, referee) -> {(prev_offset1, next_offset1), (prev_offset2, next_offset2), ...}

        vptr_type = gdb.lookup_type('uintptr_t').pointer()

        vertices[address] = (scylla_ptr.analyze(address), resolve(gdb.Value(address).reinterpret_cast(vptr_type).dereference(), cache=False))

        current_objects = [address]
        next_objects = []
        depth = 0
        start_time = time.time()
        stop = False

        while not stop:
            depth += 1
            for current_obj in current_objects:
                if value_range_override == -1:
                    value_range = vertices[current_obj][0].size
                else:
                    value_range = value_range_override
                for ptr_meta, to_off in scylla_find.find(current_obj, value_range=value_range, find_all=True):
                    if timeout_seconds > 0:
                        current_time = time.time()
                        if current_time - start_time > timeout_seconds:
                            stop = True
                            break

                    next_obj, next_off = ptr_meta.ptr, ptr_meta.offset_in_object
                    next_obj -= next_off
                    edges[(next_obj, current_obj)].add((next_off, to_off))
                    if next_obj in vertices:
                        continue

                    symbol_name = resolve(gdb.Value(next_obj).reinterpret_cast(vptr_type).dereference(), cache=False)
                    vertices[next_obj] = (ptr_meta, symbol_name)

                    next_objects.append(next_obj)

                    if max_vertices > 0 and len(vertices) >= max_vertices:
                        stop = True
                        break

            if max_depth > 0 and depth == max_depth:
                stop = True
                break

            current_objects = next_objects
            next_objects = []

        return edges, vertices

    @staticmethod
    def _do_generate_object_graph(address, output_file, max_depth, max_vertices, timeout_seconds, value_range_override):
        edges, vertices = scylla_generate_object_graph._traverse_object_graph_breadth_first(address, max_depth,
                max_vertices, timeout_seconds, value_range_override)

        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        prefix_len = len('vtable for ')

        for addr, obj_info in vertices.items():
            ptr_meta, vtable_symbol_name = obj_info
            size = ptr_meta.size
            state = "L" if ptr_meta.is_live else "F"

            addr_str = "<b>0x{:x}</b>".format(addr) if addr == address else "0x{:x}".format(addr)

            if vtable_symbol_name:
                symbol_name = vtable_symbol_name[prefix_len:] if len(vtable_symbol_name) > prefix_len else vtable_symbol_name
                output_file.write('{} [label=<{} ({}, {})<br/>{}>]; // {}\n'.format(addr, addr_str, size, state,
                    escape_html(symbol_name[:32]), vtable_symbol_name))
            else:
                output_file.write('{} [label=<{} ({}, {})>];\n'.format(addr, addr_str, size, state, ptr_meta))

        for edge, offsets in edges.items():
            a, b = edge
            output_file.write('{} -> {} [label="{}"];\n'.format(a, b, offsets))

    @staticmethod
    def generate_object_graph(address, output_file, max_depth, max_vertices, timeout_seconds, value_range_override):
        with open(output_file, 'w') as f:
            f.write('digraph G {\n')
            scylla_generate_object_graph._do_generate_object_graph(address, f, max_depth, max_vertices, timeout_seconds, value_range_override)
            f.write('}')

    def invoke(self, arg, from_tty):
        parser = argparse.ArgumentParser(description="scylla generate-object-graph")
        parser.add_argument("-o", "--output-file", action="store", type=str, default="graph.dot",
                help="Output file. Supported extensions are: dot, png, jpg, jpeg, svg and pdf."
                " Regardless of the extension, a `.dot` file will always be generated."
                " If the output is one of the graphic formats the command will convert the `.dot` file using the `dot` utility."
                " In this case the dot utility from the graphviz suite has to be installed on the machine."
                " To manually convert the `.dot` file do: `dot -Tpng graph.dot -o graph.png`.")
        parser.add_argument("-d", "--max-depth", action="store", type=int, default=5,
                help="Maximum depth to traverse the object graph. Set to -1 for unlimited depth. Default is 5.")
        parser.add_argument("-v", "--max-vertices", action="store", type=int, default=-1,
                help="Maximum amount of vertices (objects) to add to the object graph. Set to -1 to unlimited. Default is -1 (unlimited).")
        parser.add_argument("-t", "--timeout", action="store", type=int, default=-1,
                help="Maximum amount of seconds to spend building the graph. Set to -1 for no timeout. Default is -1 (unlimited).")
        parser.add_argument("-r", "--value-range-override", action="store", type=int, default=-1,
                help="The portion of the object to find references to. Same as --value-range for `scylla find`."
                " This can greatly speed up the graph generation when the graph has large objects but references are likely to point to their first X bytes."
                " Default value is -1, meaning the entire object is scanned (--value-range=sizeof(object)).")
        parser.add_argument("object", action="store", help="The object that is the starting point of the graph.")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        supported_extensions = {'dot', 'png', 'jpg', 'jpeg', 'svg', 'pdf'}
        head, tail = os.path.split(args.output_file)
        filename, extension = tail.split('.')

        if not extension in supported_extensions:
            raise ValueError("The output file `{}' has unsupported extension `{}'. Supported extensions are: {}".format(
                args.output_file, extension, supported_extensions))

        if extension != 'dot':
            dot_file = os.path.join(head, filename + '.dot')
        else:
            dot_file = args.output_file

        if args.max_depth == -1 and args.max_vertices == -1 and args.timeout == -1:
            raise ValueError("The search has to be limited by at least one of: MAX_DEPTH, MAX_VERTICES or TIMEOUT")

        scylla_generate_object_graph.generate_object_graph(int(gdb.parse_and_eval(args.object)), dot_file,
                args.max_depth, args.max_vertices, args.timeout, args.value_range_override)

        if extension != 'dot':
            subprocess.check_call(['dot', '-T' + extension, dot_file, '-o', args.output_file])


class scylla_smp_queues(gdb.Command):
    """Summarize the shard's outgoing smp queues.

    The summary takes the form of a histogram. Example:

    (gdb) scylla smp-queues
        10747 17 ->  3 ++++++++++++++++++++++++++++++++++++++++
          721 17 -> 19 ++
          247 17 -> 20 +
          233 17 -> 10 +
          210 17 -> 14 +
          205 17 ->  4 +
          204 17 ->  5 +
          198 17 -> 16 +
          197 17 ->  6 +
          189 17 -> 11 +
          181 17 ->  1 +
          179 17 -> 13 +
          176 17 ->  2 +
          173 17 ->  0 +
          163 17 ->  8 +
        1 17 ->  9 +

    Each line has the following format

        count from -> to ++++

    Where:
        count: the number of items in the queue;
        from: the shard, from which the message was sent (this shard);
        to: the shard, to which the message is sent;
        ++++: visual illustration of the relative size of this queue;

    See `scylla smp-queues --help` for more details on usage.
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla smp-queues', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
        self.queues = set()

    def _init(self):
        qs = gdb.parse_and_eval('seastar::smp::_qs')
        if qs.type.code != gdb.TYPE_CODE_PTR:
            # older Seastar use std::unique_ptr for this variable
            qs = std_unique_ptr(qs).get()
        for i in range(cpus()):
            for j in range(cpus()):
                self.queues.add(qs[i][j])
        self._queue_type = gdb.lookup_type('seastar::smp_message_queue').pointer()
        self._ptr_type = gdb.lookup_type('uintptr_t').pointer()
        self._unsigned_type = gdb.lookup_type('unsigned')
        self._queue_size = 128
        self._item_ptr_array_type = gdb.lookup_type('seastar::smp_message_queue::work_item').pointer().pointer()

    def invoke(self, arg, from_tty):
        if not self.queues:
            self._init()

        parser = argparse.ArgumentParser(description="scylla smp-queues")
        parser.add_argument("-f", "--from", action="store", type=int, required=False, dest="from_cpu",
                help="Filter for queues going from the given CPU")
        parser.add_argument("-t", "--to", action="store", type=int, required=False, dest="to_cpu",
                help="Filter for queues going to the given CPU")
        parser.add_argument("-g", "--scheduling-group", action="store", type=str, required=False,
                help="Filter for work items belonging to the specified scheduling group (by name or id)")
        parser.add_argument("-c", "--content", action="store_true",
                help="Create a histogram from the content of the queues, rather than the number of items in them")

        try:
            args = parser.parse_args(arg.split())
        except SystemExit:
            return

        sg_id = None
        if args.scheduling_group:
            for tq in get_local_task_queues():
                id_str = "{}".format(tq['_id'].cast(self._unsigned_type))
                name_str = str(tq['_name']).replace('"', '')
                if args.scheduling_group == id_str or args.scheduling_group == name_str:
                    sg_id = int(tq['_id'])

            if sg_id is None:
                gdb.write("error: non-existent scheduling-group provided for filtering: `{}', run `scylla task-queues` for a list of valid scheduling group names and ids\n".format(args.scheduling_group))
                return

        def formatter(q):
            if args.content:
                return "0x{:x} {}".format(q, resolve(q))
            else:
                return '{:2} -> {:2}'.format(*q)

        h = histogram(formatter=formatter, print_indicators=not args.content)
        empty_queues = 0

        def add_to_histogram(a, b, key=None, count=1):
            if sg_id is not None and int(key.dereference()['_sg']['_id']) != sg_id:
                return

            if args.content:
                vptr = int(gdb.Value(key).reinterpret_cast(self._ptr_type).dereference())
                h[vptr] += count
            else:
                h[(a, b)] += count

        for q in self.queues:
            a = int(q['_completed']['remote']['_id'])
            b = int(q['_pending']['remote']['_id'])
            if args.from_cpu is not None and a != args.from_cpu:
                continue
            if args.to_cpu is not None and b != args.to_cpu:
                continue

            tx_queue = std_deque(q['_tx']['a']['pending_fifo'])
            pending_queue = q['_pending']
            pending_start = int(pending_queue['read_index_']['_M_i'])
            pending_end = int(pending_queue['write_index_']['_M_i'])
            if pending_end < pending_start:
                pending_end += self._queue_size

            if args.content or args.scheduling_group:
                for item_ptr in tx_queue:
                    add_to_histogram(a, b, key=item_ptr)

                # Boost uses an aligned (to 8 bytes) buffer here.
                # We'll just assume alignment is right.
                buf = gdb.Value(pending_queue['storage_']['data_']['buf']).reinterpret_cast(self._item_ptr_array_type)

                for i in range(pending_start, pending_end):
                    add_to_histogram(a, b, key=buf[i % self._queue_size])
            else:
                count = len(tx_queue) + pending_end - pending_start
                if count != 0:
                    add_to_histogram(a, b, count=count)
                else:
                    empty_queues += 1

        gdb.write('{}\n'.format(h))
        if empty_queues > 0:
            gdb.write('omitted {} empty queues\n'.format(empty_queues))


class scylla_small_objects(gdb.Command):
    """List live objects from one of the seastar allocator's small pools

    The pool is selected with the `-o|--object-size` flag. Results are paginated by
    default as there can be millions of objects. Default page size is 20.
    To list a certain page, use the `-p|--page` flag. To find out the number of
    total objects and pages, use `--summarize`.
    To sample random pages, use `--random-page`.

    If objects have a vtable, its type is resolved and this will appear in the
    listing.

    Note that to reach a certain page, the command has to traverse the memory
    spans belonging to the pool linearly, until the desired range of object is
    found. This can take a long time for well populated pools. To speed this
    up, the span iterator is saved and reused when possible. This caching can
    only be exploited within the same pool and only with monotonically
    increasing pages.

    For usage see: scylla small-objects --help

    Examples:

    (gdb) scylla small-objects -o 32 --summarize
    number of objects: 60196912
    page size        : 20
    number of pages  : 3009845

    (gdb) scylla small-objects -o 32 -p 100
    page 100: 2000-2019
    [2000] 0x635002ecba00
    [2001] 0x635002ecba20
    [2002] 0x635002ecba40
    [2003] 0x635002ecba60
    [2004] 0x635002ecba80
    [2005] 0x635002ecbaa0
    [2006] 0x635002ecbac0
    [2007] 0x635002ecbae0
    [2008] 0x635002ecbb00
    [2009] 0x635002ecbb20
    [2010] 0x635002ecbb40
    [2011] 0x635002ecbb60
    [2012] 0x635002ecbb80
    [2013] 0x635002ecbba0
    [2014] 0x635002ecbbc0
    [2015] 0x635002ecbbe0
    [2016] 0x635002ecbc00
    [2017] 0x635002ecbc20
    [2018] 0x635002ecbc40
    [2019] 0x635002ecbc60
    """
    class small_object_iterator():
        def __init__(self, small_pool, resolve_symbols):
            self._small_pool = small_pool
            self._resolve_symbols = resolve_symbols

            self._text_ranges = get_text_ranges()
            self._vptr_type = gdb.lookup_type('uintptr_t').pointer()
            self._free_object_ptr = gdb.lookup_type('void').pointer().pointer()
            self._page_size = int(gdb.parse_and_eval('\'seastar::memory::page_size\''))
            self._free_in_pool = set()
            self._free_in_span = set()

            pool_next_free = self._small_pool['_free']
            while pool_next_free:
                self._free_in_pool.add(int(pool_next_free))
                pool_next_free = pool_next_free.reinterpret_cast(self._free_object_ptr).dereference()

            self._span_it = iter(spans())
            self._obj_it = iter([]) # initialize to exhausted iterator

        def _next_span(self):
            # Let any StopIteration bubble up, as it signals we are done with
            # all spans.
            span = next(self._span_it)
            while span.pool() != self._small_pool.address:
                span = next(self._span_it)

            self._free_in_span = set()
            span_start = int(span.start)
            span_end = int(span_start + span.size() * self._page_size)

            # span's free list
            span_next_free = span.page['freelist']
            while span_next_free:
                self._free_in_span.add(int(span_next_free))
                span_next_free = span_next_free.reinterpret_cast(self._free_object_ptr).dereference()

            return span_start, span_end

        def _next_obj(self):
            try:
                return next(self._obj_it)
            except StopIteration:
                # Don't call self._next_span() here as it might throw another StopIteration.
                pass

            span_start, span_end = self._next_span()
            self._obj_it = iter(range(span_start, span_end, int(self._small_pool['_object_size'])))
            return next(self._obj_it)

        def __next__(self):
            obj = self._next_obj()
            while obj in self._free_in_span or obj in self._free_in_pool:
                obj = self._next_obj()

            if self._resolve_symbols:
                addr = gdb.Value(obj).reinterpret_cast(self._vptr_type).dereference()
                if addr_in_ranges(self._text_ranges, addr):
                    return (obj, resolve(addr))
                else:
                    return (obj, None)
            else:
                return (obj, None)

        def __iter__(self):
            return self

    def __init__(self):
        gdb.Command.__init__(self, 'scylla small-objects', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

        self._parser = None
        self._iterator = None
        self._last_pos = 0
        self._last_object_size = None

    @staticmethod
    def get_object_sizes():
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        small_pools = cpu_mem['small_pools']
        nr = int(small_pools['nr_small_pools'])
        return [int(small_pools['_u']['a'][i]['_object_size']) for i in range(nr)]

    @staticmethod
    def find_small_pool(object_size):
        cpu_mem = gdb.parse_and_eval('\'seastar::memory::cpu_mem\'')
        small_pools = cpu_mem['small_pools']
        nr = int(small_pools['nr_small_pools'])
        for i in range(nr):
            sp = small_pools['_u']['a'][i]
            if object_size == int(sp['_object_size']):
                return sp

        return None

    def init_parser(self):
        parser = argparse.ArgumentParser(description="scylla small-objects")
        parser.add_argument("-o", "--object-size", action="store", type=int, required=True,
                help="Object size, valid sizes are: {}".format(scylla_small_objects.get_object_sizes()))
        parser.add_argument("-p", "--page", action="store", type=int, default=0, help="Page to show.")
        parser.add_argument("-s", "--page-size", action="store", type=int, default=20,
                help="Number of objects in a page. A page size of 0 turns off paging.")
        parser.add_argument("--random-page", action="store_true", help="Show a random page.")
        parser.add_argument("--summarize", action="store_true",
                help="Print the number of objects and pages in the pool.")
        parser.add_argument("--verbose", action="store_true",
                help="Print additional details on what is going on.")

        self._parser = parser

    def get_objects(self, small_pool, offset=0, count=0, resolve_symbols=False, verbose=False):
        if self._last_object_size != int(small_pool['_object_size']) or offset < self._last_pos:
            self._last_pos = 0
            self._iterator = scylla_small_objects.small_object_iterator(small_pool, resolve_symbols)

        skip = offset - self._last_pos
        if verbose:
            gdb.write('get_objects(): offset={}, count={}, last_pos={}, skip={}\n'.format(offset, count, self._last_pos, skip))

        for _ in range(skip):
            next(self._iterator)

        if count:
            objects = []
            for _ in range(count):
                objects.append(next(self._iterator))
        else:
            objects = list(self._iterator)

        self._last_pos += skip
        self._last_pos += len(objects)

        return objects

    def invoke(self, arg, from_tty):
        if self._parser is None:
            self.init_parser()

        try:
            args = self._parser.parse_args(arg.split())
        except SystemExit:
            return

        small_pool = scylla_small_objects.find_small_pool(args.object_size)
        if small_pool is None:
            raise ValueError("{} is not a valid object size for any small pools, valid object sizes are: {}", scylla_small_objects.get_object_sizes())

        if args.summarize:
            if self._last_object_size != args.object_size:
                if args.verbose:
                    gdb.write("Object size changed ({} -> {}), scanning pool.\n".format(self._last_object_size, args.object_size))
                self._num_objects = len(self.get_objects(small_pool, verbose=args.verbose))
                self._last_object_size = args.object_size
            gdb.write("number of objects: {}\n"
                      "page size        : {}\n"
                      "number of pages  : {}\n"
                .format(
                    self._num_objects,
                    args.page_size,
                    int(self._num_objects / args.page_size)))
            return

        if args.random_page:
            if self._last_object_size != args.object_size:
                if args.verbose:
                    gdb.write("Object size changed ({} -> {}), scanning pool.\n".format(self._last_object_size, args.object_size))
                self._num_objects = len(self.get_objects(small_pool, verbose=args.verbose))
                self._last_object_size = args.object_size
            page = random.randint(0, int(self._num_objects / args.page_size) - 1)
        else:
            page = args.page

        offset = page * args.page_size
        gdb.write("page {}: {}-{}\n".format(page, offset, offset + args.page_size - 1))
        for i, (obj, sym) in enumerate(self.get_objects(small_pool, offset, args.page_size, resolve_symbols=True, verbose=args.verbose)):
            if sym is None:
                sym_text = ""
            else:
                sym_text = sym
            gdb.write("[{}] 0x{:x} {}\n".format(offset + i, obj, sym_text))


class scylla_large_objects(gdb.Command):
    """List live objects from one of the seastar allocator's large pools

    The pool is selected with the `-o|--object-size` flag. Results are paginated by
    default as there can be thousands of objects. Default page size is 20.
    To list a certain page, use the `-p|--page` flag. To find out the number of
    total objects and pages, use `--summarize`.
    To sample random pages, use `--random-page`.

    For usage see: scylla large-objects --help

    Examples:

    (gdb) scylla large-objects -o 32768 --summarize
    number of objects: 9737
    page size        : 20
    number of pages  : 487

    (gdb) scylla large-objects -o 131072 --random-page
    [40] 0x60001cb40000
    [41] 0x60001cb60000
    [42] 0x60001cb80000
    [43] 0x60001cba0000
    [44] 0x60001cbc0000
    [45] 0x60001cbe0000
    [46] 0x60001cc00000
    [47] 0x60001cc20000
    [48] 0x60001cc40000
    [49] 0x60001cc60000
    [50] 0x60001cc80000
    [51] 0x60001cca0000
    [52] 0x60001ccc0000
    [53] 0x60001cce0000
    [54] 0x60001cd00000
    [55] 0x60001cd20000
    [56] 0x60001cd40000
    [57] 0x60001cd60000
    [58] 0x60001cd80000
    [59] 0x60001cda0000
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla large-objects', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)
        self._parser = None
        self._objects = None
        self._object_size = None

    def init_parser(self):
        parser = argparse.ArgumentParser(description="scylla large-objects")
        parser.add_argument("-o", "--object-size", action="store", type=int, required=True,
                            help="Object size, valid sizes are powers of two, above {}".format(int(gdb.parse_and_eval("seastar::memory::max_small_allocation"))))
        parser.add_argument("-p", "--page", action="store", type=int, default=0, help="Page to show.")
        parser.add_argument("-s", "--page-size", action="store", type=int, default=20,
                help="Number of objects in a page. A page size of 0 turns off paging.")
        parser.add_argument("--random-page", action="store_true", help="Show a random page.")
        parser.add_argument("--summarize", action="store_true",
                help="Print the number of objects and pages in the pool.")
        parser.add_argument("--verbose", action="store_true",
                help="Print additional details on what is going on.")

        self._parser = parser

    def invoke(self, arg, from_tty):
        if self._parser is None:
            self.init_parser()

        try:
            args = self._parser.parse_args(arg.split())
        except SystemExit:
            return

        object_size = int(args.object_size / int(gdb.parse_and_eval('\'seastar::memory::page_size\'')))

        if self._objects is None or object_size != self._object_size:
            if args.verbose:
                gdb.write(f"Object size changed ({self._object_size} pages -> {object_size} pages), scanning spans.\n")
            # We materialize all relevant spans. There aren't usually that many
            # of them. Rewrite this to be lazy if this proves problematic with
            # certain cores.
            self._objects = [s for s in spans() if s.is_large() and not s.is_free() and s.size() == object_size]

        self._object_size = object_size

        if args.summarize:
            gdb.write("number of objects: {}\n"
                      "page size        : {}\n"
                      "number of pages  : {}\n"
                      .format(
                          len(self._objects),
                          args.page_size,
                          math.ceil(len(self._objects) / args.page_size)))
            return

        if args.random_page:
            page = random.randint(0, int(len(self._objects) / args.page_size) - 1)
        else:
            page = args.page

        if page >= len(self._objects)/args.page_size:
            return

        start = page * args.page_size
        end = start + args.page_size
        for index, span in enumerate(self._objects[start:end]):
            gdb.write("[{}] 0x{:x}\n".format(start + index, span.start))


class scylla_compaction_tasks(gdb.Command):
    """Summarize the compaction::compaction_task_executor instances.

    The summary is created based on compaction_manager::_tasks and it takes the
    form of a histogram with the compaction type and compaction running and
    table name as keys. Example:

    (gdb) scylla compaction-task
         2116 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table_postimage_scylla_cdc_log"
          769 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table_scylla_cdc_log"
          750 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table_preimage_postimage_scylla_cdc_log"
          731 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table_preimage_scylla_cdc_log"
          293 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table"
          286 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table_preimage"
          230 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table_postimage"
           58 type=sstables::compaction_type::Compaction, running=false, "cdc_test"."test_table_preimage_postimage"
        4 type=sstables::compaction_type::Compaction, running=true , "cdc_test"."test_table_postimage_scylla_cdc_log"
        2 type=sstables::compaction_type::Compaction, running=true , "cdc_test"."test_table"
        2 type=sstables::compaction_type::Compaction, running=true , "cdc_test"."test_table_preimage_postimage_scylla_cdc_log"
        2 type=sstables::compaction_type::Compaction, running=true , "cdc_test"."test_table_preimage"
        1 type=sstables::compaction_type::Compaction, running=true , "cdc_test"."test_table_preimage_postimage"
        1 type=sstables::compaction_type::Compaction, running=true , "cdc_test"."test_table_scylla_cdc_log"
        1 type=sstables::compaction_type::Compaction, running=true , "cdc_test"."test_table_preimage_scylla_cdc_log"
    Total: 5246 instances of compaction::compaction_task_executor
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla compaction-tasks', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, arg, from_tty):
        db = find_db()
        cm = db['_compaction_manager']
        try:
            aux = std_unique_ptr(db['_compaction_manager']).get().dereference()
            cm = aux
        except:
            pass
        task_hist = histogram(print_indicators=False)

        task_list = list(std_list(cm['_tasks']))
        for task in task_list:
            try:
                task = seastar_shared_ptr(task).get().dereference()
            except:
                task = seastar_lw_shared_ptr(task).get().dereference() # Scylla 5.0 compatibility

            try:
                schema = schema_ptr(task['_compacting_table'].dereference()['_schema'])
            except:
                try:
                    schema = schema_ptr(task['compacting_table'].dereference()['_schema']) # Scylla 5.0 compatibility
                except:
                    schema = schema_ptr(task['compacting_cf'].dereference()['_schema']) # Scylla 4.6 compatibility

            try:
                key = 'type={}, state={:5}, {}'.format(task['_type'], str(task['_state']), schema.table_name())
            except:
                key = 'type={}, running={:5}, {}'.format(task['type'], str(task['compaction_running']), schema.table_name()) # Scylla 5.0 compatibility
            task_hist.add(key)

        task_hist.print_to_console()
        gdb.write('Total: {} instances of compaction::compaction_task_executor\n'.format(len(task_list)))


class Schema(object):
    def __init__(self, schema, partition_key_types, clustering_key_types):
        self.schema = schema
        self.partition_key_types = partition_key_types
        self.clustering_key_types = clustering_key_types
        self.cql_type_deserializers = dict()

    def get_cql_type_deserializer(self, type_name):
        if type_name in self.cql_type_deserializers:
            return self.cql_type_deserializers[type_name]

        if type_name == '"org.apache.cassandra.db.marshal.UTF8Type"':
            d = lambda b: b.decode('utf-8')
        else:
            raise Exception(f'Deserializer for {type_name} not implemented')

        self.cql_type_deserializers[type_name] = d
        return d

    """key must be a reference to an instance of clustering_key.
    Returns a list of values of key components. 
    The object sorts the same as the clustering key.
    """
    def parse_clustering_key(self, key):
        b = managed_bytes_printer(key['_bytes']).pure_bytes()
        return self.parse_clustering_key_bytes(b)

    """key must be a reference to an instance of partition_key.
    
    Returns a list of values of key components. 
    The object sorts the same as the partition key.
    """
    def parse_partition_key(self, key):
        b = managed_bytes_printer(key['_bytes']).pure_bytes()
        return self.parse_partition_key_bytes(b)

    """b is a bytes object representing a clustering_key in memory"""
    def parse_clustering_key_bytes(self, b):
        return self.parse_key_bytes(self.clustering_key_types, b)

    """b is a bytes object representing a partition_key in memory"""
    def parse_partition_key_bytes(self, b):
        return self.parse_key_bytes(self.partition_key_types, b)

    def deserialize_cql_value(self, abstract_type, b):
        return self.get_cql_type_deserializer(str(abstract_type['_name']))(b)

    """b is a byte representation of a compound object"""
    def parse_key_bytes(self, types, b):
        offset = 0
        type_nr = 0
        components = list()
        while offset < len(b):
            component_size = int.from_bytes(b[offset:(offset+2)], byteorder='big')
            offset += 2
            components.append(self.deserialize_cql_value(types[type_nr], b[offset:(offset+component_size)]))
            offset += component_size
            type_nr += 1
        return components


current_schema = None


def get_current_schema():
    if not current_schema:
        raise Exception('Current schema not set. Use \"scylla set-schema\" to set it.')
    return current_schema


class scylla_set_schema(gdb.Command):
    """Sets the current schema to be used by schema-aware commands.

    Setting the schema allows some commands and printers to interpret schema-dependent objects and present them
    in a more friendly form.

    Some commands require schema to work and will fail otherwise.
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla set-schema', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def get_abstract_types(self, key_type):
        return [seastar_shared_ptr(key_type).get().dereference() for key_type in std_vector(key_type['_types'])]

    def invoke(self, schema, from_tty):
        global current_schema
        if not schema:
            current_schema = None
            return
        schema = gdb.parse_and_eval(schema)
        typ = schema.type.strip_typedefs()
        if typ.code == gdb.TYPE_CODE_PTR or typ.code == gdb.TYPE_CODE_REF or typ.code == gdb.TYPE_CODE_RVALUE_REF:
            schema = schema.referenced_value()
            typ = schema.type
        if typ.name.startswith('seastar::lw_shared_ptr<'):
            schema = seastar_lw_shared_ptr(schema).get().dereference()
            typ = schema.type

        raw_schema = schema['_raw']

        gdb.write('(schema*) 0x{:x} ks={} cf={} id={} version={}\n'.format(int(schema.address), raw_schema['_ks_name'], raw_schema['_cf_name'], raw_schema['_id'], raw_schema['_version']))

        partition_key_type = self.get_abstract_types(seastar_lw_shared_ptr(schema['_partition_key_type']).get().dereference())
        clustering_key_type = self.get_abstract_types(seastar_lw_shared_ptr(schema['_clustering_key_type']).get().dereference())
        current_schema = Schema(schema, partition_key_type, clustering_key_type)


class scylla_schema(gdb.Command):
    """Pretty print a schema

    Example:
    (gdb) scylla schema $s
    (schema*) 0x604009352380 ks="scylla_bench" cf="test" id=a3eadd80-f2a7-11ea-853c-000000000004 version=47e0bf13-6cc8-3421-93c6-a9fe169b1689

    partition key: byte_order_equal=true byte_order_comparable=false is_reversed=false
        "org.apache.cassandra.db.marshal.LongType"

    clustering key: byte_order_equal=true byte_order_comparable=false is_reversed=true
        "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LongType)"

    columns:
        column_kind::partition_key  id=0 ordinal_id=0 "pk" "org.apache.cassandra.db.marshal.LongType" is_atomic=true is_counter=false
        column_kind::clustering_key id=0 ordinal_id=1 "ck" "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LongType)" is_atomic=true is_counter=false
        column_kind::regular_column id=0 ordinal_id=2 "v" "org.apache.cassandra.db.marshal.BytesType" is_atomic=true is_counter=false

    Argument is an expression that evaluates to a schema value, reference,
    pointer or shared pointer.
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla schema', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def print_key_type(self, key_type, key_name):
        gdb.write('{} key: byte_order_equal={} byte_order_comparable={} is_reversed={}\n'.format(
                key_name, key_type['_byte_order_equal'], key_type['_byte_order_comparable'], key_type['_is_reversed']))

        for key_type in std_vector(key_type['_types']):
            key_type = seastar_shared_ptr(key_type).get().dereference()
            gdb.write('    {}\n'.format(key_type['_name']))

    def invoke(self, schema, from_tty):
        schema = gdb.parse_and_eval(schema)
        typ = schema.type.strip_typedefs()
        if typ.code == gdb.TYPE_CODE_PTR or typ.code == gdb.TYPE_CODE_REF or typ.code == gdb.TYPE_CODE_RVALUE_REF:
            schema = schema.referenced_value()
            typ = schema.type
        if typ.name.startswith('seastar::lw_shared_ptr<'):
            schema = seastar_lw_shared_ptr(schema).get().dereference()
            typ = schema.type

        raw_schema = schema['_raw']

        gdb.write('(schema*) 0x{:x} ks={} cf={} id={} version={}\n'.format(int(schema.address), raw_schema['_ks_name'], raw_schema['_cf_name'], raw_schema['_id'], raw_schema['_version']))

        gdb.write('\n')
        self.print_key_type(seastar_lw_shared_ptr(schema['_partition_key_type']).get().dereference(), 'partition')

        gdb.write('\n')
        self.print_key_type(seastar_lw_shared_ptr(schema['_clustering_key_type']).get().dereference(), 'clustering')

        gdb.write('\n')
        gdb.write("columns:\n")
        for cdef in std_vector(raw_schema['_columns']):
            gdb.write("    {:27} id={} ordinal_id={} {} {} is_atomic={} is_counter={}\n".format(
                    str(cdef['kind']),
                    cdef['id'],
                    int(cdef['ordinal_id']),
                    cdef['_name'],
                    seastar_shared_ptr(cdef['type']).get().dereference()['_name'],
                    cdef['_is_atomic'],
                    cdef['_is_counter']))


class scylla_sstable_summary(gdb.Command):
    """Print content of sstable summary

    Usage: scylla sstable-summary $sst

    Where $sst has to be a sstables::sstable value or reference, or a
    sstables::shared_sstable instance.

    Example:

        (gdb) scylla sstable-summary $sst
        header: {min_index_interval = 128, size = 1, memory_size = 21, sampling_level = 128, size_at_full_sampling = 0}
        first_key: 63617373616e647261
        last_key: 63617373616e647261
        [0]: {
          token: 356242581507269238,
          key: 63617373616e647261,
          position: 0}

    Keys are printed in the hexadecimal notation.
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla sstable-summary', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)
        self.inf = gdb.selected_inferior()

    def to_hex(self, data, size):
        return bytes(self.inf.read_memory(data, size)).hex()

    def invoke(self, arg, for_tty):
        arg = gdb.parse_and_eval(arg)
        typ = arg.type.strip_typedefs()
        if typ.name.startswith("seastar::lw_shared_ptr"):
            sst = seastar_lw_shared_ptr(arg).get().dereference()
        else:
            sst = arg
        summary = seastar_lw_shared_ptr(sst['_components']['_value']).get().dereference()['summary']

        gdb.write("header: {}\n".format(summary['header']))
        gdb.write("first_key: {}\n".format(sstring(summary['first_key']['value'])))
        gdb.write("last_key: {}\n".format(sstring(summary['last_key']['value'])))

        entries = list(chunked_vector(summary['entries']))
        for i in range(len(entries)):
            e = entries[i]
            gdb.write("[{}]: {{\n  token: {},\n  key: {},\n  position: {}}}\n".format(i,
                e['token']['_data'],
                self.to_hex(e['key']['_M_str'], int(e['key']['_M_len'])),
                e['position']))


class scylla_sstable_index_cache(gdb.Command):
    """Print content of sstable partition-index cache

    Prints index pages present in the partition-cache. Promoted index is
    not printed.

    Usage: scylla sstable-index-cache $sst

    Where $sst has to be a sstables::sstable value or reference, or a
    sstables::shared_sstable instance.

    Output format is:

        [$summary_index] : [
            { key: $partition_key, token: $token, position: $position_in_data_file }
            ...
        ]
        ...

    Example:

        (gdb) scylla sstable-index-cache $sst
        [0]: [
          { key: 63617373616e647261, token: 356242581507269238, position: 0 }
        ]
        Total: 1 page(s) (1 loaded, 0 loading)

    Keys are printed in the hexadecimal notation.
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla sstable-index-cache', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        arg = gdb.parse_and_eval(arg)
        typ = arg.type
        if typ.code == gdb.TYPE_CODE_PTR or typ.code == gdb.TYPE_CODE_REF or typ.code == gdb.TYPE_CODE_RVALUE_REF:
            typ = typ.target().strip_typedefs()
        if typ.name == "sstables::partition_index_cache":
            cache = arg
        else:
            if typ.name.startswith("seastar::lw_shared_ptr"):
                sst = seastar_lw_shared_ptr(arg).get()
            else:
                sst = arg
            cache = std_unique_ptr(sst['_index_cache']).get().dereference()
        tree = bplus_tree(cache['_cache'])
        loaded_pages = 0
        loading_pages = 0
        partition_index_page_type = gdb.lookup_type('sstables::partition_index_page')
        for n in tree:
            t = n['_page'].type
            page = std_variant(n['_page'])
            if page.index() == 0:
                loading_pages += 1
                continue
            loaded_pages += 1
            page = page.get_with_type(partition_index_page_type)
            gdb.write("[{}]: [\n".format(int(n['_key'])))
            for entry in chunked_managed_vector(page['_entries']):
                entry = entry['_ptr'].dereference()['_value']
                key = entry['_key']
                token = std_optional(entry['_token'])
                if token:
                    token = str(int(token.get()['_data']))
                else:
                    token = 'null'
                position = int(entry['_position'])

                gdb.write("  {{ key: {}, token: {}, position: {} }}\n".format(key, token, position))
            gdb.write(']\n')

        gdb.write("Total: {} page(s) ({} loaded, {} loading)\n".format(loaded_pages + loading_pages, loaded_pages, loading_pages))


class permit_stats:
    def __init__(self, *args):
        if len(args) == 2:
            self.permits = 1
            self.resource_count = args[0]
            self.resource_memory = args[1]
        elif len(args) == 0: # default constructor
            self.permits = 0
            self.resource_count = 0
            self.resource_memory = 0
        else:
            raise TypeError("permit_stats.__init__(): expected 0 or 2 arguments, got {}".format(len(args)))

    def add(self, o):
        self.permits += o.permits
        self.resource_count += o.resource_count
        self.resource_memory += o.resource_memory


class scylla_read_stats(gdb.Command):
    """Summarize all active reads for the given semaphores.

    The command accepts multiple semaphores as arguments to summarize reads
    from. If no semaphores are provided, the command uses the semaphores
    from the local database instance. Reads are discovered through the
    reader_permit they own. A read is considered to be the group readers
    that belong to the same permit. Semaphores, which have no associated
    reads are omitted from the printout.

    Example:
    (gdb) scylla read-stats
    Semaphore _read_concurrency_sem with: 1/100 count and 14334414/14302576 memory resources, queued: 0, inactive=1
       permits count       memory table/description/state
             1     1     14279738 multishard_mutation_query_test.fuzzy_test/fuzzy-test/active
            16     0        53532 multishard_mutation_query_test.fuzzy_test/shard-reader/active
             1     0         1144 multishard_mutation_query_test.fuzzy_test/shard-reader/inactive
             1     0            0 *.*/view_builder/active
             1     0            0 multishard_mutation_query_test.fuzzy_test/multishard-mutation-query/active
            20     1     14334414 Total
    """
    def __init__(self):
        gdb.Command.__init__(self, 'scylla read-stats', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    @staticmethod
    def dump_reads_from_semaphore(semaphore):
        try:
            permit_list = semaphore['_permit_list']
        except gdb.error:
            gdb.write("Scylla version doesn't seem to have the permits linked yet, cannot list reads.")
            raise

        if not permit_list.type.strip_typedefs().name.startswith('boost::intrusive::list'):
            # 4.5 compatibility
            permit_list = std_unique_ptr(permit_list).get().dereference()['permits']

        state_prefix_len = len('reader_permit::state::')

        # (table, description, state) -> stats
        permit_summaries = defaultdict(permit_stats)
        total = permit_stats()

        for permit in intrusive_list(permit_list):
            try:
                schema = permit['_schema']['_p']
            except:
                # schema is already a raw pointer in older versions
                schema = permit['_schema']

            if schema:
                raw_schema = schema.dereference()['_raw']
                schema_name = "{}.{}".format(str(raw_schema['_ks_name']).replace('"', ''), str(raw_schema['_cf_name']).replace('"', ''))
            else:
                schema_name = "*.*"

            description = str(permit['_op_name_view'])[1:-1]
            state = str(permit['_state'])[state_prefix_len:]
            summary = permit_stats(int(permit['_resources']['count']), int(permit['_resources']['memory']))

            permit_summaries[(schema_name, description, state)].add(summary)
            total.add(summary)

        if not permit_summaries:
            return

        semaphore_name = str(semaphore['_name'])[1:-1]
        initial_count = int(semaphore['_initial_resources']['count'])
        initial_memory = int(semaphore['_initial_resources']['memory'])
        if semaphore['_inactive_reads'].type.name.startswith('std::map'):
            # 4.4 compatibility
            inactive_read_count = len(std_map(semaphore['_inactive_reads']))
        else:
            inactive_read_count = len(intrusive_list(semaphore['_inactive_reads']))


        try:
            waiters = int(semaphore["_stats"]["waiters"])
        except gdb.error: # 5.1 compatibility
            waiters = int(semaphore["_wait_list"]["_size"])

        gdb.write("Semaphore {} with: {}/{} count and {}/{} memory resources, queued: {}, inactive={}\n".format(
                semaphore_name,
                initial_count - int(semaphore['_resources']['count']), initial_count,
                initial_memory - int(semaphore['_resources']['memory']), initial_memory,
                waiters, inactive_read_count))

        gdb.write("{:>10} {:5} {:>12} {}\n".format('permits', 'count', 'memory', 'table/description/state'))

        permit_summaries_sorted = [(t, d, s, v) for (t, d, s), v in permit_summaries.items()]
        permit_summaries_sorted.sort(key=lambda x: x[3].resource_memory, reverse=True)

        for table, description, state, stats in permit_summaries_sorted:
            gdb.write("{:10} {:5} {:12} {}/{}/{}\n".format(stats.permits, stats.resource_count, stats.resource_memory, table, description, state))

        gdb.write("{:10} {:5} {:12} Total\n".format(total.permits, total.resource_count, total.resource_memory))

    def invoke(self, args, from_tty):
        if args:
            semaphores = [gdb.parse_and_eval(arg) for arg in args.split(' ')]
        else:
            db = find_db()
            semaphores = [db["_read_concurrency_sem"], db["_streaming_concurrency_sem"], db["_system_read_concurrency_sem"]]
            try:
                semaphores.append(db["_compaction_concurrency_sem"])
            except gdb.error:
                # 2020.1 compatibility
                pass

        for semaphore in semaphores:
            scylla_read_stats.dump_reads_from_semaphore(semaphore)


class scylla_get_config_value(gdb.Command):
    """Obtain the config item and print its value and metadata

    Example:
        (gdb) scylla get-config-value compaction_static_shares
        value: 100, type: "float", source: SettingsFile, status: Used, live: MustRestart
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla get-config-value', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    def invoke(self, cfg_item_name, from_tty):
        shard_id = current_shard()
        db = find_db(shard_id)
        cfg = db['_cfg']
        try:
            cfg_item = cfg[cfg_item_name]
        except gdb.error:
            gdb.write("Failed to obtain config item with name {}: {}\n".format(cfg_item_name, sys.exc_info()[1]))
            return

        cfg_type = cfg_item['_type']
        cfg_src = cfg_item['_source']
        cfg_status = cfg_item['_value_status']
        cfg_liveness = cfg_item['_liveness']
        cfg_value = std_vector(cfg_item['_cf']['_per_shard_values'])[shard_id]
        cfg_value = std_vector(cfg_value)[int(cfg_item['_per_shard_values_offset'])]
        cfg_value = std_unique_ptr(cfg_value).get()
        cfg_value = downcast_vptr(cfg_value)['value']['_value']

        def unqualify(name):
            name = str(name)
            return name.split('::')[-1]

        gdb.write("value: {}, type: {}, source: {}, status: {}, live: {}\n".format(
            cfg_value,
            cfg_type['_name'],
            unqualify(cfg_src),
            unqualify(cfg_status),
            unqualify(cfg_liveness)))


class scylla_range_tombstones(gdb.Command):
    """Prints and validates range tombstones in a given container.

    Currently supported containers:
        - mutation_partition

    Example:

        (gdb) scylla range-tombstones $mp
        {
          start: ['a', 'b'],
          kind: bound_kind::excl_start,
          end: ['a', 'b'],
          kind: bound_kind::incl_end,
          t: {timestamp = 1672546889091665, deletion_time = {__d = {__r = 1672546889}}}
        }
        {
          start: ['a', 'b'],
          kind: bound_kind::excl_start,
          end: ['a', 'c']
          kind: bound_kind::incl_end,
          t: {timestamp = 1673731764010123, deletion_time = {__d = {__r = 1673731764}}}
        }

    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla range-tombstones', gdb.COMMAND_USER, gdb.COMPLETE_COMMAND)

    """value is an instance of clustering_key"""
    def parse_clustering(self, value):
        return get_current_schema().parse_clustering_key(value)

    def invoke(self, partition, from_tty):
        p = gdb.parse_and_eval(partition)

        prev_end = None
        for r in intrusive_set(p['_row_tombstones']['_tombstones']):
            t = r['_tombstone']
            start = self.parse_clustering(t['start'])
            end = self.parse_clustering(t['end'])
            gdb.write('{\n  start: %s,\n  kind: %s,\n  end: %s,\n  kind: %s,\n  t: %s\n}\n' % (
                start, t['start_kind'], end, t['end_kind'], t['tomb']))

            # Detect crossed boundaries or empty ranges
            if start > end or (start == end and (str(t['start_kind']) != 'bound_kind::incl_start'
                                            or str(t['end_kind']) != 'bound_kind::incl_end')):
                gdb.write('*** Invalid boundaries! ***\n')
                return

            # Detect out of order ranges
            if prev_end:
                if start < prev_end or \
                        (start == prev_end and (str(prev['end_kind']) == 'bound_kind::incl_end'
                                                and str(t['start_kind']) == 'bound_kind::incl_start')):
                    gdb.write('*** Order violated! ***\n')
                    return
            prev_end = end
            prev = t


class scylla_gdb_func_dereference_smart_ptr(gdb.Function):
    """Dereference the pointer guarded by the smart pointer instance.

    Supported smart pointers are:
    * std::unique_ptr
    * seastar::lw_shared_ptr
    * seastar::foreign_ptr [1]

    [1] Note that `seastar::foreign_ptr` wraps another smart pointer type which
    is also dereferenced so its type has to be supported.

    Usage:
    $dereference_smart_ptr($ptr)

    Where:
    $ptr - a convenience variable or any gdb expression that evaluates
        to a smart pointer instance of a supported type.

    Returns:
    The value pointed to by the guarded pointer.

    Example:
    (gdb) p $1._read_context
    $2 = {_p = 0x60b00b068600}
    (gdb) p $dereference_smart_ptr($1._read_context)
    $3 = {<seastar::enable_lw_shared_from_this<cache::read_context>> = {<seastar::lw_shared_ptr_counter_base> = {_count = 1}, ...
    """

    def __init__(self):
        super(scylla_gdb_func_dereference_smart_ptr, self).__init__('dereference_smart_ptr')

    def invoke(self, expr):
        if isinstance(expr, gdb.Value):
            ptr = expr
        else:
            ptr = gdb.parse_and_eval(expr)

        typ = ptr.type.strip_typedefs()
        if typ.name.startswith('seastar::shared_ptr<'):
            return ptr['_p'].dereference()
        if typ.name.startswith('seastar::lw_shared_ptr<'):
            return seastar_lw_shared_ptr(ptr).get().dereference()
        elif typ.name.startswith('seastar::foreign_ptr<'):
            return self.invoke(ptr['_value'])
        elif typ.name.startswith('std::unique_ptr<'):
            return std_unique_ptr(ptr).get().dereference()

        raise ValueError("Unsupported smart pointer type: {}".format(typ.name))


class scylla_gdb_func_downcast_vptr(gdb.Function):
    """Downcast a ptr to a virtual object to a ptr of the actual object

    Usage:
    $downcast_vptr($ptr)

    Where:
    $ptr - an integer literal, a convenience variable or any gdb
        expression that evaluates to an pointer, which points to an
        virtual object.

    Returns:
    The pointer to the actual concrete object.

    Example:
    (gdb) p $1
    $2 = (mutation_reader::impl *) 0x60b03363b900
    (gdb) p $downcast_vptr(0x60b03363b900)
    $3 = (combined_mutation_reader *) 0x60b03363b900
    # The return value can also be dereferenced on the spot.
    (gdb) p *$downcast_vptr($1)
    $4 = {<mutation_reader::impl> = {_vptr.impl = 0x46a3ea8 <vtable for combined_mutation_reader+16>, _buffer = {_impl = {<std::allocator<mutation_fragment>> = ...
    """

    def __init__(self):
        super(scylla_gdb_func_downcast_vptr, self).__init__('downcast_vptr')

    def invoke(self, ptr):
        return downcast_vptr(ptr)

class reference_wrapper:
    def __init__(self, ref):
        self.ref = ref

    def get(self):
        return self.ref['_M_data']


class scylla_features(gdb.Command):
    """Prints state of Scylla gossiper features on current shard.

    Example:
    (gdb) scylla features
    "LWT": false
    "HINTED_HANDOFF_SEPARATE_CONNECTION": true
    "NONFROZEN_UDTS": true
    "XXHASH": true
    "CORRECT_COUNTER_ORDER": true
    "WRITE_FAILURE_REPLY": true
    "CDC": false
    "CORRECT_NON_COMPOUND_RANGE_TOMBSTONES": true
    "RANGE_TOMBSTONES": true
    "VIEW_VIRTUAL_COLUMNS": true
    "SCHEMA_TABLES_V3": true
    "LARGE_PARTITIONS": true
    "UDF": false
    "INDEXES": true
    "MATERIALIZED_VIEWS": true
    "DIGEST_MULTIPARTITION_READ": true
    "COUNTERS": true
    "UNBOUNDED_RANGE_TOMBSTONES": true
    "ROLES": true
    "LA_SSTABLE_FORMAT": true
    "MC_SSTABLE_FORMAT": true
    "MD_SSTABLE_FORMAT": true
    "STREAM_WITH_RPC_STREAM": true
    "ROW_LEVEL_REPAIR": true
    "TRUNCATION_TABLE": true
    "CORRECT_STATIC_COMPACT_IN_MC": true
    "DIGEST_INSENSITIVE_TO_EXPIRY": true
    "COMPUTED_COLUMNS": true
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla features', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def invoke(self, arg, for_tty):
        try:
            feature_service = sharded(gdb.parse_and_eval('debug::the_feature_service')).local()
        except Exception as e:
            gossiper = sharded(gdb.parse_and_eval('gms::_the_gossiper')).local()
            feature_service = gossiper['_feature_service']

        for (name, f) in unordered_map(feature_service['_registered_features']):
            f = reference_wrapper(f).get()
            gdb.write('%s: %s\n' % (f['_name'], f['_enabled']))

class scylla_repairs(gdb.Command):
    """ List all active repair instances for both repair masters and followers.

    Example:

       (repair_meta*) for masters: addr = 0x600005abf830, table = myks2.standard1, ip = 127.0.0.1, states = ['127.0.0.1->repair_state::get_sync_boundary_started', '127.0.0.3->repair_state::get_sync_boundary_finished'], repair_meta = (repair_meta*) 0x60400af3f8e0
       (repair_meta*) for masters: addr = 0x60000521f830, table = myks2.standard1, ip = 127.0.0.1, states = ['127.0.0.1->repair_state::get_sync_boundary_started', '127.0.0.2->repair_state::get_sync_boundary_started'], repair_meta = (repair_meta*) 0x6040103df8e0
       (repair_meta*) for follower: addr = 0x60000432a808, table = myks2.standard1, ip = 127.0.0.1, states = ['127.0.0.1->repair_state::get_sync_boundary_started', '127.0.0.2->repair_state::unknown'], repair_meta = (repair_meta*) 0x60400d73f8e0
    """

    def __init__(self):
        gdb.Command.__init__(self, 'scylla repairs', gdb.COMMAND_USER, gdb.COMPLETE_NONE, True)

    def process(self, master, rm):
        schema = rm['_schema']
        table = schema_ptr(schema).table_name().replace('"', '')
        all_nodes_state = []
        ip = str(rm['_myip']).replace('"', '')
        for n in std_vector(rm['_all_node_states']):
            all_nodes_state.append(str(n['node']).replace('"', '') + "->" + str(n['state']))
        gdb.write('(%s*) for %s: addr = %s, table = %s, ip = %s, states = %s, repair_meta = (repair_meta*) %s\n' % (rm.type, master, str(rm.address), table, ip, all_nodes_state, rm.address))

    def invoke(self, arg, for_tty):
        for rm in intrusive_list(gdb.parse_and_eval('debug::repair_meta_for_masters._repair_metas'), link='_tracker_link'):
            self.process("masters", rm)
        for rm in intrusive_list(gdb.parse_and_eval('debug::repair_meta_for_followers._repair_metas'), link='_tracker_link'):
            self.process("follower", rm)

class scylla_gdb_func_collection_element(gdb.Function):
    """Return the element at the specified index/key from the container.

    Usage:
    $collection_element($col, $key)

    Where:
    $col - a variable, or an expression that evaluates to a value of any
    supported container type.
    $key - a literal, or an expression that evaluates to an index/key type
    appropriate for the container.

    Supported container types are:
    * std::vector<> - key must be integer
    * std::list<> - key must be integer
    """
    def __init__(self):
        super(scylla_gdb_func_collection_element, self).__init__('collection_element')

    def invoke(self, collection, key):
        typ = collection.type.strip_typedefs()
        if typ.code == gdb.TYPE_CODE_PTR or typ.code == gdb.TYPE_CODE_REF or typ.code == gdb.TYPE_CODE_RVALUE_REF:
            typ = typ.target().strip_typedefs()
        if typ.name.startswith('std::vector<'):
            return std_vector(collection)[int(key)]
        elif typ.name.startswith('std::tuple<'):
            return std_tuple(collection)[int(key)]
        elif typ.name.startswith('std::__cxx11::list<'):
            return std_list(collection)[int(key)]
        elif typ.name.startswith('seastar::circular_buffer<'):
            return circular_buffer(collection)[int(key)]
        elif typ.name.startswith('boost::intrusive::list<'):
            return list(intrusive_list(collection))[int(key)]

        raise ValueError("Unsupported container type: {}".format(typ.name))


class scylla_gdb_func_sharded_local(gdb.Function):
    """Get the local instance of a sharded object

    Usage:
    $sharded_local($obj)

    Where:
    $obj - a variable, or an expression that evaluates to any `seastar::sharded`
    instance.

    Example:
    (gdb) p $sharded_local(cql3::_the_query_processor)
    $1 = (cql3::query_processor *) 0x6350001f2390
    """

    def __init__(self):
        super(scylla_gdb_func_sharded_local, self).__init__('sharded_local')

    def invoke(self, obj):
        return sharded(obj).local()


class scylla_gdb_func_variant_member(gdb.Function):
    """Get the active member of an std::variant

    Usage:
    $variant_member($obj)

    Where:
    $obj - a variable, or an expression that evaluates to any `std::variant`
    instance.

    Example:
    # $1 = (cql3::raw_value_view *) 0x6060365a7a50
    (gdb) p &$variant_member($1->_data)
    $2 = (cql3::null_value *) 0x6060365a7a50
    """

    def __init__(self):
        super(scylla_gdb_func_variant_member, self).__init__('variant_member')

    def invoke(self, obj):
        return std_variant(obj).get()

class scylla_gdb_func_coro_frame(gdb.Function):
    """Given a seastar::task* pointing to a coroutine task, convert it to a pointer to the coroutine frame.

    Usage:
    $coro_frame($ptr)

    Where:
    $ptr - any expression that evaluates to an integer. It will be interpreted as seastar::task*.

    Returns:
    The (typed) pointer to the coroutine frame.

    Example:

    1. Print the task chain:

    (gdb) scylla fiber seastar::local_engine->_current_task
    [shard  1] #0  (task*) 0x0000601008e8e970 0x000000000047aae0 vtable for seastar::internal::coroutine_traits_base<void>::promise_type + 16  (sstables::parse<unsigned int, std::pair<sstables::metadata_type, unsigned int> >(schema const&, sstables::sstable_version_types, sstables::random_access_reader&, sstables::disk_array<unsigned int, std::pair<sstables::metadata_type, unsigned int> >&) at sstables/sstables.cc:352)
    [shard  1] #1  (task*) 0x00006010092acf10 0x000000000047aae0 vtable for seastar::internal::coroutine_traits_base<void>::promise_type + 16  (sstables::parse(schema const&, sstables::sstable_version_types, sstables::random_access_reader&, sstables::statistics&) at sstables/sstables.cc:570)
    ...

    2. Examine the coroutine frame of task #1:

    (gdb) p *$coro_frame(0x00006010092acf10)
    $1 = {
      __resume_fn = 0x2485f80 <sstables::parse(schema const&, sstables::sstable_version_types, sstables::random_access_reader&, sstables::statistics&)>,
      ...
      PointerType_7 = 0x601008e67880,
      PointerType_8 = 0x601008e64900,
      ...
      __int_32_23 = 4,
      ...
      __coro_index = 0 '\000'
      ...

    3. Examine coroutine arguments.
    (Needs some know-how. They should be at lowest-indexed PointerType* or __int* entries, depending on their type. Not so easy to automate):

    (gdb) p $downcast_vptr($coro_frame(0x00006010092acf10)->PointerType_7)
    $2 = (schema *) 0x601008e67880
    (gdb) p (sstables::sstable_version_types)($coro_frame(0x00006010092acf10)->__int_32_23)
    $3 = sstables::sstable_version_types::me
    (gdb) p $downcast_vptr($coro_frame(0x00006010092acf10)->PointerType_8)
    $4 = (sstables::file_random_access_reader *) 0x601008e64900
    """

    def __init__(self):
        super(scylla_gdb_func_coro_frame, self).__init__('coro_frame')

    def invoke(self, ptr_raw):
        vptr_type = gdb.lookup_type('uintptr_t').pointer()
        ptr = gdb.Value(ptr_raw).reinterpret_cast(vptr_type)
        maybe_vptr = int(ptr.dereference())

        # Validate that the task is a coroutine.
        with gdb.with_parameter("print demangle", "on"):
            symname = gdb.execute(f"info symbol {maybe_vptr}", False, True)
        if not symname.startswith('vtable for seastar::internal::coroutine_traits_base'):
            gdb.write(f"0x{maybe_vptr:x} does not seem to point to a coroutine task.")
            return None

        # The promise object starts on the third `uintptr_t` in the frame.
        # The resume_fn pointer is the first `uintptr_t`.
        # So if the task is a coroutine, we should be able to find the resume function via offsetting by -2.
        # AFAIK both major compilers respect this convention.
        block = gdb.block_for_pc((ptr - 2).dereference())

        # Look up the coroutine frame type.
        # I don't understand why, but gdb has problems looking up the coro_frame_ty type if demangling is enabled.
        with gdb.with_parameter("demangle-style", "none"):
            coro_ty = gdb.lookup_type(f"{block.function.linkage_name}.coro_frame_ty").pointer()

        return (ptr - 2).cast(coro_ty)

# Commands
scylla()
scylla_databases()
scylla_commitlog()
scylla_keyspaces()
scylla_tables()
scylla_memory()
scylla_ptr()
scylla_mem_ranges()
scylla_mem_range()
scylla_heapprof()
scylla_lsa()
scylla_lsa_segment()
scylla_lsa_check()
scylla_segment_descs()
scylla_timers()
scylla_apply()
scylla_shard()
scylla_thread()
scylla_unthread()
scylla_threads()
scylla_task_stats()
scylla_tasks()
scylla_task_queues()
scylla_io_queues()
scylla_fiber()
scylla_find()
scylla_task_histogram()
scylla_active_sstables()
scylla_netw()
scylla_gms()
scylla_cache()
scylla_sstable_summary()
scylla_sstable_index_cache()
scylla_sstables()
scylla_memtables()
scylla_generate_object_graph()
scylla_smp_queues()
scylla_features()
scylla_repairs()
scylla_small_objects()
scylla_large_objects()
scylla_compaction_tasks()
scylla_set_schema()
scylla_schema()
scylla_read_stats()
scylla_get_config_value()
scylla_range_tombstones()


# Convenience functions
#
# List them inside `gdb` with
#   (gdb) help function
#
# To get the usage of an individual function:
#   (gdb) help function $function_name
scylla_gdb_func_dereference_smart_ptr()
scylla_gdb_func_downcast_vptr()
scylla_gdb_func_coro_frame()
scylla_gdb_func_collection_element()
scylla_gdb_func_sharded_local()
scylla_gdb_func_variant_member()

gdb.execute('set language c++')
