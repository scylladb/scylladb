#!/usr/bin/python3
#
# Copyright 2016 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

import json
import sys
import re
import glob
import argparse
import os
from string import Template
import pyparsing as pp
from functools import reduce
import textwrap
from numbers import Number

EXTENSION = '.idl.hh'
READ_BUFF = 'input_buffer'
WRITE_BUFF = 'output_buffer'
SERIALIZER = 'serialize'
DESERIALIZER = 'deserialize'
SETSIZE = 'set_size'
SIZETYPE = 'size_type'

parser = argparse.ArgumentParser(description="""Generate serializer helper function""")

parser.add_argument('-o', help='Output file', default='')
parser.add_argument('-f', help='input file', default='')
parser.add_argument('--ns', help="""namespace, when set function will be created
under the given namespace""", default='')
parser.add_argument('file', nargs='*', help="combine one or more file names for the genral include files")

config = parser.parse_args()


def reindent(indent, text):
    return textwrap.indent(textwrap.dedent(text), ' ' * indent)

def fprint(f, *args):
    for arg in args:
        f.write(arg)

def fprintln(f, *args):
    for arg in args:
        f.write(arg)
    f.write('\n')

def print_cw(f):
    fprintln(f, """
/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


 /*
  * This is an auto-generated code, do not modify directly.
  */
#pragma once
 """)

def parse_file(file_name):
    first = pp.Word(pp.alphas + "_", exact=1)
    rest = pp.Word(pp.alphanums + "_")

    number = pp.Word(pp.nums)
    identifier = pp.Combine(first + pp.Optional(rest))

    lbrace = pp.Literal('{').suppress()
    rbrace = pp.Literal('}').suppress()
    cls = pp.Literal('class')
    colon = pp.Literal(":")
    semi = pp.Literal(";").suppress()
    langle = pp.Literal("<")
    rangle = pp.Literal(">")
    equals = pp.Literal("=")
    comma = pp.Literal(",")
    lparen = pp.Literal("(")
    rparen = pp.Literal(")")
    lbrack = pp.Literal("[")
    rbrack = pp.Literal("]")
    mins = pp.Literal("-")
    struct = pp.Literal('struct')
    template = pp.Literal('template')
    final = pp.Literal('final').setResultsName("final")
    stub = pp.Literal('stub').setResultsName("stub")
    with_colon = pp.Word(pp.alphanums + "_" + ":")
    btype = with_colon
    type = pp.Forward()
    nestedParens = pp.nestedExpr('<', '>')

    tmpl = pp.Group(btype.setResultsName("template_name") + langle.suppress() + pp.Group(pp.delimitedList(type)) + rangle.suppress())
    type << (tmpl | btype)
    enum_lit = pp.Literal('enum')
    enum_class = pp.Group(enum_lit + cls)
    ns = pp.Literal("namespace")

    enum_init = equals.suppress() + pp.Optional(mins) + number
    enum_value = pp.Group(identifier + pp.Optional(enum_init))
    enum_values = pp.Group(lbrace + pp.delimitedList(enum_value) + pp.Optional(comma) + rbrace)
    content = pp.Forward()

    member_name = pp.Combine(pp.Group(identifier + pp.Optional(lparen + rparen)))
    attrib = pp.Group(lbrack.suppress() + lbrack.suppress() + pp.SkipTo(']') + rbrack.suppress() + rbrack.suppress())
    opt_attribute = pp.Optional(attrib).setResultsName("attribute")
    namespace = pp.Group(ns.setResultsName("type") + identifier.setResultsName("name") + lbrace + pp.Group(pp.OneOrMore(content)).setResultsName("content") + rbrace)
    enum = pp.Group(enum_class.setResultsName("type") + identifier.setResultsName("name") + colon.suppress() + identifier.setResultsName("underline_type") + enum_values.setResultsName("enum_values") + pp.Optional(semi).suppress())
    default_value = equals.suppress() + pp.SkipTo(';')
    class_member = pp.Group(type.setResultsName("type") + member_name.setResultsName("name") + opt_attribute + pp.Optional(default_value).setResultsName("default") + semi.suppress()).setResultsName("member")
    template_param = pp.Group(identifier.setResultsName("type") + identifier.setResultsName("name"))
    template_def = pp.Group(template + langle + pp.Group(pp.delimitedList(template_param)).setResultsName("params") + rangle)
    class_content = pp.Forward()
    class_def = pp.Group(pp.Optional(template_def).setResultsName("template") + (cls | struct).setResultsName("type") + with_colon.setResultsName("name") + pp.Optional(final) + pp.Optional(stub) + opt_attribute + lbrace + pp.Group(pp.ZeroOrMore(class_content)).setResultsName("members") + rbrace + pp.Optional(semi))
    content << (enum | class_def | namespace)
    class_content << (enum | class_def | class_member)
    rt = pp.OneOrMore(content)
    singleLineComment = "//" + pp.restOfLine
    rt.ignore(singleLineComment)
    rt.ignore(pp.cStyleComment)
    return rt.parseFile(file_name, parseAll=True)


def combine_ns(namespaces):
    return "::".join(namespaces)

def open_namespaces(namespaces):
    return "".join(map(lambda a: "namespace " + a + " { ", namespaces))

def close_namespaces(namespaces):
    return "".join(map(lambda a: "}", namespaces))

def set_namespace(namespaces):
    ns = combine_ns(namespaces)
    ns_open = open_namespaces(namespaces)
    ns_close = close_namespaces(namespaces)
    return [ns, ns_open, ns_close]

def declare_class(hout, name, ns_open, ns_close):
    clas_def = ns_open + name + ";" + ns_close
    fprintln(hout, "\n", clas_def)

def declear_methods(hout, name, template_param = ""):
    if config.ns != '':
        fprintln(hout, "namespace ", config.ns, " {")
    fprintln(hout, Template("""
template <$tmp_param>
struct serializer<$name> {
  template <typename Output>
  static void write(Output& buf, const $name& v);

  template <typename Input>
  static $name read(Input& buf);
};
""").substitute({'name' : name, 'sizetype' : SIZETYPE, 'tmp_param' : template_param }))
    if config.ns != '':
        fprintln(hout, "}")

def handle_enum(enum, hout, cout, namespaces , parent_template_param = []):
    [ns, ns_open, ns_close] = set_namespace(namespaces)
    temp_def = ", ".join(map(lambda a: a[0] + " " + a[1], parent_template_param)) if parent_template_param else ""
    template = "template <"+ temp_def +">"  if temp_def else ""
    name = enum["name"] if ns == "" else ns + "::" + enum["name"]
    declear_methods(hout, name, temp_def)
    fprintln(cout, Template("""
$template
template <typename Output>
void serializer<$name>::write(Output& buf, const $name& v) {
  serialize(buf, static_cast<$type>(v));
}

$template
template<typename Input>
$name serializer<$name>::read(Input& buf) {
  return static_cast<$name>(deserialize(buf, boost::type<$type>()));
}""").substitute({'name' : name, 'size_type' : SIZETYPE, 'type': enum['underline_type'], 'template' : template}))


def join_template(lst):
    return "<" + ", ".join([param_type(l) for l in lst]) + ">"

def param_type(lst):
    if isinstance(lst, str):
        return lst
    if len(lst) == 1:
        return lst[0]
    return lst[0] + join_template(lst[1])

def is_class(obj):
    return obj["type"] == "class" or obj["type"] == "struct"

def is_enum(obj):
    try:
        return not isinstance(obj["type"], str) and "".join(obj["type"]) == 'enumclass'
    except:
        return False
local_types = {}

def list_types(lst):
    if isinstance(lst, str):
        return [lst]
    if len(lst) == 1:
        return lst
    lt = reduce(lambda a,b: a + b, [list_types(l) for l in lst[1]])
    return lt

def list_local_types(lst):
    return {l for l in list_types(lst) if l in local_types}

def is_basic_type(lst):
    if isinstance(lst, str):
        return lst not in local_types
    if len(lst) == 1:
        return lst[0] not in local_types
    return False

def is_local_type(lst):
    return isinstance(lst, str) and lst in local_types or len(lst) == 1 and lst[0] in local_types

def get_template_name(lst):
    return lst["template_name"] if not isinstance(lst, str) and len(lst) >1 else None

def is_vector(lst):
    return get_template_name(lst) == "std::vector"

def is_variant(lst):
    return get_template_name(lst) == "boost::variant"

def is_optional(lst):
    return get_template_name(lst) == "std::experimental::optional"

created_writers = set()

def get_members(cls):
    return [p for p in cls["members"] if not is_class(p) and not is_enum(p)]

def is_final(cls):
    return "final" in cls
def get_variant_type(lst):
    if is_variant(lst):
        return "variant"
    return param_type(lst)

def variant_to_member(typ):
    return [{"name" :  get_variant_type(x), "type" : x } for x in typ if is_local_type(x) or is_variant(x)]

def variant_info(info, typ):
    [cls, namespaces, parent_template_param] = info
    return [{"members" : variant_to_member(typ)}, namespaces, parent_template_param]

stubs = set()
def is_stub(cls):
    return cls in stubs

def handle_visitors_state(info, hout, clases = []):
    [cls, namespaces, parent_template_param] = info
    name = "__".join(clases) if clases else cls["name"]
    frame = "empty_frame" if "final" in cls else "frame"
    fprintln(hout, Template("""
struct state_of_$name {
    $frame f;""").substitute({'name': name, 'frame': frame }))
    if clases:
        local_state = "state_of_" + "__".join(clases[:-1])
        fprintln(hout, Template("    $name _parent;").substitute({'name':  local_state}))
        if "final" in cls:
            fprintln(hout, Template("    state_of_$name($state parent) : _parent(parent) {}").substitute({'name':  name, 'state' : local_state}))
    fprintln(hout, "};")
    members = get_members(cls)
    member_class = clases if clases else [cls["name"]]
    for param in members:
        if is_local_type(param["type"]):
            handle_visitors_state(local_types[param_type(param["type"])], hout, member_class + [param["name"]])
        if is_variant(param["type"]):
            handle_visitors_state(variant_info(info, param["type"][1]), hout, member_class + [param["name"]])

def get_dependency(cls):
    members = get_members(cls)
    return reduce(lambda a,b: a |b, [list_local_types(m["type"]) for m in members], set())

def add_vector_node(hout, cls, members, base_state, current_node, ind):
    current = members[ind]
    typ = current["type"][1]
    fprintln(hout, Template("""
struct $node_name {
    bytes_ostream& _out;
    state_of_$base_state _state;
    place_holder _size;
    size_type _count = 0;
    $node_name(bytes_ostream& out, state_of_$base_state state)
        : _out(out)
        , _state(state)
        , _size(start_place_holder(out))
    {
    }
    $next_state end_$name() {
        _size.set(_out, _count);
    }""").substitute({'node_name': '', 'name': current["name"] }))

    fprintln(hout, "};")

def vector_add_method(current, base_state):
    if is_optional(current["type"][1][0]):
        typ = current["type"][1][0]
        res = reindent(4, """
  void write_empty() {
        serialize(_out, false);
        _count++;
  }""")
        set_optional = "serialize(_out, true);"
    else:
        typ = current["type"]
        res = ""
        set_optional = ""
    if is_basic_type(typ[1]):
        res = res + Template("""
  void add_$name($type t)  {
        $set_optional
        serialize(_out, t);
        _count++;
  }""").substitute({'type': param_type(typ[1]), 'name': current["name"], 'set_optional': set_optional})
    else:
        res = res + Template("""
  writer_of_$type add() {
        $set_optional
        _count++;
        return {_out};
  }""").substitute({'type': param_type(typ[1]), 'name': current["name"], 'set_optional': set_optional})
    return res + Template("""
  after_${basestate}__$name end_$name() && {
        _size.set(_out, _count);
        return { _out, std::move(_state) };
  }

  vector_position pos() const {
        return vector_position{_out.pos(), _count};
  }

  void rollback(const vector_position& vp) {
        _out.retract(vp.pos);
        _count = vp.count;
  }""").substitute({'name': current["name"], 'basestate' : base_state})

def add_param_writer_basic_type(name, base_state, typ, var_type = "", var_index = None, root_node = False):
    if isinstance(var_index, Number):
        var_index = "uint32_t(" + str(var_index) +")"
    set_varient_index = "serialize(_out, " + var_index +");\n" if var_index is not None else ""
    set_command = "_state.f.end(_out);" if var_type is not "" else ""
    return_command = "{ _out, std::move(_state._parent) }" if var_type is not "" and not root_node else "{ _out, std::move(_state) }"

    if typ in ['bytes', 'sstring']:
        typ += '_view'
    else:
        typ = 'const ' + typ + '&'

    return Template(reindent(4, """
        after_${base_state}__$name write_$name$var_type($typ t) && {
            $set_varient_index
            serialize(_out, t);
            $set_command
            return $return_command;
        }""")).substitute(locals())

def add_param_writer_object(name, base_state, typ, var_type = "", var_index = None, root_node = False):
    var_type1 = "_" + var_type if var_type != "" else ""
    if isinstance(var_index, Number):
        var_index = "uint32_t(" + str(var_index) +")"
    set_varient_index = "serialize(_out, " + var_index +");\n" if var_index is not None else ""
    ret = Template(reindent(4,"""
        ${base_state}__${name}$var_type1 start_${name}$var_type() && {
            $set_varient_index
            return { _out, std::move(_state) };
        }
    """)).substitute(locals())
    if not is_stub(typ) and is_local_type(typ):
        ret += add_param_writer_basic_type(name, base_state, typ, var_type, var_index, root_node)
    if is_stub(typ):
        set_command = "_state.f.end(_out);" if var_type is not "" else ""
        return_command = "{ _out, std::move(_state._parent) }" if var_type is not "" and not root_node else "{ _out, std::move(_state) }"
        ret += Template(reindent(4, """
            template<typename Serializer>
            after_${base_state}__${name} ${name}$var_type(Serializer&& f) && {
                $set_varient_index
                f(writer_of_$typ(_out));
                $set_command
                return $return_command;
            }""")).substitute(locals())
    return ret

def add_param_write(current, base_state, vector = False, root_node = False):
    typ = current["type"]
    res = ""
    if is_basic_type(typ):
        res = res + add_param_writer_basic_type(current["name"], base_state, typ)
    elif is_optional(typ):
            res = res +  Template(reindent(4, """
    after_${basestate}__$name skip_$name() && {
        serialize(_out, false);
        return { _out, std::move(_state) };
    }""")).substitute({'type': param_type(typ), 'name': current["name"], 'basestate' : base_state})
            if is_basic_type(typ[1][0]):
                res = res + add_param_writer_basic_type(current["name"], base_state, typ[1][0], "", "true")
            elif is_local_type(typ[1][0]):
                res = res + add_param_writer_object(current["name"], base_state[0][1], typ, "", "true")
            else:
                print("non supported optional type ", type[0][1])
    elif is_vector(typ):
        set_size = "_size.set(_out, 0);" if vector else "serialize(_out, size_type(0));"

        res = res +  Template("""
    ${basestate}__$name start_$name() && {
        return { _out, std::move(_state) };
    }

    after_${basestate}__$name skip_$name() && {
        $set
        return { _out, std::move(_state) };
    }
""").substitute({'type': param_type(typ), 'name': current["name"], 'basestate' : base_state, 'set' : set_size})
    elif is_local_type(typ):
        res = res + add_param_writer_object(current["name"], base_state, typ)
    elif is_variant(typ):
        for idx, p in enumerate(typ[1]):
            if is_basic_type(p):
                varient_type = param_type(p)
                res = res + add_param_writer_basic_type(current["name"], base_state, varient_type,"_" + varient_type, idx, root_node)
            elif is_variant(p):
                res = res + add_param_writer_object(current["name"], base_state, p, '_' + "variant", idx, root_node)
            elif is_local_type(p):
                res = res + add_param_writer_object(current["name"], base_state, p, '_' + param_type(p), idx, root_node)
    else:
        print ("something is wrong with type", typ)
    return res;

def get_return_struct(variant_node, clases):
    if not variant_node:
        return clases
    if clases[-2] == "variant":
        return clases[:-2]
    return clases[:-1]


def add_variant_end_method(base_state, name, clases):

    return_struct = "after_" + base_state
    return Template("""
    $return_struct  end_$name() && {
        _state.f.end(_out);
        _state._parent.f.end(_out);
        return { _out, std::move(_state._parent._parent) };
    }
""").substitute({'name': name, 'return_struct':return_struct})

def add_end_method(parents, name, variant_node = False, return_value = True):
    if variant_node:
        return add_variant_end_method(parents, name, return_value)
    base_state = parents + "__" + name
    if return_value:
        return_struct =  "after_" + base_state
        return Template("""
    $return_struct  end_$name() && {
        _state.f.end(_out);
        return { _out, std::move(_state._parent) };
    }
""").substitute({'name': name, 'return_struct':return_struct})
    return Template("""
    void  end_$name() {
        _state.f.end(_out);
    }
""").substitute({'name': name, 'basestate':base_state})

def add_vector_placeholder():
    return """    place_holder _size;
    size_type _count = 0;"""

def add_node(hout, name, member, base_state, prefix, parents, fun, is_type_vector = False, is_type_final = False):
    struct_name = prefix + name
    if member and is_type_vector:
        vector_placeholder = add_vector_placeholder()
        vector_init = "\n            , _size(start_place_holder(out))"
    else:
        vector_placeholder = ""
        vector_init = ""
    if vector_init != "" or prefix == "":
        state_init =  "_state{start_frame(out), std::move(state)}" if parents != base_state and not is_type_final else "_state(state)"
    else:
        if member and is_variant(member) and parents != base_state:
            state_init =  "_state{start_frame(out), std::move(state)}"
            sate = name
        else:
            state_init = ""
    if prefix == "writer_of_":
        constructor = Template("""$name(bytes_ostream& out)
            : _out(out)
            , _state{start_frame(out)}${vector_init}
            {}""").substitute({'name': struct_name, 'vector_init' : vector_init})
    elif state_init != "":
        constructor = Template("""$name(bytes_ostream& out, state_of_$state state)
            : _out(out)
            , $state_init${vector_init}
            {}""").substitute({'name': struct_name, 'vector_init' : vector_init, 'state' : parents, 'state_init' : state_init})
    else:
        constructor = ""
    fprintln(hout, Template("""
struct $name {
    bytes_ostream& _out;
    state_of_$state _state;
    ${vector_placeholder}
    ${constructor}
    $fun
};""").substitute({'name': struct_name, 'state' : base_state, 'fun' : fun, 'vector_placeholder': vector_placeholder, 'constructor': constructor}))

def add_vector_node(hout, member, base_state, parents):
    add_node(hout, base_state + "__" + member["name"], member["type"], base_state, "", parents, vector_add_method(member, base_state), True)

def add_variant_nodes(hout, member, param, base_state, parents, classes):
    vr = False
    par = base_state + "__" + member["name"]
    for typ in param[1]:
        if is_local_type(typ):
            handle_visitors_nodes(local_types[param_type(typ)], hout, True, classes + [member["name"], local_types[param_type(typ)][0]["name"]])
        if is_variant(typ):
            name = base_state + "__" + member["name"] + "__variant"
            new_member = {"type": typ, "name" : "variant"}
            return_struct = "after_" + par
            end_method = Template("""
    $return_struct  end_variant() && {
        _state.f.end(_out);
        return { _out, std::move(_state._parent) };
    }
""").substitute({'return_struct':return_struct})
            add_node(hout, name, None, base_state + "__" + member["name"], "after_", name, end_method)
            add_variant_nodes(hout, new_member, typ, par, name, classes + [member["name"]])
            add_node(hout, name, typ, name, "", par, add_param_write({"type": typ, "name" : "variant"}, par))
            vr = True
writers = set()

def add_nodes_when_needed(hout, info, member, base_state_name, parents, member_classes):
    if is_vector(member["type"]):
        add_vector_node(hout, member, base_state_name, base_state_name)
    elif is_variant(member["type"]):
        add_variant_nodes(hout, member, member["type"], base_state_name, parents, member_classes)
    elif is_local_type(member["type"]):
        handle_visitors_nodes(local_types[member["type"]], hout, False, member_classes + [member["name"]])

def handle_visitors_nodes(info, hout, variant_node = False, clases = []):
    global writers
    [cls, namespaces, parent_template_param] = info
    #for root node, only generate once
    if not clases:
        if cls["name"] in writers:
            return
        writers.add(cls["name"])

    members = get_members(cls)
    if clases:
        base_state_name = "__".join(clases)
        if variant_node:
            parents = "__".join(clases[:-1])
        else:
            parents = "__".join(clases[:-1])
        current_name = clases[-1]
    else:
        base_state_name = cls["name"]
        current_name = cls["name"]
        parents = ""
    member_classes = clases if clases else [current_name]
    prefix = ""  if clases else "writer_of_"
    if not members:
        add_node(hout, base_state_name, None, base_state_name, prefix, parents, add_end_method(parents, current_name, variant_node, clases), False, is_final(cls))
        return
    add_node(hout, base_state_name + "__" + members[-1]["name"], members[-1]["type"], base_state_name, "after_", base_state_name, add_end_method(parents, current_name, variant_node, clases))
    # Create writer and reader for include class
    if not variant_node:
        for member in get_dependency(cls):
            handle_visitors_nodes(local_types[member], hout)
    for ind in reversed(range(1, len(members))):
        member = members[ind]
        add_nodes_when_needed(hout, info, member, base_state_name, parents, member_classes)
        variant_state = base_state_name + "__" + member["name"] if is_variant(member["type"]) else base_state_name
        is_param_vector = is_vector(member["type"]) and  is_basic_type(member["type"][1][0])
        add_node(hout, base_state_name + "__" + members[ind - 1]["name"], member["type"], variant_state, "after_", base_state_name, add_param_write(member, base_state_name), False)
    member = members[0]
    is_param_vector = is_vector(member["type"]) and is_basic_type(member["type"][1][0])
    add_nodes_when_needed(hout, info, member, base_state_name, parents, member_classes)
    add_node(hout, base_state_name, member["type"], base_state_name, prefix, parents, add_param_write(member, base_state_name, False, not clases), False, is_final(cls))

def add_to_types(cls, namespaces, parent_template_param ):
    global local_types
    global stubs
    if "attribute" not in cls or cls["attribute"][0][0] != "writable":
        return
    local_types[cls["name"]] = [cls, namespaces, parent_template_param]
    if "stub" in cls:
        stubs.add(cls["name"])
    for param in cls["members"]:
        if is_class(param) or is_enum(param):
            continue

def sort_dependencies():
    dep_tree = {}
    res = []
    for k in local_types:
        [cls, namespaces, parent_template_param] = local_types[k]
        dep_tree[k] = get_dependency(cls)
    while (len(dep_tree) > 0):
        found = set()
        found = found | { k for k in dep_tree if not dep_tree[k]}
        res = res + [k for k in found]
        for k in found:
            dep_tree.pop(k)
        for k in dep_tree:
            if dep_tree[k]:
                dep_tree[k].difference_update(found)

    return res


def join_template_view(lst, more_types = []):
    return "<" + ", ".join([param_view_type(l) for l in lst] + more_types) + ">"

def to_view(val):
    if val in local_types:
        return val +"_view"
    return val

def param_view_type(lst):
    if isinstance(lst, str):
        return to_view(lst)
    if len(lst) == 1:
        return to_view(lst[0])
    if lst[0] == "boost::variant":
        return lst[0] + join_template_view(lst[1], ["unknown_variant_type"])
    return lst[0] + join_template_view(lst[1])

read_sizes = set()

def add_variant_read_size(hout, typ):
    global read_sizes
    t = param_view_type(typ)
    if t in read_sizes:
        return
    if not is_variant(typ):
        return
    for p in typ[1]:
        if is_variant(p):
            add_variant_read_size(hout, p)
    read_sizes.add(t)
    fprintln(hout, Template("""
template<>
inline void skip(seastar::simple_input_stream& v, boost::type<${type}>) {
    size_type ln = deserialize(v, boost::type<size_type>());
    v.skip(ln - sizeof(size_type));
}""").substitute ({'type' : t}))


    fprintln(hout, Template("""
template<typename Input>
$type deserialize(Input& v, boost::type<${type}>) {
    auto in = v;
    deserialize(in, boost::type<size_type>());
    size_type o = deserialize(in, boost::type<size_type>());
    """).substitute ({'type' : t}))
    for index, param in enumerate(typ[1]):
        fprintln(hout, Template("""
    if (o == $ind) {
        v.skip(sizeof(size_type)*2);
        return $full_type(deserialize(v, boost::type<$type>()));
    }""").substitute({'ind' : index, 'type' : param_view_type(param), 'full_type' : t}))
    fprintln(hout, '    return ' + t + '(deserialize(v, boost::type<unknown_variant_type>()));\n}')

def add_view(hout, info):
    [cls, namespaces, parent_template_param] = info
    members = get_members(cls)
    for m in members:
        add_variant_read_size(hout, m["type"])

    fprintln(hout, Template("""struct ${name}_view {
    seastar::simple_input_stream v;
    """).substitute({'name' : cls["name"]}))

    if not is_stub(cls["name"]) and is_local_type(cls["name"]):
        fprintln(hout, Template(reindent(4, """
            operator $type() const {
               auto in = v;
               return deserialize(in, boost::type<$type>());
            }
        """)).substitute({'type' : cls["name"]}))

    skip = "" if is_final(cls) else "skip(in, boost::type<size_type>());"
    for m in members:
        full_type = param_view_type(m["type"])
        fprintln(hout, Template(reindent(4, """
            $type $name() const {
               auto in = v;
               $skip
               return deserialize(in, boost::type<$type>());
            }
        """)).substitute({'name' : m["name"], 'type' : full_type, 'skip' : skip}))

        skip = skip + Template("\n       skip(in, boost::type<${type}>());").substitute({'type': full_type})

    fprintln(hout, "};")
    skip_impl = "seastar::simple_input_stream& in = v;\n       " + skip if is_final(cls) else "v.skip(read_frame_size(v));"
    if skip == "":
        skip_impl = ""

    fprintln(hout, Template("""
template<>
inline void skip(seastar::simple_input_stream& v, boost::type<${type}_view>) {
    $skip_impl
}

template<>
struct serializer<${type}_view> {
    template<typename Input>
    static ${type}_view read(Input& v) {
        ${type}_view res{v};
        skip(v, boost::type<${type}_view>());
        return res;
    }
};
""").substitute({'type' : param_type(cls["name"]), 'skip' : skip, 'skip_impl' : skip_impl}))

def add_views(hout):
    for k in sort_dependencies():
        add_view(hout, local_types[k])

def add_visitors(hout):
    if not local_types:
        return
    fprintln(hout, "\n////// State holders")
    for k in local_types:
        handle_visitors_state(local_types[k], hout)
    fprintln(hout, "\n////// Nodes")
    for k in sort_dependencies():
        handle_visitors_nodes(local_types[k], hout)
    add_views(hout)

def handle_class(cls, hout, cout, namespaces=[], parent_template_param = []):
    add_to_types(cls, namespaces, parent_template_param)
    if "stub" in cls:
        return
    [ns, ns_open, ns_close] = set_namespace(namespaces)
    tpl = "template" in cls
    template_param_list = (cls["template"][0]["params"].asList() if tpl else [])
    template_param =  ", ".join(map(lambda a: a[0] + " " + a[1], template_param_list + parent_template_param)) if (template_param_list + parent_template_param) else ""
    template = "template <"+ template_param +">"  if tpl else ""
    template_class_param = "<" + ",".join(map(lambda a: a[1], template_param_list)) + ">" if tpl else ""
    temp_def = template_param if template_param != "" else ""

    if ns == "":
        name = cls["name"]
    else:
        name = ns + "::" + cls["name"]
    full_name = name + template_class_param
    for param in cls["members"]:
        if is_class(param):
            handle_class(param, hout, cout, namespaces + [cls["name"] + template_class_param], parent_template_param + template_param_list)
        elif is_enum(param):
            handle_enum(param, hout, cout, namespaces + [cls["name"] + template_class_param], parent_template_param + template_param_list)
    declear_methods(hout, name + template_class_param, temp_def)
    modifier = "final" in cls

    fprintln(cout, Template("""
$template
template <typename Output>
void serializer<$name>::write(Output& buf, const $name& obj) {""").substitute({'func' : SERIALIZER, 'name' : full_name, 'template': template}))
    if not modifier:
        fprintln(cout, Template("""  $set_size(buf, obj);""").substitute({'func' : SERIALIZER, 'set_size' : SETSIZE, 'name' : name, 'sizetype' : SIZETYPE}))
    for param in cls["members"]:
        if is_class(param) or is_enum(param):
            continue
        fprintln(cout, Template("""  static_assert(is_equivalent<decltype(obj.$var), $type>::value, "member value has a wrong type");
    $func(buf, obj.$var);""").substitute({'func' : SERIALIZER, 'var' : param["name"], 'type' : param_type(param["type"])}))
    fprintln(cout, "}")

    fprintln(cout, Template("""
$template
template <typename Input>
$name$temp_param serializer<$name$temp_param>::read(Input& buf) {""").substitute({'func' : DESERIALIZER, 'name' : name, 'template': template, 'temp_param' : template_class_param}))
    if not modifier:
        fprintln(cout, Template("""  $size_type size = $func(buf, boost::type<$size_type>());
  Input in = buf.read_substream(size - sizeof($size_type));""").substitute({'func' : DESERIALIZER, 'size_type' : SIZETYPE}))
    else:
        fprintln(cout, """  Input& in = buf;""")
    params = []
    for index, param in enumerate(cls["members"]):
        if is_class(param) or is_enum(param):
            continue
        local_param = "__local_" + str(index)
        if "attribute" in param:
            deflt = param["default"][0] if "default" in param else param["type"] + "()"
            fprintln(cout, Template("""  $typ $local = (in.size()>0) ?
    $func(in, boost::type<$typ>()) : $default;""").substitute({'func' : DESERIALIZER, 'typ': param_type(param["type"]), 'local' : local_param, 'default': deflt}))
        else:
            fprintln(cout, Template("""  $typ $local = $func(in, boost::type<$typ>());""").substitute({'func' : DESERIALIZER, 'typ': param_type(param["type"]), 'local' : local_param}))
        params.append("std::move(" + local_param + ")")
    fprintln(cout, Template("""
  $name$temp_param res {$params};
  return res;
}""").substitute({'name' : name, 'params': ", ".join(params), 'temp_param' : template_class_param}))


def handle_objects(tree, hout, cout, namespaces=[]):
    for obj in tree:
        if is_class(obj):
            handle_class(obj, hout, cout, namespaces)
        elif  is_enum(obj):
            handle_enum(obj, hout, cout, namespaces)
        elif  obj["type"] == "namespace":
            handle_objects(obj["content"], hout, cout, namespaces + [obj["name"]])
        else:
            print("unknown type ", obj, obj["type"])

def handle_types(tree, namespaces=[]):
    for obj in tree:
        if is_class(obj):
            add_to_types(obj, namespaces, [])
        elif is_enum(obj):
            pass
        elif obj["type"] == "namespace":
            handle_types(obj["content"], namespaces + [obj["name"]])
        else:
            print("unknown type ", obj, obj["type"])

def load_file(name):
    if config.o:
        cout = open(config.o.replace('.hh', '.impl.hh'), "w+")
        hout = open(config.o, "w+")
    else:
        cout = open(name.replace(EXTENSION, '.dist.impl.hh'), "w+")
        hout = open(name.replace(EXTENSION, '.dist.hh'), "w+")
    print_cw(hout)
    fprintln(hout, """
 /*
  * The generate code should be included in a header file after
  * The object definition
  */
    """)
    print_cw(cout)
    fprintln(hout, "#include \"serializer.hh\"\n")
    if config.ns != '':
        fprintln(cout, "namespace ", config.ns, " {")
    data = parse_file(name)
    if data:
        handle_types(data)
    add_visitors(cout)
    if data:
        handle_objects(data, hout, cout)
    if config.ns != '':
        fprintln(cout, "}")
    cout.close()
    hout.close()

def general_include(files):
    name = config.o if config.o else "serializer.dist.hh"
    cout = open(name.replace('.hh', '.impl.hh'), "w+")
    hout = open(name, "w+")
    print_cw(cout)
    print_cw(hout)
    for n in files:
        fprintln(hout, '#include "' + n +'"')
        fprintln(cout, '#include "' + n.replace(".dist.hh", '.dist.impl.hh') +'"')
    cout.close()
    hout.close()
if config.file:
    general_include(config.file)
elif config.f != '':
    load_file(config.f)
