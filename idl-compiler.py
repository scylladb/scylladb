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

    tmpl = pp.Group(btype + langle.suppress() + pp.Group(pp.delimitedList(type)) + rangle.suppress())
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
    namespace = pp.Group(ns.setResultsName("type") + identifier.setResultsName("name") + lbrace + pp.Group(pp.OneOrMore(content)).setResultsName("content") + rbrace)
    enum = pp.Group(enum_class.setResultsName("type") + identifier.setResultsName("name") + colon.suppress() + identifier.setResultsName("underline_type") + enum_values.setResultsName("enum_values") + pp.Optional(semi).suppress())
    default_value = equals.suppress() + pp.SkipTo(';')
    class_member = pp.Group(type.setResultsName("type") + member_name.setResultsName("name") + pp.Optional(attrib).setResultsName("attribute") + pp.Optional(default_value).setResultsName("default") + semi.suppress()).setResultsName("member")
    template_param = pp.Group(identifier.setResultsName("type") + identifier.setResultsName("name"))
    template_def = pp.Group(template + langle + pp.Group(pp.delimitedList(template_param)).setResultsName("params") + rangle)
    class_content = pp.Forward()
    class_def = pp.Group(pp.Optional(template_def).setResultsName("template") + (cls | struct).setResultsName("type") + with_colon.setResultsName("name") + pp.Optional(final) + pp.Optional(stub) + lbrace + pp.Group(pp.OneOrMore(class_content)).setResultsName("members") + rbrace + pp.Optional(semi))
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
template <typename Output$tmp_param>
void $ser_func(Output& buf, const $name& v);

template <typename Input$tmp_param>
$name $deser_func(Input& buf, boost::type<$name>);""").substitute({'ser_func': SERIALIZER, 'deser_func' : DESERIALIZER, 'name' : name, 'sizetype' : SIZETYPE, 'tmp_param' : template_param }))
    if config.ns != '':
        fprintln(hout, "}")

def handle_enum(enum, hout, cout, namespaces , parent_template_param = []):
    [ns, ns_open, ns_close] = set_namespace(namespaces)
    temp_def = ',' + ", ".join(map(lambda a: a[0] + " " + a[1], parent_template_param)) if parent_template_param else ""
    name = enum["name"] if ns == "" else ns + "::" + enum["name"]
    declear_methods(hout, name, temp_def)
    fprintln(cout, Template("""
template<typename Output$temp_def>
void $ser_func(Output& buf, const $name& v) {
  serialize(buf, static_cast<$type>(v));
}

template<typename Input$temp_def>
$name $deser_func(Input& buf, boost::type<$name>) {
  return  static_cast<$name>(deserialize(buf, boost::type<$type>()));
}""").substitute({'ser_func': SERIALIZER, 'deser_func' : DESERIALIZER, 'name' : name, 'size_type' : SIZETYPE, 'type': enum['underline_type'], 'temp_def' : temp_def}))


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

def handle_class(cls, hout, cout, namespaces=[], parent_template_param = []):
    if "stub" in cls:
        return
    [ns, ns_open, ns_close] = set_namespace(namespaces)
    tpl = "template" in cls
    template_param_list = (cls["template"][0]["params"].asList() if tpl else [])
    template_param =  ", ".join(map(lambda a: a[0] + " " + a[1], template_param_list + parent_template_param)) if (template_param_list + parent_template_param) else ""
    template = "template <"+ template_param +">\n"  if tpl else ""
    template_class_param = "<" + ",".join(map(lambda a: a[1], template_param_list)) + ">" if tpl else ""
    temp_def = ',' + template_param if template_param != "" else ""

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
template<typename Output$temp_def>
void $func(Output& buf, const $name& obj) {""").substitute({'func' : SERIALIZER, 'name' : full_name, 'temp_def': temp_def}))
    if not modifier:
        fprintln(cout, Template("""  $set_size(buf, obj);""").substitute({'func' : SERIALIZER, 'set_size' : SETSIZE, 'name' : name, 'sizetype' : SIZETYPE}))
    for param in cls["members"]:
        if is_class(param) or is_enum(param):
            continue
        fprintln(cout, Template("""  $func(buf, obj.$var);""").substitute({'func' : SERIALIZER, 'var' : param["name"]}))
    fprintln(cout, "}")

    fprintln(cout, Template("""
template<typename Input$temp_def>
$name$temp_param $func(Input& buf, boost::type<$name$temp_param>) {""").substitute({'func' : DESERIALIZER, 'name' : name, 'temp_def': temp_def, 'temp_param' : template_class_param}))
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
    if config.ns != '':
        fprintln(cout, "namespace ", config.ns, " {")
    data = parse_file(name)
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
