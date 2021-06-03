#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2016-present ScyllaDB
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

import argparse
import pyparsing as pp
from functools import reduce
import textwrap
from numbers import Number
from pprint import pformat
from copy import copy

EXTENSION = '.idl.hh'
READ_BUFF = 'input_buffer'
WRITE_BUFF = 'output_buffer'
SERIALIZER = 'serialize'
DESERIALIZER = 'deserialize'
SETSIZE = 'set_size'
SIZETYPE = 'size_type'


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
 * Copyright 2016-present ScyllaDB
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


###
### AST Nodes
###
class BasicType:
    '''AST node that represents terminal grammar nodes for the non-template
    types, defined either inside or outside the IDL.

    These can appear either in the definition of the class fields or as a part of
    template types (template arguments).

    Basic type nodes can also be marked as `const` when used inside a template type,
    e.g. `lw_shared_ptr<const T>`. When an IDL-defined type `T` appears somewhere
    with a `const` specifier, an additional `serializer<const T>` specialization
    is generated for it.'''
    def __init__(self, name, is_const=False):
        self.name = name
        self.is_const = is_const

    def __str__(self):
        return f"<BasicType(name={self.name}, is_const={self.is_const})>"

    def __repr__(self):
        return self.__str__()


class TemplateType:
    '''AST node representing template types, for example: `std::vector<T>`.

    These can appear either in the definition of the class fields or as a part of
    template types (template arguments).

    Such types can either be defined inside or outside the IDL.'''
    def __init__(self, name, template_parameters):
        self.name = name
        # FIXME: dirty hack to translate non-type template parameters (numbers) to BasicType objects
        self.template_parameters = [
            t if isinstance(t, BasicType) or isinstance(t, TemplateType) else BasicType(name=str(t)) \
                for t in template_parameters]

    def __str__(self):
        return f"<TemplateType(name={self.name}, args={pformat(self.template_parameters)}>"

    def __repr__(self):
        return self.__str__()


class EnumValue:
    '''AST node representing a single `name=value` enumerator in the enum.

    Initializer part is optional, the same as in C++ enums.'''
    def __init__(self, name, initializer=None):
        self.name = name
        self.initializer = initializer

    def __str__(self):
        return f"<EnumValue(name={self.name}, initializer={self.initializer})>"

    def __repr__(self):
        return self.__str__()


class EnumDef:
    '''AST node representing C++ `enum class` construct.

    Consists of individual initializers in form of `EnumValue` objects.
    Should have an underlying type explicitly specified.'''
    def __init__(self, name, underlying_type, members):
        self.name = name
        self.underlying_type = underlying_type
        self.members = members

    def __str__(self):
        return f"<EnumDef(name={self.name}, underlying_type={self.underlying_type}, members={pformat(self.members)})>";

    def __repr__(self):
        return self.__str__()

    def serializer_write_impl(self, cout, template_decl, namespaces):
        name = ns_qualified_name(self.name, namespaces)

        fprintln(cout, f"""
{template_decl}
template <typename Output>
void serializer<{name}>::write(Output& buf, const {name}& v) {{
  serialize(buf, static_cast<{self.underlying_type}>(v));
}}""")


    def serializer_read_impl(self, cout, template_decl, namespaces):
        name = ns_qualified_name(self.name, namespaces)

        fprintln(cout, f"""
{template_decl}
template<typename Input>
{name} serializer<{name}>::read(Input& buf) {{
  return static_cast<{name}>(deserialize(buf, boost::type<{self.underlying_type}>()));
}}""")


class Attribute:
    ''' AST node for representing class and field attributes.

    The following attributes are supported:
     - `[[writable]]` class attribute, triggers generation of writers and views
       for a class.
     - `[[version id]] field attribute, marks that a field is available starting
       from a specific version.'''
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"[[{self.name}]]"

    def __repr__(self):
        return self.__str__()


class DataClassMember:
    '''AST node representing a data field in a class.

    Can optionally have a version attribute and a default value specified.'''
    def __init__(self, type, name, attribute=None, default_value=None):
        self.type = type
        self.name = name
        self.attribute = attribute
        self.default_value = default_value

    def __str__(self):
        return f"<DataClassMember(type={self.type}, name={self.name}, attribute={self.attribute}, default_value={self.default_value})>"

    def __repr__(self):
        return self.__str__()


class FunctionClassMember:
    '''AST node representing getter function in a class definition.

    Can optionally have a version attribute and a default value specified.

    Getter functions should be used whenever it's needed to access private
    members of a class.'''
    def __init__(self, type, name, attribute=None, default_value=None):
        self.type = type
        self.name = name
        self.attribute = attribute
        self.default_value = default_value

    def __str__(self):
        return f"<FunctionClassMember(type={self.type}, name={self.name}, attribute={self.attribute}, default_value={self.default_value})>"

    def __repr__(self):
        return self.__str__()


class ClassTemplateParam:
    '''AST node representing a single template argument of a class template
    definition, such as `typename T`.'''
    def __init__(self, typename, name):
        self.typename = typename
        self.name = name

    def __str__(self):
        return f"<ClassTemplateParam(typename={self.typename}, name={self.name})>"

    def __repr__(self):
        return self.__str__()


class ClassDef:
    '''AST node representing a class definition. Can use either `class` or `struct`
    keyword to define a class.

    The following specifiers are allowed in a class declaration:
     - `final` -- if a class is marked with this keyword it will not contain a
       size argument. Final classes cannot be extended by a future version, so
       it should be used with care.
     - `stub` -- no code will be generated for the class, it's only there for
       documentation.
    Also it's possible to specify a `[[writable]]` attribute for a class, which
    means that writers and views will be generated for the class.

    Classes are also can be declared as template classes, much the same as in C++.
    In this case the template declaration syntax mimics C++ templates.'''
    def __init__(self, name, members, final, stub, attribute, template_params):
        self.name = name
        self.members = members
        self.final = final
        self.stub = stub
        self.attribute = attribute
        self.template_params = template_params

    def __str__(self):
        return f"<ClassDef(name={self.name}, members={pformat(self.members)}, final={self.final}, stub={self.stub}, attribute={self.attribute}, template_params={self.template_params})>"

    def __repr__(self):
        return self.__str__()

    def serializer_write_impl(self, cout, template_decl, template_class_param, namespaces):
        name = ns_qualified_name(self.name, namespaces)
        full_name = name + template_class_param

        fprintln(cout, f"""
{template_decl}
template <typename Output>
void serializer<{full_name}>::write(Output& buf, const {full_name}& obj) {{""")
        if not self.final:
            fprintln(cout, f"""  {SETSIZE}(buf, obj);""")
        for member in self.members:
            if isinstance(member, ClassDef) or isinstance(member, EnumDef):
                continue
            fprintln(cout, f"""  static_assert(is_equivalent<decltype(obj.{member.name}), {param_type(member.type)}>::value, "member value has a wrong type");
  {SERIALIZER}(buf, obj.{member.name});""")
        fprintln(cout, "}")


    def serializer_read_impl(self, cout, template_decl, template_class_param, namespaces):
        is_final = self.final
        name = ns_qualified_name(self.name, namespaces)

        fprintln(cout, f"""
{template_decl}
template <typename Input>
{name}{template_class_param} serializer<{name}{template_class_param}>::read(Input& buf) {{
 return seastar::with_serialized_stream(buf, [] (auto& buf) {{""")
        if not self.members:
            if not is_final:
                fprintln(cout, f"""  {SIZETYPE} size = {DESERIALIZER}(buf, boost::type<{SIZETYPE}>());
  buf.skip(size - sizeof({SIZETYPE}));""")
        elif not is_final:
            fprintln(cout, f"""  {SIZETYPE} size = {DESERIALIZER}(buf, boost::type<{SIZETYPE}>());
  auto in = buf.read_substream(size - sizeof({SIZETYPE}));""")
        else:
            fprintln(cout, """  auto& in = buf;""")
        params = []
        local_names = {}
        for index, param in enumerate(self.members):
            if isinstance(param, ClassDef) or isinstance(param, EnumDef):
                continue
            local_param = "__local_" + str(index)
            local_names[param.name] = local_param
            if param.attribute:
                deflt = param_type(param.type) + "()"
                if param.default_value:
                    deflt = param.default_value
                if deflt in local_names:
                    deflt = local_names[deflt]
                fprintln(cout, f"""  auto {local_param} = (in.size()>0) ?
    {DESERIALIZER}(in, boost::type<{param_type(param.type)}>()) : {deflt};""")
            else:
                fprintln(cout, f"""  auto {local_param} = {DESERIALIZER}(in, boost::type<{param_type(param.type)}>());""")
            params.append("std::move(" + local_param + ")")
        fprintln(cout, f"""
  {name}{template_class_param} res {{{", ".join(params)}}};
  return res;
 }});
}}""")


    def serializer_skip_impl(self, cout, template_decl, template_class_param, namespaces):
        is_final = self.final
        name = ns_qualified_name(self.name, namespaces)

        fprintln(cout, f"""
{template_decl}
template <typename Input>
void serializer<{name}{template_class_param}>::skip(Input& buf) {{
 seastar::with_serialized_stream(buf, [] (auto& buf) {{""")
        if not is_final:
            fprintln(cout, f"""  {SIZETYPE} size = {DESERIALIZER}(buf, boost::type<{SIZETYPE}>());
  buf.skip(size - sizeof({SIZETYPE}));""")
        else:
            for m in get_members(self):
                full_type = param_view_type(m.type)
                fprintln(cout, f"  ser::skip(buf, boost::type<{full_type}>());")
        fprintln(cout, """ });\n}""")


class NamespaceDef:
    '''AST node representing a namespace scope.

    It has the same meaning as in C++ or other languages with similar facilities.

    A namespace can contain one of the following top-level constructs:
     - namespaces
     - class definitions
     - enum definitions'''
    def __init__(self, name, members):
        self.name = name
        self.members = members

    def __str__(self):
        return f"<NamespaceDef(name={self.name}, members={pformat(self.members)})>"

    def __repr__(self):
        return self.__str__()

###
### Parse actions, which transform raw tokens into structured representation: specialized AST nodes
###
def basic_type_parse_action(tokens):
    return BasicType(name=tokens[0])


def template_type_parse_action(tokens):
    return TemplateType(name=tokens['template_name'], template_parameters=tokens["template_parameters"].asList())


# Will be used after parsing is complete to determine which local types
# have usages with `const` specifiers: depending on that we should generate
# a serializer specialization for `const` type too.
types_with_const_appearances = set()


def type_parse_action(tokens):
    if len(tokens) == 1:
        return tokens[0]
    # If we have two tokens in type parse action then
    # it's because we have BasicType production with `const`
    # NOTE: template types cannot have `const` modifier at the moment,
    # this wouldn't parse.
    tokens[1].is_const = True
    types_with_const_appearances.add(tokens[1].name)
    return tokens[1]


def enum_value_parse_action(tokens):
    initializer = None
    if len(tokens) == 2:
        initializer = tokens[1]
    return EnumValue(name=tokens[0], initializer=initializer)


def enum_def_parse_action(tokens):
    return EnumDef(name=tokens['name'], underlying_type=tokens['underlying_type'], members=tokens['enum_values'].asList())


def attribute_parse_action(tokens):
    return Attribute(name=tokens[0])


def class_member_parse_action(tokens):
    member_name = tokens['name']
    attribute = tokens['attribute'][0] if 'attribute' in tokens else None
    default = tokens['default'][0] if 'default' in tokens else None
    if not isinstance(member_name, str): # accessor function declaration
        return FunctionClassMember(type=tokens["type"], name=member_name[0], attribute=attribute, default_value=default)
    # data member
    return DataClassMember(type=tokens["type"], name=member_name, attribute=attribute, default_value=default)


def class_def_parse_action(tokens):
    is_final = 'final' in tokens
    is_stub = 'stub' in tokens
    class_members = tokens['members'].asList() if 'members' in tokens else []
    attribute = tokens['attribute'][0] if 'attribute' in tokens else None
    template_params = None
    if 'template' in tokens:
        template_params = [ClassTemplateParam(typename=tp[0], name=tp[1]) for tp in tokens['template']]
    return ClassDef(name=tokens['name'], members=class_members, final=is_final, stub=is_stub, attribute=attribute, template_params=template_params)


def namespace_parse_action(tokens):
    return NamespaceDef(name=tokens['name'], members=tokens['ns_members'].asList())


def parse_file(file_name):

    number = pp.pyparsing_common.signed_integer
    identifier = pp.pyparsing_common.identifier

    lbrace = pp.Literal('{').suppress()
    rbrace = pp.Literal('}').suppress()
    cls = pp.Keyword('class').suppress()
    colon = pp.Literal(":").suppress()
    semi = pp.Literal(";").suppress()
    langle = pp.Literal("<").suppress()
    rangle = pp.Literal(">").suppress()
    equals = pp.Literal("=").suppress()
    comma = pp.Literal(",").suppress()
    lparen = pp.Literal("(")
    rparen = pp.Literal(")")
    lbrack = pp.Literal("[").suppress()
    rbrack = pp.Literal("]").suppress()
    struct = pp.Keyword('struct').suppress()
    template = pp.Keyword('template').suppress()
    final = pp.Keyword('final')
    stub = pp.Keyword('stub')
    const = pp.Keyword('const')
    ns_qualified_ident = pp.delimitedList(identifier, "::", combine=True)
    enum_lit = pp.Keyword('enum').suppress()
    ns = pp.Keyword("namespace").suppress()

    btype = ns_qualified_ident.copy()
    btype.setParseAction(basic_type_parse_action)

    type = pp.Forward()

    tmpl = ns_qualified_ident("template_name") + langle + pp.Group(pp.delimitedList(type | number))("template_parameters") + rangle
    tmpl.setParseAction(template_type_parse_action)

    type <<= tmpl | (pp.Optional(const) + btype)
    type.setParseAction(type_parse_action)

    enum_class = enum_lit - cls

    enum_init = equals - number
    enum_value = identifier - pp.Optional(enum_init)
    enum_value.setParseAction(enum_value_parse_action)

    enum_values = lbrace - pp.delimitedList(enum_value) - pp.Optional(comma) - rbrace
    enum = enum_class - identifier("name") - colon - identifier("underlying_type") - enum_values("enum_values") + pp.Optional(semi)
    enum.setParseAction(enum_def_parse_action)

    content = pp.Forward()

    attrib = lbrack - lbrack - pp.SkipTo(']') - rbrack - rbrack
    attrib.setParseAction(attribute_parse_action)
    opt_attribute = pp.Optional(attrib)("attribute")

    default_value = equals - pp.SkipTo(';')
    member_name = pp.Combine(identifier - pp.Optional(lparen - rparen)("function_marker"))
    class_member = type("type") - member_name("name") - opt_attribute - pp.Optional(default_value)("default") - semi
    class_member.setParseAction(class_member_parse_action)

    template_param = pp.Group(identifier("type") - identifier("name"))
    template_def = template - langle - pp.delimitedList(template_param)("params") - rangle
    class_content = pp.Forward()
    class_def = pp.Optional(template_def)("template") + (cls | struct) - ns_qualified_ident("name") - \
        pp.Optional(final)("final") - pp.Optional(stub)("stub") - opt_attribute - \
        lbrace - pp.ZeroOrMore(class_content)("members") - rbrace - pp.Optional(semi)
    class_content <<= enum | class_def | class_member
    class_def.setParseAction(class_def_parse_action)

    namespace = ns - identifier("name") - lbrace - pp.OneOrMore(content)("ns_members") - rbrace
    namespace.setParseAction(namespace_parse_action)

    content <<= enum | class_def | namespace

    for varname in ("enum", "class_def", "class_member", "content", "namespace", "template_def"):
        locals()[varname].setName(varname)

    rt = pp.OneOrMore(content)
    rt.ignore(pp.cppStyleComment)
    return rt.parseFile(file_name, parseAll=True)


def combine_ns(namespaces):
    return "::".join(namespaces)


def declare_methods(hout, name, template_param=""):
    fprintln(hout, f"""
template <{template_param}>
struct serializer<{name}> {{
  template <typename Output>
  static void write(Output& buf, const {name}& v);

  template <typename Input>
  static {name} read(Input& buf);

  template <typename Input>
  static void skip(Input& buf);
}};
""")

    if name in types_with_const_appearances:
        fprintln(hout, f"""
template <{template_param}>
struct serializer<const {name}> : public serializer<{name}>
{{}};
""")


def ns_qualified_name(name, namespaces):
    return name if not namespaces else combine_ns(namespaces) + "::" + name


def template_params_str(template_params):
    if not template_params:
        return ""
    return ", ".join(map(lambda param: param.typename + " " + param.name, template_params))


def handle_enum(enum, hout, cout, namespaces, parent_template_param=[]):
    temp_def = template_params_str(parent_template_param)
    template_decl = "template <" + temp_def + ">" if temp_def else ""
    name = ns_qualified_name(enum.name, namespaces)
    declare_methods(hout, name, temp_def)

    enum.serializer_write_impl(cout, template_decl, namespaces)
    enum.serializer_read_impl(cout, template_decl, namespaces)


def join_template(template_params):
    return "<" + ", ".join([param_type(p) for p in template_params]) + ">"


def param_type(t):
    if isinstance(t, BasicType):
        return 'const ' + t.name if t.is_const else t.name
    elif isinstance(t, TemplateType):
        return t.name + join_template(t.template_parameters)


def flat_type(t):
    if isinstance(t, BasicType):
        return t.name
    elif isinstance(t, TemplateType):
        return (t.name + "__" + "_".join([flat_type(p) for p in t.template_parameters])).replace('::', '__')


local_types = {}


def list_types(t):
    if isinstance(t, BasicType):
        return [t.name]
    elif isinstance(t, TemplateType):
        return reduce(lambda a, b: a + b, [list_types(p) for p in t.template_parameters])


def list_local_types(t):
    return {l for l in list_types(t) if l in local_types}


def is_basic_type(t):
    return isinstance(t, BasicType) and t.name not in local_types


def is_local_type(t):
    if isinstance(t, str): # e.g. `t` is a local class name
        return t in local_types
    return t.name in local_types


def get_template_name(lst):
    return lst["template_name"] if not isinstance(lst, str) and len(lst) > 1 else None


def is_vector(t):
    return isinstance(t, TemplateType) and (t.name == "std::vector" or t.name == "utils::chunked_vector")


def is_variant(t):
    return isinstance(t, TemplateType) and (t.name == "boost::variant" or t.name == "std::variant")


def is_optional(t):
    return isinstance(t, TemplateType) and t.name == "std::optional"


created_writers = set()


def get_member_name(name):
    return name if not name.endswith('()') else name[:-2]


def get_members(cls):
    return [p for p in cls.members if not isinstance(p, ClassDef) and not isinstance(p, EnumDef)]


def is_final(cls):
    return cls.final


def get_variant_type(t):
    if is_variant(t):
        return "variant"
    return param_type(t)


def variant_to_member(template_parameters):
    return [DataClassMember(name=get_variant_type(x), type=x) for x in template_parameters if is_local_type(x) or is_variant(x)]


def variant_info(info, template_parameters):
    [cls, namespaces, parent_template_param] = info
    variant_info_cls = copy(cls) # shallow copy of cls
    variant_info_cls.members = variant_to_member(template_parameters)
    return [variant_info_cls, namespaces, parent_template_param]


stubs = set()


def is_stub(cls):
    return cls in stubs


def handle_visitors_state(info, cout, classes=[]):
    [cls, namespaces, parent_template_param] = info
    name = "__".join(classes) if classes else cls.name
    frame = "empty_frame" if cls.final else "frame"
    fprintln(cout, f"""
template<typename Output>
struct state_of_{name} {{
    {frame}<Output> f;""")
    if classes:
        local_state = "state_of_" + "__".join(classes[:-1]) + '<Output>'
        fprintln(cout, f"    {local_state} _parent;")
        if cls.final:
            fprintln(cout, f"    state_of_{name}({local_state} parent) : _parent(parent) {{}}")
    fprintln(cout, "};")
    members = get_members(cls)
    member_class = classes if classes else [cls.name]
    for m in members:
        if is_local_type(m.type):
            handle_visitors_state(local_types[param_type(m.type)], cout, member_class + [m.name])
        if is_variant(m.type):
            handle_visitors_state(variant_info(info, m.type.template_parameters), cout, member_class + [m.name])


def get_dependency(cls):
    members = get_members(cls)
    return reduce(lambda a, b: a | b, [list_local_types(m.type) for m in members], set())


def optional_add_methods(typ):
    res = reindent(4, """
    void skip()  {
        serialize(_out, false);
    }""")
    if is_basic_type(typ):
        added_type = typ
    elif is_local_type(typ):
        added_type = param_type(typ) + "_view"
    else:
        print("non supported optional type ", typ)
        raise "non supported optional type " + param_type(typ)
    res = res + reindent(4, f"""
    void write(const {added_type}& obj) {{
        serialize(_out, true);
        serialize(_out, obj);
    }}""")
    if is_local_type(typ):
        res = res + reindent(4, f"""
    writer_of_{param_type(typ)}<Output> write() {{
        serialize(_out, true);
        return {{_out}};
    }}""")
    return res


def vector_add_method(current, base_state):
    typ = current.type
    res = ""
    if is_basic_type(typ.template_parameters[0]):
        res = res + f"""
  void add_{current.name}({param_type(typ.template_parameters[0])} t)  {{
        serialize(_out, t);
        _count++;
  }}"""
    else:
        res = res + f"""
  writer_of_{flat_type(typ.template_parameters[0])}<Output> add() {{
        _count++;
        return {{_out}};
  }}"""
        res = res + f"""
  void add({param_view_type(typ.template_parameters[0])} v) {{
        serialize(_out, v);
        _count++;
  }}"""
    return res + f"""
  after_{base_state}__{current.name}<Output> end_{current.name}() && {{
        _size.set(_out, _count);
        return {{ _out, std::move(_state) }};
  }}

  vector_position pos() const {{
        return vector_position{{_out.pos(), _count}};
  }}

  void rollback(const vector_position& vp) {{
        _out.retract(vp.pos);
        _count = vp.count;
  }}"""


def add_param_writer_basic_type(name, base_state, typ, var_type="", var_index=None, root_node=False):
    if isinstance(var_index, Number):
        var_index = "uint32_t(" + str(var_index) + ")"
    create_variant_state = f"auto state = state_of_{base_state}__{name}<Output> {{ start_frame(_out), std::move(_state) }};" if var_index and root_node else ""
    set_variant_index = f"serialize(_out, {var_index});\n" if var_index is not None else ""
    set_command = ("_state.f.end(_out);" if not root_node else "state.f.end(_out);") if var_type != "" else ""
    return_command = "{ _out, std::move(_state._parent) }" if var_type != "" and not root_node else "{ _out, std::move(_state) }"

    allow_fragmented = False
    if typ.name in ['bytes', 'sstring']:
        typename = typ.name + '_view'
        allow_fragmented = True
    else:
        typename = 'const ' + typ.name + '&'

    writer = reindent(4, """
        after_{base_state}__{name}<Output> write_{name}{var_type}({typename} t) && {{
            {create_variant_state}
            {set_variant_index}
            serialize(_out, t);
            {set_command}
            return {return_command};
        }}""").format(**locals())
    if allow_fragmented:
        writer += reindent(4, """
        template<typename FragmentedBuffer>
        requires FragmentRange<FragmentedBuffer>
        after_{base_state}__{name}<Output> write_fragmented_{name}{var_type}(FragmentedBuffer&& fragments) && {{
            {set_variant_index}
            serialize_fragmented(_out, std::forward<FragmentedBuffer>(fragments));
            {set_command}
            return {return_command};
        }}""").format(**locals())
    return writer


def add_param_writer_object(name, base_state, typ, var_type="", var_index=None, root_node=False):
    var_type1 = "_" + var_type if var_type != "" else ""
    if isinstance(var_index, Number):
        var_index = "uint32_t(" + str(var_index) + ")"
    create_variant_state = f"auto state = state_of_{base_state}__{name}<Output> {{ start_frame(_out), std::move(_state) }};" if var_index and root_node else ""
    set_variant_index = f"serialize(_out, {var_index});\n" if var_index is not None else ""
    state = "std::move(_state)" if not var_index or not root_node else "std::move(state)"
    ret = reindent(4, """
        {base_state}__{name}{var_type1}<Output> start_{name}{var_type}() && {{
            {create_variant_state}
            {set_variant_index}
            return {{ _out, {state} }};
        }}
    """).format(**locals())
    if not is_stub(typ.name) and is_local_type(typ):
        ret += add_param_writer_basic_type(name, base_state, typ, var_type, var_index, root_node)
    if is_stub(typ.name):
        typename = typ.name
        set_command = "_state.f.end(_out);" if var_type != "" else ""
        return_command = "{ _out, std::move(_state._parent) }" if var_type != "" and not root_node else "{ _out, std::move(_state) }"
        ret += reindent(4, """
            template<typename Serializer>
            after_{base_state}__{name}<Output> {name}{var_type}(Serializer&& f) && {{
                {set_variant_index}
                f(writer_of_{typename}<Output>(_out));
                {set_command}
                return {return_command};
            }}""").format(**locals())
    return ret


def add_param_write(current, base_state, vector=False, root_node=False):
    typ = current.type
    res = ""
    name = get_member_name(current.name)
    if is_basic_type(typ):
        res = res + add_param_writer_basic_type(name, base_state, typ)
    elif is_optional(typ):
        res = res + reindent(4, f"""
    after_{base_state}__{name}<Output> skip_{name}() && {{
        serialize(_out, false);
        return {{ _out, std::move(_state) }};
    }}""")
        if is_basic_type(typ.template_parameters[0]):
            res = res + add_param_writer_basic_type(name, base_state, typ.template_parameters[0], "", "true")
        elif is_local_type(typ.template_parameters[0]):
            res = res + add_param_writer_object(name, base_state[0][1], typ, "", "true")
        else:
            print("non supported optional type ", typ.template_parameters[0])
    elif is_vector(typ):
        set_size = "_size.set(_out, 0);" if vector else "serialize(_out, size_type(0));"

        res = res + f"""
    {base_state}__{name}<Output> start_{name}() && {{
        return {{ _out, std::move(_state) }};
    }}

    after_{base_state}__{name}<Output> skip_{name}() && {{
        {set_size}
        return {{ _out, std::move(_state) }};
    }}
"""
    elif is_local_type(typ):
        res = res + add_param_writer_object(name, base_state, typ)
    elif is_variant(typ):
        for idx, p in enumerate(typ.template_parameters):
            if is_basic_type(p):
                res = res + add_param_writer_basic_type(name, base_state, p, "_" + param_type(p), idx, root_node)
            elif is_variant(p):
                res = res + add_param_writer_object(name, base_state, p, '_' + "variant", idx, root_node)
            elif is_local_type(p):
                res = res + add_param_writer_object(name, base_state, p, '_' + param_type(p), idx, root_node)
    else:
        print("something is wrong with type", typ)
    return res


def get_return_struct(variant_node, classes):
    if not variant_node:
        return classes
    if classes[-2] == "variant":
        return classes[:-2]
    return classes[:-1]


def add_variant_end_method(base_state, name, classes):

    return_struct = "after_" + base_state + '<Output>'
    return f"""
    {return_struct}  end_{name}() && {{
        _state.f.end(_out);
        _state._parent.f.end(_out);
        return {{ _out, std::move(_state._parent._parent) }};
    }}
"""


def add_end_method(parents, name, variant_node=False, return_value=True):
    if variant_node:
        return add_variant_end_method(parents, name, return_value)
    base_state = parents + "__" + name
    if return_value:
        return_struct = "after_" + base_state + '<Output>'
        return f"""
    {return_struct}  end_{name}() && {{
        _state.f.end(_out);
        return {{ _out, std::move(_state._parent) }};
    }}
"""
    return f"""
    void  end_{name}() {{
        _state.f.end(_out);
    }}
"""


def add_vector_placeholder():
    return """    place_holder<Output> _size;
    size_type _count = 0;"""


def add_node(cout, name, member, base_state, prefix, parents, fun, is_type_vector=False, is_type_final=False):
    struct_name = prefix + name
    if member and is_type_vector:
        vector_placeholder = add_vector_placeholder()
        vector_init = "\n            , _size(start_place_holder(out))"
    else:
        vector_placeholder = ""
        vector_init = ""
    if vector_init != "" or prefix == "":
        state_init = "_state{start_frame(out), std::move(state)}" if parents != base_state and not is_type_final else "_state(state)"
    else:
        if member and is_variant(member) and parents != base_state:
            state_init = "_state{start_frame(out), std::move(state)}"
        else:
            state_init = ""
    if prefix == "writer_of_":
        constructor = f"""{struct_name}(Output& out)
            : _out(out)
            , _state{{start_frame(out)}}{vector_init}
            {{}}"""
    elif state_init != "":
        constructor = f"""{struct_name}(Output& out, state_of_{parents}<Output> state)
            : _out(out)
            , {state_init}{vector_init}
            {{}}"""
    else:
        constructor = ""
    fprintln(cout, f"""
template<typename Output>
struct {struct_name} {{
    Output& _out;
    state_of_{base_state}<Output> _state;
    {vector_placeholder}
    {constructor}
    {fun}
}};""")


def add_vector_node(cout, member, base_state, parents):
    if member.type.template_parameters[0].name:
        add_template_writer_node(cout, member.type.template_parameters[0])
    add_node(cout, base_state + "__" + member.name, member.type, base_state, "", parents, vector_add_method(member, base_state), True)


optional_nodes = set()


def add_optional_node(cout, typ):
    global optional_nodes
    full_type = flat_type(typ)
    if full_type in optional_nodes:
        return
    optional_nodes.add(full_type)
    fprintln(cout, reindent(0, f"""
template<typename Output>
struct writer_of_{full_type} {{
    Output& _out;
    {optional_add_methods(typ.template_parameters[0])}
}};"""))


def add_variant_nodes(cout, member, param, base_state, parents, classes):
    par = base_state + "__" + member.name
    for typ in param.template_parameters:
        if is_local_type(typ):
            handle_visitors_nodes(local_types[param_type(typ)], cout, True, classes + [member.name, local_types[param_type(typ)][0].name])
        if is_variant(typ):
            name = base_state + "__" + member.name + "__variant"
            new_member = copy(member) # shallow copy
            new_member.type = typ
            new_member.name = "variant"
            return_struct = "after_" + par
            end_method = f"""
    {return_struct}<Output>  end_variant() && {{
        _state.f.end(_out);
        return {{ _out, std::move(_state._parent) }};
    }}
"""
            add_node(cout, name, None, base_state + "__" + member.name, "after_", name, end_method)
            add_variant_nodes(cout, new_member, typ, par, name, classes + [member.name])
            add_node(cout, name, typ, name, "", par, add_param_write(new_member, par))


writers = set()


def add_template_writer_node(cout, typ):
    if is_optional(typ):
        add_optional_node(cout, typ)


def add_nodes_when_needed(cout, info, member, base_state_name, parents, member_classes):
    if is_vector(member.type):
        add_vector_node(cout, member, base_state_name, base_state_name)
    elif is_variant(member.type):
        add_variant_nodes(cout, member, member.type, base_state_name, parents, member_classes)
    elif is_local_type(member.type):
        handle_visitors_nodes(local_types[member.type.name], cout, False, member_classes + [member.name])


def handle_visitors_nodes(info, cout, variant_node=False, classes=[]):
    global writers
    [cls, namespaces, parent_template_param] = info
    # for root node, only generate once
    if not classes:
        if cls.name in writers:
            return
        writers.add(cls.name)

    members = get_members(cls)
    if classes:
        base_state_name = "__".join(classes)
        if variant_node:
            parents = "__".join(classes[:-1])
        else:
            parents = "__".join(classes[:-1])
        current_name = classes[-1]
    else:
        base_state_name = cls.name
        current_name = cls.name
        parents = ""
    member_classes = classes if classes else [current_name]
    prefix = "" if classes else "writer_of_"
    if not members:
        add_node(cout, base_state_name, None, base_state_name, prefix, parents, add_end_method(parents, current_name, variant_node, classes), False, is_final(cls))
        return
    add_node(cout, base_state_name + "__" + get_member_name(members[-1].name), members[-1].type, base_state_name, "after_", base_state_name, add_end_method(parents, current_name, variant_node, classes))
    # Create writer and reader for include class
    if not variant_node:
        for member in get_dependency(cls):
            handle_visitors_nodes(local_types[member], cout)
    for ind in reversed(range(1, len(members))):
        member = members[ind]
        add_nodes_when_needed(cout, info, member, base_state_name, parents, member_classes)
        variant_state = base_state_name + "__" + get_member_name(member.name) if is_variant(member.type) else base_state_name
        add_node(cout, base_state_name + "__" + get_member_name(members[ind - 1].name), member.type, variant_state, "after_", base_state_name, add_param_write(member, base_state_name), False)
    member = members[0]
    add_nodes_when_needed(cout, info, member, base_state_name, parents, member_classes)
    add_node(cout, base_state_name, member.type, base_state_name, prefix, parents, add_param_write(member, base_state_name, False, not classes), False, is_final(cls))


def add_to_types(cls, namespaces, parent_template_param):
    global local_types
    global stubs
    if not cls.attribute or cls.attribute.name != 'writable':
        return
    local_types[cls.name] = [cls, namespaces, parent_template_param]
    if cls.stub:
        stubs.add(cls.name)


def sort_dependencies():
    dep_tree = {}
    res = []
    for k in local_types:
        [cls, namespaces, parent_template_param] = local_types[k]
        dep_tree[k] = get_dependency(cls)
    while (len(dep_tree) > 0):
        found = sorted(k for k in dep_tree if not dep_tree[k])
        res = res + [k for k in found]
        for k in found:
            dep_tree.pop(k)
        for k in dep_tree:
            if dep_tree[k]:
                dep_tree[k].difference_update(found)

    return res


def join_template_view(lst, more_types=[]):
    return "<" + ", ".join([param_view_type(l) for l in lst] + more_types) + ">"


def to_view(val):
    if val in local_types:
        return val + "_view"
    return val


def param_view_type(t):
    if isinstance(t, BasicType):
        return to_view(t.name)
    elif isinstance(t, TemplateType):
        additional_types = []
        if t.name == "boost::variant" or t.name == "std::variant":
            additional_types.append("unknown_variant_type")
        return t.name + join_template_view(t.template_parameters, additional_types)


read_sizes = set()


def add_variant_read_size(hout, typ):
    global read_sizes
    t = param_view_type(typ)
    if t in read_sizes:
        return
    if not is_variant(typ):
        return
    for p in typ.template_parameters:
        if is_variant(p):
            add_variant_read_size(hout, p)
    read_sizes.add(t)
    fprintln(hout, f"""
template<typename Input>
inline void skip(Input& v, boost::type<{t}>) {{
  return seastar::with_serialized_stream(v, [] (auto& v) {{
    size_type ln = deserialize(v, boost::type<size_type>());
    v.skip(ln - sizeof(size_type));
  }});
}}""")

    fprintln(hout, f"""
template<typename Input>
{t} deserialize(Input& v, boost::type<{t}>) {{
  return seastar::with_serialized_stream(v, [] (auto& v) {{
    auto in = v;
    deserialize(in, boost::type<size_type>());
    size_type o = deserialize(in, boost::type<size_type>());
    """)
    for index, param in enumerate(typ.template_parameters):
        fprintln(hout, f"""
    if (o == {index}) {{
        v.skip(sizeof(size_type)*2);
        return {t}(deserialize(v, boost::type<{param_view_type(param)}>()));
    }}""")
    fprintln(hout, f'    return {t}(deserialize(v, boost::type<unknown_variant_type>()));\n  }});\n}}')


def add_view(cout, info):
    [cls, namespaces, parent_template_param] = info
    members = get_members(cls)
    for m in members:
        add_variant_read_size(cout, m.type)

    fprintln(cout, f"""struct {cls.name}_view {{
    utils::input_stream v;
    """)

    if not is_stub(cls.name) and is_local_type(cls.name):
        fprintln(cout, reindent(4, f"""
            operator {cls.name}() const {{
               auto in = v;
               return deserialize(in, boost::type<{cls.name}>());
            }}
        """))

    skip = "" if is_final(cls) else "ser::skip(in, boost::type<size_type>());"
    local_names = {}
    for m in members:
        name = get_member_name(m.name)
        local_names[name] = "this->" + name + "()"
        full_type = param_view_type(m.type)
        if m.attribute:
            deflt = m.default_value if m.default_value else param_type(m.type) + "()"
            if deflt in local_names:
                deflt = local_names[deflt]
            deser = f"(in.size()>0) ? {DESERIALIZER}(in, boost::type<{full_type}>()) : {deflt}"
        else:
            deser = f"{DESERIALIZER}(in, boost::type<{full_type}>())"

        fprintln(cout, reindent(4, """
            auto {name}() const {{
              return seastar::with_serialized_stream(v, [this] (auto& v) -> decltype({f}(std::declval<utils::input_stream&>(), boost::type<{full_type}>())) {{
               auto in = v;
               {skip}
               return {deser};
              }});
            }}
        """).format(f=DESERIALIZER, **locals()))

        skip = skip + f"\n       ser::skip(in, boost::type<{full_type}>());"

    fprintln(cout, "};")
    skip_impl = "auto& in = v;\n       " + skip if is_final(cls) else "v.skip(read_frame_size(v));"
    if skip == "":
        skip_impl = ""

    fprintln(cout, f"""
template<>
struct serializer<{cls.name}_view> {{
    template<typename Input>
    static {cls.name}_view read(Input& v) {{
      return seastar::with_serialized_stream(v, [] (auto& v) {{
        auto v_start = v;
        auto start_size = v.size();
        skip(v);
        return {cls.name}_view{{v_start.read_substream(start_size - v.size())}};
      }});
    }}
    template<typename Output>
    static void write(Output& out, {cls.name}_view v) {{
        v.v.copy_to(out);
    }}
    template<typename Input>
    static void skip(Input& v) {{
      return seastar::with_serialized_stream(v, [] (auto& v) {{
        {skip_impl}
      }});
    }}
}};
""")


def add_views(cout):
    for k in sort_dependencies():
        add_view(cout, local_types[k])


def add_visitors(cout):
    if not local_types:
        return
    add_views(cout)
    fprintln(cout, "\n////// State holders")
    for k in local_types:
        handle_visitors_state(local_types[k], cout)
    fprintln(cout, "\n////// Nodes")
    for k in sort_dependencies():
        handle_visitors_nodes(local_types[k], cout)


def handle_class(cls, hout, cout, namespaces=[], parent_template_param=[]):
    add_to_types(cls, namespaces, parent_template_param)
    if cls.stub:
        return
    is_tpl = cls.template_params is not None
    template_param_list = cls.template_params if is_tpl else []
    template_params = template_params_str(template_param_list + parent_template_param)
    template_decl = "template <" + template_params + ">" if is_tpl else ""
    template_class_param = "<" + ",".join(map(lambda a: a.name, template_param_list)) + ">" if is_tpl else ""

    name = ns_qualified_name(cls.name, namespaces)
    full_name = name + template_class_param
    # Handle sub-types: can be either enum or class
    for member in cls.members:
        if isinstance(member, ClassDef):
            handle_class(member, hout, cout, namespaces + [cls.name + template_class_param], parent_template_param + template_param_list)
        elif isinstance(member, EnumDef):
            handle_enum(member, hout, cout, namespaces + [cls.name + template_class_param], parent_template_param + template_param_list)
    declare_methods(hout, full_name, template_params)

    cls.serializer_write_impl(cout, template_decl, template_class_param, namespaces)
    cls.serializer_read_impl(cout, template_decl, template_class_param, namespaces)
    cls.serializer_skip_impl(cout, template_decl, template_class_param, namespaces)


def handle_objects(tree, hout, cout, namespaces=[]):
    for obj in tree:
        if isinstance(obj, ClassDef):
            handle_class(obj, hout, cout, namespaces)
        elif isinstance(obj, EnumDef):
            handle_enum(obj, hout, cout, namespaces)
        elif isinstance(obj, NamespaceDef):
            handle_objects(obj.members, hout, cout, namespaces + [obj.name])
        else:
            print(f"Unknown type: {obj}")


def handle_types(tree, namespaces=[]):
    for obj in tree:
        if isinstance(obj, ClassDef):
            add_to_types(obj, namespaces, [])
        elif isinstance(obj, EnumDef):
            pass
        elif isinstance(obj, NamespaceDef):
            handle_types(obj.members, namespaces + [obj.name])
        else:
            print(f"Unknown object type: {obj}")


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
        fprintln(hout, f"namespace {config.ns} {{")
        fprintln(cout, f"namespace {config.ns} {{")
    data = parse_file(name)
    if data:
        handle_types(data)
        handle_objects(data, hout, cout)
    add_visitors(cout)
    if config.ns != '':
        fprintln(hout, f"}} // {config.ns}")
        fprintln(cout, f"}} // {config.ns}")
    cout.close()
    hout.close()


def general_include(files):
    name = config.o if config.o else "serializer.dist.hh"
    # Header file containing implementation of serializers and other supporting classes 
    cout = open(name.replace('.hh', '.impl.hh'), "w+")
    # Header file with serializer declarations
    hout = open(name, "w+")
    print_cw(cout)
    print_cw(hout)
    for n in files:
        fprintln(hout, '#include "' + n + '"')
        fprintln(cout, '#include "' + n.replace(".dist.hh", '.dist.impl.hh') + '"')
    cout.close()
    hout.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Generate serializer helper function""")

    parser.add_argument('-o', help='Output file', default='')
    parser.add_argument('-f', help='input file', default='')
    parser.add_argument('--ns', help="""namespace, when set function will be created
    under the given namespace""", default='')
    parser.add_argument('file', nargs='*', help="combine one or more file names for the genral include files")

    config = parser.parse_args()
    if config.file:
        general_include(config.file)
    elif config.f != '':
        load_file(config.f)
