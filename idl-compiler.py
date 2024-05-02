#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2016-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later

import argparse
import pyparsing as pp
from functools import reduce
import textwrap
from numbers import Number
from pprint import pformat
from copy import copy
from typing import List
import os.path

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

// SPDX-License-Identifier: AGPL-3.0-or-later

 /*
  * This is an auto-generated code, do not modify directly.
  */
#pragma once
 """)


###
### AST Nodes
###
class ASTBase:
    name: str
    ns_context: List[str]

    def __init__(self, name):
        self.name = name

    @staticmethod
    def combine_ns(namespaces):
        return "::".join(namespaces)

    def ns_qualified_name(self):
        return self.name if not self.ns_context \
            else self.combine_ns(self.ns_context) + "::" + self.name

class Include(ASTBase):
    '''AST node representing a single `include file/module`.'''

    def __init__(self, name):
        super().__init__(name)
        sfx = '.idl.hh'
        self.is_module = name.endswith(sfx)
        if self.is_module:
            self.module_name = self.name[0:-len(sfx)]

    def __str__(self):
        return f"<Include(name={self.name}, is_module={self.is_module})>"

    def __repr__(self):
        return self.__str__()

class BasicType(ASTBase):
    '''AST node that represents terminal grammar nodes for the non-template
    types, defined either inside or outside the IDL.

    These can appear either in the definition of the class fields or as a part of
    template types (template arguments).

    Basic type nodes can also be marked as `const` when used inside a template type,
    e.g. `lw_shared_ptr<const T>`. When an IDL-defined type `T` appears somewhere
    with a `const` specifier, an additional `serializer<const T>` specialization
    is generated for it.'''
    def __init__(self, name, is_const=False):
        super().__init__(name)
        self.is_const = is_const

    def __str__(self):
        return f"<BasicType(name={self.name}, is_const={self.is_const})>"

    def __repr__(self):
        return self.__str__()

    def to_string(self):
        if self.is_const:
            return 'const ' + self.name
        return self.name


class TemplateType(ASTBase):
    '''AST node representing template types, for example: `std::vector<T>`.

    These can appear either in the definition of the class fields or as a part of
    template types (template arguments).

    Such types can either be defined inside or outside the IDL.'''
    def __init__(self, name, template_parameters):
        super().__init__(name)
        # FIXME: dirty hack to translate non-type template parameters (numbers) to BasicType objects
        self.template_parameters = [
            t if isinstance(t, BasicType) or isinstance(t, TemplateType) else BasicType(name=str(t)) \
                for t in template_parameters]

    def __str__(self):
        return f"<TemplateType(name={self.name}, args={pformat(self.template_parameters)}>"

    def __repr__(self):
        return self.__str__()

    def to_string(self):
        res = self.name + '<'
        res += ', '.join([p.to_string() for p in self.template_parameters])
        res += '>'
        return res


class EnumValue(ASTBase):
    '''AST node representing a single `name=value` enumerator in the enum.

    Initializer part is optional, the same as in C++ enums.'''
    def __init__(self, name, initializer=None):
        super().__init__(name)
        self.initializer = initializer

    def __str__(self):
        return f"<EnumValue(name={self.name}, initializer={self.initializer})>"

    def __repr__(self):
        return self.__str__()


class EnumDef(ASTBase):
    '''AST node representing C++ `enum class` construct.

    Consists of individual initializers in form of `EnumValue` objects.
    Should have an underlying type explicitly specified.'''
    def __init__(self, name, underlying_type, members):
        super().__init__(name)
        self.underlying_type = underlying_type
        self.members = members

    def __str__(self):
        return f"<EnumDef(name={self.name}, underlying_type={self.underlying_type}, members={pformat(self.members)})>";

    def __repr__(self):
        return self.__str__()

    def serializer_write_impl(self, cout):
        name = self.ns_qualified_name()

        fprintln(cout, f"""
{self.template_declaration}
template <typename Output>
void serializer<{name}>::write(Output& buf, const {name}& v) {{
  serialize(buf, static_cast<{self.underlying_type}>(v));
}}""")


    def serializer_read_impl(self, cout):
        name = self.ns_qualified_name()

        fprintln(cout, f"""
{self.template_declaration}
template<typename Input>
{name} serializer<{name}>::read(Input& buf) {{
  return static_cast<{name}>(deserialize(buf, boost::type<{self.underlying_type}>()));
}}""")


class Attributes(ASTBase):
    ''' AST node for representing class and field attributes.

    The following attributes are supported:
     - `[[writable]]` class attribute, triggers generation of writers and views
       for a class.
     - `[[version id]] field attribute, marks that a field is available starting
       from a specific version.'''
    def __init__(self, attr_items=[]):
        super().__init__('attributes')
        self.attr_items = attr_items

    def __str__(self):
        return f"[[{', '.join([a for a in self.attr_items])}]]"

    def __repr__(self):
        return self.__str__()

    def empty(self):
        return not self.attr_items


class DataClassMember(ASTBase):
    '''AST node representing a data field in a class.

    Can optionally have a version attribute and a default value specified.'''
    def __init__(self, type, name, attribute=None, default_value=None):
        super().__init__(name)
        self.type = type
        self.attribute = attribute
        self.default_value = default_value

    def __str__(self):
        return f"<DataClassMember(type={self.type}, name={self.name}, attribute={self.attribute}, default_value={self.default_value})>"

    def __repr__(self):
        return self.__str__()


class FunctionClassMember(ASTBase):
    '''AST node representing getter function in a class definition.

    Can optionally have a version attribute and a default value specified.

    Getter functions should be used whenever it's needed to access private
    members of a class.'''
    def __init__(self, type, name, attribute=None, default_value=None):
        super().__init__(name)
        self.type = type
        self.attribute = attribute
        self.default_value = default_value

    def __str__(self):
        return f"<FunctionClassMember(type={self.type}, name={self.name}, attribute={self.attribute}, default_value={self.default_value})>"

    def __repr__(self):
        return self.__str__()


class ClassTemplateParam(ASTBase):
    '''AST node representing a single template argument of a class template
    definition, such as `typename T`.'''
    def __init__(self, typename, name):
        super().__init__(name)
        self.typename = typename

    def __str__(self):
        return f"<ClassTemplateParam(typename={self.typename}, name={self.name})>"

    def __repr__(self):
        return self.__str__()


class ClassDef(ASTBase):
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
        super().__init__(name)
        self.members = members
        self.final = final
        self.stub = stub
        self.attribute = attribute
        self.template_params = template_params

    def __str__(self):
        return f"<ClassDef(name={self.name}, members={pformat(self.members)}, final={self.final}, stub={self.stub}, attribute={self.attribute}, template_params={self.template_params})>"

    def __repr__(self):
        return self.__str__()

    def serializer_write_impl(self, cout):
        name = self.ns_qualified_name()
        full_name = name + self.template_param_names_str

        fprintln(cout, f"""
{self.template_declaration}
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


    def serializer_read_impl(self, cout):
        name = self.ns_qualified_name()

        fprintln(cout, f"""
{self.template_declaration}
template <typename Input>
{name}{self.template_param_names_str} serializer<{name}{self.template_param_names_str}>::read(Input& buf) {{
 return seastar::with_serialized_stream(buf, [] (auto& buf) {{""")
        if not self.members:
            if not self.final:
                fprintln(cout, f"""  {SIZETYPE} size = {DESERIALIZER}(buf, boost::type<{SIZETYPE}>());
  buf.skip(size - sizeof({SIZETYPE}));""")
        elif not self.final:
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
  {name}{self.template_param_names_str} res {{{", ".join(params)}}};
  return res;
 }});
}}""")


    def serializer_skip_impl(self, cout):
        name = self.ns_qualified_name()

        fprintln(cout, f"""
{self.template_declaration}
template <typename Input>
void serializer<{name}{self.template_param_names_str}>::skip(Input& buf) {{
 seastar::with_serialized_stream(buf, [] (auto& buf) {{""")
        if not self.final:
            fprintln(cout, f"""  {SIZETYPE} size = {DESERIALIZER}(buf, boost::type<{SIZETYPE}>());
  buf.skip(size - sizeof({SIZETYPE}));""")
        else:
            for m in get_members(self):
                full_type = param_view_type(m.type)
                fprintln(cout, f"  ser::skip(buf, boost::type<{full_type}>());")
        fprintln(cout, """ });\n}""")


class RpcVerbParam(ASTBase):
    """AST element representing a single argument in an RPC verb declaration.
    Consists of:
    * Argument type
    * Argument name (optional)
    * Additional attributes (only [[version]] attribute is supported).

    If the name is omitted, then this argument will have a placeholder name of form `_N`, where N is the index
    of the argument in the argument list for an RPC verb.

    If the [[version]] attribute is specified, then handler function signature for an RPC verb will contain this
    argument as an `rpc::optional<>`.
    If the [[unique_ptr]] attribute is specified then handler function signature for an RPC verb will contain this
    argument as an `foreign_ptr<unique_ptr<>>`
    If the [[lw_shared_ptr]] attribute is specified then handler function signature for an RPC verb will contain this
    argument as an `foreign_ptr<lw_shared_ptr<>>`
    If the [[ref]] attribute is specified the send function signature will contain this type as const reference"""
    def __init__(self, type, name, attributes=Attributes()):
        self.type = type
        self.name = name
        self.attributes = attributes

    def __str__(self):
        return f"<RpcVerbParam(type={self.type}, name={self.name}, attributes={self.attributes})>"

    def __repr__(self):
        return self.__str__()

    def is_optional(self):
        return True in [a.startswith('version') for a in self.attributes.attr_items]

    def is_lw_shared(self):
        return True in [a.startswith('lw_shared_ptr') for a in self.attributes.attr_items]

    def is_unique(self):
        return True in [a.startswith('unique_ptr') for a in self.attributes.attr_items]

    def is_ref(self):
        return True in [a.startswith('ref') for a in self.attributes.attr_items]


    def to_string(self):
        res = self.type.to_string()
        if self.is_optional():
            res = 'rpc::optional<' + res + '>'
        if self.name:
            res += ' '
            res += self.name
        return res

    def to_string_send_fn_signature(self):
        res = self.type.to_string()
        if self.is_ref():
            res = 'const ' + res + '&'
        if self.name:
            res += ' '
            res += self.name
        return res

    def to_string_handle_ret_value(self):
        res = self.type.to_string()
        if self.is_unique():
            res = 'foreign_ptr<std::unique_ptr<' + res + '>>'
        elif self.is_lw_shared():
            res = 'foreign_ptr<lw_shared_ptr<' + res + '>>'
        return res


class RpcVerb(ASTBase):
    """AST element representing an RPC verb declaration.

    `my_verb` RPC verb declaration corresponds to the
    `netw::messaging_verb::MY_VERB` enumeration value to identify the
    new RPC verb.

    For a given `idl_module.idl.hh` file, a registrator class named
    `idl_module_rpc_verbs` will be created if there are any RPC verbs
    registered within the IDL module file.

    These are the methods being created for each RPC verb:

            static void register_my_verb(netw::messaging_service* ms, std::function<return_value(args...)>&&);
            static future<> unregister_my_verb(netw::messaging_service* ms);
            static future<> send_my_verb(netw::messaging_service* ms, netw::msg_addr id, args...);

    Each method accepts a pointer to an instance of messaging_service
    object, which contains the underlying seastar RPC protocol
    implementation, that is used to register verbs and pass messages.

    There is also a method to unregister all verbs at once:

            static future<> unregister(netw::messaging_service* ms);

    The following attributes are supported when declaring an RPC verb
    in the IDL:

    - [[with_client_info]] - the handler will contain a const reference to
      an `rpc::client_info` as the first argument.
    - [[with_timeout]] - an additional time_point parameter is supplied
      to the handler function and send* method uses send_message_*_timeout
      variant of internal function to actually send the message.
      Incompatible with [[cancellable]].
    - [[cancellable]] - an additional abort_source& parameter is supplied
      to the handler function and send* method uses send_message_*_cancellable
      variant of internal function to actually send the message.
      Incompatible with [[with_timeout]].
    - [[one_way]] - the handler function is annotated by
      future<rpc::no_wait_type> return type to designate that a client
      doesn't need to wait for an answer.

    The `-> return_values` clause is optional for two-way messages. If omitted,
    the return type is set to be `future<>`.
    For one-way verbs, the use of return clause is prohibited and the
    signature of `send*` function always returns `future<>`."""
    def __init__(self, name, parameters, return_values, with_client_info, with_timeout, cancellable, one_way):
        super().__init__(name)
        self.params = parameters
        self.return_values = return_values
        self.with_client_info = with_client_info
        self.with_timeout = with_timeout
        self.cancellable = cancellable
        self.one_way = one_way

    def __str__(self):
        return f"<RpcVerb(name={self.name}, params={self.params}, return_values={self.return_values}, with_client_info={self.with_client_info}, with_timeout={self.with_timeout}, cancellable={self.cancellable}, one_way={self.one_way})>"

    def __repr__(self):
        return self.__str__()

    def send_function_name(self):
        send_fn = 'send_message'
        if self.one_way:
            send_fn += '_oneway'
        if self.with_timeout:
            send_fn += '_timeout'
        if self.cancellable:
            send_fn += '_cancellable'
        return send_fn

    def handler_function_return_values(self):
        if self.one_way:
            return 'future<rpc::no_wait_type>'
        if not self.return_values:
            return 'future<>'
        l = len(self.return_values)
        ret = 'rpc::tuple<' if l > 1 else ''
        for t in self.return_values:
            ret = ret + t.to_string_handle_ret_value() + ', '
        ret = ret[:-2]
        if l > 1:
            ret = ret + '>'
        return f"future<{ret}>"

    def send_function_return_type(self):
        if self.one_way or not self.return_values:
            return 'future<>'
        l = len(self.return_values)
        ret = 'rpc::tuple<' if l > 1 else ''
        for t in self.return_values:
            ret = ret + t.to_string() + ', '
        ret = ret[:-2]
        if l > 1:
            ret = ret + '>'
        return f"future<{ret}>"

    def messaging_verb_enum_case(self):
        return f'netw::messaging_verb::{self.name.upper()}'

    def handler_function_parameters_str(self):
        res = []
        if self.with_client_info:
            res.append(RpcVerbParam(type=BasicType(name='rpc::client_info&', is_const=True), name='info'))
        if self.with_timeout:
            res.append(RpcVerbParam(type=BasicType(name='rpc::opt_time_point'), name='timeout'))
        if self.params:
            res.extend(self.params)
        return ', '.join([p.to_string() for p in res])

    def send_function_signature_params_list(self, include_placeholder_names):
        res = 'netw::messaging_service* ms, netw::msg_addr id'
        if self.with_timeout:
            res += ', netw::messaging_service::clock_type::time_point timeout'
        if self.cancellable:
            res += ', abort_source& as'
        if self.params:
            for idx, p in enumerate(self.params):
                res += ', ' + p.to_string_send_fn_signature()
                if include_placeholder_names and not p.name:
                    res += f' _{idx + 1}'
        return res

    def send_message_argument_list(self):
        res = f'ms, {self.messaging_verb_enum_case()}, id'
        if self.with_timeout:
            res += ', timeout'
        if self.cancellable:
            res += ', as'
        if self.params:
            for idx, p in enumerate(self.params):
                res += ', ' + f'std::move({p.name if p.name else f"_{idx + 1}"})'
        return res

    def send_function_invocation(self):
        res = 'return ' + self.send_function_name()
        if not (self.one_way):
            res += '<' + self.send_function_return_type() + '>'
        res += '(' + self.send_message_argument_list() + ');'
        return res

class NamespaceDef(ASTBase):
    '''AST node representing a namespace scope.

    It has the same meaning as in C++ or other languages with similar facilities.

    A namespace can contain one of the following top-level constructs:
     - namespaces
     - class definitions
     - enum definitions'''
    def __init__(self, name, members):
        super().__init__(name)
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

def include_parse_action(tokens):
    return Include(name=tokens[0])

def template_type_parse_action(tokens):
    return TemplateType(name=tokens['template_name'], template_parameters=tokens["template_parameters"].asList())


def type_parse_action(tokens):
    if len(tokens) == 1:
        return tokens[0]
    # If we have two tokens in type parse action then
    # it's because we have BasicType production with `const`
    # NOTE: template types cannot have `const` modifier at the moment,
    # this wouldn't parse.
    tokens[1].is_const = True
    return tokens[1]


def enum_value_parse_action(tokens):
    initializer = None
    if len(tokens) == 2:
        initializer = tokens[1]
    return EnumValue(name=tokens[0], initializer=initializer)


def enum_def_parse_action(tokens):
    return EnumDef(name=tokens['name'], underlying_type=tokens['underlying_type'], members=tokens['enum_values'].asList())


def attributes_parse_action(tokens):
    items = []
    for attr_clause in tokens:
        # Split individual attributes inside each attribute clause by commas and strip extra whitespace characters
        items += [arg.strip() for arg in attr_clause.split(',')]
    return Attributes(attr_items=items)


def class_member_parse_action(tokens):
    member_name = tokens['name']
    raw_attrs = tokens['attributes']
    attribute = raw_attrs.attr_items[0] if not raw_attrs.empty() else None
    default = tokens['default'][0] if 'default' in tokens else None
    if not isinstance(member_name, str): # accessor function declaration
        return FunctionClassMember(type=tokens["type"], name=member_name[0], attribute=attribute, default_value=default)
    # data member
    return DataClassMember(type=tokens["type"], name=member_name, attribute=attribute, default_value=default)


def class_def_parse_action(tokens):
    is_final = 'final' in tokens
    is_stub = 'stub' in tokens
    class_members = tokens['members'].asList() if 'members' in tokens else []
    raw_attrs = tokens['attributes']
    attribute = raw_attrs.attr_items[0] if not raw_attrs.empty() else None
    template_params = None
    if 'template' in tokens:
        template_params = [ClassTemplateParam(typename=tp[0], name=tp[1]) for tp in tokens['template']]
    return ClassDef(name=tokens['name'], members=class_members, final=is_final, stub=is_stub, attribute=attribute, template_params=template_params)


def rpc_verb_param_parse_action(tokens):
    type = tokens['type']
    name = tokens['ident'] if 'ident' in tokens else None
    attrs = tokens['attrs']
    return RpcVerbParam(type=type, name=name, attributes=attrs)

def rpc_verb_return_val_parse_action(tokens):
    type = tokens['type']
    attrs = tokens['attrs']
    return RpcVerbParam(type=type, name='', attributes=attrs)

def rpc_verb_parse_action(tokens):
    name = tokens['name']
    raw_attrs = tokens['attributes']
    params = tokens['params'] if 'params' in tokens else []
    with_timeout = not raw_attrs.empty() and 'with_timeout' in raw_attrs.attr_items
    cancellable = not raw_attrs.empty() and 'cancellable' in raw_attrs.attr_items
    with_client_info = not raw_attrs.empty() and 'with_client_info' in raw_attrs.attr_items
    one_way = not raw_attrs.empty() and 'one_way' in raw_attrs.attr_items
    if one_way and 'return_values' in tokens:
        raise Exception(f"Invalid return type specification for one-way RPC verb '{name}'")
    if with_timeout and cancellable:
        raise Exception(f"Error in verb {name}: [[with_timeout]] cannot be used together with [[cancellable]] in the same verb")
    return RpcVerb(name=name, parameters=params, return_values=tokens.get('return_values'), with_client_info=with_client_info, with_timeout=with_timeout, cancellable=cancellable, one_way=one_way)


def namespace_parse_action(tokens):
    return NamespaceDef(name=tokens['name'], members=tokens['ns_members'].asList())


def parse_file(file_name):
    '''Parse the input from the file using IDL grammar syntax and generate AST'''

    number = pp.pyparsing_common.signed_integer
    identifier = pp.pyparsing_common.identifier

    include_kw = pp.Keyword('#include').suppress()
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
    dcolon = pp.Literal("::")
    ns_qualified_ident = pp.Combine(pp.Optional(dcolon) + pp.delimitedList(identifier, "::", combine=True))
    enum_lit = pp.Keyword('enum').suppress()
    ns = pp.Keyword("namespace").suppress()
    verb = pp.Keyword("verb").suppress()

    btype = ns_qualified_ident.copy()
    btype.setParseAction(basic_type_parse_action)

    type = pp.Forward()

    tmpl = ns_qualified_ident("template_name") + langle + pp.Group(pp.delimitedList(type | number))("template_parameters") + rangle
    tmpl.setParseAction(template_type_parse_action)

    type <<= tmpl | (pp.Optional(const) + btype)
    type.setParseAction(type_parse_action)

    include_stmt = include_kw - pp.QuotedString('"')
    include_stmt.setParseAction(include_parse_action)

    enum_class = enum_lit - cls

    enum_init = equals - number
    enum_value = identifier - pp.Optional(enum_init)
    enum_value.setParseAction(enum_value_parse_action)

    enum_values = lbrace - pp.delimitedList(enum_value) - pp.Optional(comma) - rbrace
    enum = enum_class - identifier("name") - colon - identifier("underlying_type") - enum_values("enum_values") + pp.Optional(semi)
    enum.setParseAction(enum_def_parse_action)

    content = pp.Forward()

    attrib = lbrack - lbrack - pp.SkipTo(']') - rbrack - rbrack
    opt_attributes = pp.ZeroOrMore(attrib)("attributes")
    opt_attributes.setParseAction(attributes_parse_action)

    default_value = equals - pp.SkipTo(';')
    member_name = pp.Combine(identifier - pp.Optional(lparen - rparen)("function_marker"))
    class_member = type("type") - member_name("name") - opt_attributes - pp.Optional(default_value)("default") - semi
    class_member.setParseAction(class_member_parse_action)

    template_param = pp.Group(identifier("type") - identifier("name"))
    template_def = template - langle - pp.delimitedList(template_param)("params") - rangle
    class_content = pp.Forward()
    class_def = pp.Optional(template_def)("template") + (cls | struct) - ns_qualified_ident("name") - \
        pp.Optional(final)("final") - pp.Optional(stub)("stub") - opt_attributes - \
        lbrace - pp.ZeroOrMore(class_content)("members") - rbrace - pp.Optional(semi)
    class_content <<= enum | class_def | class_member
    class_def.setParseAction(class_def_parse_action)

    rpc_verb_param = type("type") - pp.Optional(identifier)("ident") - opt_attributes("attrs")
    rpc_verb_param.setParseAction(rpc_verb_param_parse_action)
    rpc_verb_params = pp.delimitedList(rpc_verb_param)

    rpc_verb_return_val = type("type") - opt_attributes("attrs")
    rpc_verb_return_val.setParseAction(rpc_verb_return_val_parse_action)
    rpc_verb_return_vals = pp.delimitedList(rpc_verb_return_val)

    rpc_verb = verb - opt_attributes - identifier("name") - \
        lparen.suppress() - pp.Optional(rpc_verb_params("params")) - rparen.suppress() - \
        pp.Optional(pp.Literal("->").suppress() - rpc_verb_return_vals("return_values")) - pp.Optional(semi)
    rpc_verb.setParseAction(rpc_verb_parse_action)

    namespace = ns - identifier("name") - lbrace - pp.OneOrMore(content)("ns_members") - rbrace
    namespace.setParseAction(namespace_parse_action)

    content <<= include_stmt | enum | class_def | rpc_verb | namespace

    for varname in ("include_stmt", "enum", "class_def", "class_member", "content", "namespace", "template_def"):
        locals()[varname].setName(varname)

    rt = pp.OneOrMore(content)
    rt.ignore(pp.cppStyleComment)
    return rt.parseFile(file_name, parseAll=True)


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

    fprintln(hout, f"""
template <{template_param}>
struct serializer<const {name}> : public serializer<{name}>
{{}};
""")


def template_params_str(template_params):
    if not template_params:
        return ""
    return ", ".join(map(lambda param: param.typename + " " + param.name, template_params))


def handle_enum(enum, hout, cout):
    '''Generate serializer declarations and definitions for an IDL enum'''
    temp_def = template_params_str(enum.parent_template_params)
    name = enum.ns_qualified_name()
    declare_methods(hout, name, temp_def)

    enum.serializer_write_impl(cout)
    enum.serializer_read_impl(cout)


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
local_writable_types = {}
rpc_verbs = {}


def resolve_basic_type_ref(type: BasicType):
    if type.name not in local_types:
        raise KeyError(f"Failed to resolve type reference for '{type.name}'")
    return local_types[type.name]


def list_types(t):
    if isinstance(t, BasicType):
        return [t.name]
    elif isinstance(t, TemplateType):
        return reduce(lambda a, b: a + b, [list_types(p) for p in t.template_parameters])


def list_local_writable_types(t):
    return {l for l in list_types(t) if l in local_writable_types}


def is_basic_type(t):
    return isinstance(t, BasicType) and t.name not in local_writable_types


def is_local_writable_type(t):
    if isinstance(t, str): # e.g. `t` is a local class name
        return t in local_writable_types
    return t.name in local_writable_types


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


def get_variant_type(t):
    if is_variant(t):
        return "variant"
    return param_type(t)


def variant_to_member(template_parameters):
    return [DataClassMember(name=get_variant_type(x), type=x) for x in template_parameters if is_local_writable_type(x) or is_variant(x)]


def variant_info(cls, template_parameters):
    variant_info_cls = copy(cls) # shallow copy of cls
    variant_info_cls.members = variant_to_member(template_parameters)
    return variant_info_cls


stubs = set()


def is_stub(cls):
    return cls in stubs


def handle_visitors_state(cls, cout, classes=[]):
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
        if is_local_writable_type(m.type):
            handle_visitors_state(local_writable_types[param_type(m.type)], cout, member_class + [m.name])
        if is_variant(m.type):
            handle_visitors_state(variant_info(cls, m.type.template_parameters), cout, member_class + [m.name])


def get_dependency(cls):
    members = get_members(cls)
    return reduce(lambda a, b: a | b, [list_local_writable_types(m.type) for m in members], set())


def optional_add_methods(typ):
    res = reindent(4, """
    void skip()  {
        serialize(_out, false);
    }""")
    if is_basic_type(typ):
        added_type = typ
    elif is_local_writable_type(typ):
        added_type = param_type(typ) + "_view"
    else:
        print("non supported optional type ", typ)
        raise "non supported optional type " + param_type(typ)
    res = res + reindent(4, f"""
    void write(const {added_type}& obj) {{
        serialize(_out, true);
        serialize(_out, obj);
    }}""")
    if is_local_writable_type(typ):
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
    if not is_stub(typ.name) and is_local_writable_type(typ):
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
        ret += reindent(4, """
            template<typename AsyncSerializer>
            future<after_{base_state}__{name}<Output>> {name}{var_type}_gently(AsyncSerializer&& f) && {{
                {set_variant_index}
                return futurize_invoke(f, writer_of_{typename}<Output>(_out)).then([this] {{
                    {set_command}
                    return after_{base_state}__{name}<Output>{return_command};
                }});
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
        elif is_local_writable_type(typ.template_parameters[0]):
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
    elif is_local_writable_type(typ):
        res = res + add_param_writer_object(name, base_state, typ)
    elif is_variant(typ):
        for idx, p in enumerate(typ.template_parameters):
            if is_basic_type(p):
                res = res + add_param_writer_basic_type(name, base_state, p, "_" + param_type(p), idx, root_node)
            elif is_variant(p):
                res = res + add_param_writer_object(name, base_state, p, '_' + "variant", idx, root_node)
            elif is_local_writable_type(p):
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
        if is_local_writable_type(typ):
            handle_visitors_nodes(local_writable_types[param_type(typ)], cout, True, classes + [member.name, local_writable_types[param_type(typ)].name])
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


def add_nodes_when_needed(cout, member, base_state_name, parents, member_classes):
    if is_vector(member.type):
        add_vector_node(cout, member, base_state_name, base_state_name)
    elif is_variant(member.type):
        add_variant_nodes(cout, member, member.type, base_state_name, parents, member_classes)
    elif is_local_writable_type(member.type):
        handle_visitors_nodes(local_writable_types[member.type.name], cout, False, member_classes + [member.name])


def handle_visitors_nodes(cls, cout, variant_node=False, classes=[]):
    global writers
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
        add_node(cout, base_state_name, None, base_state_name, prefix, parents, add_end_method(parents, current_name, variant_node, classes), False, cls.final)
        return
    add_node(cout, base_state_name + "__" + get_member_name(members[-1].name), members[-1].type, base_state_name, "after_", base_state_name, add_end_method(parents, current_name, variant_node, classes))
    # Create writer and reader for include class
    if not variant_node:
        for member in get_dependency(cls):
            handle_visitors_nodes(local_writable_types[member], cout)
    for ind in reversed(range(1, len(members))):
        member = members[ind]
        add_nodes_when_needed(cout, member, base_state_name, parents, member_classes)
        variant_state = base_state_name + "__" + get_member_name(member.name) if is_variant(member.type) else base_state_name
        add_node(cout, base_state_name + "__" + get_member_name(members[ind - 1].name), member.type, variant_state, "after_", base_state_name, add_param_write(member, base_state_name), False)
    member = members[0]
    add_nodes_when_needed(cout, member, base_state_name, parents, member_classes)
    add_node(cout, base_state_name, member.type, base_state_name, prefix, parents, add_param_write(member, base_state_name, False, not classes), False, cls.final)


def register_local_type(cls):
    global local_types
    local_types[cls.name] = cls


def register_writable_local_type(cls):
    global local_writable_types
    global stubs
    if not cls.attribute or cls.attribute != 'writable':
        return
    local_writable_types[cls.name] = cls
    if cls.stub:
        stubs.add(cls.name)


def register_rpc_verb(verb):
    global rpc_verbs
    rpc_verbs[verb.name] = verb


def sort_dependencies():
    dep_tree = {}
    res = []
    for k in local_writable_types:
        cls = local_writable_types[k]
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
    if val in local_writable_types:
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


def element_type(t):
    assert isinstance(t, TemplateType)
    assert len(t.template_parameters) == 1
    assert t.name != "boost::variant" and t.name != "std::variant"
    return param_view_type(t.template_parameters[0])


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


def add_view(cout, cls):
    members = get_members(cls)
    for m in members:
        add_variant_read_size(cout, m.type)

    fprintln(cout, f"""struct {cls.name}_view {{
    utils::input_stream v;
    """)

    if not is_stub(cls.name) and is_local_writable_type(cls.name):
        fprintln(cout, reindent(4, f"""
            operator {cls.name}() const {{
               auto in = v;
               return deserialize(in, boost::type<{cls.name}>());
            }}
        """))

    skip = "" if cls.final else "ser::skip(in, boost::type<size_type>());"
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

        if is_vector(m.type):
            elem_type = element_type(m.type)
            fprintln(cout, reindent(4, """
                auto {name}() const {{
                  return seastar::with_serialized_stream(v, [] (auto& v) {{
                   auto in = v;
                   {skip}
                   return vector_deserializer<{elem_type}>(in);
                  }});
                }}
            """).format(f=DESERIALIZER, **locals()))
        else:
            fprintln(cout, reindent(4, """
                auto {name}() const {{
                  return seastar::with_serialized_stream(v, [this] (auto& v) -> decltype({f}(std::declval<utils::input_stream&>(), boost::type<{full_type}>())) {{
                   std::ignore = this;
                   auto in = v;
                   {skip}
                   return {deser};
                  }});
                }}
            """).format(f=DESERIALIZER, **locals()))

        skip = skip + f"\n       ser::skip(in, boost::type<{full_type}>());"

    fprintln(cout, "};")
    skip_impl = "auto& in = v;\n       " + skip if cls.final else "v.skip(read_frame_size(v));"
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
        add_view(cout, local_writable_types[k])


def add_visitors(cout):
    if not local_writable_types:
        return
    add_views(cout)
    fprintln(cout, "\n////// State holders")
    for k in local_writable_types:
        handle_visitors_state(local_writable_types[k], cout)
    fprintln(cout, "\n////// Nodes")
    for k in sort_dependencies():
        handle_visitors_nodes(local_writable_types[k], cout)


def handle_include(obj, hout, cout):
    '''Generate #include directives.
    '''

    if obj.is_module:
        fprintln(hout, f'#include "{obj.module_name}.dist.hh"')
        fprintln(cout, f'#include "{obj.module_name}.dist.impl.hh"')
    else:
        fprintln(hout, f'#include "{obj.name}"')

def handle_includes(tree, hout, cout):
    for obj in tree:
        if isinstance(obj, Include):
            handle_include(obj, hout, cout)
        else:
            pass

def handle_class(cls, hout, cout):
    '''Generate serializer class declarations and definitions for a class
    defined in IDL.
    '''
    if cls.stub:
        return
    is_tpl = cls.template_params is not None
    template_param_list = cls.template_params if is_tpl else []
    template_params = template_params_str(template_param_list + cls.parent_template_params)
    template_class_param = "<" + ",".join(map(lambda a: a.name, template_param_list)) + ">" if is_tpl else ""

    name = cls.ns_qualified_name()
    full_name = name + template_class_param
    # Handle sub-types: can be either enum or class
    for member in cls.members:
        if isinstance(member, ClassDef):
            handle_class(member, hout, cout)
        elif isinstance(member, EnumDef):
            handle_enum(member, hout, cout)
    declare_methods(hout, full_name, template_params)

    cls.serializer_write_impl(cout)
    cls.serializer_read_impl(cout)
    cls.serializer_skip_impl(cout)


def handle_objects(tree, hout, cout):
    '''Main generation procedure: traverse AST and generate serializers for
    classes/enums defined in the current IDL.
    '''
    for obj in tree:
        if isinstance(obj, ClassDef):
            handle_class(obj, hout, cout)
        elif isinstance(obj, EnumDef):
            handle_enum(obj, hout, cout)
        elif isinstance(obj, NamespaceDef):
            handle_objects(obj.members, hout, cout)
        elif isinstance(obj, RpcVerb):
            pass
        elif isinstance(obj, Include):
            pass
        else:
            print(f"Unknown type: {obj}")


def generate_rpc_verbs_declarations(hout, module_name):
    fprintln(hout, f"\n// RPC verbs defined in the '{module_name}' module\n")
    fprintln(hout, f'struct {module_name}_rpc_verbs {{')
    for name, verb in rpc_verbs.items():
        fprintln(hout, reindent(4, f'''static void register_{name}(netw::messaging_service* ms,
    std::function<{verb.handler_function_return_values()} ({verb.handler_function_parameters_str()})>&&);
static future<> unregister_{name}(netw::messaging_service* ms);
static {verb.send_function_return_type()} send_{name}({verb.send_function_signature_params_list(include_placeholder_names=False)});
'''))

    fprintln(hout, reindent(4, 'static future<> unregister(netw::messaging_service* ms);'))
    fprintln(hout, '};\n')


def generate_rpc_verbs_definitions(cout, module_name):
    fprintln(cout, f"\n// RPC verbs defined in the '{module_name}' module")
    for name, verb in rpc_verbs.items():
        fprintln(cout, f'''
void {module_name}_rpc_verbs::register_{name}(netw::messaging_service* ms,
        std::function<{verb.handler_function_return_values()} ({verb.handler_function_parameters_str()})>&& f) {{
    register_handler(ms, {verb.messaging_verb_enum_case()}, std::move(f));
}}

future<> {module_name}_rpc_verbs::unregister_{name}(netw::messaging_service* ms) {{
    return ms->unregister_handler({verb.messaging_verb_enum_case()});
}}

{verb.send_function_return_type()} {module_name}_rpc_verbs::send_{name}({verb.send_function_signature_params_list(include_placeholder_names=True)}) {{
    {verb.send_function_invocation()}
}}''')

    fprintln(cout, f'''
future<> {module_name}_rpc_verbs::unregister(netw::messaging_service* ms) {{
    return when_all_succeed({', '.join([f'unregister_{v}(ms)' for v in rpc_verbs.keys()])}).discard_result();
}}
''')


def generate_rpc_verbs(hout, cout, module_name):
    if not rpc_verbs:
        return
    generate_rpc_verbs_declarations(hout, module_name)
    generate_rpc_verbs_definitions(cout, module_name)


def handle_types(tree):
    '''Traverse AST and record all locally defined types, i.e. defined in
    the currently processed IDL file.
    '''
    for obj in tree:
        if isinstance(obj, ClassDef):
            register_local_type(obj)
            register_writable_local_type(obj)
        elif isinstance(obj, RpcVerb):
            register_rpc_verb(obj)
        elif isinstance(obj, EnumDef):
            pass
        elif isinstance(obj, NamespaceDef):
            handle_types(obj.members)
        elif isinstance(obj, Include):
            pass
        else:
            print(f"Unknown object type: {obj}")


def setup_additional_metadata(tree, ns_context = [], parent_template_params=[]):
    '''Cache additional metadata for each type declaration directly in the AST node.

    This currently includes namespace info and template parameters for the
    parent scope (applicable only to enums and classes).'''
    for obj in tree:
        if isinstance(obj, NamespaceDef):
            setup_additional_metadata(obj.members, ns_context + [obj.name])
        elif isinstance(obj, EnumDef):
            obj.ns_context = ns_context

            obj.parent_template_params = parent_template_params

            obj.template_declaration = "template <" + template_params_str(parent_template_params) + ">" \
                if parent_template_params else ""
        elif isinstance(obj, ClassDef):
            obj.ns_context = ns_context
            # need to account for nested types
            current_scope = obj.name
            if obj.template_params:
                # current scope name should consider template classes as well
                current_scope += "<" + ",".join(tp.name for tp in obj.template_params) + ">"

            obj.template_param_names_str = "<" + ",".join(map(lambda a: a.name, obj.template_params)) + ">" \
                if obj.template_params else ""

            obj.parent_template_params = parent_template_params

            obj.template_declaration = "template <" + template_params_str(obj.template_params + obj.parent_template_params) + ">" \
                if obj.template_params else ""

            nested_template_params = parent_template_params + obj.template_params if obj.template_params else []

            setup_additional_metadata(obj.members, ns_context + [current_scope], nested_template_params)


def load_file(name):
    cname = config.o.replace('.hh', '.impl.hh') if config.o else name.replace(EXTENSION, '.dist.impl.hh')
    hname = config.o or name.replace(EXTENSION, '.dist.hh')
    cout = open(cname, "w+")
    hout = open(hname, "w+")
    print_cw(hout)
    fprintln(hout, """
 /*
  * The generate code should be included in a header file after
  * The object definition
  */
    """)
    fprintln(hout, "#include \"serializer.hh\"\n")

    print_cw(cout)
    fprintln(cout, f"#include \"{os.path.basename(hname)}\"")
    fprintln(cout, "#include \"serializer_impl.hh\"")
    fprintln(cout, "#include \"serialization_visitors.hh\"")

    def maybe_open_namespace(printed=False):
        if config.ns != '' and not printed:
            fprintln(hout, f"namespace {config.ns} {{")
            fprintln(cout, f"namespace {config.ns} {{")
            printed = True
        return printed

    data = parse_file(name)
    if data:
        handle_includes(data, hout, cout)
        printed = maybe_open_namespace()
        setup_additional_metadata(data)
        handle_types(data)
        handle_objects(data, hout, cout)

        module_name = os.path.basename(name)
        module_name = module_name[:module_name.find('.')]
        generate_rpc_verbs(hout, cout, module_name)
    maybe_open_namespace(printed)
    add_visitors(cout)
    if config.ns != '':
        fprintln(hout, f"}} // {config.ns}")
        fprintln(cout, f"}} // {config.ns}")
    cout.close()
    hout.close()


def general_include(files):
    '''Write serialization-related header includes in the generated files'''
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
    parser.add_argument('file', nargs='*', help="combine one or more file names for the general include files")

    config = parser.parse_args()
    if config.file:
        general_include(config.file)
    elif config.f != '':
        load_file(config.f)
