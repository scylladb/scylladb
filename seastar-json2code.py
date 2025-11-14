#!/usr/bin/env python3

# C++ Code generation utility from Swagger definitions.
# This utility support Both the swagger 1.2 format
#    https://github.com/OAI/OpenAPI-Specification/blob/master/versions/1.2.md
# And the 2.0 format
#    https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md
#
# Swagger 2.0 is not only different in its structure (apis have moved, and
# models are now under definitions) It also moved from multiple file structure
# to a single file.
# To keep the multiple file support, each group of APIs will be placed in a single file
# Each group can have a .def.json file with its definitions (What used to be models)
# Because the APIs in definitions are snippets, they are not legal json objects
# and need to be formated as such so that a json parser would work.

import json
import sys
import re
import glob
import argparse
import os
import textwrap
from string import Template

parser = argparse.ArgumentParser(description="""Generate C++ class for json
handling from swagger definition""")

parser.add_argument('--outdir', help='the output directory', default='autogen')
parser.add_argument('-o', help='Output file', default='')
parser.add_argument('-f', help='input file', default='api-java.json')
parser.add_argument('-ns', help="""namespace when set struct will be created
under the namespace""", default='')
parser.add_argument('-jsoninc', help='relative path to the jsaon include',
                    default='json/')
parser.add_argument('-jsonns', help='set the json namespace', default='json')
parser.add_argument('-indir', help="""when set all json file in the given
directory will be parsed, do not use with -f""", default='')
parser.add_argument('-debug', help='debug level 0 -quite,1-error,2-verbose',
                    default='1', type=int)
parser.add_argument('-combined', help='set the name of the combined file',
                    default='autogen/pathautogen.ee')
parser.add_argument('--create-cc', dest='create_cc', action='store_true', default=False,
                    help='Put global variables in a .cc file')
config = parser.parse_args()


valid_vars = {'string': 'sstring', 'int': 'int', 'integer': 'int', 'double': 'double',
             'float': 'float', 'long': 'long', 'boolean': 'bool', 'char': 'char',
             'datetime': 'json::date_time'}

current_file = ''

spacing = "    "
def getitem(d, key, name):
    item = d.get(key)
    if item is None:
        raise Exception(f"'{key}' not found in {name}")
    return item

def fprint(f, *args):
    for arg in args:
        f.write(arg)

def fprintln(f, *args):
    for arg in args:
        f.write(arg)
    f.write('\n')


def open_namespace(f, ns=config.ns):
    fprintln(f, "namespace ", ns , ' {\n')


def close_namespace(f):
    fprintln(f, '}')


def add_include(f, includes):
    for include in includes:
        fprintln(f, '#include ', include)
    fprintln(f, "")

def trace_verbose(*params):
    if config.debug > 1:
        print(''.join(params))


def trace_err(*params):
    if config.debug > 0:
        print(current_file + ':' + ''.join(params))


def valid_type(param):
    if param in valid_vars:
        return valid_vars[param]
    trace_err("Type [", param, "] not defined")
    return param

# Map from json list types to the C++ implementing type
LIST_TO_IMPL = {"array": "json_list", "chunked_array": "json_chunked_list"}

def is_array_type(type: str):
    return type in LIST_TO_IMPL

def type_change(param: str, member):
    if is_array_type(param):
        if "items" not in member:
            trace_err("array without item declaration in ", param)
            return ""
        item = member["items"]
        if "type" in item:
            t = item["type"]
        elif "$ref" in item:
            t = item["$ref"]
        else:
            trace_err("array items with no type or ref declaration ", param)
            return ""
        return LIST_TO_IMPL[param] + "< " + valid_type(t) + " >"

    return "json_element< " + valid_type(param) + " >"



def print_ind_comment(f, ind, comment):
    fprintln(f, ind, "/**")
    fprintln(f, ind, " * ", comment)
    fprintln(f, ind, " */")

def print_comment(f, comment):
    print_ind_comment(f, spacing, comment)

def print_copyrights(f):
    fprintln(f, "/*")
    fprintln(f, "* Copyright (C) 2014 Cloudius Systems, Ltd.")
    fprintln(f, "*")
    fprintln(f, "* This work is open source software, licensed under the",
           " terms of the")
    fprintln(f, "* BSD license as described in the LICENSE f in the top-",
           "level directory.")
    fprintln(f, "*")
    fprintln(f, "*  This is an Auto-Generated-code  ")
    fprintln(f, "*  Changes you do in this file will be erased on next",
           " code generation")
    fprintln(f, "*/\n")


def print_h_file_headers(f, name):
    print_copyrights(f)
    fprintln(f, "#ifndef __JSON_AUTO_GENERATED_" + name)
    fprintln(f, "#define __JSON_AUTO_GENERATED_" + name + "\n")


def clean_param(param):
    match = re.match(r"^\{\s*([^\}]+)\s*}", param)
    if match:
        return [match.group(1), False]
    return [param, True]


def get_parameter_by_name(obj, name):
    for p in obj["parameters"]:
        if p["name"] == name:
            return p
    trace_err ("No Parameter declaration found for ", name)


def clear_path_ending(path):
    if not path or path[-1] != '/':
        return path
    return path[0:-1]


class Parameter:
    '''represents a parameter

    TODO: return an instance of Parameter from get_parameter_by_name()
    '''
    def __init__(self, definition):
        self.definition = definition

    @property
    def name(self):
        return self.definition['name']

    @property
    def is_required(self):
        # check if a parameter is query required.
        # It will return true if the required flag is set
        # and if it is a query parameter, both swagger 1.2 'paramType' and
        # swagger 2.0 'in' attributes are supported
        if "required" not in self.definition:
            return False
        if not self.definition["required"]:
            return False
        if self.definition.get("paramType") == "query":
            return True
        if self.definition.get("in") == "query":
            return True
        return False

    @property
    def enum(self):
        return self.definition.get('enum')


def add_path(f, path, details):
    if "summary" in details:
        print_comment(f, details["summary"])
    param_starts = path.find("{")
    if param_starts >= 0:
        path_reminder = path[param_starts:]
        vals = path.split("/")
        vals.reverse()
        fprintln(f, spacing, 'path_description::add_path("', clear_path_ending(vals.pop()),
           '",', details["method"], ',"', details["nickname"], '")')
        while vals:
            param, is_url = clean_param(vals.pop())
            if is_url:
                fprintln(f, spacing, '  ->pushurl("', param, '")')
            else:
                param_type = get_parameter_by_name(details, param)
                if ("allowMultiple" in param_type and
                    param_type["allowMultiple"]):
                    fprintln(f, spacing, '  ->pushparam("', param, '",true)')
                else:
                    fprintln(f, spacing, '  ->pushparam("', param, '")')
    else:
        fprintln(f, spacing, 'path_description::add_path("', clear_path_ending(path), '",',
           details["method"], ',"', details["nickname"], '")')
    if "parameters" in details:
        for param_definition in details["parameters"]:
            param = Parameter(param_definition)
            if param.is_required:
                fprintln(f, spacing, '  ->pushmandatory_param("', param.name, '")')
    fprintln(f, spacing, ";")


def not_first():
    '''
    Returns True when gets called for the first time, False otherwise

    used as the predicate parameter of textwrap.indent(), so the first
    line is not indented. this helps to preserve the Python code's logical
    indention in the template, and allows us to put something like::

      blah = textwrap.indent("""\
           foo bar
               blah blah
           foo bar
    """
    '''
    _is_first = True

    def should_indent(_):
        nonlocal _is_first
        first = _is_first
        _is_first = False
        return not first
    return should_indent


def generate_code_from_enum(nickname, type_name, enums):
    def indent_body(s, level):
        return textwrap.indent(s, level * '    ', not_first())

    enum_list = ',\n'.join(enums + ['NUM_ITEMS'])
    decl = Template('''\
    namespace ns_$nickname {
        enum class $type_name {
            $enum_list
        };
        $type_name str2$type_name(const sstring& str);
   }
   ''').substitute(nickname=nickname,
                   type_name=type_name,
                   enum_list=indent_body(enum_list, 3))

    name_list = ',\n'.join(f'"{enum}"' for enum in enums)
    parse_func = Template('''\
    $type_name str2$type_name(const sstring& str) {
        static const std::string_view arr[] = {
            $name_list
        };
        int i;
        for (i = 0; i < $num_enums; i++) {
            if (arr[i] == str) {
                return ($type_name)i;
            }
        }
        return ($type_name)i;
    }
    ''').substitute(type_name=type_name,
                    name_list=indent_body(name_list, 3),
                    num_enums=len(enums))

    return decl, parse_func


def add_operation(hfile, ccfile, path, oper):
    if "summary" in oper:
        print_ind_comment(hfile, '', oper["summary"])

    param_starts = path.find("{")
    base_url = path
    vals = []
    if param_starts >= 0:
        vals = path[param_starts:].split("/")
        vals.reverse()
        base_url = path[:param_starts]

    nickname = getitem(oper, "nickname", oper)
    if config.create_cc:
        fprintln(hfile, f'extern const path_description {nickname};')
        maybe_static = ''
    else:
        maybe_static = 'static '
    normalized_path = clear_path_ending(base_url)
    http_method = oper["method"]
    fprintln(ccfile, f'{maybe_static}const path_description {nickname}("{normalized_path}",{http_method},"{nickname}",')
    fprint(ccfile, '{')
    first = True
    while vals:
        path_param, is_url = clean_param(vals.pop())
        if path_param == "":
            continue
        if first:
            first = False
        else:
            fprint(ccfile, "\n,")
        if is_url:
            path_param = f"/{path_param}"
            component_type = 'FIXED_STRING'
        elif get_parameter_by_name(oper, path_param).get("allowMultiple",
                                                         False):
            component_type = 'PARAM_UNTIL_END_OF_PATH'
        else:
            component_type = 'PARAM'
        fprint(ccfile, f'{{"{path_param}", path_description::url_component_type::{component_type}}}')
    fprint(ccfile, '}')
    fprint(ccfile, ',{')
    enum_definitions = ""
    if "enum" in oper:
        enum_wrapper = create_enum_wrapper(nickname, "return_type", oper["enum"])
        enum_definitions = Template('''
namespace ns_$nickname {
$enum_wrapper
}
''').substitute(nickname=nickname, enum_wrapper=enum_wrapper.rstrip())
    funcs = ""
    required_query_params = []
    for param in oper.get("parameters", []):
        query_param = Parameter(param)
        if query_param.is_required:
            required_query_params.append(query_param)
        if query_param.enum is not None:
            enum_decl, parse_func = generate_code_from_enum(nickname,
                                                            query_param.name,
                                                            query_param.enum)
            enum_definitions += enum_decl
            funcs += parse_func
    fprint(ccfile, '\n,'.join(f'"{param.name}"' for param in required_query_params))
    fprintln(ccfile, '});')
    fprintln(hfile, enum_definitions)
    open_namespace(ccfile, f'ns_{nickname}')
    fprintln(ccfile, funcs)
    close_namespace(ccfile)


def get_base_name(param):
    return os.path.basename(param)


def is_model_valid(name, model):
    if name in valid_vars:
        return ""
    properties = getitem(model[name], "properties", name)
    for var in properties:
        type = getitem(properties[var], "type", name + ":" + var)
        if is_array_type(type):
            items = getitem(properties[var], "items", name + ":" + var)
            try:
                type = getitem(items, "type", name + ":" + var + ":items")
            except Exception as e:
                try:
                    type = getitem(items, "$ref", name + ":" + var + ":items")
                except:
                    raise e
        if type not in valid_vars:
            if type not in model:
                raise Exception("Unknown type '" + type + "' in Model '" + name + "'")
            return type
    valid_vars[name] = name
    return ""

def resolve_model_order(data):
    res = []
    models = set()
    for model_name in data:
        visited = set(model_name)
        missing = is_model_valid(model_name, data)
        resolved = missing == ''
        if not resolved:
            stack = [model_name]
            while not resolved:
                if missing in visited:
                    raise Exception("Cyclic dependency found: " + missing)
                missing_depends = is_model_valid(missing, data)
                if missing_depends == '':
                    if missing not in models:
                        res.append(missing)
                        models.add(missing)
                    resolved = len(stack) == 0
                    if not resolved:
                        missing = stack.pop()
                else:
                    stack.append(missing)
                    missing = missing_depends
        elif model_name not in models:
            res.append(model_name)
            models.add(model_name)
    return res


def create_enum_wrapper(model_name, name, values):
    enum_name = f"{model_name}_{name}"
    wrapper = name + "_wrapper"

    def indent_body(s, level):
        return textwrap.indent(s, level * '    ', not_first())

    case_clauses = "\n".join(
        Template('''case $enum_name::$enum_entry: return "\\"$enum_entry\\"";''').substitute(
            enum_name=enum_name, enum_entry=enum_entry) for enum_entry in values)
    res = Template("""\
    virtual std::string to_json() const {
        switch(v) {
        $case_clauses
        default: return "\\"Unknown\\"";
        }
    }""").substitute(wrapper=wrapper,
                     case_clauses=indent_body(case_clauses, 2))

    case_clauses = "\n".join(
        Template("""case T::$enum_entry: v = $enum_name::$enum_entry; break;""").substitute(
            enum_name=enum_name, enum_entry=enum_entry) for enum_entry in values)
    res += Template("""
    template<class T>
    $wrapper(const T& _v) {
        switch(_v) {
        $case_clauses
        default: v = $enum_name::NUM_ITEMS;
        }
    }""").substitute(wrapper=wrapper,
                     case_clauses=indent_body(case_clauses, 2),
                     enum_name=enum_name)

    case_clauses = "\n".join(
        Template('''case $enum_name::$enum_entry: return T::$enum_entry;''').substitute(
            enum_name=enum_name, enum_entry=enum_entry) for enum_entry in values)
    res += Template("""
    template<class T>
    operator T() const {
        switch(v) {
        $case_clauses
        default: return T::$value;
        }
    }""").substitute(case_clauses=indent_body(case_clauses, 2),
                     enum_name=enum_name,
                     value=values[0])

    res += Template("""
    typedef typename std::underlying_type<$enum_name>::type pos_type;
    $wrapper& operator++() {
        v = static_cast<$enum_name>(static_cast<pos_type>(v) + 1);
        return *this;
    }
    $wrapper & operator++(int) {
        return ++(*this);
    }
    bool operator==(const  $wrapper& c) const {
        return v == c.v;
    }
    bool operator!=(const $wrapper& c) const {
        return v != c.v;
    }
    bool operator<=(const $wrapper& c) const {
        return static_cast<pos_type>(v) <= static_cast<pos_type>(c.v);
    }
    std::make_signed_t<pos_type> operator-(const $wrapper& c) const {
        return static_cast<pos_type>(v) - static_cast<pos_type>(c.v);
    }
    static $wrapper begin() {
        return $wrapper($enum_name::$value);
    }
    static $wrapper end() {
        return $wrapper($enum_name::NUM_ITEMS);
    }
    static auto /* iota_view */ all_items() {
        return std::ranges::iota_view<$wrapper, $wrapper>(begin(), end());
    }
    $enum_name v;""").substitute(enum_name=enum_name,
                                 wrapper=wrapper,
                                 value=values[0])
    enum_wrapper = Template("""
    enum class $enum_name { $enumerators, NUM_ITEMS };
    struct $wrapper : public json::jsonable {
        $wrapper() = default;
        $body
    };
    """).substitute(enum_name=enum_name,
                    enumerators=", ".join(values),
                    wrapper=wrapper,
                    body=indent_body(res, 1).lstrip())
    return enum_wrapper.lstrip('\n')


def to_operation(opr, data):
    data["method"] = opr.upper()
    data["nickname"] = data["operationId"]
    return data

def to_path(path, data):
    data["operations"] = [to_operation(k, data[k]) for k in data]
    data["path"] = path

    return data

def create_h_file(data, hfile_name, api_name, init_method, base_api):
    if config.o != '':
        final_hfile_name = config.o
    else:
        final_hfile_name = config.outdir + "/" + hfile_name
    hfile = open(final_hfile_name, "w")

    if config.create_cc:
        ccfile = open(final_hfile_name.rstrip('.hh') + ".cc", "w")
        add_include(ccfile, ['"{}"'.format(final_hfile_name)])
        open_namespace(ccfile, "seastar")
        open_namespace(ccfile, "httpd")
        open_namespace(ccfile, api_name)
    else:
        ccfile = hfile
    print_h_file_headers(hfile, api_name)
    add_include(hfile, ['<seastar/core/sstring.hh>',
                        '<seastar/json/json_elements.hh>',
                        '<seastar/http/json_path.hh>'])

    add_include(hfile, ['<iostream>', '<ranges>'])
    open_namespace(hfile, "seastar")
    open_namespace(hfile, "httpd")
    open_namespace(hfile, api_name)

    def indent(s):
        return textwrap.indent(s.rstrip(), '        ', not_first())

    if "models" in data:
        models_order = resolve_model_order(data["models"])
        for model_name in models_order:
            model = data["models"][model_name]
            if 'description' in model:
                print_ind_comment(hfile, "", model["description"])
            fprintln(hfile, "struct ", model_name, " : public json::json_base {")
            member_init = ''
            member_assignment = ''
            member_move_assignment = ''
            member_copy = ''
            for member_name in model["properties"]:
                member = model["properties"][member_name]
                if "description" in member:
                    print_comment(hfile, member["description"])
                if "enum" in member:
                    fprintln(hfile, create_enum_wrapper(model_name, member_name, member["enum"]))
                    fprintln(hfile, f"    {config.jsonns}::json_element<{member_name}_wrapper> {member_name};\n")
                else:
                    type_name = type_change(member["type"], member)
                    fprintln(hfile, f"    {config.jsonns}::{type_name} {member_name};\n")
                member_init += f'add(&{member_name}, "{member_name}");\n'
                member_assignment += f'{member_name} = e.{member_name};\n'
                member_move_assignment += f'{member_name} = std::move(e.{member_name});\n'
                member_copy += f'e.{member_name} = {member_name} ;\n'

            functions = Template('''
    void register_params() {
        $member_init
    }
    $model_name() {
        register_params();
    }
    $model_name(const $model_name& e) {
        register_params();
        $member_assignment
    }
    $model_name($model_name&& e) {
        register_params();
        $member_move_assignment
    }
    template<class T>
    $model_name& operator=(const T& e) {
        $member_assignment
        return *this;
    }
    $model_name& operator=(const $model_name& e) {
        $member_assignment
        return *this;
    }
    $model_name& operator=($model_name&& e) {
        $member_move_assignment
        return *this;
    }
    template<class T>
    $model_name& update(T& e) {
        $member_copy
        return *this;
    }''').substitute(model_name=model_name,
                     member_init=indent(member_init),
                     member_assignment=indent(member_assignment),
                     member_move_assignment=indent(member_move_assignment),
                     member_copy=indent(member_copy))
            fprintln(hfile, functions.lstrip('\n'))
            fprintln(hfile, "};\n\n")

 #   print_ind_comment(hfile, "", "Initialize the path")
#    fprintln(hfile, init_method + "(const std::string& description);")
    fprintln(hfile, 'static const sstring name = "', base_api, '";')
    for item in data["apis"]:
        path = item["path"]
        for oper in item.get("operations", []):
            add_operation(hfile, ccfile, path, oper)

    close_namespace(hfile)
    close_namespace(hfile)
    close_namespace(hfile)
    if config.create_cc:
        close_namespace(ccfile)
        close_namespace(ccfile)
        close_namespace(ccfile)

    hfile.write("#endif //__JSON_AUTO_GENERATED_HEADERS\n")
    hfile.close()

def remove_leading_comma(data):
    return re.sub(r'^\s*,', '', data)

def format_as_json_object(data):
    return "{" + remove_leading_comma(data) + "}"

def check_for_models(data, param):
    model_name = param.replace(".json", ".def.json")
    if not os.path.isfile(model_name):
        return
    try:
        with open(model_name) as myfile:
            json_data = myfile.read()
            def_data = json.loads(format_as_json_object(json_data))
            data["models"] = def_data
    except Exception as e:
        type, value, tb = sys.exc_info()
        print("Bad formatted JSON definition file '" + model_name + "' error ", value.message)
        sys.exit(-1)

def set_apis(data):
    return {"apis": [to_path(p, data[p]) for p in data]}

def parse_file(param, combined):
    global current_file
    trace_verbose("parsing ", param, " file")
    with open(param) as myfile:
        json_data = myfile.read()
    try:
        data = json.loads(json_data)
    except Exception as e:
        try:
            # the passed data is not a valid json, so maybe its a swagger 2.0
            # snippet, format it as json and try again
            # set_apis and check_for_models will create an object with a similiar format
            # to a swagger 1.2 so the code generation would work
            data = set_apis(json.loads(format_as_json_object(json_data)))
            check_for_models(data, param)
        except:
            # The problem is with the file,
            # just report the error and exit.
            type, value, tb = sys.exc_info()
            print("Bad formatted JSON file '" + param + "' error ", value.message)
            sys.exit(-1)
    try:
        base_file_name = get_base_name(param)
        current_file = base_file_name
        hfile_name = base_file_name + ".hh"
        api_name = base_file_name.replace('.', '_')
        base_api = base_file_name.replace('.json', '')
        init_method = "void " + api_name + "_init_path"
        trace_verbose("creating ", hfile_name)
        if (combined):
            fprintln(combined, '#include "', base_file_name, ".cc", '"')
        create_h_file(data, hfile_name, api_name, init_method, base_api)
    except Exception as e:
        print("Error while parsing JSON file '" + param + "' error " + str(e))
        sys.exit(-1)

if "indir" in config and config.indir != '':
    combined = open(config.combined, "w")
    for f in glob.glob(os.path.join(config.indir, "*.json")):
        parse_file(f, combined)
else:
    parse_file(config.f, None)
