#!/usr/bin/env python
import json
import sys
import re
import glob
import argparse
import os

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
config = parser.parse_args()


valid_vars = {'string': 'sstring', 'int': 'int', 'double': 'double',
             'float': 'float', 'long': 'long', 'boolean': 'bool', 'char': 'char',
             'datetime': 'json::date_time'}

current_file = ''

spacing = "    "
def getitem(d, key, name):
    if key in d:
        return d[key]
    else:
        raise Exception("'" + key + "' not found in " + name)

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


def type_change(param, member):
    if param == "array":
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
        return "json_list< " + valid_type(t) + " >"
    return "json_element< " + valid_type(param) + " >"



def print_ind_comment(f, ind, *params):
    fprintln(f, ind, "/**")
    for s in params:
        fprintln(f, ind, " * ", s)
    fprintln(f, ind, " */")

def print_comment(f, *params):
    print_ind_comment(f, spacing, *params)

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
    match = re.match(r"(^[^\}]+)\s*}", param)
    if match:
        return match.group(1)
    return param


def get_parameter_by_name(obj, name):
    for p in obj["parameters"]:
        if p["name"] == name:
            return p
    trace_err ("No Parameter declaration found for ", name)


def clear_path_ending(path):
    if not path or path[-1] != '/':
        return path
    return path[0:-1]


def add_path(f, path, details):
    if "summary" in details:
        print_comment(f, details["summary"])

    if "{" in path:
        vals = path.split("{")
        vals.reverse()
        fprintln(f, spacing, 'path_description::add_path("', clear_path_ending(vals.pop()),
           '",', details["method"], ',"', details["nickname"], '")')
        while vals:
            param = clean_param(vals.pop())
            param_type = get_parameter_by_name(details, param)
            if ("allowMultiple" in param_type and
                param_type["allowMultiple"] == True):
                fprintln(f, spacing, '  ->pushparam("', param, '",true)')
            else:
                fprintln(f, spacing, '  ->pushparam("', param, '")')
    else:
        fprintln(f, spacing, 'path_description::add_path("', clear_path_ending(path), '",',
           details["method"], ',"', details["nickname"], '")')
    if "parameters" in details:
        for param in details["parameters"]:
            if "required" in param and param["required"] and  param["paramType"] == "query":
                fprintln(f, spacing, '  ->pushmandatory_param("', param["name"], '")')
    fprintln(f, spacing, ";")


def get_base_name(param):
    return os.path.basename(param)


def is_model_valid(name, model):
    if name in valid_vars:
        return ""
    properties = getitem(model[name], "properties", name)
    for var in properties:
        type = getitem(properties[var], "type", name + ":" + var)
        if type == "array":
            type = getitem(getitem(properties[var], "items", name + ":" + var), "type", name + ":" + var + ":items")
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

def create_h_file(data, hfile_name, api_name, init_method, base_api):
    if config.o != '':
        hfile = open(config.o, "w")
    else:
        hfile = open(config.outdir + "/" + hfile_name, "w")
    print_h_file_headers(hfile, api_name)
    add_include(hfile, ['"core/sstring.hh"', '"' + config.jsoninc +
                       'json_elements.hh"', '"http/json_path.hh"'])

    add_include(hfile, ['<iostream>', '<boost/range/irange.hpp>'])
    open_namespace(hfile, "httpd")
    open_namespace(hfile, api_name)

    if "models" in data:
        models_order = resolve_model_order(data["models"])
        for model_name in models_order:
            model = data["models"][model_name]
            if 'description' in model:
                print_ind_comment(hfile, "", model["description"])
            fprintln(hfile, "struct ", model_name, " : public json::json_base {")
            member_init = ''
            member_assignment = ''
            member_copy = ''
            for member_name in model["properties"]:
                member = model["properties"][member_name]
                if "description" in member:
                    print_comment(hfile, member["description"])
                if "enum" in member:
                    enum_name = model_name + "_" + member_name
                    fprintln(hfile, "  enum class ", enum_name, " {")
                    for enum_entry in member["enum"]:
                        fprintln(hfile, "  ", enum_entry, ", ")
                    fprintln(hfile, "NUM_ITEMS};")
                    wrapper = member_name + "_wrapper"
                    fprintln(hfile, "  struct ", wrapper, " : public jsonable  {")
                    fprintln(hfile, "    ", wrapper, "() = default;")
                    fprintln(hfile, "    virtual std::string to_json() const {")
                    fprintln(hfile, "      switch(v) {")
                    for enum_entry in member["enum"]:
                        fprintln(hfile, "      case ", enum_name, "::", enum_entry, ": return \"\\\"", enum_entry, "\\\"\";")
                    fprintln(hfile, "      default: return \"Unknown\";")
                    fprintln(hfile, "      }")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    template<class T>")
                    fprintln(hfile, "    ", wrapper, "(const T& _v) {")
                    fprintln(hfile, "      switch(_v) {")
                    for enum_entry in member["enum"]:
                        fprintln(hfile, "      case T::", enum_entry, ": v = ", enum_name, "::", enum_entry, "; break;")
                    fprintln(hfile, "      default: v = ", enum_name, "::NUM_ITEMS;")
                    fprintln(hfile, "      }")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    template<class T>")
                    fprintln(hfile, "    operator T() const {")
                    fprintln(hfile, "      switch(v) {")
                    for enum_entry in member["enum"]:
                        fprintln(hfile, "      case ", enum_name, "::", enum_entry, ": return T::", enum_entry, ";")
                    fprintln(hfile, "      default: return T::", member["enum"][0], ";")
                    fprintln(hfile, "      }")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    typedef typename std::underlying_type<", enum_name, ">::type pos_type;")
                    fprintln(hfile, "    ", wrapper,"& operator++() {")
                    fprintln(hfile, "      v = static_cast<", enum_name,">(static_cast<pos_type>(v) + 1);")
                    fprintln(hfile, "      return *this;")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    ", wrapper, "& operator++(int) {")
                    fprintln(hfile, "      return ++(*this);")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    bool operator==(const ", wrapper, "& c) const {")
                    fprintln(hfile, "      return v == c.v;")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    bool operator!=(const ", wrapper, "& c) const {")
                    fprintln(hfile, "      return v != c.v;")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    bool operator<=(const ", wrapper, "& c) const {")
                    fprintln(hfile, "      return static_cast<pos_type>(v) <= static_cast<pos_type>(c.v);")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    static ", wrapper, " begin() {")
                    fprintln(hfile, "      return ", wrapper, "(", enum_name, "::", member["enum"][0], ");")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    static ", wrapper, " end() {")
                    fprintln(hfile, "      return ", wrapper, "(", enum_name, "::NUM_ITEMS);")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    static boost::integer_range<", wrapper, "> all_items() {")
                    fprintln(hfile, "      return boost::irange(begin(), end());")
                    fprintln(hfile, "    }")
                    fprintln(hfile, "    ", enum_name, " v;")
                    fprintln(hfile, "  };")
                    fprintln(hfile, "  ", config.jsonns, "::json_element<",
                           member_name, "_wrapper> ",
                           member_name, ";\n")
                else:
                    fprintln(hfile, "  ", config.jsonns, "::",
                           type_change(member["type"], member), " ",
                           member_name, ";\n")
                member_init += "  add(&" + member_name + ',"'
                member_init += member_name + '");\n'
                member_assignment += "  " + member_name + " = " + "e." + member_name + ";\n"
                member_copy += "  e." + member_name + " = " + member_name + ";\n"
            fprintln(hfile, "void register_params() {")
            fprintln(hfile, member_init)
            fprintln(hfile, '}')

            fprintln(hfile, model_name, '() {')
            fprintln(hfile, '  register_params();')
            fprintln(hfile, '}')
            fprintln(hfile, model_name, '(const ' + model_name + ' & e) {')
            fprintln(hfile, '  register_params();')
            fprintln(hfile, member_assignment)
            fprintln(hfile, '}')
            fprintln(hfile, "template<class T>")
            fprintln(hfile, model_name, "& operator=(const ", "T& e) {")
            fprintln(hfile, member_assignment)
            fprintln(hfile, "  return *this;")
            fprintln(hfile, "}")
            fprintln(hfile, model_name, "& operator=(const ", model_name, "& e) {")
            fprintln(hfile, member_assignment)
            fprintln(hfile, "  return *this;")
            fprintln(hfile, "}")
            fprintln(hfile, "template<class T>")
            fprintln(hfile, model_name, "& update(T& e) {")
            fprintln(hfile, member_copy)
            fprintln(hfile, "  return *this;")
            fprintln(hfile, "}")
            fprintln(hfile, "};\n\n")

 #   print_ind_comment(hfile, "", "Initialize the path")
#    fprintln(hfile, init_method + "(const std::string& description);")
    fprintln(hfile, 'static const sstring name = "', base_api, '";')
    for item in data["apis"]:
        path = item["path"]
        if "operations" in item:
            for oper in item["operations"]:
                if "summary" in oper:
                    print_comment(hfile, oper["summary"])
                vals = path.split("{")
                vals.reverse()

                fprintln(hfile, 'static const path_description ', getitem(oper, "nickname", oper), '("', clear_path_ending(vals.pop()),
                       '",', oper["method"], ',"', oper["nickname"], '",')
                fprint(hfile, '{')
                first = True
                while vals:
                    path_param = clean_param(vals.pop())
                    path_param_type = get_parameter_by_name(oper, path_param)
                    if first == True:
                        first = False
                    else:
                        fprint(hfile, "\n,")
                    if ("allowMultiple" in path_param_type and
                        path_param_type["allowMultiple"] == True):
                        fprint(hfile, '{', '"', path_param , '", true', '}')
                    else:
                        fprint(hfile, '{', '"', path_param , '", false', '}')
                fprint(hfile, '}')
                fprint(hfile, ',{')
                first = True
                if "parameters" in oper:
                    enum_definitions = ""
                    for param in oper["parameters"]:
                        if "required" in param and param["required"] and  param["paramType"] == "query":
                            if first == True:
                                first = False
                            else:
                                fprint(hfile, "\n,")
                            fprint(hfile, '"', param["name"], '"')
                        if "enum" in param:
                            enum_definitions = enum_definitions + 'namespace ns_' + oper["nickname"] + '{\n'
                            enm = param["name"]
                            enum_definitions = enum_definitions + 'enum class ' + enm + ' {'
                            for val in param["enum"]:
                                enum_definitions = enum_definitions + val + ", "
                            enum_definitions = enum_definitions + 'NUM_ITEMS};\n'
                            enum_definitions = enum_definitions + enm + ' str2' + enm + '(const sstring& str) {\n'
                            enum_definitions = enum_definitions + '  static const sstring arr[] = {"' + '","'.join(param["enum"]) + '"};\n'
                            enum_definitions = enum_definitions + '  int i;\n'
                            enum_definitions = enum_definitions + '  for (i=0; i < ' + str(len(param["enum"])) + '; i++) {\n'
                            enum_definitions = enum_definitions + '    if (arr[i] == str) {return (' + enm + ')i;}\n}\n'
                            enum_definitions = enum_definitions + '  return (' + enm + ')i;\n'
                            enum_definitions = enum_definitions + '}\n}\n'

                fprintln(hfile, '});')
                fprintln(hfile, enum_definitions)

    close_namespace(hfile)
    close_namespace(hfile)
    hfile.write("#endif //__JSON_AUTO_GENERATED_HEADERS\n")
    hfile.close()

def parse_file(param, combined):
    global current_file
    trace_verbose("parsing ", param, " file")
    try:
        json_data = open(param)
        data = json.load(json_data)
        json_data.close()
    except:
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
    except:
        type, value, tb = sys.exc_info()
        print("Error while parsing JSON file '" + param + "' error ", value.message)
        sys.exit(-1)

if "indir" in config and config.indir != '':
    combined = open(config.combined, "w")
    for f in glob.glob(os.path.join(config.indir, "*.json")):
        parse_file(f, combined)
else:
    parse_file(config.f, None)
