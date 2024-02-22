#!/usr/bin/env python3

import argparse
import re
import yaml
import json
import inspect

from encodings import undefined

parser = argparse.ArgumentParser(description='get metrics descriptions from file', conflict_handler="resolve")
parser.add_argument('-p', '--prefix', default="scylla_", help='the prefix added to the metrics names')
parser.add_argument('-o', '--out-file', default="metrics.out", help='The output file')
parser.add_argument('-c', '--config-file', default="metrics-config.yml", help='The configuration file used to add extra data missing in the code')
parser.add_argument('-v', '--verbose', action='store_true', default=False, help='When set prints verbose information')
parser.add_argument('file', help='the file to parse')

args = parser.parse_args()

current_group = ""
gr = re.compile(r'.*(\.|->)add_group\(\s*(.*)')
desc = re.compile(r'.*..::descrs\( *("[^"]+")(.*)')
alternative_name = re.compile(r'([^,]*),')
metric = re.compile(r'.*..::make_(absolute|counter|current|derive|gauge|histogram|queue|summary|total|total_operations|queue_length|total_bytes|current_bytes)\((.*)')
string_content = re.compile(r'\s*"([^"]+)"\s*,.*')
string_match = re.compile(r'"([^"]+)"')
sstring_match = re.compile(r'\s*sstring\(\s*("[^"]+")\s*\)\s*')
metrics_directive = re.compile(r'.*@metrics\s*([^=]+)\s*=\s*(\[[^\]]*\]).*')
format_match = re.compile(r'\s*(?:seastar::)?format\(\s*"([^"]+)"\s*,\s*(.*)\s*')

with open(args.config_file, 'r') as file:
    metrics_information = yaml.safe_load(file)
clean_name = args.file[2:] if args.file.startswith('./') else args.file
param_mapping = {}
groups = {}
allowmismatch = False
if clean_name in metrics_information:
    if (isinstance(metrics_information[clean_name], str) and metrics_information[clean_name] == "skip") or "skip" in metrics_information[clean_name]:
        exit(0)
    if "allowmismatch" in metrics_information[clean_name]:
        allowmismatch = metrics_information[clean_name]["allowmismatch"]
param_mapping =  metrics_information[clean_name]["params"] if clean_name in metrics_information and "params" in metrics_information[clean_name] else {}
groups = metrics_information[clean_name]["groups"] if clean_name in metrics_information and "groups" in metrics_information[clean_name] else {}

def verbose(*arg):
    if args.verbose:
        print(*arg)
def get_end_part(str):
    parenthes_count = 0
    for idx, c in enumerate(str):
        if c == ',' and parenthes_count == 0:
            return str[:idx]
        if c ==')':
            parenthes_count -= 1
        if c =='(':
            parenthes_count += 1
    return None

def find_end_parenthes(str, pc):
    for idx, c in enumerate(str):
        if c ==')':
            pc -= 1
        if c =='(':
            pc += 1
        if pc == 0:
            return str[:idx]
    return str

def split_paterns(str):
    res = []
    pos = 0
    b = 0
    cur_str = ""
    while pos < len(str):
        if str[pos] == '+':
            res.append(cur_str)
            cur_str = ""
        elif str[pos] == '(':
            s = find_end_parenthes(str[pos+1:], 1)
            cur_str += "(" +s +")"
            pos += len(s) + 1
        else:
            cur_str += str[pos]
        pos += 1
    if cur_str:
        res.append(cur_str)
    return res

def validate_parameter(txt, err=""):
    if isinstance(txt, str):
        txt = [txt]
    for t in txt:
        if t not in param_mapping:
            print("Could not resolve param", err, t)
            return None
        if not param_mapping[t]:
            print("Could not resolve param is empty", err, txt)
            return None
    return txt
def sort_by_index(arr,ind):
    return [arr[i] for i in ind]

def make_name_list(names, err):
    param = []
    format_string = ""
    for txt in names:
        if isinstance(txt, dict):
            format_string += txt['str']
            param = param + ([txt['param']] if isinstance(txt['param'], str) else txt['param'])
        else:
            if txt[0] == '"':
                format_string += txt[1:-1]
            else:
                param = param + validate_parameter(txt, "(make_name_list:"+ str(inspect.getframeinfo(inspect.currentframe()).lineno) +")"+err)
                format_string += "{}"
                if not param:
                    print("make_name_list:"+ str(inspect.getframeinfo(inspect.currentframe()).lineno), names)
                    return None
    if not param:
        return [format_string]
    order_params = list(enumerate(param))
    sorted_indexed_array = sorted(order_params, key=lambda x: x[1])
    indexed_array = [index for index, value in sorted_indexed_array]
    verbose("make_name_list", param)
    param.sort()
    param_keys = ';'.join(param)
    if param_keys not in param_mapping:
        print("Parameter not found", param_keys, err)
        exit(-1)
    for p in param_mapping[param_keys]:
        if not p:
            print("empty (make_name_list:"+ str(inspect.getframeinfo(inspect.currentframe()).lineno) +")"+err, param)
            exit(-1)
    verbose("make_name_list", [format_string.format(p) for p in param_mapping[param[0]]] if len(param) == 1 else [format_string.format(*sort_by_index(p,indexed_array)) for p in param_mapping[param_keys]])
    return [format_string.format(p) for p in param_mapping[param[0]]] if len(param) == 1 else [format_string.format(*sort_by_index(p,indexed_array)) for p in param_mapping[param_keys]]

def get_decription(str):
    b = str.find('::description(') + len('::description(')
    p = b
    parenthes_count = 1
    while p < len(str):
        if str[p] == '"':
            p = str.find('"',p+1)
            if p <0:
                break
        if str[p] == '(':
            parenthes_count += 1
        if str[p] == ')':
            parenthes_count -= 1
        if parenthes_count == 0:
            return str[b:p]
        p = p + 1
    return None

def merge_strings(str, str2):
    if str and str.rstrip()[-1] == '"':
        if str2 and str2.lstrip()[0] == '"':
            return str.rstrip()[:-1] + str2.lstrip()[1:]
    return str.rstrip() + ' ' + str2.lstrip()

def clear_string(str):
    m = sstring_match.match(str)
    if m:
        return m.group(1)
    m = format_match.match(str)
    if m:
        params =  [p.strip() for p in find_end_parenthes(m.group(2), 1).split(',')]
        return {'str': m.group(1), 'param': params[0] if len(params) == 1 else params}
    return str.strip()

metrics = {}
multi_line = False
names = undefined
typ = undefined
line_number = 0;
current_metric = ""
parenthes_count = 0
serching_group = False
with open(args.file) as file:
    for line in file:
        if str(line_number) in groups:
            current_group = groups[str(line_number)]
            verbose("found group from config ", groups[str(line_number)])
        if serching_group:
            m = string_content.match(line)
            if not m:
                line_number += 1
                continue
            current_group = m.group(1)
            serching_group = False
            verbose("group found on new line", current_group)
        m = metric.match(line)
        if m and not current_group:
            print("new name found with no group", args.file, line_number, line)
            exit(-1)
        if current_metric or m:
            if gr.match(line):
                print("add group found unexpectedly", args.file, line_number, line)
                exit(-1)
            if current_metric and m:
                print("new metrics was found while parsing the previous one", args.file, line_number, line)
                exit(-1)
            ln = line.replace('\\"','#').rstrip()
            current_metric = merge_strings(current_metric, ln)
            no_string = re.sub(string_match, '', ln)
            parenthes_count += no_string.count('(')
            parenthes_count -= no_string.count(')')
            if parenthes_count <= 0:
                verbose(current_metric, args.file, line_number)
                m = metric.match(current_metric)
                typ = m.group(1) # type is taken from the make_metrics part
                prt = m.group(2)
                m = string_content.match(m.group(2))
                if not m:
                    multi_part_name = get_end_part(prt)
                    #m = alternative_name.match(prt)
                    verbose("multi part name ", multi_part_name)
                    if multi_part_name:
                        names = [clear_string(s) for s in multi_part_name.split('+')]
                    else:
                        print("names not found", args.file, line_number, line, current_metric)
                        exit(-1)
                else:
                    names = ['"'  + m.group(1) + '"']
                desc_str = get_decription(current_metric)
                if desc_str:
                    m = string_match.match(desc_str)
                    if m:
                        descrs = [desc_str]
                    else:
                        descrs = [clear_string(s) for s in split_paterns(desc_str)]
                else:
                    print("description not found", args.file, line_number, line, current_metric)
                    exit(-1)
                name_list = make_name_list(names, args.file+" "+str(line_number))
                if not name_list:
                    print("no name list", current_metric)
                    exit(-1)
                description_list = make_name_list(descrs, args.file+" "+str(line_number))
                current_groups = current_group if isinstance(current_group, list) else [current_group]
                for cg in current_groups:
                    for idx, base_name in enumerate(name_list):
                        name = args.prefix + cg + "_" + base_name
                        description = description_list[0].replace('#','"') if len(description_list) == 1 else description_list[idx].replace('#','\\"')
                        if not allowmismatch and name in metrics and description !=  metrics[name][1]:
                            print('description problem, different descriptions found', args.file, line_number, names, typ, line, name, metrics[name][1], description)
                            print(metrics[name][1])
                            print(description)
                            exit(-1)
                        metrics[name] = [typ, description, cg, base_name, line_number]
                current_metric = ""
                parenthes_count = 0
        else:
            m = gr.match(line)
            if m:
                current_group = m.group(2)
                if not current_group:
                    verbose("empty group found")
                    serching_group = True
                m = string_content.match(current_group)
                if m:
                    current_group = m.group(1)
                else:
                    m = alternative_name.match(current_group)
                    if m:
                        current_group = param_mapping[m.group(1)] if m.group(1) in param_mapping else m.group(1)
                        verbose("Alternative group", args.file, line_number, current_group)
            m = metrics_directive.match(line)
            if m:
                param_mapping[m.group(1).strip()] = json.loads(m.group(2))
        line_number += 1

with open(args.out_file, "a") as fo:
    for l in metrics.keys():
        fo.write(l.replace('-','_') +'|' + metrics[l][0] +'|' + metrics[l][1] +'|' + metrics[l][2] +'|'+ metrics[l][3]+'|'+ args.file + ":"+ str(metrics[l][4])+ '\n')
