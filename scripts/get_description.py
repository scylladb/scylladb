#!/usr/bin/env python3

import argparse
import re
import yaml
import json
import inspect
from pathlib import Path

from encodings import undefined

gr = re.compile(r'.*(\.|->)add_group\(\s*(.*)')
desc = re.compile(r'.*..::descrs\( *("[^"]+")(.*)')
alternative_name = re.compile(r'([^,]*),')
metric = re.compile(r'.*..::make_(absolute|counter|current|derive|gauge|histogram|queue|summary|total|total_operations|queue_length|total_bytes|current_bytes)\((.*)')
string_content = re.compile(r'\s*"([^"]+)"\s*,.*')
string_match = re.compile(r'"([^"]+)"')
sstring_match = re.compile(r'\s*sstring\(\s*("[^"]+")\s*\)\s*')
metrics_directive = re.compile(r'.*@metrics\s*([^=]+)\s*=\s*(\[[^\]]*\]).*')
format_match = re.compile(r'\s*(?:seastar::)?format\(\s*"([^"]+)"\s*,\s*(.*)\s*')

def handle_error(message, strict=True, verbose_mode=False):
    if strict:
        print(f"[ERROR] {message}")
        exit(1)
    elif verbose_mode:
        print(f"[WARNING] {message}")

def verbose(verb, *arg):
    if verb:
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

def validate_parameter(txt, param_mapping, err="", strict=True, verbose_mode=False):
    if isinstance(txt, str):
        txt = [txt]
    for t in txt:
        if t not in param_mapping:
            handle_error(f"Could not resolve param {err} {t}", strict, verbose_mode)
            return None
        if not param_mapping[t]:
            handle_error(f"Could not resolve param is empty {err} {txt}", strict, verbose_mode)
            return None
    return txt
def sort_by_index(arr,ind):
    return [arr[i] for i in ind]

def make_name_list(names, err, param_mapping, verb=None, strict=True):
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
                param = param + validate_parameter(txt, param_mapping, "(make_name_list:"+ str(inspect.getframeinfo(inspect.currentframe()).lineno) +")"+err, strict, verb)
                format_string += "{}"
                if not param:
                    handle_error(f"make_name_list:{inspect.getframeinfo(inspect.currentframe()).lineno} {names}", strict, verb)
                    return None
    if not param:
        return [format_string]
    order_params = list(enumerate(param))
    sorted_indexed_array = sorted(order_params, key=lambda x: x[1])
    indexed_array = [index for index, value in sorted_indexed_array]
    verbose(verb, "make_name_list", param)
    param.sort()
    param_keys = ';'.join(param)
    if param_keys not in param_mapping:
        handle_error(f"Parameter not found: {param_keys} {err}", strict)
        return None
    for p in param_mapping[param_keys]:
        if not p:
            handle_error(f"empty (make_name_list:{inspect.getframeinfo(inspect.currentframe()).lineno}){err} {param}", strict)
            return None
    verbose(verb, "make_name_list", [format_string.format(p) for p in param_mapping[param[0]]] if len(param) == 1 else [format_string.format(*sort_by_index(p,indexed_array)) for p in param_mapping[param_keys]])
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

def get_metrics_information(config_file):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def get_metrics_from_file(file_name, prefix, metrics_information, verb=None, strict=True):
    current_group = ""
    # Normalize path for cross-platform compatibility
    # Convert absolute paths to relative and backslashes to forward slashes
    clean_name = file_name
    if file_name.startswith('./'):
        clean_name = file_name[2:]
    else:
        # Handle absolute paths (Windows: C:\..., Unix: /...)
        file_path = Path(file_name)
        if file_path.is_absolute():
            try:
                # Try to make it relative to current directory
                clean_name = str(file_path.relative_to(Path.cwd()))
            except ValueError:
                # If not relative to cwd, just use the name as-is
                clean_name = file_name
    # Normalize backslashes to forward slashes for config file compatibility
    clean_name = clean_name.replace('\\', '/')
    param_mapping = {}
    groups = {}
    if clean_name in metrics_information:
        if (isinstance(metrics_information[clean_name], str) and metrics_information[clean_name] == "skip") or "skip" in metrics_information[clean_name]:
            return {}
    param_mapping =  metrics_information[clean_name]["params"] if clean_name in metrics_information and "params" in metrics_information[clean_name] else {}
    groups = metrics_information[clean_name]["groups"] if clean_name in metrics_information and "groups" in metrics_information[clean_name] else {}

    metrics = {}
    names = undefined
    typ = undefined
    line_number = 0;
    current_metric = ""
    parenthes_count = 0
    serching_group = False
    with open(file_name) as file:
        for line in file:
            if str(line_number) in groups:
                current_group = groups[str(line_number)]
                verbose(verb, "found group from config ", groups[str(line_number)])
            if serching_group:
                m = string_content.match(line)
                if not m:
                    line_number += 1
                    continue
                current_group = m.group(1)
                serching_group = False
                verbose(verb, "group found on new line", current_group)
            m = metric.match(line)
            # Check if add_group and metric are on the same line
            if m and not current_group:
                gr_match = gr.match(line)
                if gr_match:
                    # Extract group from add_group on the same line
                    current_group = gr_match.group(2)
                    m_str = string_content.match(current_group)
                    if m_str:
                        current_group = m_str.group(1)
                    else:
                        m_alt = alternative_name.match(current_group)
                        if m_alt:
                            current_group = param_mapping[m_alt.group(1)] if m_alt.group(1) in param_mapping else m_alt.group(1)
                    verbose(verb, "group found on same line as metric", current_group)
                else:
                    handle_error(f"new name found with no group {file_name} {line_number} {line}", strict)
                    continue
            if current_metric or m:
                # Only error if add_group appears after we've started processing a metric
                if current_metric and gr.match(line):
                    handle_error(f"add group found unexpectedly {file_name} {line_number} {line}", strict)
                    continue
                if current_metric and m:
                    handle_error(f"new metrics was found while parsing the previous one {file_name} {line_number} {line}", strict)
                    continue
                ln = line.replace('\\"','#').rstrip()
                current_metric = merge_strings(current_metric, ln)
                no_string = re.sub(string_match, '', ln)
                parenthes_count += no_string.count('(')
                parenthes_count -= no_string.count(')')
                if parenthes_count <= 0:
                    verbose(verb, current_metric, file_name, line_number)
                    m = metric.match(current_metric)
                    typ = m.group(1) # type is taken from the make_metrics part
                    prt = m.group(2)
                    m = string_content.match(m.group(2))
                    if not m:
                        multi_part_name = get_end_part(prt)
                        #m = alternative_name.match(prt)
                        verbose(verb, "multi part name ", multi_part_name)
                        if multi_part_name:
                            names = [clear_string(s) for s in multi_part_name.split('+')]
                        else:
                            handle_error(f"names not found {file_name} {line_number} {line} {current_metric}", strict)
                            continue
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
                        handle_error(f"description not found {file_name} {line_number} {line} {current_metric}", strict)
                        continue
                    name_list = make_name_list(names, file_name+" "+str(line_number), param_mapping, verb, strict)
                    if not name_list:
                        handle_error(f"no name list {current_metric}", strict)
                        continue
                    description_list = make_name_list(descrs, file_name+" "+str(line_number), param_mapping, verb, strict)
                    current_groups = current_group if isinstance(current_group, list) else [current_group]
                    for cg in current_groups:
                        for idx, base_name in enumerate(name_list):
                            name = prefix + cg + "_" + base_name
                            description = description_list[0].replace('#','"') if len(description_list) == 1 else description_list[idx].replace('#','\\"')
                            if name in metrics:
                                if description != metrics[name][1]:
                                    verbose(verb, f'Note: Multiple descriptions found for {name}, using first one from {metrics[name][4]}')
                                continue
                            metrics[name] = [typ, description, cg, base_name, file_name + ":" + str(line_number)]
                    current_metric = ""
                    parenthes_count = 0
            else:
                m = gr.match(line)
                if m:
                    current_group = m.group(2)
                    if not current_group:
                        verbose(verb, "empty group found")
                        serching_group = True
                    m = string_content.match(current_group)
                    if m:
                        current_group = m.group(1)
                    else:
                        m = alternative_name.match(current_group)
                        if m:
                            current_group = param_mapping[m.group(1)] if m.group(1) in param_mapping else m.group(1)
                            verbose(verb, "Alternative group", file_name, line_number, current_group)
                m = metrics_directive.match(line)
                if m:
                    param_mapping[m.group(1).strip()] = json.loads(m.group(2))
            line_number += 1
    return metrics

def write_metrics_to_file(out_file, metrics, fmt="pipe"):
    with open(out_file, "a") as fo:
        if fmt == "yml":
            yaml.dump(metrics,fo,sort_keys=False)
        if fmt == "pipe":
            for l in metrics.keys():
                fo.write(l.replace('-','_')+'|' +'|'.join(metrics[l])+ '\n')


def validate_all_metrics(prefix, config_file, verbose=False, strict=True):
    """Validate all metrics files and report issues"""

    print("Validating all metrics files...")

    # Use pathlib to find all .cc files (cross-platform)
    try:
        current_dir = Path('.')
        all_cc_files = list(current_dir.rglob('*.cc'))

        # Filter files that contain '::description'
        metric_files = []
        for cc_file in all_cc_files:
            try:
                with open(cc_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    if '::description' in content:
                        # Normalize path to use forward slashes for cross-platform compatibility
                        normalized_path = str(cc_file).replace('\\', '/')
                        metric_files.append(normalized_path)
            except Exception as e:
                if verbose:
                    print(f"[WARN] Could not read {cc_file}: {e}")
                continue

    except Exception as e:
        print(f"[ERROR] Error finding metrics files: {e}")
        return False


    total_files = len(metric_files)

    print(f"Found {total_files} files with metrics")

    failed_files = []
    total_metrics = 0
    metrics_info = get_metrics_information(config_file)

    for file_path in metric_files:
        try:
            metrics = get_metrics_from_file(file_path, prefix, metrics_info, verbose, strict)
            metrics_count = len(metrics)
            total_metrics += metrics_count
            if verbose:
                print(f"[OK] {file_path} - {metrics_count} metrics")
            else:
                print(f"[OK] {file_path}")
        except Exception as e:
            print(f"[ERROR] {file_path}")
            print(f"   Error: {str(e)}")
            failed_files.append((file_path, str(e)))

    if failed_files:
        print("\n[ERROR] METRICS VALIDATION FAILED")
        print("Failed files:")
        for file_path, error in failed_files:
            print(f"   - {file_path}: {error}")
        print(f"\nAdd missing parameters to {config_file}")
        return False
    else:
        working_files = total_files - len(failed_files)
        coverage_pct = (working_files * 100 // total_files) if total_files > 0 else 0

        print(f"\n[SUCCESS] All metrics files validated successfully")
        print(f"   Files: {working_files}/{total_files} working ({coverage_pct}% coverage)")
        print(f"   Metrics: {total_metrics} total processed")
        return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='get metrics descriptions from file', conflict_handler="resolve")
    parser.add_argument('-p', '--prefix', default="scylla_", help='the prefix added to the metrics names')
    parser.add_argument('-o', '--out-file', default="metrics.out", help='The output file')
    parser.add_argument('-c', '--config-file', default="metrics-config.yml", help='The configuration file used to add extra data missing in the code')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='When set prints verbose information')
    parser.add_argument('-F', '--format', default="pipe", help='Set the output format, can be pipe, or yml')
    parser.add_argument('--validate', action='store_true', help='Validate all metrics files instead of processing single file')
    parser.add_argument('--non-strict', action='store_true', help='Continue with warnings instead of failing on errors (useful for multiversion builds)')
    parser.add_argument('file', nargs='?', help='the file to parse (not needed with --validate)')

    args = parser.parse_args()

    # Determine strict mode
    strict_mode = not args.non_strict

    if args.validate:
        success = validate_all_metrics(args.prefix, args.config_file, args.verbose, strict_mode)
        exit(0 if success else 1)

    if not args.file:
        parser.error('file argument is required when not using --validate')

    metrics = get_metrics_from_file(args.file, args.prefix, get_metrics_information(args.config_file), args.verbose, strict_mode)
    write_metrics_to_file(args.out_file, metrics, args.format)
