#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import yaml
import json
import argparse
from subprocess import Popen, PIPE, call
import glob
from functools import reduce
import logging
from datetime import datetime

def run_remote(cmd, node_list, ssh_auth):
    '''
    Execute a command via ssh on multiple hosts in parallel. Exits on non-0 exit status
    or when stderr isn't empty.

    Args:
      cmd (str): command string
      node_list (list): list of nodes (IPs or FQDNs) to run the command on
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      out (dict): output dictionary containing out[nodes]['stdout']
    '''
    if 'pkey' in ssh_auth.keys():
        sshopts = ('ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '
                   '-o LogLevel=ERROR -i %s' % ssh_auth['pkey'])
    elif 'password' in ssh_auth.keys():
        sshopts = ('sshpass -p %s ssh -o StrictHostKeyChecking=no '
                   '-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR' % ssh_auth['password'])
    else:
        logger.error('Invalid ssh credentials in configuration, exiting')
        sys.exit(1)
    procs = []
    for node in node_list:
        c = ('%s %s@%s \'sudo %s\' ' % (sshopts, ssh_auth['user'], node, cmd))
        logger.debug('Node: %s CMD: %s', node, c)
        p = Popen(c, shell=True, stdout=PIPE, stderr=PIPE)
        procs.append([p, node])
    out = {}
    for proc in procs:
        try:
            stdout, stderr = proc[0].communicate()
        except:
            logger.error('Exception occured on %s', proc[1], exc_info=True)
            out[proc[1]]['stdout'] = []
            out[proc[1]]['stderr'] = ['Failed %s' % c]
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")
        out[proc[1]] = {}
        out[proc[1]]['stdout'] = stdout.splitlines()
        out[proc[1]]['stderr'] = stderr.splitlines()
        logger.debug('node: %s stdout: %s', proc[1], stdout)
        if out[proc[1]]['stderr'] or proc[0].returncode != 0:
            out[proc[1]]['stderr'].append('Exit status %s' % str(proc[0].returncode))
            logger.error('node: %s stderr: %s', proc[1], stderr)
            sys.exit(1)
    return out

def get_args():
    desc = ('Restore a set of sstable files to a working Scylla cluster \n\n'
            'See restore_config.yml for configuration details\n\n')
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--config', '-c', help='Use specified configuration file path',
                        required=False, default='./restore_config.yml')
    parser.add_argument('--resume', '-r', help='Resume failed file copies from a restore plan file',
                        required=False)
    parser.add_argument('-l', '--loglevel', help='Set loglevel, default is INFO',
                        required=False, default='INFO')
    args = parser.parse_args()
    return args

def read_config(filename):
    '''
    Read the configuration file and return a config dictionary.

    Args:
      filename (str): file name in cwd or full path to a config file

    Returns:
      config (dict): configuration dictionary
    '''
    with open(filename, 'r') as f:
        conf = f.read()
    config = yaml.load(conf, Loader=yaml.SafeLoader)
    logger.debug('config: %s', str(config))
    return config

def get_cluster_nodes_df(nodes, path, ssh_auth):
    '''
    Run df on a set of nodes and return a dictionary of the current disk status

    Args:
      nodes (list): list of nodes to run df on
      path (str): location in which df should be run (typically /var/lib/scylla/data)
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      cluster (dict): disk free information for the set of nodes
                        {
                            node: {
                                'size': disk_size,
                                'free': free_space
                            }
                        }

    '''
    cmd = ('df %s '% path)
    out = run_remote(cmd, nodes, ssh_auth)
    cluster = {}
    for node in nodes:
        cluster[node] = {}
        try:
            df = out[node]['stdout'][-1].split()
        except Exception as e:
            logger.error("Failure reading disk space on %s: %s", str(out[node]['stdout']),
                                                                            exc_info=True)
            sys.exit(1)
        dsize = df[1]
        dfree = df[3]
        cluster[node]['size'] = dsize
        cluster[node]['free'] = dfree
    logger.debug('df info: %s', str(cluster))
    return cluster

def get_cluster_hosts(host, ssh_auth):
    '''
    Read the list of hosts in the destination cluster, from one of the nodes. Returns a list of node IPs

    Args:
      host (str): IP/FQDN of a node in the cluster
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      cluster (list): list of IP addresses for nodes in the cluster
    '''
    cmd = ('curl --fail -S -s -X GET %s/failure_detector/endpoints' % API_URL)
    logger.debug('Retrieving the cluster nodes')
    out = run_remote(cmd, [host], ssh_auth)
    cluster = []
    try:
        _j = list(out[host]['stdout'])[0]
        j = json.loads(_j)
        for i in j:
            cluster.append(i['addrs'])
    except Exception as e:
        logger.error("Exception reading the cluster nodes list %s", str(out), exc_info=True)
        sys.exit(1)
    logger.debug('cluster nodes found: %s', str(cluster))
    return cluster

def check_schema_version(nodes, ssh_auth):
    '''
    Confirm the schema versions aren't split across the destination cluster nodes. Exit if they are.

    Args:
      nodes (list): list of nodes to check the schema versions on
      ssh_auth (dict): dictionary containing ssh authentication parameters
    '''
    cmd = ('curl --fail -S -s -X GET %s/storage_service/schema_version' % API_URL)
    out = run_remote(cmd, nodes, ssh_auth)
    versions = []
    try:
        for node in nodes:
            versions.append(out[node]['stdout'][0])
    except Exception as e:
        logger.error("Failed to read schema versions from cluster %s", str(out), exc_info=True)
    if len(set(versions)) != 1:
        logger.error('Schema version mismatch found %s', str(out))
        sys.exit(1)

# def get_ks_ranges(host, keyspace, ssh_auth):
#     '''
#     Read the token range for a given node and keyspace. Returns a dictionary describing the token ring.
#
#     Args:
#       host (str): Node to access and read the data from
#       keyspace (str): keyspace to read the ring data for
#       ssh_auth (dict): dictionary containing ssh authentication parameters
#
#     Returns:
#       ring (dict): token ring dictionary for the node/keyspace set
#     '''
#     cmd = ('curl --fail -S -X GET -s %s/storage_service/describe_ring/%s' % (API_URL, keyspace))
#     out = run_remote(cmd, [host], ssh_auth)
#     try:
#         _j = out[host]['stdout'][0]
#         ring = json.loads(_j)
#     except Exception as e:
#         logger.error("Failed to read ring data for keyspace %s on %s\n %s", keyspace, host,
#                                                                             str(out), exc_info=True)
#         sys.exit(1)
#     logger.debug('ring on %s for %s: \n %s ', host, keyspace, str(ring))
#     return ring

def configure_ssh_auth(config):
    '''
    Parse the ssh configuration options and turn them into a usable dictionary

    Args:
      config (dict): config dictionary returned by read_config()

    Returns:
      ssh_auth (dict): ssh authentication dictionary
                        {
                            'user': ssh username,
                            'pkey': path to private ssh key file (optional),
                            'password': ssh password (optional)
                        }
    '''
    ssh_auth = {}
    ssh_auth['user'] = config['dest_ssh_user']
    if config['dest_host_auth'] == 'key':
        ssh_auth['pkey'] = config['dest_ssh_key_path']
    elif config['dest_host_auth'] == 'password':
        ssh_auth['password'] = config['dest_ssh_password']
    else:
        logger.error('Invalid ssh authentication options in configuration YAML file')
        sys.exit(1)
    logger.info('Trying to connect to %s', config['dest_host'])
    out = run_remote('hostname', [config['dest_host']], ssh_auth)
    logger.debug('ssh_auth: %s', str(ssh_auth))
    return ssh_auth

def parse_schema_file(schema_file):
    '''
    Read the source schema file and return a dictionary with keyspaces and tables to restore

    Args:
      schema_file (str): path to the schema dump file.

      It is assumed the file has been received from a DESCRIBE SCHEMA dump, and thus all the
      keyspace/tables are read from the lines containing "CREATE TABLE ks.table ...".
      Other formats are not supported.

    Returns:
      keyspaces (dict): dictionary of keyspace:[list of tables]
    '''
    def read_schema_file(filename):
        try:
            with open(filename, 'r') as f:
                schema_data = f.readlines()
        except Exception as e:
            logger.error("Failed reading schema file", exc_info=True)
            sys.exit(1)
        logger.debug('raw schema file: \n%s', str(schema_data))
        return schema_data
    schema = read_schema_file(schema_file)
    keyspaces = {}
    for line in schema:
        if line.upper().startswith('CREATE TABLE'):
            s = line.split()
            try:
                if 'IF EXISTS' in line.upper():
                    ks = s[4].split('.')[0]
                    tb = s[4].split('.')[1]
                else:
                    ks = s[2].split('.')[0]
                    tb = s[2].split('.')[1]
                if ks in keyspaces.keys():
                    keyspaces[ks].append(tb)
                else:
                    keyspaces[ks] = [tb]
            except IndexError as e:
                logger.error('Malformed CREATE TABLE line in %s\n%s', line, str(e))
                sys.exit(1)
            except Exception as e:
                logger.error('Error parsing the source schema:\n %s', str(e))
                sys.exit(1)
    logger.debug('Read schema from schema dump: %s', str(keyspaces))
    return keyspaces

def get_destination_dirs(schema, config, ssh_auth):
    '''
    Access the system tables in the destination cluster and gather the UUIDs of the table directories.
    These will be used to determine the destination paths for the restore files.

    This function also makes sure that the restored schema items are present in the destination cluster.

    Args:
      schema (dict): schema containing the keyspaces and tables for the restored dataset
      config (dict): configuration values dictionary
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      dest_dirs (list): list of destination paths for the restored schema
    '''
    def accumulate(acc, item):
        # accumulator function for the reduce()
        return acc + len(item[1])
    host_string = ("CQLSH_HOST=%s" % config['cql_host'])
    cmd = [host_string, 'cqlsh']
    if config['cql_user'] != 'None':
        cmd.append("-u " + config['cql_user'])
    if config['cql_password'] != 'None':
        cmd.append('-p ' + config['cql_password'])
    cmd.append('-e "SELECT * FROM system_schema.tables ;"')
    cmd = ' '.join(cmd)
    _out = run_remote(cmd, [config['dest_host']], ssh_auth)
    out = _out[config['dest_host']]['stdout']
    dest_dirs = []
    for line in out:
        if len(line) > 0:
            if line.split()[0] in list(schema.keys()):
                rec = [x for x in line.split() if x != '|']
                try:
                    uuid = rec[17]
                    uuid = rec[17].replace('-', '')
                except IndexError as e:
                    logger.error('Error reading system tables line %s\n%s', str(rec), str(e))
                    sys.exit(1)
                d = []
                d.append(config['scylla_data_path'])
                d.append(rec[0])
                d.append(rec[1] + '-' + uuid)
                dest_dirs.append('/'.join(d))
    if len(dest_dirs) != reduce(accumulate, schema.items(), 0):
        logger.error('Schema items missing, please create the destination schema first')
        sys.exit(1)
    logger.info('destination dirs: %s', str(dest_dirs))
    return dest_dirs

def get_toc_file_list(path, schema):
    '''
    Scan the restored file list for TOC files and return their list

    Args:
      path (str): root location of the restored file set
      schema (dict): restored schema dictionary

    Returns:
      files (list): list of TOC files in the restore set (full path)
    '''
    if path.endswith('/'):
        path += "**"
    else:
        path += "/**"
    _files = glob.glob(path, recursive=True)
    logger.debug('files detected unfiltered: %s', str(_files))
    _files = [f for f in _files if os.path.isfile(f) and f.split('/')[-1].endswith('-TOC.txt')]
    files = []
    for f in _files:
        try:
            ks = f.split('/')[-3]
            tb = f.split('/')[-2].split('-')[0]
            if ks in schema.keys():
                if tb in schema[ks]:
                    files.append(f)
        except Exception as e:
            logger.error('Error reading file list at %s', f, exc_info=True)
            sys.exit(1)
    logger.debug('files found: %s', str(files))
    return files

def get_file_details(file_path):
    '''
    Read and parse the file name, path, generation and size

    Args:
      file_path (str): file anme or full path

    Returns:
      file (dict): dictionary containing file details (path, table, keyspace, source node,
                   size, version, generation)
    '''
    file = {}
    file['file_path'] = '/'.join(file_path.split('/')[:-1])
    file['file_name'] = file_path.split('/')[-1]
    file['table'] = file_path.split('/')[-2].split('-')[0]
    file['keyspace'] = file_path.split('/')[-3]
    file['src_node'] = file_path.split('/')[-4]
    try:
        file['size'] = os.path.getsize(file_path)
    except Exception as e:
        logger.error('Could not read file %s', file_path, exc_info=True)
        sys.exit(1)
    if file['file_name'].startswith('mc-') or file['file_name'].startswith('la-'):
    # format is {version}-{generation}-{format}-{component}.{extension}
        file['file_gen'] = file['file_name'].split('-')[1]
        file['file_ver'] = file['file_name'].split('-')[0]
        file['base'] = file['file_path'] + '/' + file['file_name']
        file['base'] = '-'.join(file['base'].split('-')[:-2])
    elif '-ka-' in file['file_name'] and file['file_name'].split('-')[2] == 'ka':
    # format is {keyspace}-{table}-{version}-{generation}-{componentname}.{extension}
        file['file_gen'] = file['file_name'].split('-')[3]
        file['file_ver'] = file['file_name'].split('-')[2]
        file['base'] = file['file_path'] + '/' + file['file_name']
        file['base'] = '-'.join(file['base'].split('-')[:-1])
    else:
        logger.error('Illegal filename in restore set: %s', file_path)
        sys.exit(1)
    logger.debug('%s file details: %s', file_path, str(file))
    return file

#def get_destination_hosts(ring, file, node_list):
#   '''
#   For a given token ring and file, calculate and return the list of nodes that overlap the file's token range
#   and should be used as restore destinations.
#
#   Args:
#       ring (dict): token ring from the destination cluster
#       file (dict): file dictionary (see get_file_details())
#       node_list (list): list of destination nodes to consider as restore candidates
#
#   Returns:
#       dest_nodes (list): list of nodes the file should be copied to
#   '''
#    dest_nodes = []
#    for i in ring[file['keyspace']]:
#        if int(file['first_token']) <= int(i['start_token']) and int(file['last_token']) >= int(i['end_token']):
#            dest_nodes += [x for x in i['endpoints'] if x in node_list]
#    logger.debug('destination nodes for %s: %s', file, str(dest_nodes))
#    return list(set(dest_nodes))

def copy_file(file_path, node_list, dest_path, bwlimit, copy_procs, ssh_auth):
    '''
    Copy a file from restore set to a set of nodes/paths

    Args:
      file_path (str): source file location in the restore file set
      node_list (list): list of ndoes to copy the file to
      dest_path (str): destination path in the cluster nodes
      bwlimit (str|int): bandwidth limit for the rsync process
      copy_procs (str|int): number of copy processes to run in parallel
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      success (bool): True when the copy operation succeeded for all the destination nodes
    '''
    def run_rsync(file_path, node_list, dest_path, bwlimit, copy_procs, ssh_auth):
    # NOTE: if the output is too large a deadlock is possible. Rsync shouldn't spit out enough output to trigger
    # the deadlock, but in case an rsync process seems to be stuck:
    # switch subprocess.poll() to subprocess.communicate() to avoid the deadlock
    # See deadlock warnings at: https://docs.python.org/3/library/subprocess.html
        success = True
        for node in node_list:
            cmd = ('%s %s %s %s@%s:/%s/' % (rsync, sshopts, file_path,
                                            ssh_auth['user'], node, dest_path))
            logger.debug('rsync: %s', cmd)
            p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            procs.append([p, cmd])
        while success and len(procs) > 0:
            for proc in procs:
                if proc[0].poll() == None: # Still running
                    pass
                elif proc[0].poll() == 0:  # Successfully exited
                    out, err = proc[0].communicate()
                    logger.debug('completed: stdout: %s stderr: %s', out, err)
                    procs.remove(proc)
                    if len(err.splitlines()) > 0:
                        logger.error('failed on rsync %s with %s', out, err)
                        success = False
                else:                      # Failed
                    success = False
                    try:
                        logger.error('failed %s :: %s', proc[1], str(proc[0].communicate()))
                    except Exception as e:
                        logger.error('failed on rsync', exc_info=True)
                        # exit is in the calling func because delete_failed_bundle() is required
        if not success:
        # kill off all the other rsync processes if one failed
            for p in procs:
                logger.warning('Killing leftover rsync process for %s', str(p[1]))
                p[0].kill()
        return success
    if 'pkey' in ssh_auth.keys():
        sshopts = ('-e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '
                   '-o LogLevel=ERROR -i %s"' % ssh_auth['pkey'])
    elif 'password' in ssh_auth.keys():
        sshopts = ('-e "sshpass -p %s ssh -o StrictHostKeyChecking=no '
                   '-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"' % ssh_auth['password'])
    else:
        logger.error('Invalid ssh credentials in configuration, exiting')
        sys.exit(1)
    success = True
    procs = []
    if not bwlimit or bwlimit == 'None':
        rsync = ('rsync -az --rsync-path=\"sudo /usr/bin/rsync\"')
    else:
        rsync = ('rsync -az --bwlimit=%s --rsync-path=\"sudo /usr/bin/rsync\"' % str(bwlimit))
    if int(copy_procs) >= len(node_list) or int(copy_procs) == 0:
        success = run_rsync(file_path=file_path, node_list=node_list, dest_path=dest_path,
                            bwlimit=bwlimit, copy_procs=copy_procs, ssh_auth=ssh_auth)
    else:
        node_list = [node_list[i:i + copy_procs] for i in range(0, len(node_list), copy_procs)]
        logger.debug('splitting rsync processes into %s size chunks: %s', str(copy_procs), str(node_list))
        for sublist in node_list:
            _success = run_rsync(file_path, sublist, dest_path, bwlimit, copy_procs, ssh_auth)
            if not _success:
                success = False
                return success
                # return early. exit is in the calling func because delete_failed_bundle() is required
    return success

# def build_restore_plan(file_list, ring, dest_dirs, src_nodes, dest_nodes):
def build_restore_plan(file_list, dest_dirs, src_nodes, dest_nodes):
    '''
    Process the arguments and produce a restore plan - a dict/json containing restore bundles.
    A bundle is a sub-dictionary containing a set of 9 sstables, their source node, the destination nodes
    and the destination paths. The script iterates over the restore plan sequentially, and copies files
    as the plan specifies.

    Args:
      file_list (list): list of source files to add to the plan
      dest_dirs (list): list of destination dirs in the cluster
      src_nodes (list): list of source nodes. The bundles are grouped by source node in order to avoid file
                        name duplication at the destination
      dest_nodes (list): list of destination cluster nodes to copy the files to.

    Returns:
      d (dict): the restore plan
                {
                    source_node: {
                        base: { [1]
                            'files': [sstable1, sstable2, ...], [2]
                            'nodes': [dest_node1, dest_node2, ...], [3]
                            'dest_dir': '/destination/node/path/ks/table/upload'
                            'status': 'pending|done'
                        }

                    }
                }

    [1] the "base" looks like /path/to/source_node/keyspace/table/mc-XY
    [2] the file list is read from the TOC file
    [3] currently the default is all nodes.
    '''
    d = {}
    logger.info('start building restore plan')
    for sn in src_nodes:
        d[sn] = {}
    for fnum in range(len(file_list)):
        f = file_list[fnum]
        file = get_file_details(f)
        if file['base'] not in d[file['src_node']].keys():
            d[file['src_node']][file['base']] = {'files': []}
        try:
            with open(f, 'r') as fd:
                toc = fd.readlines()
        except Exception as e:
            logger.error('Failed to open TOC file %s', f, exc_info=True)
            sys.exit(1)
        logger.debug('TOC for %s: %s', file_list[fnum], str(toc))

        d[file['src_node']][file['base']]['bundle_size'] = 0
        for line in toc:
        # mc-<generation>-<big>-<component>
        # la-<generation>-<big>-<component>
        # <keyspace>-<column_family>-ka-<generation>-<component>
            if file['file_ver'] in ['la', 'mc']:
                d[file['src_node']][file['base']]['files'].append(file['base'] + '-big-' + line.strip())
                #first, last = get_sstable_tokens(file['base'] + '-big-Summary.db')
            else:
                d[file['src_node']][file['base']]['files'].append(file['base'] + '-' + line.strip())
                #first, last = get_sstable_tokens(file['base'] + '-Summary.db')
            #d[file['src_node']][file['base']]['first_token'] = first
            #d[file['src_node']][file['base']]['last_token'] = last
            #file['first_token'] = first
            #file['last_token'] = last
            #d[file['src_node']][file['base']]['nodes'] = get_destination_hosts(ring, file, dest_nodes)
            d[file['src_node']][file['base']]['nodes'] = dest_nodes
            for item in dest_dirs:
                dks = item.split('/')[-2]
                dtable = item.split('/')[-1].split('-')[0]
                if dks == file['keyspace'] and dtable == file['table']:
                    d[file['src_node']][file['base']]['dest_dir'] = item + '/upload'

        d[file['src_node']][file['base']]['status'] = 'pending'
        for i in d[file['src_node']][file['base']]['files']:
            d[file['src_node']][file['base']]['bundle_size'] += get_file_details(i)['size']
        logger.debug('Processing file %i of %i', ((fnum + 1)*9, len(file_list)*9))
    return d

def run_refresh(nodes, ks, table, path, uidgid, ssh_auth):
    '''
    Run refresh, cleanup and set file permissions on a set of nodes for a specified ks/table

    Args:
      nodes (list): list of nodes to refresh
      ks (str): keyspace to refresh
      table (str): table to refresh
      path (str): location on the nodes to monitor for refresh completion (upload dir should be empty when
                  refresh is done)
      uidgid (str): UID:GID for the scylla user the database runs under
      ssh_auth (dict): dictionary containing ssh authentication parameters
    '''
    #TODO: Add timeout
    logger.info('running refresh on %s/%s in %s on nodes: %s', ks, table, path, str(nodes))
    update_permissions(nodes, path, uidgid, ssh_auth)
    stop_compactions(nodes, ssh_auth)
    cmd = ('curl --fail -S -s -X POST %s/storage_service/sstables/%s?cf=%s' % (API_URL, ks, table))
    out = run_remote(cmd, nodes, ssh_auth)
    loaded = 0
    while loaded != len(nodes):
        cmd = ('sudo ls -1 %s | wc -l' % path)
        out = run_remote(cmd, nodes, ssh_auth)
        for node in nodes:
            if out[node]['stdout'][0] == '0':
                loaded += 1
    run_cleanup(nodes=nodes, keyspace=ks, ssh_auth=ssh_auth)

def update_permissions(nodes, path, uidgid, ssh_auth):
    '''
    Set the file permissions in path to uid:gid

    Args:
      nodes (list): list of nodes to update permissions on
      path (str): path on the nodes where file permissions should be set
      uidgid (str): UID:GID string to set
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      out (dict): output of the chown command on all the nodes
    '''
    cmd = ('sudo chown -R %s %s' % (uidgid, path))
    logger.info('Updating permissions to %s on %s in %s', uidgid, str(nodes), path)
    out = run_remote(cmd, nodes, ssh_auth)
    return out

def verify_scp_possible(nodes, path, ssh_auth):
    '''
    Verifies the ssh user has write access in path. The function will connect over ssh and try to create a file (using sudo).
    Will return False if the touch command fails.

    Args:
      nodes (list): list of node to check write access on
      path (str): path on the nodes to test for write access
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      success (bool): True if the path is writeable, False if not
    '''
    success = True
    cmd = ('sudo touch %s/test && sudo rm -f %s/test' % (path, path))
    logger.info('Checking for sudo write access at %s on nodes %s', path, str(nodes))
    out = run_remote(cmd, nodes, ssh_auth)
    return success

def stop_compactions(nodes, ssh_auth):
    '''
    Stops currently running compactions. This is not perfect as this will not stop new compactions, but it can ease the
    refresh process a bit, until compactions stop locking the storage and this is no longer needed.

    Args:
      node (list): nodes to issue the STOP command on
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      out (dict): outputs from all the nodes after running the compaction stop command
      '''
    cmd = ('curl --fail -S -s -X POST %s/compaction_manager/stop_compaction?type=COMPACTION' % API_URL)
    logger.info('Issuing a STOP to current compactions on nodes %s', str(nodes))
    out = run_remote(cmd, nodes, ssh_auth)
    return out

def run_cleanup(nodes, keyspace, ssh_auth):
    '''
    Run cleanup on the specified nodes/keyspaces. This will remove the data irrelevant to the node after refresh is done
    to ingest the data in the upload directories.

    Args:
      nodes (list): list of nodes to run cleanup on
      keyspace (str): keyspace to cleanup
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      out (dict): output from the cleanup command on all the nodes
    '''
    cmd = ('curl --fail -S -s -X POST %s/storage_service/keyspace_cleanup/%s' % (API_URL, keyspace))
    logger.info('Cleaning up keyspace %s on nodes %s', keyspace, str(nodes))
    out = run_remote(cmd, nodes, ssh_auth)
    return out

def check_df(bundle, config, ssh_auth):
    '''
    Compares the size of a restore bundle to the disk space available on the nodes and decides whether refresh/cleanup
    needs to be run to free up space before copying the bundle over. The disk space required is double the bundle size
    or the nodes need to remain under the free space quota (percentage) specified in the configuration file.

    Args:
      bundle (dict): file bundle (9 sstables) from a source node with a list of destination nodes
      config (dict): configuration provided by the user
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      refresh (bool): True is refresh is required, False if it is safe to copy the bundle.
    '''
    refresh = False

    src_size = bundle['bundle_size']
    # Check free space on hosts
    logger.debug('check for required refresh: %s', str(bundle))
    df = get_cluster_nodes_df(bundle['nodes'], config['scylla_data_path'], ssh_auth)
    for node in list(df.keys()):
        if 'free' in df[node].keys():
            # Ensure there's enough space for the incoming file set
            if int(df[node]['free']) <= int(src_size) * 2:
                logger.warning('Node %s does not have enough space for restored files: %s', node, str(src_size))
                refresh = True
            #Check percentage free
            p = round(int(df[node]['free']) * 100 / int(df[node]['size']))
            if p < int(config['free_space_percent_allowed']):
                logger.warning('Node %s is below the set free space threshold (%s percent free)', node, str(p))
                refresh = True
        else:
            logger.error('could not read df from %s', node)
            refresh = True
    return refresh

def restore_bundle(bundle, config, uidgid, ssh_auth):
    '''
    Copy a single bundle to a set of nodes taking all the necessary steps

    Args:
      bundle (dict): sstable file bundle (file list, destination node list etc)
      config (dict): user provided configuration
      uidgid (str): scylla file permissions
      ssh_auth (dict): dictionary containing ssh authentication parameters

    Returns:
      success (bool): True if all the files in the bundles successfully got copied to all the destination nodes
    '''
    success = True
    refresh = check_df(bundle, config, ssh_auth)
    try:
        ks = bundle['dest_dir'].split('/')[-3]
        table = bundle['dest_dir'].split('/')[-2].split('-')[0]
    except:
        logger.error('Invalid data in bundle %s', str(bundle))
        sys.exit(1)
    refresh_counter = 0 #we'll try to refresh multiple times before failing
    while refresh and refresh_counter < 3:
        run_refresh(bundle['nodes'], ks, table, bundle['dest_dir'], uidgid, ssh_auth)
        refresh = check_df(bundle, config, ssh_auth)
        refresh_counter += 1
    if refresh_counter >= 3:
        logger.error('Out of disk space on some of the nodes, with refresh/cleanup not sufficient to proceed')
        sys.exit(1)
    for file_num in range(len(bundle['files'])):
        success = copy_file(bundle['files'][file_num], bundle['nodes'],
                            bundle['dest_dir'], config['rsync_bwlimit'],
                            config['copy_processes'], ssh_auth)
        if not success: # no need to wait if something already failed
            return success # exit is processed in the calling function because delete_failed_bundle() is required
    return success

def execute_restore_plan(dest_nodes, schema, rplan, config, uidgid, file_list, ssh_auth):
    '''
    Iterate over source nodes and bundles within the restore plan and copy the file bundles as specified.
    Each finished bundle is marked as 'done', so that is resume is called, we can skip the already copied bundles

    Args:
      dest_nodes (list): list of destination nodes to copy bundles to
      schema (dict): the backed up schema in a {ks:[table,...]} format
      rplan (str): filename where the restore plan is stored and updated as the restore progresses.
      config (dict): user provided configuration
      uidgid (str): scylla file permissions
      file_list (list): the original list of TOC files in the restore set
      ssh_auth (dict): dictionary containing ssh authentication parameters
    '''
    def delete_partial_bundle(base, nodes, path, ssh_auth):
        '''
        If a bundle failed to copy in it's entirety, we stop the copy process and need to also cleanup the files
        that did copy over.

        Args:
          base (str): the bundle key /path/to/src_node/ks/table/mc-XY
          nodes (list): list of nodes to run deletion on
          path (str): dir path on the nodes to delete in
          ssh_auth (dict): dictionary containing ssh authentication parameters

        Returns:
          success (bool): True if delete completed successfully
        '''
        #TODO: re-check with ka sstables!
        success = True
        counter = 0
        while counter < len(nodes):
            b = base.split('/')[-1]
            cmd = ('sh -c "sudo rm -f %s/%s-*"' % (path, b))
            logger.debug('deleting leftovers from bundle %s in %s on nodes %s', base, path, str(nodes))
            run_remote(cmd, nodes, ssh_auth)
            cmd = ('sh -c "sudo ls -l %s/| grep %s- | wc -l"' % (path, b))
            logger.debug('checking for leftovers in %s on nodes %s', path, str(nodes))
            out = run_remote(cmd, nodes, ssh_auth)
            for node in nodes:
                if out[node]['stdout'] == ['0']:
                    counter += 1
                if out[node]['stderr'] != []:
                    success = False
        return success

    def find_largest_bundle(bundles):
        '''
        Since we are dealing with a dict (not sortable) and we need to try and restore the largest bundles
        first, this function will return the largest "pending" bundle in a source_node

        Args:
          bundles (dict): All the bundles per source_node

        Returns:
          largest (str): key/base of largest bundle
        '''
        prev_size = 0
        largest = ''
        for i in bundles.keys():
            if bundles[i]['bundle_size'] > prev_size and bundles[i]['status'] == 'pending':
                prev_size = bundles[i]['bundle_size']
                largest = i
        logger.debug('Largest bundle in set is %s', largest)
        return largest
    # Always go through the file
    with open(rplan, 'r') as f:
        restore_plan = json.loads(f.read())
    nodes = []
    dest_dirs = []
    logger.info('Beginning file copy: %s files, to %s nodes from %s source nodes',
                 str(len(file_list)*9), str(len(dest_nodes)), str(len(restore_plan.keys())))
    for sn in restore_plan.keys(): # iterate over source node dirs, to avoid filename conflicts
        logger.info('Source node: %s', sn)
        c = 0

        rc = find_largest_bundle(restore_plan[sn]) # restore_candidate
        while rc != '':
            c += 1
            logger.info('Copying file bundle %i of %i', c, len(restore_plan[sn].keys()))
            nodes += restore_plan[sn][rc]['nodes']
            dest_dirs.append(restore_plan[sn][rc]['dest_dir'])
            success = restore_bundle(restore_plan[sn][rc], config, uidgid, ssh_auth)
            if not success:
                delete_partial_bundle(rc, restore_plan[sn][rc]['nodes'],
                                        restore_plan[sn][rc]['dest_dir'], ssh_auth)
                logger.error('Failed to copy %s-* files, stopping', rc)
                sys.exit(1)
            else:
                # Update the in-memory running restore_plan and dump it's changes into the file
                restore_plan[sn][rc]['status'] = 'done'
                update_restore_plan(rplan, restore_plan)
            rc = find_largest_bundle(restore_plan[sn])
        logger.info('Ingesting copied files')
        nodes = list(set(nodes))
        dest_dirs = list(set(dest_dirs))
        for ks in schema.keys():
            for table in schema[ks]:
                for path in dest_dirs:
                    if path.split('/')[-2].split('-')[0] == table and path.split('/')[-3] == ks:
                        logger.info('Refreshing %s/%s located in %s', ks, table, path)
                        run_refresh(nodes, ks, table, path, uidgid, ssh_auth)
                        if not success:
                            sys.exit(1) #logging/output already done on failed refresh

def get_src_nodes(file_list):
    '''
    Scan the restore file set and gather the source node list

    Args:
      file_list (list): list of all the files (full paths) in the restore set

    Returns:
      src_nodes (list): list of source nodes
    '''
    src_nodes = []
    for f in file_list:
        src_nodes.append(f.split('/')[-4])
    logger.info('Source node list: %s', str(list(set(src_nodes))))
    return list(set(src_nodes))

def save_restore_plan(timestamp, restore_plan):
    '''
    Save the restore plan in a json file for later use

    Args:
      timestamp (str): timestamp for the running restore
      restore_plan (dict): dictionary containing the restore plan

    Returns:
      rplan (str): filename for the restore plan. Consists of timestamp + '-restore-plan.json'
    '''
    rplan = timestamp + '-restore-plan.json'
    with open(rplan, 'w') as f:
        json.dump(restore_plan, f, indent=2)
    logger.info('Saved restore plan in %s', rplan)
    return rplan

def update_restore_plan(rplan, restore_plan):
    '''
    Update the restore plan file with new data. This is always an overwrite.

    Args:
      rplan (str): filename for the restore plan
      restore_plan (dict): the full restore plan to dump into file
    '''
    logger.debug('Updating restore plan file %s', rplan)
    with open(rplan, 'w') as f:
        json.dump(restore_plan, f, indent=2)

def main(conf, resume):
    '''
    Args:
      conf (str): configuration yaml file
      resume (str|None): either a restore plan to resume or None for a new restore
    '''
    if resume:
        timestamp = '-'.join(resume.split('-')[:-2])
        logger.info('Resuming restore from %s', timestamp)
    else:
        timestamp = datetime.now().strftime("%b-%d-%Y-%H-%M-%S")
        logger.info('Restore plan will be stored in %s-restore-plan.json', timestamp)

    config = read_config(conf)
    logger.debug('Config read %s', str(config))

    global API_URL
    API_URL = config['api_url']

    # Configure ssh access details
    ssh_auth = configure_ssh_auth(config)

    # Read and parse the schema (we only need keyspaces and tables)
    schema = parse_schema_file(config['schema_file'])

    # Get list of cluster node IPs
    nodes = get_cluster_hosts(config['dest_host'], ssh_auth)
    logger.info('Cluster node list: %s', str(nodes))

    # Verify config "limit_to_nodes" setting
    if 'limit_to_nodes' in config.keys():
        if set(config['limit_to_nodes']).issubset(set(nodes)):
            logger.info('Detected a limited subset of nodes to use: %s', str(config['limit_to_nodes']))
            nodes = config['limit_to_nodes']
            logger.info('will be using limited node set: %s', str(nodes))
        else:
            logger.error('Nodes specified in configuration are not present in destination cluster')
            sys.exit(1)

    # Ensure schema versions are up to date
    logger.info('Verifying schema version in sync')
    check_schema_version(nodes, ssh_auth)

    # Find out the destination directories from the cluster
    dest_dirs = get_destination_dirs(schema, config, ssh_auth)

    # Check the ssh user has write permissions in the directories
    logger.info('Verifying node directories are writeable:')
    for dest_dir in dest_dirs:
        success = verify_scp_possible(nodes, dest_dir, ssh_auth=ssh_auth)
        if success:
            logger.info('- %s', dest_dir)
        # no need for else because the check will exit on verify_scp_possible()

    # Read the token ranges host affinity from the database per keyspace
    #ring = {}
    #for ks in schema.keys():
    #    ring[ks] = get_ks_ranges(config['dest_host'], ks, ssh_auth)

    # Map restored files
    file_list = get_toc_file_list(config['backup_root_path'], schema)
    src_nodes = get_src_nodes(file_list)
    uidgid = config['scylla_user'] + ':' + config['scylla_group']

    # check for resume or new restore plan
    if resume:
        rplan = resume
    else:
        # Build a new restore plan
        #restore_plan = build_restore_plan(file_list, ring, dest_dirs, src_nodes, nodes)
        restore_plan = build_restore_plan(file_list, dest_dirs, src_nodes, nodes)
        rplan = save_restore_plan(timestamp, restore_plan)

    # Run restore
    execute_restore_plan(nodes,schema, rplan, config, uidgid, file_list, ssh_auth)

    logger.info('Finished copying restore files')
    logger.info('Restore complete')

# root
args = get_args()

# logging configuration - double handlers for both file and stdout logging.
logging.basicConfig(format="%(asctime)s::%(name)s::%(levelname)s::%(message)s",
                            level=args.loglevel,
                            handlers=[logging.FileHandler("scylla_restore.log"),
                                      logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)
logger.info('Starting restore')

if args.resume and os.path.isfile(args.resume):
    logger.info("Retrying from %s", args.resume)
    main(args.config, args.resume)
else:
    main(args.config, None)


