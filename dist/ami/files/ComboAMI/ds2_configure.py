#!/usr/bin/env python
### Script provided by DataStax.
import base64

import exceptions
import glob
import json
import os
import random
import re
import shlex
import subprocess
import sys
import tempfile
import time
import traceback
import urllib2
import urllib

import gzip
import StringIO
from email.parser import Parser

from argparse import ArgumentParser

import logger
import conf

instance_data = {}
config_data = {}
config_data['conf_path'] = os.path.expanduser("/var/lib/scylla/conf/")
config_data['opsc_conf_path'] = os.path.expanduser("/etc/opscenter/")
options = False

def exit_path(errorMsg, append_msg=False):
    if not append_msg:
        # Remove passwords from printing: -p
        p = re.search('(-p\s+)(\S*)', instance_data['userdata'])
        if p:
            instance_data['userdata'] = instance_data['userdata'].replace(p.group(2), '****')

        # Remove passwords from printing: --password
        p = re.search('(--password\s+)(\S*)', instance_data['userdata'])
        if p:
            instance_data['userdata'] = instance_data['userdata'].replace(p.group(2), '****')

        append_msg = " Aborting installation.\n\nPlease verify your settings:\n{0}".format(instance_data['userdata'])

    errorMsg += append_msg

    logger.error(errorMsg)
    conf.set_config("AMI", "Error", errorMsg)

    raise AttributeError(errorMsg)

def curl_instance_data(url):
    while True:
        try:
            req = urllib2.Request(url)
            return req
        except urllib2.HTTPError:
            logger.info("Failed to grab %s..." % url)
            time.sleep(5)

def read_instance_data(req):
    data = urllib2.urlopen(req).read()
    try:
        stream = StringIO.StringIO(data)
        gzipper = gzip.GzipFile(fileobj=stream)
        return gzipper.read()
    except IOError:
        stream = StringIO.StringIO(data)
        return stream.read()

def is_multipart_mime(data):
    match = re.search('Content-Type: multipart', data)
    if match: return True

def get_user_data(req):
    data = read_instance_data(req)
    if is_multipart_mime(data):
        message = Parser().parsestr(data)
        for part in message.walk():
            if (part.get_content_type() == 'text/plaintext'):
                match = re.search('totalnodes', part.get_payload())
                if (match): return part.get_payload()
    else:
        return data

def get_ec2_data():
    conf.set_config("AMI", "CurrentStatus", "Installation started")

    # Try to get EC2 User Data
    try:
        req = curl_instance_data('http://169.254.169.254/latest/user-data/')
        instance_data['userdata'] = get_user_data(req)

        logger.info("Started with user data set to:")
        logger.info(instance_data['userdata'])

        # Trim leading Rightscale UserData
        instance_data['userdata'] = instance_data['userdata'][instance_data['userdata'].find('--'):]

        if len(instance_data['userdata']) < 2:
            raise Exception

        logger.info("Using user data:")
        logger.info(instance_data['userdata'])
    except Exception, e:
        instance_data['userdata'] = '--totalnodes 1 --version Community --clustername "Test Cluster - No AMI Parameters"'
        logger.info("No userdata found. Starting 1 node clusters, by default.")

    # Find internal instance type
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/instance-type')
    instancetype = urllib2.urlopen(req).read()
    logger.info("Using instance type: %s" % instancetype)
    logger.info("meta-data:instance-type: %s" % instancetype)

    if instancetype in ['t1.micro', 'm1.small', 'm1.medium']:
        exit_path("t1.micro, m1.small, and m1.medium instances are not supported. At minimum, use an m1.large instance.")

    # Find internal IP address for seed list
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/local-ipv4')
    instance_data['internalip'] = urllib2.urlopen(req).read()
    logger.info("meta-data:local-ipv4: %s" % instance_data['internalip'])

    # Find public hostname
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/public-hostname')
    try:
        instance_data['publichostname'] = urllib2.urlopen(req).read()
        if not instance_data['publichostname']:
            raise
        logger.info("meta-data:public-hostname: %s" % instance_data['publichostname'])
    except:
        # For VPC's and certain setups, this metadata may not be available
        # In these cases, use the internal IP address
        instance_data['publichostname'] = instance_data['internalip']
        logger.info("meta-data:public-hostname: <same as local-ipv4>")

    # # Find public IP
    # req = curl_instance_data('http://169.254.169.254/latest/meta-data/public-ipv4')
    # try:
    #     instance_data['publicip'] = urllib2.urlopen(req).read()
    #     logger.info("meta-data:public-ipv4: %s" % instance_data['publicip'])
    # except:
    #     # For VPC's and certain setups, this metadata may not be available
    #     # In these cases, use the internal IP address
    #     instance_data['publicip'] = instance_data['internalip']
    #     logger.info("meta-data:public-ipv4: <same as local-ipv4>")

    # Find launch index for token splitting
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/ami-launch-index')
    instance_data['launchindex'] = int(urllib2.urlopen(req).read())
    logger.info("meta-data:ami-launch-index: %s" % instance_data['launchindex'])

    # Find reservation-id for cluster-id and jmxpass
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/reservation-id')
    instance_data['reservationid'] = urllib2.urlopen(req).read()
    logger.info("meta-data:reservation-id: %s" % instance_data['reservationid'])

    instance_data['clustername'] = instance_data['reservationid']

def parse_ec2_userdata():
    # Setup parser
    parser = ArgumentParser()

    # Option that requires either: Enterprise or Community
    parser.add_argument("--version", action="store", type=str, dest="version")
    # Option that specifies how the ring will be divided
    parser.add_argument("--totalnodes", action="store", type=int, dest="totalnodes")
    # Option that specifies the cluster's name
    parser.add_argument("--clustername", action="store", type=str, dest="clustername")
    # Option that allows for a release version of Enterprise or Community e.g. 1.0.2
    parser.add_argument("--release", action="store", type=str, dest="release")
    # Option that forces the rpc binding to the internal IP address of the instance
    parser.add_argument("--rpcbinding", action="store_true", dest="rpcbinding", default=False)

    # Option for multi-region
    parser.add_argument("--multiregion", action="store_true", dest="multiregion", default=False)
    parser.add_argument("--seeds", action="store", dest="seeds")
    parser.add_argument("--opscenterip", action="store", dest="opscenterip")

    # Option that specifies how the number of Analytics nodes
    parser.add_argument("--analyticsnodes", action="store", type=int, dest="analyticsnodes")
    # Option that specifies how the number of Search nodes
    parser.add_argument("--searchnodes", action="store", type=int, dest="searchnodes")
    # Option that forces Hadoop analytics nodes over Spark analytics nodes
    parser.add_argument("--hadoop", action="store_true", dest="hadoop")

    # Option that specifies the CassandraFS replication factor
    parser.add_argument("--cfsreplicationfactor", action="store", type=int, dest="cfsreplication")

    # Option that specifies the username
    parser.add_argument("--username", action="store", type=str, dest="username")
    # Option that specifies the password
    parser.add_argument("--password", action="store", type=str, dest="password")

    # Option that specifies the installation of OpsCenter on the first node
    parser.add_argument("--opscenter", action="store", type=str, dest="opscenter")
    # Option that specifies an alternative reflector.php
    parser.add_argument("--reflector", action="store", type=str, dest="reflector")

    # Unsupported dev options
    # Option that allows for just configuring opscenter
    parser.add_argument("--opscenteronly", action="store_true", dest="opscenteronly")
    # Option that allows for just configuring RAID0 on the attached drives
    parser.add_argument("--raidonly", action="store_true", dest="raidonly")
    # Option that allows for an OpsCenter to enable the SSL setting
    parser.add_argument("--opscenterssl", action="store_true", dest="opscenterssl")
    # Option that enforces a bootstrapping node
    parser.add_argument("--bootstrap", action="store_true", dest="bootstrap", default=False)
    # Option that enforces vnodes
    parser.add_argument("--vnodes", action="store_true", dest="vnodes", default=False)
    # Option that allows for an emailed report of the startup diagnostics
    parser.add_argument("--email", action="store", type=str, dest="email")
    # Option that allows heapsize to be changed
    parser.add_argument("--heapsize", action="store", type=str, dest="heapsize")
    # Option that allows an interface port for OpsCenter to be set
    parser.add_argument("--opscenterinterface", action="store", type=str, dest="opscenterinterface")
    # Option that allows a custom reservation id to be set
    parser.add_argument("--customreservation", action="store", type=str, dest="customreservation")
    # Option that allows custom scripts to be executed
    parser.add_argument("--base64postscript", action="store", type=str, dest="base64postscript")
    # Option that allows to download and execute a custom script
    parser.add_argument("--postscript_url", action="store", type=str, dest="postscript_url")

    # Grab provided reflector through provided userdata
    global options
    try:
        (options, unknown) = parser.parse_known_args(shlex.split(instance_data['userdata']))
    except:
        exit_path("One of the options was not set correctly.")

    if not options.analyticsnodes:
        options.analyticsnodes = 0
    if not options.searchnodes:
        options.searchnodes = 0

    if os.path.isfile('/etc/datastax_ami.conf'):
        if options.version and options.version.lower() == "community":
            logger.error("The Dynamic DataStax AMI will automatically install DataStax Enterprise.")
        conf.set_config("AMI", "Type", "Enterprise")
    else:
        if options.version:
            if options.version.lower() == "community":
                conf.set_config("AMI", "Type", "Community")
            elif options.version.lower() == "enterprise":
                conf.set_config("AMI", "Type", "Enterprise")
            else:
                exit_path("Invalid --version (-v) argument.")

def use_ec2_userdata():
    if not options:
        exit_path("No parsed options found.")

    if not options.totalnodes:
        exit_path("Missing required --totalnodes (-n) switch.")

    if (options.analyticsnodes + options.searchnodes) > options.totalnodes:
        exit_path("Total nodes assigned (--analyticsnodes + --searchnodes) > total available nodes (--totalnodes)")

    if conf.get_config("AMI", "Type") == "Community" and (options.cfsreplication or options.analyticsnodes or options.searchnodes):
        exit_path('CFS Replication, Analytics Nodes, and Search Node settings can only be set in DataStax Enterprise installs.')

    if options.email:
        logger.info('Setting up diagnostic email using: {0}'.format(options.email))
        conf.set_config("AMI", "Email", options.email)

    if options.clustername:
        logger.info('Using cluster name: {0}'.format(options.clustername))
        instance_data['clustername'] = options.clustername

    if options.customreservation:
        instance_data['reservationid'] = options.customreservation

    if options.seeds:
        instance_data['seeds'] = options.seeds

    if options.opscenterip:
        instance_data['opscenterip'] = options.opscenterip

    options.realtimenodes = (options.totalnodes - options.analyticsnodes - options.searchnodes)
    options.seed_indexes = [0, options.realtimenodes, options.realtimenodes + options.analyticsnodes]

    logger.info('Using cluster size: {0}'.format(options.totalnodes))
    conf.set_config("Cassandra", "TotalNodes", options.totalnodes)
    logger.info('Using seed indexes: {0}'.format(options.seed_indexes))

    if options.reflector:
        logger.info('Using reflector: {0}'.format(options.reflector))


def get_seed_list():
    # Read seed list from reflector
    index_set = set(options.seed_indexes)
    if options.totalnodes in index_set:
        index_set.remove(options.totalnodes)
    expected_responses = len(index_set)

    time_in_loop = time.time()
    continue_loop = True
    logger.info('Reflector loop...')
    while continue_loop:
        if time.time() - time_in_loop > 10 * 60:
            exit_path('EC2 is experiencing some issues and has not allocated all of the resources in under 10 minutes.', '\n\nAborting the clustering of this reservation. Please try again.')

        if options.reflector:
            reflector = options.reflector
        else:
            reflector = 'http://reflector2.datastax.com/reflector2.php'

        req = urllib2.Request('{0}?indexid={1}&reservationid={2}&internalip={3}&externaldns={4}&second_seed_index={5}&third_seed_index={6}'.format(
                                    reflector,
                                    instance_data['launchindex'],
                                    instance_data['reservationid'],
                                    instance_data['internalip'],
                                    instance_data['publichostname'],
                                    options.seed_indexes[1],
                                    options.seed_indexes[2]
                             ))
        req.add_header('User-agent', 'DataStaxSetup')
        try:
            response = urllib2.urlopen(req).read()
            response = json.loads(response)

            status =  "{0} Reflector: Received {1} of {2} responses from: {3}".format(
                            time.strftime("%m/%d/%y-%H:%M:%S", time.localtime()),
                            response['number_of_returned_ips'],
                            expected_responses,
                            response['seeds']
                      )
            conf.set_config("AMI", "CurrentStatus", status)
            logger.info(status)

            if response['number_of_returned_ips'] == expected_responses:
                conf.set_config("OpsCenter", "DNS", response['opscenter_dns'])

                config_data['seed_list'] = set(response['seeds'])
                config_data['opscenterseed'] = response['seeds'][0]

                continue_loop = False
            else:
                time.sleep(2 + random.randint(0, options.totalnodes / 4 + 1))
        except:
            if expected_responses == 1:
                conf.set_config("AMI", "CurrentStatus", "Bypassing reflector for 1 node cluster...")

                conf.set_config("OpsCenter", "DNS", instance_data['publichostname'])

                config_data['seed_list'] = set([instance_data['internalip']])
                config_data['opscenterseed'] = instance_data['internalip']

                continue_loop = False

            traceback.print_exc(file=sys.stdout)
            time.sleep(2 + random.randint(0, 5))

def construct_yaml():
    with open(os.path.join(config_data['conf_path'], 'scylla.yaml'), 'r') as f:
        yaml = f.read()

    # Create the seed list
    seeds_yaml = ','.join(config_data['seed_list'])

    if options.seeds:
        if options.bootstrap:
            # Do not include current node while bootstrapping
            seeds_yaml = options.seeds
        else:
            # Add current node to seed list for multi-region setups
            seeds_yaml = seeds_yaml + ',' + options.seeds

    # Set seeds for DSE/C
    p = re.compile('seeds:.*')
    yaml = p.sub('seeds: "{0}"'.format(seeds_yaml), yaml)

    # Set listen_address
    p = re.compile('listen_address:.*')
    yaml = p.sub('listen_address: {0}'.format(instance_data['internalip']), yaml)

    # Set rpc_address
    p = re.compile('rpc_address:.*')
    if options.rpcbinding:
        yaml = p.sub('rpc_address: {0}'.format(instance_data['internalip']), yaml)
    else:
        yaml = p.sub('rpc_address: 0.0.0.0', yaml)

        # needed for 2.1+
        p = re.compile('# broadcast_rpc_address:.*')
        yaml = p.sub('broadcast_rpc_address: {0}'.format(instance_data['internalip']), yaml)

    if options.multiregion:
        # multiregion: --rpcbinding is implicitly true
        yaml = p.sub('rpc_address: {0}'.format(instance_data['internalip']), yaml)
        yaml = yaml.replace('endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch', 'endpoint_snitch: org.apache.cassandra.locator.Ec2MultiRegionSnitch')
        yaml = yaml.replace('endpoint_snitch: SimpleSnitch', 'endpoint_snitch: Ec2MultiRegionSnitch')
        p = re.compile('# broadcast_address: 1.2.3.4')
        req = curl_instance_data('http://169.254.169.254/latest/meta-data/public-ipv4')
        instance_data['externalip'] = urllib2.urlopen(req).read()
        logger.info("meta-data:external-ipv4: %s" % instance_data['externalip'])
        yaml = p.sub('broadcast_address: {0}'.format(instance_data['externalip']), yaml)

# XXX: Commented out to use SimpleSnitch
    # Uses the EC2Snitch for Community Editions
#    if conf.get_config("AMI", "Type") == "Community":
#        yaml = yaml.replace('endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch', 'endpoint_snitch: org.apache.cassandra.locator.GossipingPropertyFileSnitch')
#        yaml = yaml.replace('endpoint_snitch: SimpleSnitch', 'endpoint_snitch: GossipingPropertyFileSnitch')

    # Set cluster_name to reservationid
    instance_data['clustername'] = instance_data['clustername'].strip("'").strip('"')
    yaml = yaml.replace("cluster_name: 'Test Cluster'", "cluster_name: '{0}'".format(instance_data['clustername']))

    # Set auto_bootstrap
    if options.bootstrap:
        if 'auto_bootstrap' in yaml:
            p = re.compile('auto_bootstrap:.*')
            yaml = p.sub('auto_bootstrap: true', yaml)
        else:
            yaml += "\nauto_bootstrap: true\n"
    else:
        if 'auto_bootstrap' in yaml:
            p = re.compile('auto_bootstrap:.*')
            yaml = p.sub('auto_bootstrap: false', yaml)
        else:
            yaml += "\nauto_bootstrap: false\n"

    if conf.get_config('Cassandra', 'partitioner') == 'random_partitioner':
        # Construct token for an equally split ring
        logger.info('Cluster tokens: {0}'.format(config_data['tokens']))

        if instance_data['launchindex'] < options.seed_indexes[1]:
            token = config_data['tokens'][0][instance_data['launchindex']]

        if options.seed_indexes[1] <= instance_data['launchindex'] and instance_data['launchindex'] < options.seed_indexes[2]:
            token = config_data['tokens'][1][instance_data['launchindex'] - options.realtimenodes]

        if options.seed_indexes[2] <= instance_data['launchindex']:
            token = config_data['tokens'][2][instance_data['launchindex'] - options.realtimenodes - options.analyticsnodes]

        p = re.compile( 'initial_token:.*')
        yaml = p.sub('initial_token: {0}'.format(token), yaml)

    elif conf.get_config('Cassandra', 'partitioner') == 'murmur':
        if conf.get_config('Cassandra', 'vnodes') == 'True' or options.vnodes:
            p = re.compile( '# num_tokens:.*')
            yaml = p.sub('num_tokens: 256', yaml)
        else:
            if instance_data['launchindex'] < options.seed_indexes[1]:
                tokens = [((2**64 / options.realtimenodes) * i) - 2**63 for i in range(options.realtimenodes)]
                token = str(tokens[instance_data['launchindex']])

            if options.seed_indexes[1] <= instance_data['launchindex'] and instance_data['launchindex'] < options.seed_indexes[2]:
                tokens = [((2**64 / options.analyticsnodes) * i) - 2**63 for i in range(options.analyticsnodes)]
                token = str(tokens[instance_data['launchindex'] - options.realtimenodes] + 10000)

            if options.seed_indexes[2] <= instance_data['launchindex']:
                tokens = [((2**64 / options.searchnodes) * i) - 2**63 for i in range(options.searchnodes)]
                token = str(tokens[instance_data['launchindex'] - options.realtimenodes - options.analyticsnodes] + 20000)

            p = re.compile( 'initial_token:.*')
            yaml = p.sub('initial_token: {0}'.format(token), yaml)

    with open(os.path.join(config_data['conf_path'], 'scylla.yaml'), 'w') as f:
        f.write(yaml)

    logger.info('scylla.yaml configured.')


if __name__ == '__main__':
    print("Waiting for cloud-init to finish...")
    time.sleep(10)
    try:
    	get_ec2_data()
    except urllib2.HTTPError:
        exit_path("Clusters backed by Spot Instances are not supported.")

    parse_ec2_userdata()
    use_ec2_userdata()
    get_seed_list()
    construct_yaml()
