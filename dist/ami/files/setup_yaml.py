#!/usr/bin/env python

import urllib2
import re
import time

instance_data = {}

def curl_instance_data(url):
    while True:
        try:
            req = urllib2.Request(url)
            return req
        except urllib2.HTTPError:
            print("Failed to grab %s..." % url)
            time.sleep(5)

def get_ec2_data():
    # Find internal instance type
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/instance-type')
    instancetype = urllib2.urlopen(req).read()
    print("Using instance type: %s" % instancetype)
    print("meta-data:instance-type: %s" % instancetype)

    if instancetype in ['t1.micro', 'm1.small', 'm1.medium']:
        exit_path("t1.micro, m1.small, and m1.medium instances are not supported. At minimum, use an m1.large instance.")

    # Find internal IP address for seed list
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/local-ipv4')
    instance_data['internalip'] = urllib2.urlopen(req).read()
    print("meta-data:local-ipv4: %s" % instance_data['internalip'])

    # Find public hostname
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/public-hostname')
    try:
        instance_data['publichostname'] = urllib2.urlopen(req).read()
        if not instance_data['publichostname']:
            raise
        print("meta-data:public-hostname: %s" % instance_data['publichostname'])
    except:
        # For VPC's and certain setups, this metadata may not be available
        # In these cases, use the internal IP address
        instance_data['publichostname'] = instance_data['internalip']
        print("meta-data:public-hostname: <same as local-ipv4>")

    # # Find public IP
    # req = curl_instance_data('http://169.254.169.254/latest/meta-data/public-ipv4')
    # try:
    #     instance_data['publicip'] = urllib2.urlopen(req).read()
    #     print("meta-data:public-ipv4: %s" % instance_data['publicip'])
    # except:
    #     # For VPC's and certain setups, this metadata may not be available
    #     # In these cases, use the internal IP address
    #     instance_data['publicip'] = instance_data['internalip']
    #     print("meta-data:public-ipv4: <same as local-ipv4>")

    # Find launch index for token splitting
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/ami-launch-index')
    instance_data['launchindex'] = int(urllib2.urlopen(req).read())
    print("meta-data:ami-launch-index: %s" % instance_data['launchindex'])

    # Find reservation-id for cluster-id and jmxpass
    req = curl_instance_data('http://169.254.169.254/latest/meta-data/reservation-id')
    instance_data['reservationid'] = urllib2.urlopen(req).read()
    print("meta-data:reservation-id: %s" % instance_data['reservationid'])

    instance_data['clustername'] = instance_data['reservationid']


def construct_yaml():
    with open('/var/lib/scylla/conf/scylla.yaml', 'r') as f:
        yaml = f.read()

    # Set listen_address
    p = re.compile('listen_address:.*')
    yaml = p.sub('listen_address: {0}'.format(instance_data['internalip']), yaml)

    # Set rpc_address
    p = re.compile('rpc_address:.*')
    yaml = p.sub('rpc_address: 0.0.0.0', yaml)

    # Set cluster_name to reservationid
    instance_data['clustername'] = instance_data['clustername'].strip("'").strip('"')
    yaml = yaml.replace("cluster_name: 'Test Cluster'", "cluster_name: '{0}'".format(instance_data['clustername']))

    if 'auto_bootstrap' in yaml:
        p = re.compile('auto_bootstrap:.*')
        yaml = p.sub('auto_bootstrap: false', yaml)
    else:
        yaml += "\nauto_bootstrap: false\n"

    with open('/var/lib/scylla/conf/scylla.yaml', 'w') as f:
        f.write(yaml)

    print('scylla.yaml configured.')


if __name__ == '__main__':
    print("Waiting for cloud-init to finish...")
    time.sleep(10)
    get_ec2_data()
    construct_yaml()
