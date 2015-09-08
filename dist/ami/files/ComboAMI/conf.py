#!/usr/bin/env python
### Script provided by DataStax.

import ConfigParser

configfile = '/home/fedora/ami.conf'

config = ConfigParser.RawConfigParser()
config.read(configfile)
try:
    config.add_section('AMI')
    config.add_section('Cassandra')
    config.add_section('OpsCenter')
except:
    pass


def set_config(section, variable, value):
    config.set(section, variable, value)
    with open(configfile, 'wb') as configtext:
        config.write(configtext)

def get_config(section, variable):
    try:
        config.read(configfile)
        return config.get(section, variable.lower())
    except:
        return False
