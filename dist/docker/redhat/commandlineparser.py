import argparse


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--developer-mode', default='1', choices=['0', '1'], dest='developerMode')
    parser.add_argument('--seeds', default=None, help="specify seeds - if left empty will use container's own IP")
    parser.add_argument('--cpuset', default=None, help="e.g. --cpuset 0-3 for the first four CPUs")
    parser.add_argument('--broadcast-address', default=None, dest='broadcastAddress')
    return parser.parse_args()
