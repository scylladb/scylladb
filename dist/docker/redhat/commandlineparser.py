import argparse


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--developer-mode', default='1', choices=['0', '1'], dest='developerMode')
    parser.add_argument('--experimental', default=0, choices=['0', '1'])
    parser.add_argument('--seeds', default=None, help="specify seeds - if left empty will use container's own IP")
    parser.add_argument('--cpuset', default=None, help="e.g. --cpuset 0-3 for the first four CPUs")
    parser.add_argument('--smp', default=None, help="e.g --smp 2 to use two CPUs")
    parser.add_argument('--memory', default=None, help="e.g. --memory 1G to use 1 GB of RAM")
    parser.add_argument('--overprovisioned', default='0', choices=['0', '1'], help="run in overprovisioned environment")
    parser.add_argument('--listen-address', default=None, dest='listenAddress')
    parser.add_argument('--broadcast-address', default=None, dest='broadcastAddress')
    parser.add_argument('--broadcast-rpc-address', default=None, dest='broadcastRpcAddress')
    parser.add_argument('--api-address', default=None, dest='apiAddress')
    return parser.parse_args()
