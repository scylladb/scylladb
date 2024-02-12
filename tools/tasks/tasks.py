#!/usr/bin/env python3

import argparse
from command import Command
from executor import Executor


def get_help(cmd: Command) -> str:
    match cmd:
        case Command.LIST_MODULES:
            return "Get all modules names"
        case Command.LIST_TASKS:
            return "Get a list of tasks"
        case Command.WATCH_TASKS:
            return "Get a list of tasks and refresh "
        case Command.GET_STATUS:
            return "Get task status"
        case Command.WAIT:
            return "Wait for a task to complete"
        case Command.GET_TREE:
            return "Get statuses of the task and all its descendants"
        case Command.ABORT:
            return "Abort running task and its descendants"
        case Command.SET_TTL:
            return "Set ttl in seconds and get last value"

def parse_stats_request(parser: argparse.ArgumentParser):
    parser.add_argument('-m', '--module', required=True, help="The module to query about")
    parser.add_argument('--internal', action='store_true', help="Set if internal tasks should be shown")
    parser.add_argument('-k','--keyspace', help="The keyspace to query about")
    parser.add_argument('-t','--table', help="The table to query about")

def parse_status_request(parser: argparse.ArgumentParser):
    parser.add_argument('--id', required=True, help="The uuid of a task to query about")

def parse_abort_request(parser: argparse.ArgumentParser):
    parser.add_argument('--id', required=True, help="The uuid of a task to query about")

def parse_ttl_request(parser: argparse.ArgumentParser):
    parser.add_argument('--ttl', required=True, type=int, help="The number of seconds for which the tasks will be kept in memory after it finishes")


description = '\n'.join(['TODO'])
parser = argparse.ArgumentParser(description=description)
parser.add_argument('-n', '--node', default="localhost", help="Address of a node (default: localhost)")
parser.add_argument('-p', '--port', default=10000, help="Port (default: 10000)")

subparsers = parser.add_subparsers(dest='cmd')
subparsers.required = True

for cmd in Command:
    subparser = subparsers.add_parser(cmd.value, help=get_help(cmd))
    match cmd:
        case Command.LIST_TASKS | Command.WATCH_TASKS:
            parse_stats_request(subparser)
        case Command.GET_STATUS | Command.WAIT | Command.GET_TREE:
            parse_status_request(subparser)
        case Command.ABORT:
            parse_abort_request(subparser)
        case Command.SET_TTL:
            parse_ttl_request(subparser)

args = parser.parse_args()
executor = Executor(args)
executor.execute()
