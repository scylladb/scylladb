from argparse import Namespace
from command import Command
from printresponse import print_response
import requests
import time


class Executor:
    def __init__(self, args: Namespace):
        self.args = args
        self.url = self._get_url()
        self.params = self._get_params()

    def _get_url(self):
        endpoint = f"http://{self.args.node}:{self.args.port}/task_manager/"
        match self.args.cmd:
            case Command.LIST_MODULES:
                return endpoint + "list_modules"
            case Command.LIST_TASKS | Command.WATCH_TASKS:
                return endpoint + f"list_module_tasks/{self.args.module}"
            case Command.GET_STATUS:
                return endpoint + f"task_status/{self.args.id}"
            case Command.WAIT:
                return endpoint + f"wait_task/{self.args.id}"
            case Command.GET_TREE:
                return endpoint + f"task_status_recursive/{self.args.id}"
            case Command.ABORT:
                return endpoint + f"abort_task/{self.args.id}"
            case Command.SET_TTL:
                return endpoint + "ttl"
        
    def _get_params(self):
        params = dict()
        match self.args.cmd:
            case Command.LIST_TASKS | Command.WATCH_TASKS:
                if self.args.internal is not None:
                    params['internal'] = self.args.internal
                if self.args.keyspace is not None:
                    params["keyspace"] = self.args.keyspace
                if self.args.table is not None:
                    params["table"] = self.args.table
            case Command.SET_TTL:
                params['ttl'] = self.args.ttl
        return params

    def execute(self):
        if self.args.cmd in [Command.ABORT, Command.SET_TTL]:
            r = requests.post(url=self.url, params=self.params)
            print_response(r, self.args.cmd)
        elif self.args.cmd == Command.WATCH_TASKS:
            while True:
                r = requests.get(url=self.url, params=self.params)
                print_response(r, self.args.cmd)
                time.sleep(10)
        else:
            r = requests.get(url=self.url, params=self.params)
            print_response(r, self.args.cmd)
