#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import re
import subprocess
from itertools import chain

import pytest

import docker
from dtest_class import Tester

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.single_node
class TestScyllaHelpCommand(Tester):
    def test_scylla_help_does_not_contain_duplicate_args(self):
        scylla_help_text = self.get_scylla_help_text()
        args_list = self.get_args_from_help_text(scylla_help_text)
        self.args_list_should_not_contain_duplicates(args_list)

    def get_scylla_help_text(self):
        self.cluster.populate(1)
        node1 = self.cluster.nodelist()[0]
        docker_image = getattr(self.cluster, "docker_image", None)
        if docker_image is not None:
            client = docker.from_env()
            # couldn't use simpler way of running (detach=False) due rc=1 causing exception to be raised
            container = client.containers.run(image=docker_image, command="--help", entrypoint="/bin/scylla", detach=True)
            container.wait(timeout=5)
            help_text = container.logs().decode()
        else:
            cli_args = [self.cluster.manager.server_get_exe(server_id=node1.server_id), "--help"]
            logger.debug(f"running command: {' '.join([str(a) for a in cli_args])}")
            help_text = subprocess.run(cli_args, capture_output=True, text=True, check=False).stdout
        assert "Scylla options:" in help_text, f"Scylla help text is wrong: {help_text}"
        return help_text

    def get_args_from_help_text(self, help_text):
        # Regexp for parsing arguments from the output of `scylla --help' command:
        # $ scylla --help
        # ...
        #   -h [ --help ]                         show help message
        #   --version                             print version number and exit
        #   --options-file arg                    configuration file (i.e.
        # ...
        #   -W [ --workdir ] arg                  The directory in which Scylla will put
        # ...
        args = re.compile(r"^  (?:(?:(?P<short_arg>-\w) \[ (?P<long_arg>--[\w-]+) \])|(?P<arg>--[\w-]+))(?P<val> arg)?", re.M).findall(help_text)
        args = [x.strip() for x in chain.from_iterable(args) if x and x.strip() != "arg"]
        assert len(args) > 100, f"seems scylla args were not extracted properly. Found args: {args}"
        return args

    def args_list_should_not_contain_duplicates(self, args):
        duplicates = set([x for x in args if args.count(x) > 1])
        assert not duplicates, f"Scylla help text contains argument duplicates but shouldn't: {duplicates}"
