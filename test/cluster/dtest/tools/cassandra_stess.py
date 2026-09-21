import logging
import re

from tools.stress_thread_docker import DockerBasedStressThread


class CassandraStressError(Exception):
    pass


logger = logging.getLogger(__name__)

cloudconf_regex = re.compile(r"-cloudconf\s*file=(.*?)($|\s)")

profile_regex = re.compile(r"profile=(.*?)($|\s)")


class CassandraStressDocker(DockerBasedStressThread):
    def __init__(self, node, stress_cmd, **kwargs):
        options_implying_node = ["-node ", "-cloudconf "]
        if any(opt in stress_cmd for opt in options_implying_node):
            # no need to specify address
            pass
        else:
            stress_cmd += f" -node {node.address()}"

        if cloudconf_match := cloudconf_regex.search(stress_cmd):
            config_path = cloudconf_match.group(1)
            kwargs["volumes"] = [*kwargs.get("volumes", []), f"{config_path}:{config_path}"]

        if match := profile_regex.search(stress_cmd):
            profile = match.group(1)
            kwargs["volumes"] = [*kwargs.get("volumes", []), f"{profile}:{profile}"]

        super().__init__(stress_cmd=stress_cmd, node=node, container_name="cassandra-stress", **kwargs)
