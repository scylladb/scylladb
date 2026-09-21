import logging
import re
import subprocess

logger = logging.getLogger(__name__)


def execute_iptables_command(cmd):
    logger.debug(f"Executing following command: {cmd}")
    p_open = subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output, err = p_open.communicate()
    assert p_open.returncode == 0, err
    output = output.decode()
    logger.debug(output)
    return output


class IPTableRule:
    def __init__(  # noqa: PLR0913
        self,
        protocol: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        source_port: str or int = None,
        destination_port: str or int = None,
        target: str | None = None,
    ):
        self.action = None
        self.protocol = protocol
        self.source = source
        self.destination = destination
        self.source_port = source_port
        self.destination_port = destination_port
        self.target = target
        self._translate = {
            "protocol": "--proto {}",
            "source": "--source {}",
            "destination": "--destination {}",
            "source_port": "--source-port {}",
            "destination_port": "--destination-port {}",
            "target": "--jump {}",
        }

    def to_string(self):
        cmd_list = []
        for field_name, field_format in self._translate.items():
            field_value = getattr(self, field_name)
            if field_value is not None:
                cmd_list.append(self._translate[field_name].format(field_value))
        cmd_str = " ".join(cmd_list)
        return cmd_str

    def __str__(self):
        return self.to_string()

    __repr__ = __str__


class IPTable:
    def __init__(self, chain_name):
        """
        sudo iptables -nL
        Chain INPUT (policy ACCEPT)
        target     prot opt source               destination

        Chain FORWARD (policy DROP)
        target     prot opt source               destination
        DOCKER-USER  all  --  0.0.0.0/0            0.0.0.0/0
        DOCKER-ISOLATION-STAGE-1  all  --  0.0.0.0/0            0.0.0.0/0
        ACCEPT     all  --  0.0.0.0/0            0.0.0.0/0            ctstate RELATED,ESTABLISHED
        DOCKER     all  --  0.0.0.0/0            0.0.0.0/0
        ACCEPT     all  --  0.0.0.0/0            0.0.0.0/0
        ACCEPT     all  --  0.0.0.0/0            0.0.0.0/0

        Chain DOCKER-USER (1 references)
        target     prot opt source               destination
        RETURN     all  --  0.0.0.0/0            0.0.0.0/0
        """
        self._parsers = {
            # The "found_chain_names" regex returns the list of names of the chains that appear in "sudo iptables -nL"
            "chain_names": re.compile(r"^[Cc]hain\s(?P<chain_name>[-\w\d]+)", flags=re.MULTILINE),
            # Each chain contains a list of rules, that contains the following fields for each rule: target, port, opt,
            #  source, destination, and filed without the name.
            # example:
            "rule": re.compile(r"^.*\s+(?P<target>DROP|ACCEPT|RETURN)\s+(?P<protocol>\w+)\s+(?P<opt>--)\s+" r"(?P<source>[/.\w\d]+)\s+(?P<destination>[/.\w\d]+)\s+(?P<extra_details>.+)$"),
            "extra_details": [
                re.compile(r".*spt:(?P<source_port>\d+)"),
                re.compile(r".*dpt:(?P<destination_port>\d+)"),
            ],
        }
        self.chain_name = chain_name

    @property
    def chain_names(self):
        output = execute_iptables_command(cmd="sudo iptables -L")
        return self._parsers["chain_names"].findall(output)

    @property
    def rules(self):
        rules = []
        output = execute_iptables_command(cmd=f"iptables -nL {self.chain_name} --line-numbers")
        for line in output.splitlines()[2:]:
            params = self._parsers["rule"].match(line).groupdict()
            params.pop("opt", None)
            extra_details = params.pop("extra_details")
            for regex in self._parsers["extra_details"]:
                match = regex.match(extra_details)
                if match:
                    params.update(match.groupdict())
            rule = IPTableRule(**params)
            rules.append(rule)
        return rules

    def add_rule(self, rule: IPTableRule):
        execute_iptables_command(f"sudo iptables --append {self.chain_name} {rule.to_string()}")

    def delete_rule(self, rule: IPTableRule):
        execute_iptables_command(f"sudo iptables --delete {self.chain_name} {rule.to_string()}")

    def is_rule_exists(self, rule: IPTableRule):
        cmd_str = rule.to_string()
        output = execute_iptables_command(f"sudo iptables --check {self.chain_name} {cmd_str}")
        if output.lower().startswith("iptables: bad rule"):
            return False
        return True

    def delete_chain(self, chain=None):
        chain = chain or self.chain_name
        execute_iptables_command(f"sudo iptables --flush {chain}")
        rule = IPTableRule(protocol="all", target=chain)
        execute_iptables_command(cmd=f"sudo iptables --delete INPUT {rule.to_string()}")
        execute_iptables_command(cmd=f"sudo iptables --delete-chain {chain}")

    def create_new_chain(self, chain=None):
        chain = chain or self.chain_name
        execute_iptables_command(f"sudo iptables --new {chain}")
        rule = IPTableRule(protocol="all", target=chain)
        execute_iptables_command(cmd=f"sudo iptables --append INPUT {rule.to_string()}")
