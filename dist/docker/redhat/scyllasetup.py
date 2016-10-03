import subprocess
import logging
import yaml
import os

class ScyllaSetup:
    def __init__(self, arguments):
        self._developerMode = arguments.developerMode
        self._seeds = arguments.seeds
        self._cpuset = arguments.cpuset
        self._listenAddress = arguments.listenAddress
        self._broadcastAddress = arguments.broadcastAddress
        self._broadcastRpcAddress = arguments.broadcastRpcAddress
        self._smp = arguments.smp
        self._memory = arguments.memory
        self._overprovisioned = arguments.overprovisioned

    def _run(self, *args, **kwargs):
        logging.info('running: {}'.format(args))
        subprocess.check_call(*args, **kwargs)

    def developerMode(self):
        self._run(['/usr/lib/scylla/scylla_dev_mode_setup', '--developer-mode', self._developerMode])

    def cpuSet(self):
        if self._cpuset is None:
            return
        self._run(['/usr/lib/scylla/scylla_cpuset_setup', '--cpuset', self._cpuset])

    def io(self):
        self._run(['/usr/lib/scylla/scylla_io_setup'])

    def scyllaYAML(self):
        configuration = yaml.load(open('/etc/scylla/scylla.yaml'))
        if self._listenAddress is None:
            self._listenAddress = subprocess.check_output(['hostname', '-i']).decode('ascii').strip()
        configuration['listen_address'] = self._listenAddress
        configuration['rpc_address'] = self._listenAddress
        if self._seeds is None:
            if self._broadcastAddress is not None:
                self._seeds = self._broadcastAddress
            else:
                self._seeds = self._listenAddress
        configuration['seed_provider'] = [
                {'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                 'parameters': [{'seeds': self._seeds}]}
                ]
        if self._broadcastAddress is not None:
            configuration['broadcast_address'] = self._broadcastAddress
        if self._broadcastRpcAddress is not None:
            configuration['broadcast_rpc_address'] = self._broadcastRpcAddress
        with open('/etc/scylla/scylla.yaml', 'w') as file:
            yaml.dump(configuration, file)

    def cqlshrc(self):
        home = os.environ['HOME']
        hostname = subprocess.check_output(['hostname', '-i']).decode('ascii').strip()
        with open("%s/.cqlshrc" % home, "w") as cqlshrc:
            cqlshrc.write("[connection]\nhostname = %s\n" % hostname)

    def arguments(self):
        args = ""
        if self._memory is not None:
            args += "--memory %s" % self._memory
        if self._smp is not None:
            args += " --smp %s" % self._smp
        if self._overprovisioned == "1":
            args += " --overprovisioned"
        with open("/etc/scylla.d/docker.conf", "w") as cqlshrc:
            cqlshrc.write("SCYLLA_DOCKER_ARGS=\"%s\"\n" % args)
