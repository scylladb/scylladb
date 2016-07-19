import subprocess
import logging
import yaml


class ScyllaSetup:
    def __init__(self, arguments):
        self._developerMode = arguments.developerMode
        self._seeds = arguments.seeds
        self._cpuset = arguments.cpuset
        self._broadcastAddress = arguments.broadcastAddress

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
        IP = subprocess.check_output(['hostname', '-i']).decode('ascii').strip()
        configuration['listen_address'] = IP
        configuration['rpc_address'] = IP
        if self._seeds is None:
            self._seeds = IP
        configuration['seed_provider'] = [
                {'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                 'parameters': [{'seeds': self._seeds}]}
                ]
        if self._broadcastAddress is not None:
            configuration['broadcast_address'] = self._broadcastAddress
        with open('/etc/scylla/scylla.yaml', 'w') as file:
            yaml.dump(configuration, file)

    def enableServices(self):
        self._run('systemctl enable scylla-server', shell=True)
        self._run('systemctl enable scylla-jmx', shell=True)
