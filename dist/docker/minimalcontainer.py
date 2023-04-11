import os
import sys
import pwd
import glob
import subprocess
from pathlib import Path
sys.path.append('/opt/scylladb/scripts')
from scylla_util import sysconfig_parser

scylla_bin_env = {
    'SCYLLA_HOME': '/var/lib/scylla',
    'SCYLLA_CONF': '/etc/scylla',
    'GNUTLS_SYSTEM_PRIORITY_FILE': '/opt/scylladb/libreloc/gnutls.config',
    'LD_LIBRARY_PATH': '/opt/scylladb/libreloc',
    'UBSAN_OPTIONS': 'suppressions=/opt/scylladb/libexec/ubsan-suppressions.supp'
}

def is_minimal_container():
    return os.path.exists('/opt/scylladb/SCYLLA-MINIMAL-CONTAINER-FILE')

def run_script(args, kwargs):
    python_bin = glob.glob('/opt/scylladb/python3/libexec/python3.*.bin')[0]
    cmdline = args[0]
    exec_path = Path(cmdline[0]).absolute()
    cmdline[0] = str(exec_path.parent / 'libexec' / exec_path.name)
    cmdline = ['/opt/scylladb/python3/libexec/ld.so', python_bin, '-s'] + cmdline
    kwargs['env'] = {'PYTHONPATH': '{}:{}'.format(str(exec_path.parent), str(exec_path.parent / 'libexec'))}
    subprocess.check_call(*[cmdline], **kwargs)

def io_setup():
    cmdline = ['/opt/scylladb/libexec/iotune', '--format', 'envfile', '--options-file', '/etc/scylla.d/io.conf', '--properties-file', '/etc/scylla.d/io_properties.yaml', '--evaluation-directory', '/var/lib/scylla/data', '--evaluation-directory', '/var/lib/scylla/commitlog', '--evaluation-directory', '/var/lib/scylla/hints', '--evaluation-directory', '/var/lib/scylla/view_hints']
    subprocess.run(cmdline, check=True, env=scylla_bin_env)

def generate_cmdline(arguments):
    cmdline = ['/opt/scylladb/libexec/scylla']
    scylla_server_conf = sysconfig_parser('/etc/default/scylla-server')
    scylla_args = scylla_server_conf.get('SCYLLA_ARGS')
    cmdline += scylla_args.split()
    if arguments.developerMode == '0' and arguments.io_setup == '1':
        io_conf = sysconfig_parser('/etc/scylla.d/io.conf')
        seastar_io = io_conf.get('SEASTAR_IO')
        cmdline += seastar_io.split()
    if arguments.developerMode == '1':
        dev_mode_conf = sysconfig_parser('/etc/scylla.d/dev-mode.conf')
        dev_mode = dev_mode_conf.get('DEV_MODE')
        cmdline += dev_mode.split()
    if arguments.cpuset:
        cpuset_conf = sysconfig_parser('/etc/scylla.d/cpuset.conf')
        cpuset = cpuset_conf.get('CPUSET')
        cmdline += cpuset.split()
    docker_conf = sysconfig_parser('/etc/scylla.d/docker.conf')
    scylla_docker_args = docker_conf.get('SCYLLA_DOCKER_ARGS')
    cmdline += scylla_docker_args.split()
    return cmdline

def execve_as(path, args, env, user):
    pw_record = pwd.getpwnam(user)
    os.setgid(pw_record.pw_uid)
    os.setuid(pw_record.pw_gid)
    env |= {'USER': user}
    os.execve(path, args, env)

def exec_scylla(arguments):
    cmdline = generate_cmdline(arguments)
    execve_as(cmdline[0], cmdline, scylla_bin_env, 'nonroot')
