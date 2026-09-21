import logging
import os
import re
import time
from ast import literal_eval
from datetime import UTC, datetime, timedelta
from enum import Enum
from pprint import pformat
from re import findall
from typing import Literal

import pytest
import yaml
from cassandra import ConsistencyLevel
from ccmlib import common
from ccmlib.scylla_node import ScyllaNode
from dateutil.parser import parse

from dtest_class import WaitTimeoutExpiredError, create_cf, create_ks, wait_for
from dtest_config import DTestConfig
from dtest_setup import DTestSetup, copy_logs
from dtest_setup_overrides import DTestSetupOverrides
from tools.data import insert_c1c2, insert_c1c2_with_clustering

logger = logging.getLogger(__name__)

SPACE_PLACEHOLDER = r"SPACE"
C1_PREFIX = "value%d"
C2_PREFIX = "other_value%d"


class ComparableHealthCheckField:
    __hash__ = False

    def __eq__(self, other):
        for field_name, field_value in self.__dict__.items():
            if field_value is not None and field_value != getattr(other, field_name):
                logger.warning(f'The value of "{field_name}" is "{getattr(other, field_name)}", but expected value is "{field_value}"')
                return False
        return True

    def __str__(self):
        return "\n".join(f"{field_name}={field_value}" for field_name, field_value in self.__dict__.items())


class Status(ComparableHealthCheckField):
    def __init__(self, status=None, uptime=None, uptime_type=None):
        self.status = status
        self.uptime = uptime
        self.uptime_type = uptime_type


class Uptime(ComparableHealthCheckField):
    def __init__(self, hours=None, minutes=None, seconds=None):
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds


class Memory(ComparableHealthCheckField):
    def __init__(self, size=None, unit=None):
        self.size = size
        self.unit = unit


class ScyllaManagerError(Exception):
    """
    A custom exception for Manager related errors
    """


class ScyllaManagerParserError(ScyllaManagerError):
    pass


class HostSsl(Enum):
    ON = "ON"
    OFF = "OFF"

    @classmethod
    def from_str(cls, output_str):
        if "SSL" in output_str:
            return HostSsl.ON
        return HostSsl.OFF


class HostRestStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    TIMEOUT = "TIMEOUT"
    UNAUTHORIZED = "UNAUTHORIZED"
    HTTP = "HTTP"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            if output_str == "-":
                return cls.DOWN
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError(f"Could not recognize returned host rest status: {output_str}")


class TaskStatus(Enum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"
    ERROR = "ERROR"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    WAITING = "WAITING"
    STARTING = "STARTING"
    ABORTED = "ABORTED"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            output_str = output_str if len(output_str) == 1 else output_str.split()[0]
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError(f"Could not recognize returned task status: {output_str}")

    @classmethod
    def all_members(cls):
        return cls._member_map_.values()  # pylint:disable=no-member


class AlternatorStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"


class CqlStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"


class NodeStatus(Enum):
    UP = "UN"
    DOWN = "DN"


class MgrUtils:
    @staticmethod
    def verify_errorless_result(cmd, stdout, stderr):
        if stderr:
            logger.debug(f"Encountered an error on '{cmd}' command response: {stdout!s}")
            raise ScyllaManagerError(f"Encountered an error on '{cmd}' command response: {stderr}")


class ScyllaManagerApiBase:
    def __init__(self, sctool, cmd_translate_dict, parsers=None):
        self.sctool = sctool
        self.cmd_translate_dict = cmd_translate_dict
        self.parsers = parsers or {}

    @staticmethod
    def create_sctool_command(cmd_options: dict, cmd_hierarchy: list or str):
        if isinstance(cmd_hierarchy, list):
            options_list = cmd_hierarchy.copy()
        else:
            options_list = str(cmd_hierarchy).split()
        for option_key, option_value in cmd_options.items():
            if option_value is None:
                continue
            # see https://github.com/scylladb/scylla-manager/issues/3066
            if isinstance(option_value, bool):
                options_list.append(str(f"{option_key}={str(option_value).lower()}"))
                continue

            options_list.append(option_key)
            if "cron" in option_key:
                options_list.append(f"{SPACE_PLACEHOLDER}".join(map(str, option_value)))
            elif isinstance(option_value, list):
                options_list.append(r",".join(map(str, option_value)))
            else:
                options_list.append(str(option_value))
        return options_list

    def create_command_options(self, cmd_options_dict: dict):
        return {self.cmd_translate_dict[option_key]: option_value for option_key, option_value in cmd_options_dict.items() if option_value is not None and option_key in self.cmd_translate_dict}

    def parse_output(self, output, regex_name):
        def convert_to_real_type(_field):
            try:
                if _field is None:
                    return None
                return literal_eval(_field)
            except (ValueError, SyntaxError):
                return _field

        result, regex_result = {}, {}
        if regex_name not in self.parsers:
            raise ScyllaManagerParserError(f"There is no parser named '{regex_name}'.\nThe following parsers are exists: '{list(self.parsers)}'")
        regexes = self.parsers[regex_name]
        if isinstance(regexes, list):
            for field_regex in regexes:
                parser_result = field_regex.match(output)
                if parser_result:
                    regex_result.update(parser_result.groupdict())
        else:
            parser_result = regexes.match(output)
            if parser_result:
                regex_result.update(parser_result.groupdict())
        if not regex_result:
            raise ScyllaManagerParserError(f"The following output could not be parsed:\n'{output}'")

        for field_name, field_value in regex_result.items():
            if field_value is not None:
                if "," in field_value:
                    result[field_name] = [convert_to_real_type(field) for field in field_value.split(",")]
                else:
                    result[field_name] = convert_to_real_type(field_value)
        return result


class ScyllaManagerBackupApi(ScyllaManagerApiBase):
    def __init__(self, sctool):
        cmd_translate_dict = {
            "dc_names": "--dc",
            "dry_run": "--dry-run",
            "keyspace_list": "--keyspace",
            "location_list": "--location",
            "num_retries": "--num-retries",
            "rate_limit_list": "--rate-limit",
            "retention": "--retention",
            "is_show_tables": "--show-tables",
            "snapshot_parallel_list": "--snapshot-parallel",
            "cron": "--cron",
            "name": "--name",
            "window": "--window",
            "timezone": "--timezone",
            "upload_parallel_list": "--upload-parallel",
            "cluster_name": "--cluster",
            "enabled": "--enabled",
            "show_all_clusters": "--all-clusters",
            "delimiter": "--delimiter",
            "snapshot_tag": "--snapshot-tag",
            "with_version": "--with-version",
            "max_date": "--max-date",
            "min_date": "--min-date",
            "method": "--method",
        }
        super().__init__(sctool=sctool, cmd_translate_dict=cmd_translate_dict)

    def backup(  # noqa: PLR0913
        self,
        dc_names: list or str = None,
        dry_run: bool | None = None,
        keyspace_list: list or str = None,
        location_list: list or str = None,
        num_retries: int | None = None,
        rate_limit_list: list or str = None,
        retention: int | None = None,
        is_show_tables: bool | None = None,
        snapshot_parallel_list: list or str = None,
        cron: list | None = None,
        name: str | None = None,
        window: list | None = None,
        timezone: str | None = None,
        upload_parallel_list: list or str = None,
        cluster_name: str | None = None,
        sctool_kwargs: dict | None = None,
        method: Literal["auto", "native", "rclone"] | None = None,
    ):
        """
        Schedules backups
        Usage:
          sctool backup [flags]
          sctool backup [command]
        Available Commands:
          delete      Deletes backup snapshot
          files       Lists files in backup
          list        Lists available backups
          update      Modifies a backup task
        Flags:
          --dc list                  a comma-separated list of datacenter glob patterns, e.g. 'dc1,!otherdc*' used
            to specify the DCs to include or exclude from backup
          --dry-run                  validates and prints backup information without scheduling a backup
          -i, --interval string          task schedule interval e.g. 3d2h10m, valid units are d, h, m, s (default "0")
          -K, --keyspace list            a comma-separated list of keyspace/tables glob patterns, e.g.
           'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup
          -L, --location list            a comma-separated list of backup locations in the format
           [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different
           datacenters are being used to upload data to different locations. <name> must be an alphanumeric string
           and may contain a dash and or a dot, but other characters are forbidden. The only supported storage
           <provider> at the moment is s3
          --method
            Specify the API used for uploading files:
                - `auto`: Use the native API when possible, otherwise use the Rclone API.
                - `native`: Scylla server uploads directly to backup location (supports only S3 provider).
                - `rclone`: Scylla Manager Agent uploads to backup location.
          -r, --num-retries int          the number of times a scheduled task will retry to run before failing
           (default 3)
          --rate-limit list          a comma-separated list of megabytes (MiB) per second rate limits expressed in the
            format [<dc>:]<limit>. The <dc>: part is optional and only needed when different datacenters need different
            upload limits. Set to 0 for no limit (default 100)
          --retention int            The number of backups which are to be stored (default 3)
          --show-tables              print all table names for a keyspace
          --snapshot-parallel list   a comma-separated list of snapshot parallelism limits in the format
            [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected
            datacenters. If The <dc>: part is not set, the limit is global (e.g. 'dc1:2,5') the runs are parallel in n
            nodes (2 in dc1) and n nodes in all the other datacenters
          -s, --start-date string        specifies the task start date expressed in the RFC3339 format or
           now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s (default "now")
          --upload-parallel list     a comma-separated list of upload parallelism limits in the format
          [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected
          datacenters. If The <dc>: part is not set the limit is global (e.g. 'dc1:2,5') the runs are parallel in n
          nodes (2 in dc1) and n nodes in all the other datacenters
        Global Flags:
              --api-cert-file path   path to HTTPS client certificate to access Scylla Manager server
              --api-key-file path    path to HTTPS client key to access Scylla Manager server
              --api-url URL          URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
          -c, --cluster name         Specifies the target cluster name or ID
        Use "sctool backup [command] --help" for more information about a command.
        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/2.1/sctool/#backup
        """
        options = self.create_command_options(cmd_options_dict=locals())
        stdout = self.sctool.run(cmd=self.create_sctool_command(cmd_options=options, cmd_hierarchy="backup"), **(sctool_kwargs or {"is_verify_errorless_result": True}))[0]
        return BackupTask(task_id=stdout[0][0].strip(), cluster_id=cluster_name, scylla_manager=self.sctool.scylla_manager)

    def update(  # noqa: PLR0913
        self,
        backup_id: str,
        dc_names: list or str = None,
        dry_run: bool | None = None,
        enabled: str | None = None,
        keyspace_list: list or str = None,
        location_list: list or str = None,
        name: str | None = None,
        window: list | None = None,
        timezone: str | None = None,
        num_retries: int | None = None,
        rate_limit_list: list or str = None,
        retention: int | None = None,
        is_show_tables: bool | None = None,
        snapshot_parallel_list: list or str = None,
        cron: list | None = None,
        upload_parallel_list: list or str = None,
        cluster_name: str | None = None,
        sctool_kwargs: dict | None = None,
    ):
        """
        Modifies a backup task
        Usage:
          sctool backup update <type/task-id> [flags]
        Flags:
          --dc list                  a comma-separated list of datacenter glob patterns, e.g. 'dc1,!otherdc*' used
             to specify the DCs to include or exclude from backup
          --dry-run                  validates and prints backup information without scheduling a backup
          -e, --enabled string           enabled (default "true")
          -i, --interval string          task schedule interval e.g. 3d2h10m, valid units are d, h, m, s
            (default "0")
          -K, --keyspace list            a comma-separated list of keyspace/tables glob patterns, e.g.
            'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup
          -L, --location list            a comma-separated list of backup locations in the format
            [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different
            datacenters are being used to upload data to different locations. <name> must be an alphanumeric string
            and may contain a dash and or a dot, but other characters are forbidden. The only supported storage
            <provider> at the moment is s3
          -r, --num-retries int          the number of times a scheduled task will retry to run before failing (
            default 3)
          --rate-limit list          a comma-separated list of megabytes (MiB) per second rate limits expressed in
            the format [<dc>:]<limit>. The <dc>: part is optional and only needed when different datacenters need
            different upload limits. Set to 0 for no limit (default 100)
          --retention int            The number of backups which are to be stored (default 3)
          --show-tables              print all table names for a keyspace
          --snapshot-parallel list   a comma-separated list of snapshot parallelism limits in the format
            [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected
            datacenters. If The <dc>: part is not set, the limit is global (e.g. 'dc1:2,5') the runs are parallel
            in n nodes (2 in dc1) and n nodes in all the other datacenters
          -s, --start-date string        specifies the task start date expressed in the RFC3339 format or
            now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s (default "now")
          --upload-parallel list     a comma-separated list of upload parallelism limits in the format
            [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected
            datacenters. If The <dc>: part is not set the limit is global (e.g. 'dc1:2,5') the runs are parallel
            in n nodes (2 in dc1) and n nodes in all the other datacenters
        Global Flags:
              --api-cert-file path   path to HTTPS client certificate to access Scylla Manager server
              --api-key-file path    path to HTTPS client key to access Scylla Manager server
              --api-url URL          URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
          -c, --cluster name         Specifies the target cluster name or ID
        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/2.1/sctool/#backup-update
        """
        options = self.create_command_options(cmd_options_dict=locals())
        return self.sctool.run(cmd=self.create_sctool_command(cmd_options=options, cmd_hierarchy=["backup", "update", backup_id]), **(sctool_kwargs or {"is_verify_errorless_result": True}))


class ScyllaManagerTaskApi(ScyllaManagerApiBase):
    def __init__(self, sctool):
        cmd_translate_dict = {
            "is_show_all_tasks": "--all",
            "sort": "--sort",
            "status": "--status",
            "task_type": "--type",
            "cluster_name": "--cluster",
        }
        parsers = {
            "arguments": [
                re.compile(r".*-K\s(\')?(?P<keyspace_list>[\d\w,]+)(\')?($|\s)"),
                re.compile(r".*L\s(\')?(?P<location_list>[\d\w:-]+)(\')?($|\s)"),
                re.compile(r".*\s--retention\s(\')?(?P<retention>\d+)(\')?($|\s)"),
                re.compile(r".*\s--rate-limit\s(\')?(?P<rate_limit>[\d,]+)(\')?($|\s)"),
                re.compile(r"--host\s'?(?P<host>(\d+.){3}\d+)'?($|\s)"),
                re.compile(r".*\s--snapshot-parallel\s(\')?(?P<snapshot_parallel_list>[\d,]+)(\')?($|\s)"),
                re.compile(r".*\s--upload-parallel\s(\')?(?P<upload_parallel_list>[\d,]+)(\')?($|\s)"),
                re.compile(r".*\s--intensity\s(\')?(?P<intensity>[\d]+)(\')?($|\s)"),
                re.compile(r".*\s--parallel\s(\')?(?P<parallel>[\d]+)(\')?($|\s)"),
            ],
        }
        super().__init__(sctool=sctool, cmd_translate_dict=cmd_translate_dict, parsers=parsers)

    def list(  # noqa: PLR0913
        self,
        is_show_all_tasks: bool | None = None,
        sort: str | None = None,
        status: str | None = None,
        task_type: str | None = None,
        cluster_name: str | None = None,
        sctool_kwargs: dict | None = None,
    ):
        """
        Shows available tasks and their last run status
        Usage:
          sctool task list [flags]
        Flags:
          -a, --all             list disabled tasks as well
          --sort string     returned results will be sorted by given key, valid values:
          [start-time next-activation end-time status]
          -s, --status string   filter tasks according to last run status
          -t, --type string     task type
        Global Flags:
              --api-cert-file path   path to HTTPS client certificate to access Scylla Manager server
              --api-key-file path    path to HTTPS client key to access Scylla Manager server
              --api-url URL          URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
          -c, --cluster name         Specifies the target cluster name or ID
        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/2.1/sctool/#task-list
        """
        options = self.create_command_options(cmd_options_dict=locals())
        return self.sctool.run(cmd=self.create_sctool_command(cmd_options=options, cmd_hierarchy=["tasks"]), **(sctool_kwargs or {"is_verify_errorless_result": True}))


class ScyllaManagerStatusApi(ScyllaManagerApiBase):
    def __init__(self, sctool):
        cmd_translate_dict = {
            "cluster_name": "--cluster",
        }
        parsers = {
            "Datacenter": re.compile(r"(?P<data_center>[\w\d]+)"),
            "": re.compile(r"(?P<status>\w+)"),
            "Alternator": re.compile(r"(?P<alternator_status>\w+)\s\((?P<alternator_timeout>\d+)" r"(?P<alternator_timeout_type>\w+)\)"),
            "CQL": re.compile(r"(?P<cql_status>\w+)\s\((?P<cql_timeout>\d+)(?P<cql_timeout_type>\w+)\)"),
            "REST": re.compile(r"(?P<rest_status>\w+)\s\((?P<rest_timeout>\d+)(?P<rest_timeout_type>\w+)\)"),
            "Address": re.compile(r"(?P<address>[\d.]+)"),
            "Uptime": re.compile(r"((?P<hours>\d+)h)?((?P<minutes>\d+)m)?((?P<seconds>\d+)s)?"),
            "CPUs": re.compile(r"(?P<cpus>\d+)"),
            "Memory": re.compile(r"((?P<memory_size>[\d.]+)(?P<memory_unit>\w+))"),
            "Scylla": re.compile(r"(?P<scylla_version>[\w\d.-]+)"),
            "Agent": re.compile(r"(?P<agent_version>[\w\d.-]+)"),
            "Host ID": re.compile(r"(?P<host_id>[\w\d.-]+)"),
        }
        super().__init__(sctool=sctool, cmd_translate_dict=cmd_translate_dict, parsers=parsers)

    def status(self, cluster_name: str | None = None, sctool_kwargs: dict | None = None):
        """
        Shows cluster status

        Usage:
          sctool status [flags]

        Flags:

        Global Flags:
              --api-cert-file path   path to HTTPS client certificate to access Scylla Manager server
              --api-key-file path    path to HTTPS client key to access Scylla Manager server
              --api-url URL          URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
          -c, --cluster name         Specifies the target cluster name or ID

        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/2.1/sctool/#status
        """
        options = self.create_command_options(cmd_options_dict=locals())
        return self.sctool.run(cmd=self.create_sctool_command(cmd_options=options, cmd_hierarchy=["status"]), **(sctool_kwargs or {"is_verify_errorless_result": True}))


class ScyllaManagerRepairApi(ScyllaManagerApiBase):
    def __init__(self, sctool):
        cmd_translate_dict = {
            "dc_names": "--dc",
            "dry_run": "--dry-run",
            "enabled": "--enabled",
            "is_fail_fast": "--fail-fast",
            "intensity": "--intensity",
            "keyspace_list": "--keyspace",
            "num_retries": "--num-retries",
            "parallel": "--parallel",
            "is_show_tables": "--show-tables",
            "small_table_threshold": "--small-table-threshold",
            "ignore_down_hosts": "--ignore-down-hosts",
            "cron": "--cron",
            "name": "--name",
            "window": "--window",
            "timezone": "--timezone",
            "cluster_name": "--cluster",
            "host": "--host",
        }
        parsers = {}
        super().__init__(sctool=sctool, cmd_translate_dict=cmd_translate_dict, parsers=parsers)

    def repair(  # noqa: PLR0913
        self,
        dc_names: list or str = None,
        dry_run: bool | None = None,
        ignore_down_hosts: bool | None = None,
        is_fail_fast: bool | None = None,
        intensity: float | None = None,
        name: str | None = None,
        window: list | None = None,
        timezone: str | None = None,
        keyspace_list: list or str = None,
        num_retries: int | None = None,
        parallel: int | None = None,
        is_show_tables: bool | None = None,
        small_table_threshold: str | None = None,
        cron: list | None = None,
        cluster_name: str | None = None,
        sctool_kwargs: dict | None = None,
        host: str | None = None,
    ):
        """
        Usage:
          sctool repair [flags]
          sctool repair [command]
        Available Commands:
          control     Changes settings of running repairs to control speed and load
          update      Modifies a repair task
        Flags:
              --dc list                        a comma-separated list of datacenter glob patterns, e.g. 'dc1,!otherdc*',
                                                used to specify the DCs to include or exclude from repair
              --dry-run                        validate and print repair information without scheduling a repair
              --fail-fast                      stop repair on first error
              --intensity float                integer >= 1 or a decimal between (0,1), higher values may result in
                                                higher speed and cluster load. 0 value means repair at maximum intensity
                                                 (default 1)
          -i, --interval string                task schedule interval e.g. 3d2h10m, valid units are d, h, m, s
                                                (default "0")
          -K, --keyspace list                  a comma-separated list of keyspace/tables glob patterns, e.g.
                                                'keyspace,!keyspace.table_prefix_*' used to include or exclude
                                                keyspaces from backup
          -r, --num-retries int                the number of times a scheduled task will retry to run before failing
                                                (default 3)
              --parallel int                   The maximum number of repair jobs to run in parallel, each node can
                                                participate in at most one repair at any given time.
                                               Default is means system will repair at maximum parallelism
              --show-tables                    print all table names for a keyspace. Used only in conjunction with
                                                --dry-run
              --small-table-threshold string   enable small table optimization for tables of size lower than given
                                                threshold. Supported units [B, MiB, GiB, TiB] (default "1GiB")
          -s, --start-date string              specifies the task start date expressed in the RFC3339 format or
                                                now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s
                                                (default "now")
        Global Flags:
              --api-cert-file path   path to HTTPS client certificate to access Scylla Manager server
              --api-key-file path    path to HTTPS client key to access Scylla Manager server
              --api-url URL          URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
          -c, --cluster name         Specifies the target cluster name or ID

        Use "sctool repair [command] --help" for more information about a command.
        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/2.1/sctool/#repair
        """
        options = self.create_command_options(cmd_options_dict=locals())
        stdout = self.sctool.run(cmd=self.create_sctool_command(cmd_options=options, cmd_hierarchy="repair"), **(sctool_kwargs or {"is_verify_errorless_result": True}))[0]
        return RepairTask(task_id=stdout[0][0].strip(), cluster_id=cluster_name, scylla_manager=self.sctool.scylla_manager)

    def update(  # noqa: PLR0913
        self,
        repair_id: str,
        dc_names: list or str = None,
        dry_run: bool | None = None,
        enabled: str | None = None,
        is_fail_fast: bool | None = None,
        intensity: float | None = None,
        ignore_down_hosts: bool | None = None,
        keyspace_list: list or str = None,
        num_retries: int | None = None,
        parallel: int | None = None,
        is_show_tables: bool | None = None,
        small_table_threshold: str | None = None,
        cron: list | None = None,
        cluster_name: str | None = None,
        sctool_kwargs: dict | None = None,
        host: str | None = None,
        name: str | None = None,
        timezone: str | None = None,
        window: list | None = None,
    ):
        """
        Usage:
          sctool repair update <type/task-id> [flags]
        Flags:
              --dc list                        a comma-separated list of datacenter glob patterns, e.g.
                                                'dc1,!otherdc*', used to specify the DCs to include or exclude from
                                                 repair
              --dry-run                        validate and print repair information without scheduling a repair
          -e, --enabled string                 enabled (default "true")
              --fail-fast                      stop repair on first error
              --intensity float                integer >= 1 or a decimal between (0,1), higher values may result in
                                                higher speed and cluster load. 0 value means repair at maximum
                                                intensity (default 1)
          -i, --interval string                task schedule interval e.g. 3d2h10m, valid units are d, h, m, s
                                                (default "0")
          -K, --keyspace list                  a comma-separated list of keyspace/tables glob patterns, e.g.
                                                'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces
                                                 from backup
          -r, --num-retries int                the number of times a scheduled task will retry to run before failing
                                                (default 3)
              --parallel int                   The maximum number of repair jobs to run in parallel, each node can
                                                participate in at most one repair at any given time.
                                               Default is means system will repair at maximum parallelism
              --show-tables                    print all table names for a keyspace. Used only in conjunction with
                                                --dry-run
              --small-table-threshold string   enable small table optimization for tables of size lower than given
                                                threshold. Supported units [B, MiB, GiB, TiB] (default "1GiB")
          -s, --start-date string              specifies the task start date expressed in the RFC3339 format or
                                                now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s
                                                (default "now")
        Global Flags:
              --api-cert-file path   path to HTTPS client certificate to access Scylla Manager server
              --api-key-file path    path to HTTPS client key to access Scylla Manager server
              --api-url URL          URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
          -c, --cluster name         Specifies the target cluster name or ID

        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/2.1/sctool/#repair-update
        """
        options = self.create_command_options(cmd_options_dict=locals())
        return self.sctool.run(cmd=self.create_sctool_command(cmd_options=options, cmd_hierarchy=["repair", "update", repair_id]), **(sctool_kwargs or {"is_verify_errorless_result": True}))


class ScyllaManagerBase:
    def __init__(self, _id, scylla_manager):
        self.id = _id
        self.sctool = SCTool(scylla_manager=scylla_manager)
        self.backup_api = ScyllaManagerBackupApi(sctool=self.sctool)
        self.task_api = ScyllaManagerTaskApi(sctool=self.sctool)
        self.status_api = ScyllaManagerStatusApi(sctool=self.sctool)
        self.repair_api = ScyllaManagerRepairApi(sctool=self.sctool)
        self.scylla_manager = scylla_manager

    def get_property(self, parsed_table, column_name, is_search_substring=False, identifier=None):
        identifier = identifier or self.id
        return self.sctool.get_table_value(parsed_table=parsed_table, column_name=column_name, identifier=identifier, is_search_substring=is_search_substring)


class ScyllaManagerTool(ScyllaManagerBase):
    """
    Provides communication with scylla-manager, operating sctool commands and ssh-scripts.
    """

    def __init__(self, scylla_manager):
        ScyllaManagerBase.__init__(self, _id="MANAGER", scylla_manager=scylla_manager)
        sleep = 5
        logger.debug(f"Sleep {sleep} seconds, waiting for manager service ready to respond")
        time.sleep(sleep)
        logger.debug(f"Initiating Scylla-Manager, version: {self.version}")
        self.DEFAULT_USER = "centos"

    def restart_manager_server(self, gently):
        self.scylla_manager.stop(gently)
        self.scylla_manager.start()

    @property
    def version(self):
        cmd = "version"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def parsed_cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        stdout, _stderr = self.cluster_list
        return self.sctool.get_table_complete_column(parsed_table=stdout, column_name="name")

    def get_cluster(self, cluster_name):
        """
        Returns Manager Cluster object by a given name if exist, else returns none.
        """
        # ╭──────────────────────────────────────┬──────────╮
        # │ ID                                   │ name     │
        # ├──────────────────────────────────────┼──────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ Dev_Test │
        # │ bf6571ef-21d9-4cf1-9f67-9d05bc07b32e │ Prod     │
        # ╰──────────────────────────────────────┴──────────╯
        try:
            cluster_id = self.sctool.get_table_value(parsed_table=self.cluster_list, column_name="ID", identifier=cluster_name)
        except ScyllaManagerError as e:
            logger.debug(f"Cluster name not found in Scylla-Manager: {e}")
            return None

        return ManagerCluster(scylla_manager=self.scylla_manager, cluster_id=cluster_id)

    def _get_cluster_hosts_ip(self, db_cluster):
        return [node_data[1] for node_data in self._get_cluster_hosts_with_ips(db_cluster=db_cluster)]

    def _get_cluster_hosts_with_ips(self, db_cluster):
        ip_addr_attr = "public_ip_address"
        return [[n, getattr(n, ip_addr_attr)] for n in db_cluster.nodes]

    def add_cluster(  # noqa: PLR0913
        self,
        name,
        node=None,
        db_cluster=None,
        client_encrypt=None,
        user=None,
        create_user=None,
        single_node=False,
        by_name=False,
        method=None,
    ):
        """
        :param name: cluster name
        :param node: cluster node IP
        :param db_cluster: scylla cluster
        :param client_encrypt: is TSL client encryption enable/disable
        :return: ManagerCluster

        Add a cluster to manager

        Usage:
          sctool cluster add [flags]

        Flags:
          -h, --help                      help for add
              --host string               hostname or IP of one of the cluster nodes
          -n, --name alias                alias you can give to your cluster
              --ssh-identity-file path    path to identity file containing SSH private key
              --ssh-user name             SSH user name used to connect to the cluster nodes
              --ssl-user-cert-file path   path to client certificate when using client/server encryption with require_client_auth enabled
              --ssl-user-key-file path    path to key associated with ssl-user-cert-file

        Global Flags:
              --api-url URL    URL of Scylla Manager server (default "https://127.0.0.1:56443/api/v1")
          -c, --cluster name   target cluster name or ID

        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/1.4/add-a-cluster/
          https://docs.scylladb.com/operating-scylla/manager/1.4/sctool/#cluster-add


        """
        if not any([node, db_cluster]):
            raise ScyllaManagerError("Neither host or db_cluster parameter were given to Manager add_cluster")
        logger.debug(f"Adding a cluster to scylla-manager, named: {name}")
        node = node or self._get_cluster_hosts_ip(db_cluster=db_cluster)[0]  # TODO: adjust  _get_cluster_hosts_ip()
        user = user or self.DEFAULT_USER
        ssh_user = create_user or "scylla-manager"
        host = node.address()

        # Wait for the manager agent on all cluster nodes to be ready before
        # issuing 'sctool cluster add'. The agent may crash and restart (e.g.
        # due to transient IMDSv2 failures), leaving port 10001 temporarily
        # unbound. Without this guard the connectivity check inside sctool
        # fails with "connection refused" and the whole cluster add is aborted.
        _agent_port = 10001
        _nodes_to_check = db_cluster.nodelist() if db_cluster is not None else node.cluster.nodelist()
        for _n in _nodes_to_check:
            if not _n.is_running():
                continue
            _addr = _n.address()
            logger.debug(f"Waiting for scylla-manager-agent on {_addr}:{_agent_port} before cluster add")
            if not common.check_socket_listening((_addr, _agent_port), timeout=120):
                raise ScyllaManagerError(f"scylla-manager-agent on {_addr}:{_agent_port} did not become reachable within 120s")

        cluster_add_cmd = f"cluster add --host {host} --name {name}"
        versions, _ = self.version
        client_version = versions[0][0].split()[2]
        cluster_add_cmd += f" --auth-token {node.scylla_manager.auth_token}"
        res_cluster_add, stderr = self.sctool.run(cmd=cluster_add_cmd)
        if not res_cluster_add or "Cluster added" not in stderr:
            raise ScyllaManagerError(f"Encountered an error on 'sctool cluster add' command response: {res_cluster_add}")
        # cluster_id = res_cluster_add.stdout.split('\n')[0]  # return ManagerCluster instance with the manager's new cluster-id
        cluster_id = res_cluster_add[0][0]
        return ManagerCluster(scylla_manager=self.scylla_manager, cluster_id=cluster_id, client_encrypt=client_encrypt, method=method)

    def upgrade(self, scylla_mgmt_upgrade_to_repo):
        raise ScyllaManagerError("Not converted from SCT to Dtest code")
        # manager_from_version = self.version
        # logger.debug('Running Manager upgrade from: {} to version in repo: {}'.format(
        #     manager_from_version, scylla_mgmt_upgrade_to_repo))
        # self.manager_node.upgrade_mgmt(scylla_mgmt_repo=scylla_mgmt_upgrade_to_repo)
        # new_manager_version = self.version
        # logger.debug('The Manager version after upgrade is: {}'.format(new_manager_version))
        # return new_manager_version

    def rollback_upgrade(self, manager_node):
        raise NotImplementedError


class SCTool:
    def __init__(self, scylla_manager):
        self.scylla_manager = scylla_manager

    def run(self, cmd: str | list[str], is_verify_errorless_result=False, parse_table_res=True, is_multiple_tables=False):
        list_cmd = cmd.copy() if isinstance(cmd, list) else cmd.split()
        for i in range(len(list_cmd)):
            list_cmd[i] = list_cmd[i].replace(SPACE_PLACEHOLDER, " ")
        logger.debug(f"Issuing: 'sctool {list_cmd}'")
        try:
            stdout, stderr = self.scylla_manager.sctool(cmd=list_cmd)

        except Exception as e:  # noqa: BLE001
            raise ScyllaManagerError(f"Encountered an error on sctool command: {list_cmd}: {e}")

        logger.debug("sctool command result:")
        logger.debug(msg=stdout)
        # Sometimes, the "stderr" variable contains a NOTICE message (The command ran successfully and this message
        # is not an error)
        if stderr.startswith("NOTICE"):
            logger.warning(stderr)
            stderr = ""
        if is_verify_errorless_result:
            MgrUtils.verify_errorless_result(cmd=list_cmd, stdout=stdout, stderr=stderr)
        if parse_table_res:
            parsed_table = self.parse_result_table(stdout=stdout)
            if is_multiple_tables:
                dict_parsed_tables = self.parse_result_multiple_tables(parsed_table=parsed_table)
                return dict_parsed_tables, stderr
            return parsed_table, stderr
        return stdout, stderr

    def parse_result_table(self, stdout):
        parsed_table = []
        lines = stdout.splitlines()
        filtered_lines = [line for line in lines if line]
        if filtered_lines:
            if "╭" in stdout:
                filtered_lines = [line.replace("│", "|") for line in filtered_lines if not (line.startswith(("╭", "├", "╰")))]  # filter out the dashes lines
            else:
                filtered_lines = [line for line in filtered_lines if not line.startswith("+")]  # filter out the dashes lines
        for line in filtered_lines:
            list_line = [s if s else "EMPTY" for s in line.split("|")]  # filter out spaces and "|" column seperators
            list_line_no_spaces = [s.split() for s in list_line if s != "EMPTY"]
            list_line_with_multiple_words_join = []
            for words in list_line_no_spaces:
                list_line_with_multiple_words_join.append(" ".join(words))
            if list_line_with_multiple_words_join:
                parsed_table.append(list_line_with_multiple_words_join)
        return parsed_table

    def parse_result_multiple_tables(self, parsed_table):
        """

        # Datacenter: us-eastscylla_node_east
        # ╭──────────┬────────────────╮
        # │ CQL      │ Host           │
        # ├──────────┼────────────────┤
        # │ UP (1ms) │ 18.233.164.181 │
        # ╰──────────┴────────────────╯
        # Datacenter: us-west-2scylla_node_west
        # ╭────────────┬───────────────╮
        # │ CQL        │ Host          │
        # ├────────────┼───────────────┤
        # │ UP (180ms) │ 54.245.183.30 │
        # ╰────────────┴───────────────╯
        # the above output example was translated to a single table that includes both 2 DC's values:
        # [['Datacenter: us-eastscylla_node_east'],
        #  ['CQL', 'Host'],
        #  ['UP (1ms)', '18.233.164.181'],
        #  ['Datacenter: us-west-2scylla_node_west'],
        #  ['CQL', 'Host'],
        #  ['UP (180ms)', '54.245.183.30']]
        :param parsed_table:
        :return:
        """
        if not any(len(line) == 1 for line in parsed_table):  # "1" means a table title like DC-name is found.
            return {"single_table": parsed_table}

        dict_res_tables = {}
        cur_table = None
        for line in parsed_table:
            if len(line) == 1:  # "1" means it is the table title like DC name.
                cur_table = line[0]
                dict_res_tables[cur_table] = []
            else:
                dict_res_tables[cur_table].append(line)
        return dict_res_tables

    def get_table_value(self, parsed_table, identifier, column_name=None, is_search_substring=False):
        """

        :param parsed_table:
        :param column_name:
        :param identifier:
        :param is_search_substring:
        :return:
        """

        # example expected parsed_table input is:
        # [['Host', 'Status', 'RTT'],
        #  ['18.234.77.216', 'UP', '0.92761'],
        #  ['54.203.234.42', 'DOWN', '0']]
        # usage flow example: mgr_cluster1.host -> mgr_cluster1.get_property -> get_table_value

        if not parsed_table or not self._is_found_in_table(parsed_table=parsed_table, identifier=identifier, is_search_substring=is_search_substring):
            raise ScyllaManagerError(f"Encountered an error retrieving sctool table value: {identifier} not found in: {parsed_table!s}")
        column_titles = [title.upper() for title in parsed_table[0]]  # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError(f"Column name: {column_name} not found in table: {parsed_table}")
        column_name_index = column_titles.index(column_name.upper()) if column_name else 1  # "1" is used in a case like "task progress" where no column names exist.
        ret_val = "N/A"
        for row in parsed_table:
            if is_search_substring:
                if any(identifier in cur_str for cur_str in row):
                    ret_val = row[column_name_index]
                    break
            elif identifier in row:
                ret_val = row[column_name_index]
                break
        logger.debug(f"{identifier} {column_name} value is:{ret_val}")
        return ret_val

    def get_table_complete_column(self, parsed_table, column_name):
        column_titles = [title.upper() for title in parsed_table[0]]  # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError(f"Column name: {column_name} not found in table: {parsed_table}")

        column_name_index = column_titles.index(column_name.upper())
        column_values = [row[column_name_index] for row in parsed_table[1:]]
        return column_values

    def _is_found_in_table(self, parsed_table, identifier, is_search_substring=False):
        full_rows_list = []
        for row in parsed_table:
            full_rows_list += row
        if is_search_substring:
            return any(identifier in cur_str for cur_str in full_rows_list)

        return identifier in full_rows_list


class ManagerTask(ScyllaManagerBase):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ScyllaManagerBase.__init__(self, _id=task_id, scylla_manager=scylla_manager)
        self.cluster_id = cluster_id

    def stop(self):
        cmd = f"stop {self.id} -c {self.cluster_id}"
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return self.wait_and_get_final_status(timeout=30, step=3)

    def start(self, continue_task=True):
        cmd = f"start {self.id} -c {self.cluster_id}"
        if not continue_task:
            cmd += " --no-continue"
        self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        list_expected_task_status = [status for status in TaskStatus.all_members() if status != TaskStatus.STOPPED]
        if isinstance(self, HealthcheckTask):  # Checking the progress of healthcheck task is no longer possible
            return self.wait_for_status(list_status=list_expected_task_status, check_task_progress=False, timeout=30, step=3)
        return self.wait_for_status(list_status=list_expected_task_status, check_task_progress=True, timeout=30, step=3)

    def _add_kwargs_to_cmd(self, cmd, **kwargs):
        for k, v in kwargs.items():
            cmd += f" --{k}={v}"
        return cmd

    def delete_task(self):
        cmd = f"stop --delete {self.id} -c {self.cluster_id}"
        self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    def task_list(self):
        cmd = f"tasks -c {self.cluster_id} --all"
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout, stderr

    def update(self, **kwargs):
        """
          -e, --enabled string      enabled (default "true")
          -h, --help                help for update
          -i, --interval string     task schedule interval e.g. 3d2h10m, valid units are d, h, m, s (default "0")
          -r, --num-retries int     task schedule number of retries (default 3)
          -s, --start-date string   task start date in RFC3339 form or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s (default "now")

        :param kwargs:
        :return:
        """

        cmd_mapping = {"enabled": "--enabled", "num_retries": "--num-retries", "cron": "--cron"}
        cmd_arguments = []
        for k, v in kwargs.items():
            cmd_arguments.append(f"{cmd_mapping[k]}={v}")

        task_type = self.id[: self.id.find("/")]
        cmd = f"{task_type} update {self.id} -c {self.cluster_id} {' '.join(cmd_arguments)}"
        stdout, _ = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    def get_task_info_dict(self):
        info_dict = {}
        cmd = f"info {self.id} -c {self.cluster_id}"
        stdout, _ = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        # Output example:
        # ./sctool --api-url=http://127.0.33.1:5080/api/v1 info --cluster=cluster1 backup/e775390c-9106-4717-a0c7-04016a0416c7
        # Name:	backup/e775390c-9106-4717-a0c7-04016a0416c7
        # Tz:	Asia/Jerusalem
        # Retry:	3 (initial backoff 10m)
        #
        # Properties:
        # - keyspace: ks1,ks2,ks3
        # - location: s3:backup-bucket
        #
        # +--------------------------------------+------------------------+----------+--------+
        # | ID                                   | Start time             | Duration | Status |
        # +--------------------------------------+------------------------+----------+--------+
        # | 2c6b70ea-74ad-11ed-aeb4-f4ee08c9cc47 | 12 Dec 22 14:57:44 IST | 0s       | DONE   |
        # +--------------------------------------+------------------------+----------+--------+
        info_lines = [line[0] for line in stdout if len(line) == 1]
        for line in info_lines:
            if ":" in line:
                name, value = (string.strip() for string in line.split(":", maxsplit=1))
                if name.startswith("-"):
                    name = name[2:]
                info_dict[name] = value
        history_table_lines = [line for line in stdout if len(line) > 1]
        # The info command returns some unnecessary values: task_name, cron, retry
        # The number of the extra info is not set, so I just search the for the lines that its length is larger than
        # 1, which are the history table (See above)
        info_dict["history"] = history_table_lines
        return info_dict

    @property
    def history(self):
        """
        Gets the task's history table
        """
        # ╭──────────────────────────────────────┬────────────────────────┬────────────────────────┬──────────┬───────╮
        # │ id                                   │ start time             │ end time               │ duration │ status│
        # ├──────────────────────────────────────┼────────────────────────┼────────────────────────┼──────────┼───────┤
        # │ e4f70414-ebe7-11e8-82c4-12c0dad619c2 │ 19 Nov 18 10:43:04 UTC │ 19 Nov 18 10:43:04 UTC │ 0s       │ NEW   │
        # │ 7f564891-ebe6-11e8-82c3-12c0dad619c2 │ 19 Nov 18 10:33:04 UTC │ 19 Nov 18 10:33:04 UTC │ 0s       │ NEW   │
        # │ 19b58cb3-ebe5-11e8-82c2-12c0dad619c2 │ 19 Nov 18 10:23:04 UTC │ 19 Nov 18 10:23:04 UTC │ 0s       │ NEW   │
        # │ b414cde5-ebe3-11e8-82c1-12c0dad619c2 │ 19 Nov 18 10:13:04 UTC │ 19 Nov 18 10:13:04 UTC │ 0s       │ NEW   │
        # │ 4e741c3d-ebe2-11e8-82c0-12c0dad619c2 │ 19 Nov 18 10:03:04 UTC │ 19 Nov 18 10:03:04 UTC │ 0s       │ NEW   │
        # ╰──────────────────────────────────────┴────────────────────────┴────────────────────────┴──────────┴───────╯
        return self.get_task_info_dict()["history"]

    @property
    def history_list(self):
        history_table = self.history
        _, _, _, keys, *value_rows = history_table
        complete_list = []

        for row in value_rows:
            row_dict = dict(zip(keys, row))
            complete_list.append(row_dict)
        return complete_list

    def info(self, **kwargs):
        # Name:	backup/80789574-3695-4cda-9974-8f9d5ce331b5
        # Cron:   no activations scheduled
        # Tz:	Asia/Jerusalem
        # Retry:	11 (initial backoff 10m)
        #
        # Properties:
        # - keyspace: 'new_keyspace1'
        # - location: 's3:newbackup-bucket'
        # - rate-limit: '1'
        # - retention: 12
        # - snapshot-parallel: '1,2,3'
        # - upload-parallel: '4,5,6'
        #
        # +--------------------------------------+------------------------+----------+--------+
        # | ID                                   | Start time             | Duration | Status |
        # +--------------------------------------+------------------------+----------+--------+
        # | a8b559b0-0df6-11ee-8609-f4ee08c9cc47 | 18 Jun 23 19:39:13 IDT | 0s       | DONE   |
        # +--------------------------------------+------------------------+----------+--------+
        cmd = f"info {self.id} -c {self.cluster_id}"
        stdout, _ = self.sctool.run(cmd=cmd, is_verify_errorless_result=True, **kwargs)
        return stdout

    @property
    def properties(self):
        def parse_line(line_string):
            name, value = (string.strip() for string in line_string.split(": "))
            if "- " in name:  # Like "- intensity: 2"
                name = name[2:]
            value = value.replace("'", "")
            if " (" in value:  # Like "Retry:	3 (initial backoff 10m)"
                value = value[: value.find(" (")]
            if "," in value:  # Like "- keyspace: keyspace1,keyspace12"
                value = [int(i) if i.isdigit() else i for i in value.split(",")]
            if type(value) is str and value.isdigit():  # Like "- parallel: 1"
                value = int(value)
            return name, value

        properties_dict = {}
        info_lines = self.info()
        for line_list in info_lines:
            line = line_list[0]
            if ": " in line:
                property_name, property_value = parse_line(line_string=line)
                properties_dict[property_name] = property_value
        return properties_dict

    @property
    def next_run(self):
        """
        Gets the task's next run value
        """
        # ╭──────────────────────────────────────────────┬──────────────┬────────┬──────────┬─────────┬───────┬────────────────────────┬────────────┬─────────┬────────────────────────╮
        # │ Task                                         │ Schedule     │ Window │ Timezone │ Success │ Error │ Last Success           │ Last Error │ Status  │ Next                   │
        # ├──────────────────────────────────────────────┼──────────────┼────────┼──────────┼─────────┼───────┼────────────────────────┼────────────┼─────────┼────────────────────────┤
        # │ backup/d569639a-897a-4a86-9ce1-3be1a94f0389  │              │        │ Etc/UTC  │ 1       │ 0     │ 04 Jul 23 18:20:37 UTC │            │ DONE    │                        │
        # │ healthcheck/cql                              │ @every 15s   │        │ Etc/UTC  │ 52      │ 0     │ 04 Jul 23 18:33:36 UTC │            │ DONE    │ 04 Jul 23 18:33:51 UTC │
        # │ healthcheck/rest                             │ @every 1m0s  │        │ Etc/UTC  │ 13      │ 0     │ 04 Jul 23 18:33:33 UTC │            │ DONE    │ 04 Jul 23 18:34:33 UTC │
        # │ healthcheck/alternator                       │ @every 15s   │        │ Etc/UTC  │ 51      │ 0     │ 04 Jul 23 18:33:22 UTC │            │ DONE    │ 04 Jul 23 18:33:37 UTC │
        # │ repair/all-weekly                            │ 0 23 * * SAT │        │ Etc/UTC  │ 0       │ 0     │                        │            │ NEW     │ 08 Jul 23 23:00:00 UTC │
        # │ restore/489d544d-afe6-494d-bbad-a3b273d042de │              │        │ Etc/UTC  │ 0       │ 0     │                        │            │ RUNNING │                        │
        # ╰──────────────────────────────────────────────┴──────────────┴────────┴──────────┴─────────┴───────┴────────────────────────┴────────────┴─────────┴────────────────────────╯
        stdout, _ = self.task_list()
        time_str = self.get_property(parsed_table=stdout, column_name="Next")
        if not time_str:
            return None
        return parse(time_str).astimezone(UTC)

    @property
    def status(self):
        """
        Gets the task's status
        """
        stdout, _stderr = self.task_list()
        str_status = self.get_property(parsed_table=stdout, column_name="status", is_search_substring=True)
        return TaskStatus.from_str(str_status)

        # expecting output of:
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 05 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ repair/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯

    def progress_details(self, **kwargs):
        """
        Gets the repair task's progress details
        """
        cmd = f"progress {self.id} -c {self.cluster_id}"
        stdout, stderr = self.sctool.run(cmd=cmd, **kwargs)
        return stdout, stderr

    @property
    def progress(self):
        """
        Gets the repair task's progress
        """
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return " 0%"
        # expecting output of:
        #  Status:           RUNNING
        #  Start time:       26 Mar 19 19:40:21 UTC
        #  Duration: 6s
        #  Progress: 0.12%
        #  Datacenters:
        #    - us-eastscylla_node_east
        #  ╭────────────────────┬───────╮
        #  │ system_auth        │ 0.47% │
        #  │ system_distributed │ 0.00% │
        #  │ system_traces      │ 0.00% │
        #  │ keyspace1          │ 0.00% │
        #  ╰────────────────────┴───────╯
        # [['Status: RUNNING'], ['Start time: 26 Mar 19 19:40:21 UTC'], ['Duration: 6s'], ['Progress: 0.12%'], ... ]
        progress = "N/A"
        for task_property in self.progress_details(is_verify_errorless_result=True)[0]:
            if task_property[0].startswith("Progress"):
                progress = task_property[0].split()[1]
                break
        return progress

    @property
    def start_time(self):
        """
        Gets the repair task's start time
        """
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return "01 Jan 70 00:00:00 UTC"
        for task_property in self.progress_details(is_verify_errorless_result=True)[0]:
            if task_property[0].startswith("Start time"):
                return task_property[0].split(": ")[1].strip()

    def has_percentage_reached_minimum(self, min_percentage):
        current_percentage = self.progress.strip()
        current_percentage_num = float(current_percentage[:-1])
        return current_percentage_num >= min_percentage

    def wait_for_minimal_progress_percentage(self, minimal_percentage, timeout=600, step=20):
        try:
            wait_for(func=self.has_percentage_reached_minimum, step=step, timeout=timeout, min_percentage=minimal_percentage)
        except WaitTimeoutExpiredError:
            logger.warning(f"Task {self.id} failed to reach a progress of {minimal_percentage} in {timeout} seconds")
            raise

    def full_progress_string(self):
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return " 0%"
        stdout_list, _stderr = self.progress_details(is_verify_errorless_result=True)
        # sctool.run returns stdout_list as a list of lists, each of them containing a row of the output
        stdout_list = [line[0] for line in stdout_list]
        full_stdout_string = "\n".join(stdout_list)
        return full_stdout_string

    def is_status_in_list(self, list_status, check_task_progress=False):
        """
        Check if the status of a given task is in list
        :param list_status:
        :return:
        """
        status = self.status
        if check_task_progress and status not in [TaskStatus.NEW, TaskStatus.STARTING]:  # check progress for all statuses except 'NEW' / 'STARTING'
            ###
            # The reasons for the below (un-used) assignment are:
            # * check that progress command works on varios task statuses (that was how manager bug #856 found).
            # * print the progress to log in cases needed for failures/performance analysis.
            ###
            progress = self.progress
            logger.debug(f"Task {self.id} progress is: {progress}")
        return status in list_status

    def wait_for_status(
        self,
        list_status,
        check_task_progress=True,
        timeout=600,
        step=20,
        log_progress_on_failure=True,
    ):
        text = f"Waiting until task: {self.id} reaches status of: {list_status}"
        try:
            is_status_reached = wait_for(func=self.is_status_in_list, step=step, text=text, list_status=list_status, check_task_progress=check_task_progress, timeout=timeout)
        except WaitTimeoutExpiredError:
            if log_progress_on_failure:
                logger.warning(f"Task {self.id} failed to reach a status from {list_status}\nTask Progress:\n{self.full_progress_string()}\n")
            raise
        return is_status_reached

    def wait_and_get_final_status(self, timeout=600, step=20):
        """
        1) Wait for task to reach a 'final' status. meaning one of: done/error/stopped
        2) return the final status.
        :return:
        """
        list_final_status = [TaskStatus.ERROR, TaskStatus.STOPPED, TaskStatus.DONE, TaskStatus.ABORTED]
        logger.debug(f"Waiting for task: {self.id} getting to a final status ({[str(s) for s in list_final_status]})..")
        res = self.wait_for_status(list_status=list_final_status, timeout=timeout, step=step)
        if not res:
            raise ScyllaManagerError(f"Unexpected result on waiting for task {self.id} status")
        return self.status

    def is_task_disabled(self):
        try:
            stdout, _stderr = self.task_list()
            self.get_property(parsed_table=stdout, column_name="status", is_search_substring=False, identifier="*" + self.id)
            return True
        except ScyllaManagerError as err:
            if "Encountered an error retrieving sctool table value" in err.args[0]:
                return False
            raise err

    def enabled(self, is_enabled):
        return self.update(enabled=is_enabled)


class RepairTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

    def update(  # noqa: PLR0913
        self,
        dc_names: list or str = None,
        dry_run: bool | None = None,
        enabled: str | None = None,
        is_fail_fast: bool | None = None,
        intensity: float | None = None,
        keyspace_list: list or str = None,
        host: str | None = None,
        name: str | None = None,
        window: list | None = None,
        timezone: str | None = None,
        num_retries: int | None = None,
        is_show_tables: bool | None = None,
        small_table_threshold: str | None = None,
        cron: list | None = None,
        sctool_kwargs: dict | None = None,
        ignore_down_hosts: bool | None = None,
        **kwargs,
    ):
        if kwargs:
            raise ScyllaManagerError(f"The following variables are unused '{pformat(kwargs)}'")
        return self.repair_api.update(
            repair_id=self.id,
            dc_names=dc_names,
            dry_run=dry_run,
            enabled=enabled,
            host=host,
            is_fail_fast=is_fail_fast,
            intensity=intensity,
            ignore_down_hosts=ignore_down_hosts,
            keyspace_list=keyspace_list,
            num_retries=num_retries,
            is_show_tables=is_show_tables,
            small_table_threshold=small_table_threshold,
            cron=cron,
            name=name,
            window=window,
            timezone=timezone,
            cluster_name=self.cluster_id,
            sctool_kwargs=sctool_kwargs,
        )


class HealthcheckTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)


class BackupValidateTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

    def get_file_status_summary(self, wait_for_task_ending=True):
        """
        Output example:
            Arguments:	-L s3:backup-bucket
            Status:		ERROR
            Cause:		broken snapshots: sm_20210602120609UTC
            Start time:	02 Jun 21 15:06:20 IDT
            End time:	02 Jun 21 15:06:20 IDT
            Duration:	0s

            Scanned files:	251
            Missing files:	1
            Orphaned files:	0
        OR
            Orphaned files:	1 (0B)
            Deleted files:	1
        """
        if wait_for_task_ending:
            self.wait_and_get_final_status(step=5)
        assert self.status in [TaskStatus.DONE, TaskStatus.ERROR], f"Can't get file summary since the task is in {self.status} status"
        progress_string = self.full_progress_string()
        file_status_dict = dict()
        # Cannot use yaml.safe_load due to lines like:
        # "Cause:		broken snapshots: sm_20210602120609UTC"
        for line in progress_string.splitlines():
            if line:
                title, value = line.split(":", maxsplit=1)
                pure_value = value.strip()
                if "(" in pure_value:
                    pure_value = pure_value[: pure_value.find("(")].strip()
                file_status_dict[title.strip()] = pure_value if not pure_value.isdigit() else int(pure_value)
        return file_status_dict


class BackupTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

    def get_snapshot_tag(self):
        # TODO: Add an option to choose from one of the tags to restore from, using backup list
        stdout, stderr = self.progress_details(parse_table_res=False)
        if stderr:
            raise ScyllaManagerError(f"Failure for sctool sctool progress command:\n{stderr}")
        snapshot_line = [line for line in stdout.splitlines() if "snapshot tag" in line.lower()]
        # Returns the following:
        # Snapshot Tag:	sm_20200106093455UTC
        # (when executed manually, the title and value is separated by \t instead
        snapshot_tag = snapshot_line[0].split(":")[1].strip()
        return snapshot_tag

    def update(  # noqa: PLR0913
        self,
        dc_names: list or str = None,
        dry_run: bool | None = None,
        enabled: str | None = None,
        keyspace_list: list or str = None,
        location_list: list or str = None,
        name: str | None = None,
        window: list | None = None,
        timezone: str | None = None,
        num_retries: int | None = None,
        rate_limit_list: list or str = None,
        retention: int | None = None,
        is_show_tables: bool | None = None,
        snapshot_parallel_list: list or str = None,
        cron: list | None = None,
        upload_parallel_list: list or str = None,
        sctool_kwargs: dict | None = None,
        **kwargs,
    ):
        if kwargs:
            raise ScyllaManagerError(f"The following variables are unused '{pformat(kwargs)}'")
        return self.backup_api.update(
            backup_id=self.id,
            dc_names=dc_names,
            dry_run=dry_run,
            enabled=enabled,
            keyspace_list=keyspace_list,
            location_list=location_list,
            num_retries=num_retries,
            rate_limit_list=rate_limit_list,
            retention=retention,
            is_show_tables=is_show_tables,
            snapshot_parallel_list=snapshot_parallel_list,
            cron=cron,
            name=name,
            window=window,
            timezone=timezone,
            upload_parallel_list=upload_parallel_list,
            cluster_name=self.cluster_id,
            sctool_kwargs=sctool_kwargs,
        )


class RestoreTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

    def update(  # noqa: PLR0913
        self,
        batch_size: int | None = None,
        keyspace_list: list | None = None,
        restore_schema: bool = False,
        restore_data: bool = False,
        location_list: list | None = None,
        snapshot_tag: str | None = None,
    ):
        cmd = f"restore update {self.id} -c {self.cluster_id}"
        if batch_size:
            cmd += f" --batch-size {batch_size}"  # The manager's default is 2
        if keyspace_list:
            keyspace_names = ",".join(keyspace_list)
            cmd += f" --keyspace {keyspace_names} "
        if restore_schema:
            cmd += " --restore-schema"
        if restore_data:
            cmd += " --restore-tables"
        if location_list:
            locations_names = ",".join(location_list)
            cmd += f" --location {locations_names} "
        if snapshot_tag:
            cmd += f" --snapshot-tag {snapshot_tag}"
        stdout, _ = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout


class RestTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)


class HostHealth(ComparableHealthCheckField):
    def __init__(self, datacenter_name, **kwargs):
        self.datacenter = datacenter_name
        self.node_status = NodeStatus(kwargs.pop("status"))
        self.alternator = Status(
            status=((kwargs.get("alternator_status") and AlternatorStatus(kwargs.pop("alternator_status"))) or None), uptime=kwargs.pop("alternator_timeout", None), uptime_type=kwargs.pop("alternator_timeout_type", None)
        )
        self.cql = Status(status=((kwargs.get("cql_status") and CqlStatus(kwargs.pop("cql_status"))) or None), uptime=kwargs.pop("cql_timeout", None), uptime_type=kwargs.pop("cql_timeout_type", None))
        self.rest = Status(status=(kwargs.get("rest_status") and HostRestStatus(kwargs.pop("rest_status"))) or None, uptime=kwargs.pop("rest_timeout", None), uptime_type=kwargs.pop("rest_timeout_type", None))
        self.address = kwargs.pop("address")
        self.uptime = Uptime(hours=kwargs.pop("hours", None), minutes=kwargs.pop("minutes", None), seconds=kwargs.pop("seconds", None))
        self.cpus = kwargs.pop("cpus", None)
        self.memory = Memory(size=kwargs.pop("memory_size", None), unit=kwargs.pop("memory_unit", None))
        self.scylla_version = kwargs.pop("scylla_version", None)
        self.agent_version = kwargs.pop("agent_version", None)
        self.host_id = kwargs.pop("host_id")
        self.error_messages = []

        if kwargs:
            raise ValueError(f"The following variables are unused: {pformat(kwargs)}")


class ManagerCluster(ScyllaManagerBase):
    def __init__(self, scylla_manager, cluster_id, client_encrypt=False, method=None):
        if not scylla_manager:
            raise ScyllaManagerError("Cannot create a Manager Cluster where no 'scylla-manager' parameter is given")
        ScyllaManagerBase.__init__(self, _id=cluster_id, scylla_manager=scylla_manager)
        self.method = method
        self.client_encrypt = client_encrypt

    def run_restore_command(  # noqa: PLR0913
        self,
        batch_size: int | None = None,
        keyspace_list: list | None = None,
        restore_schema: bool = False,
        restore_data: bool = False,
        location_list: list | None = None,
        snapshot_tag: str | None = None,
    ):
        cmd = f"restore -c {self.id}"
        # restore schema does not support '--method' flag
        if self.method is not None and not restore_schema:
            cmd += f" --method={self.method}"
        if batch_size:
            cmd += f" --batch-size {batch_size}"  # The manager's default is 2
        if keyspace_list:
            keyspace_names = ",".join(keyspace_list)
            cmd += f" --keyspace {keyspace_names} "
        if restore_schema:
            cmd += " --restore-schema"
        if restore_data:
            cmd += " --restore-tables"
        if location_list:
            locations_names = ",".join(location_list)
            cmd += f" --location {locations_names} "
        if snapshot_tag:
            cmd += f" --snapshot-tag {snapshot_tag}"

        stdout, stderr = self.sctool.run(cmd=cmd, parse_table_res=False)
        if stderr:
            logger.error("Encountered an error on %s command response", cmd)
            raise ScyllaManagerError(stderr)

        task_id = stdout.strip()
        logger.debug("Created task id is: %s", task_id)
        return RestoreTask(task_id=task_id, cluster_id=self.id, scylla_manager=self.scylla_manager)

    def run_backup_command(  # noqa: PLR0912, PLR0913, PLR0915
        self,
        dc_list=None,
        dry_run=None,
        force=None,
        keyspace_list=None,
        name=None,
        window=None,
        timezone=None,
        location_list=None,
        num_retries=None,
        rate_limit_list=None,
        retention=None,
        show_tables=None,
        snapshot_parallel_list=None,
        cron=None,
        upload_parallel_list=None,
        purge_only=None,
    ):
        cmd = f"backup -c {self.id}"

        if self.method is not None:
            cmd += f" --method={self.method}"
        if dc_list is not None:
            dc_names = ",".join(dc_list)
            cmd += f" --dc {dc_names} "
        if dry_run is not None:
            cmd += " --dry-run"
        if force is not None:
            cmd += " --force"
        if keyspace_list is not None:
            keyspaces_names = ",".join(keyspace_list)
            cmd += f" --keyspace {keyspaces_names} "
        if location_list is not None:
            locations_names = ",".join(location_list)
            cmd += f" --location {locations_names} "
        if num_retries is not None:
            cmd += f" --num-retries {num_retries}"
        if rate_limit_list is not None:
            rate_limit_string = ",".join(rate_limit_list)
            cmd += f" --rate-limit {rate_limit_string} "
        if retention is not None:
            cmd += f" --retention {retention} "
        if show_tables is not None:
            cmd += f" --show-tables {show_tables} "
        if name is not None:
            cmd += f" --name {name} "
        if snapshot_parallel_list is not None:
            snapshot_parallel_string = ",".join(snapshot_parallel_list)
            cmd += f" --snapshot-parallel {snapshot_parallel_string} "
        if window is not None:
            time_window_string = ",".join(window)
            cmd += f" --window {time_window_string} "
        if timezone is not None:
            cmd += f" --timezone {timezone} "
        if cron is not None:
            cron_string = f"{SPACE_PLACEHOLDER}".join(str(char) for char in cron)
            cmd += f" --cron {cron_string} "
        if upload_parallel_list is not None:
            upload_parallel_string = ",".join(upload_parallel_list)
            cmd += f" --upload-parallel {upload_parallel_string} "
        if purge_only is not None:
            cmd += " --purge-only"

        stdout, stderr = self.sctool.run(cmd=cmd, parse_table_res=False)
        if not stdout:
            raise ScyllaManagerError(f"Unknown failure for sctool '{cmd}' command")

        if stderr:
            logger.debug(f"Encountered an error on '{cmd}' command response")
            raise ScyllaManagerError(stderr)

        task_id = stdout.strip()
        logger.debug(f"Created task id is: {task_id}")
        return BackupTask(task_id=task_id, cluster_id=self.id, scylla_manager=self.scylla_manager)

    # scylla-manager does track cluster state, as result there are scenarios
    # when scylla-manager know only dead nodes of the cluster, so you have to point it to a live node to catch up
    def update_cluster_host(self, host):
        cmd = f"cluster update -c {self.id} --host {host}"
        _, stderr = self.sctool.run(cmd=cmd, parse_table_res=False)
        if stderr:
            raise ScyllaManagerError(f"sctool '{cmd}' returned error {stderr}")

    def run_backup_validate_command(
        self,
        delete_orphaned_files=None,
        location_list=None,
        num_retries=None,
        parallel=None,
        cron=None,
    ):
        cmd = f"backup validate -c {self.id}"

        if delete_orphaned_files is not None:
            cmd += " --delete-orphaned-files"
        if location_list is not None:
            locations_names = ",".join(location_list)
            cmd += f" --location {locations_names} "
        if num_retries is not None:
            cmd += f" --num-retries {num_retries}"
        if parallel is not None:
            cmd += f" --parallel {parallel} "
        if cron is not None:
            cron_string = " ".join(str(char) for char in cron)
            cmd += f" --cron '{cron_string}' "

        stdout, stderr = self.sctool.run(cmd=cmd, parse_table_res=False)
        if not stdout:
            raise ScyllaManagerError(f"No output for sctool '{cmd}' command")

        if stderr:
            logger.error(f"Encountered an error on '{cmd}' command response")
            raise ScyllaManagerError(stderr)

        task_id = stdout.strip()
        logger.info(f"Created task id is: {task_id}")
        return BackupValidateTask(task_id=task_id, cluster_id=self.id, scylla_manager=self.scylla_manager)

    def get_backup_files_dict(self, snapshot_tag, all_clusters=False):
        command = f" -c {self.id} backup files --snapshot-tag {snapshot_tag}"
        if all_clusters:
            command += " --all-clusters"
        # The sctool backup files command prints the s3 paths of all of the files that are required to restore the
        # cluster from the backup
        snapshot_files, _stderr = self.sctool.run(command)
        snapshot_file_list = [file_path_list[0] for file_path_list in snapshot_files]
        # sctool.run returns a list of lists, each of them is a 1 length list that contains the row.
        # This list comprehension turns the list into a list of strings (rows) instead
        return self.snapshot_files_to_dict(snapshot_file_list)

    def snapshot_files_to_dict(self, snapshot_file_lines):
        per_node_keyspaces_and_tables_backup_files = {}
        for line in snapshot_file_lines:
            s3_file_path, keyspace_and_table = (string.strip() for string in line.split(" "))
            node_id = s3_file_path[s3_file_path.find("/node/") + len("/node/") : s3_file_path.find("/keyspace")]
            keyspace, table = keyspace_and_table.split("/")
            if node_id not in per_node_keyspaces_and_tables_backup_files:
                per_node_keyspaces_and_tables_backup_files[node_id] = {}
            if keyspace not in per_node_keyspaces_and_tables_backup_files[node_id]:
                per_node_keyspaces_and_tables_backup_files[node_id][keyspace] = {}
            if table not in per_node_keyspaces_and_tables_backup_files[node_id][keyspace]:
                per_node_keyspaces_and_tables_backup_files[node_id][keyspace][table] = []
            per_node_keyspaces_and_tables_backup_files[node_id][keyspace][table].append(s3_file_path)
        return per_node_keyspaces_and_tables_backup_files

    def delete_backup(self, snapshot_tag):
        self.sctool.run(f"-c {self.id} backup delete --snapshot-tag={snapshot_tag}")

    def delete(self):
        """
        $ sctool cluster delete
        """

        cmd = f"cluster delete -c {self.id}"
        stdout, _stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    def update(self, name=None, host=None, ssh_identity_file=None, ssh_user=None, client_encrypt=None, port=None):  # noqa: PLR0913
        """
        $ sctool cluster update --help
        Modify a cluster

        Usage:
          sctool cluster update [flags]

        Flags:
          -h, --help                     help for update
              --host string              hostname or IP of one of the cluster nodes
          -n, --name alias               alias you can give to your cluster
              --ssh-identity-file path   path to identity file containing SSH private key
              --ssh-user name            SSH user name used to connect to the cluster nodes
        """
        cmd = f"cluster update -c {self.id}"
        if name:
            cmd += f" --name {name}"
        if host:
            cmd += f" --host {host}"
        if ssh_identity_file:
            cmd += f" --ssh-identity-file {ssh_identity_file}"
        if ssh_user:
            cmd += f" --ssh-user {ssh_user}"
        if port:
            cmd += f" --port {port}"
        stdout, _stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    @property
    def _cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        stdout, _stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    @property
    def name(self):
        """
        Gets the Cluster name as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        return self.get_property(parsed_table=self._cluster_list, column_name="name")

    @property
    def ssh_user(self):
        """
        Gets the Cluster ssh_user as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        return self.get_property(parsed_table=self._cluster_list, column_name="ssh user")

    def get_task_list(self):
        cmd = f"tasks -c {self.id}"
        stdout, _stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    @property
    def repair_task_list(self):
        """
        Gets the Cluster's  Task list
        """
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 26 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ repair/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        repair_task_list = []
        table_res = self.get_task_list()
        if len(table_res) > 1:  # if there are any tasks in list - add them as RepairTask generated objects.
            repair_task_rows_list = [row for row in table_res[1:] if row[0].startswith("repair/")]
            for row in repair_task_rows_list:
                repair_task_list.append(RepairTask(task_id=row[0], cluster_id=self.id, scylla_manager=self.scylla_manager))
        return repair_task_list

    def get_healthcheck_task(self):
        healthcheck_id = self.sctool.get_table_value(parsed_table=self.get_task_list(), column_name="task", identifier="healthcheck/cql", is_search_substring=True)
        # return the manager's health-check-task object with the found id
        return HealthcheckTask(task_id=healthcheck_id, cluster_id=self.id, scylla_manager=self.scylla_manager)

    def get_healthcheck_alternator_task(self):
        healthcheck_id = self.sctool.get_table_value(parsed_table=self.get_task_list(), column_name="task", identifier="healthcheck/alternator", is_search_substring=True)
        # return the manager's health-check-task object with the found id
        return HealthcheckTask(task_id=healthcheck_id, cluster_id=self.id, scylla_manager=self.scylla_manager)

    def get_rest_task(self):
        rest_id = self.sctool.get_table_value(parsed_table=self.get_task_list(), column_name="task", identifier="healthcheck/rest", is_search_substring=True)
        # return the manager's rest-task object with the found id
        return RestTask(task_id=rest_id, cluster_id=self.id, scylla_manager=self.scylla_manager)

    def get_hosts_health(self) -> dict[str, HostHealth]:
        """
        Gets the Manager's Cluster Nodes status

        $ sctool status -c bla
        Datacenter: dc1
        ╭────┬─────────────┬──────────┬────────────┬───────────┬──────┬──────────┬────────┬─────────────────────────────┬──────────────────────────────────────╮
        │    │ CQL         │ REST     │ Address    │ Uptime    │ CPUs │ Memory   │ Scylla │ Agent                       │ Host ID                              │
        ├────┼─────────────┼──────────┼────────────┼───────────┼──────┼──────────┼────────┼─────────────────────────────┼──────────────────────────────────────┤
        │ UN │ UP (0ms)    │ UP (0ms) │ 127.0.95.1 │ 198h18m6s │ 4    │ 15.55GiB │ 4.4.0  │ 2.5.rc1-0.20210811.a12c3aac │ 790eec5b-ed8c-497b-bbf4-1a30cdaba447 │
        │ UN │ UP (0ms)    │ UP (0ms) │ 127.0.95.2 │ 198h18m6s │ 4    │ 15.55GiB │ 4.4.0  │ 2.5.rc1-0.20210811.a12c3aac │ 7a768df8-c433-4610-ad3f-655ba2003091 │
        │ UN │ ERROR (0ms) │ UP (0ms) │ 127.0.95.3 │ 198h18m6s │ 4    │ 15.55GiB │ 4.4.0  │ 2.5.rc1-0.20210811.a12c3aac │ 8facb590-0d6c-458c-8862-6e4243b9e8ee │
        ╰────┴─────────────┴──────────┴────────────┴───────────┴──────┴──────────┴────────┴─────────────────────────────┴──────────────────────────────────────╯
        Errors:
        - 127.0.82.1 CQL: fetch TLS config: get SSL user cert from secrets store: not found
        - 127.0.82.2 CQL: fetch TLS config: get SSL user cert from secrets store: not found
        - 127.0.82.3 CQL: fetch TLS config: get SSL user cert from secrets store: not found
        """
        dict_hosts_health, health_details = {}, {}
        output = self.status_api.status(cluster_name=self.id)[0]

        datacenter_key, datacenter_value = output[0][0].split(":", maxsplit=1)
        datacenter_name = self.status_api.parse_output(output=datacenter_value.strip(), regex_name=datacenter_key.strip())
        table_headers = output[1]
        table_contents = output[2:]
        error_messages = []
        if ["Errors:"] in table_contents:
            error_title_index = table_contents.index(["Errors:"])
            error_messages = table_contents[error_title_index + 1 :]
            table_contents = table_contents[:error_title_index]
        for line in table_contents:
            [health_details.update(self.status_api.parse_output(output=value, regex_name=name)) for value, name in zip(line, table_headers) if value != "-"]
            host_health_object = HostHealth(datacenter_name=datacenter_name, **health_details)
            health_details.clear()
            dict_hosts_health[host_health_object.address] = host_health_object
        for message in error_messages:
            message_string = message[0]  # The message string is in a 1 length list
            node_ip = re.search(r"\d+\.\d+\.\d+\.\d+", message_string)[0]
            dict_hosts_health[node_ip].error_messages.append(message_string)

        return dict_hosts_health

    @staticmethod
    def _extract_value_with_regex(string, regex_pattern, default_value="N/A"):
        value_list = findall(pattern=regex_pattern, string=string)
        if len(value_list) == 1:
            return value_list[0]
        return default_value

    def suspend(self, on_resume_start_tasks=False, duration=None):
        cmd = f"suspend -c {self.id}"
        if on_resume_start_tasks:
            cmd += " --on-resume-start-tasks"
        if duration is not None:
            cmd += f" --duration {duration}"
        self.sctool.run(cmd=cmd)

    def resume(self, start_tasks=True):
        cmd = f"resume -c {self.id}"
        if start_tasks:
            cmd += " --start-tasks"
        self.sctool.run(cmd=cmd)


class ScyllaManagerMixin:
    method = None

    def config_and_create_cluster(self, topology, extra_config_options=None, cluster=None):
        if cluster is None:
            cluster = self.cluster
        extra_config_options = extra_config_options if extra_config_options else dict()
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False, **extra_config_options})
        cluster.populate(topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        return cluster.nodelist()

    def _create_mgr_cluster(self, node, name):
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node, name=name, method=self.method)

        return mgr_cluster

    def update_scylla_manager_yaml(self, append_data: dict):
        config_path = os.path.join(self.cluster.get_path(), common.SCYLLAMANAGER_DIR, common.SCYLLAMANAGER_CONF)

        with open(config_path) as f:
            yaml_content = yaml.safe_load(f)

        yaml_content.update(append_data)

        with open(config_path, "w") as f:
            yaml.safe_dump(yaml_content, f)

    @pytest.fixture(scope="function", autouse=False)
    def secondary_cluster(self, request):
        dtest_config = DTestConfig()
        dtest_config.setup(request)
        dtest_setup = DTestSetup(dtest_config=dtest_config, setup_overrides=DTestSetupOverrides(), cluster_name="test", prefix="dtest-secondary-")
        manager_install_dir = dtest_setup.prepare_scylla_manager() if request.node.get_closest_marker("scylla_manager") else None
        dtest_setup.initialize_cluster(DTestSetup.create_ccm_cluster, skip_manager_server=True, manager_install_dir=manager_install_dir)
        dtest_setup.cluster.set_configuration_options(values={"ring_delay_ms": 10000})

        yield dtest_setup.cluster

        rep_setup = getattr(request.node, "rep_setup", None)
        rep_call = getattr(request.node, "rep_call", None)
        failed = getattr(rep_setup, "failed", False) or getattr(rep_call, "failed", False)
        try:
            if not dtest_setup.allow_log_errors:
                try:
                    dtest_setup.check_errors_all_nodes()
                except AssertionError:
                    failed = True
                    raise
        finally:
            try:
                # save the logs for inspection
                if (failed and dtest_config.delete_logs == "passed") or dtest_config.delete_logs == "none":
                    copy_logs(request, dtest_setup)
            except Exception as e:  # noqa: BLE001
                logger.error("Error saving log: %s", str(e))
            finally:
                dtest_setup.cleanup_cluster()

    @staticmethod
    def create_c1_c2_with_clustering_key(  # noqa: PLR0913
        session,
        keyspace_name,
        table_name,
        partition_key_name="pkey",
        partition_key_type="int",
        clustering_key_name="ckey",
        clustering_key_type="int",
    ):
        session.execute(f"create table {keyspace_name}.{table_name} ( {partition_key_name} {partition_key_type}, {clustering_key_name} {clustering_key_type}, c1 text, c2 text, PRIMARY KEY({partition_key_name}, {clustering_key_name}));")

    def insert_data_from_ranges(self, healthy_node, keyspace_table_and_key_range, rf=2, use_clustering_key=False, partition_key_value=1):
        """

        :param healthy_node: node in UN status
        :param keyspace_table_and_key_range: a dict that contains what rows to insert, per table in each keyspace, like so:
        {
            keyspace_name:
            {
                table_name: key_range[]
            }
        }
        :param use_clustering_key:
        :param partition_key_value:
        :return:
        """
        session = self.patient_cql_connection(healthy_node)
        keyspace_list_rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
        keyspace_list = [row.keyspace_name for row in keyspace_list_rows]

        for keyspace in keyspace_table_and_key_range:
            if keyspace not in keyspace_list:
                create_ks(session=session, name=keyspace, rf=rf)
            table_list_rows = session.execute(f"SELECT table_name FROM system_schema.tables where keyspace_name='{keyspace}';")
            table_list = [row.table_name for row in table_list_rows]

            for table, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                if table not in table_list:
                    if use_clustering_key:
                        self.create_c1_c2_with_clustering_key(session=session, keyspace_name=keyspace, table_name=table)
                    else:
                        create_cf(session=session, name=f"{keyspace}.{table}", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")

                if use_clustering_key:
                    insert_c1c2_with_clustering(session=session, clustering_key_values=range(*key_range), ks=keyspace, cf=table, partition_key_set_value=partition_key_value)
                else:
                    insert_c1c2(session=session, keys=range(*key_range), consistency=ConsistencyLevel.ALL, c1_values=[C1_PREFIX % i for i in range(*key_range)], c2_values=[C2_PREFIX % i for i in range(*key_range)], ks=keyspace, cf=table)

    def clean_up_tables(self, node: ScyllaNode, keyspace_and_tables_dict: dict):
        """
        :param node:
        :param keyspace_and_tables_dict: a dict that contains a list of names of the tables to truncate in each keyspace.
        :return:
        """
        session = self.patient_cql_connection(node)

        for keyspace, table_list in keyspace_and_tables_dict.items():
            for table in table_list:
                session.execute(f"TRUNCATE {keyspace}.{table}")


def create_cron_list_from_timedelta(minutes=0, hours=0):
    destined_time = datetime.now() + timedelta(hours=hours, minutes=minutes)
    cron_list = [destined_time.minute, destined_time.hour, "*", "*", "*"]
    return cron_list
