import contextlib
import logging
import subprocess
from collections import namedtuple
from collections.abc import Generator
from pathlib import Path
from textwrap import dedent

from ccmlib.node import ToolError

from tools.cassandra_stess import CassandraStressDocker

logger = logging.getLogger(__name__)


def create_stress_compatible_table(  # noqa: PLR0913
    self,
    node,
    rf=1,
    gc_grace_seconds=864000,
    default_time_to_live=0,
    speculative_retry="'99.0PERCENTILE'",
    compaction="'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '100'",
):
    session = self.patient_cql_connection(node)
    session.execute(
        f"""CREATE KEYSPACE keyspace1 WITH replication = {{
    'class': 'NetworkTopologyStrategy',
    'replication_factor': {rf} }};"""
    )

    session.execute(
        f"""CREATE TABLE keyspace1.standard1(
    key blob PRIMARY KEY,
    "C0" blob,
    "C1" blob,
    "C2" blob,
    "C3" blob,
    "C4" blob,
    ) WITH bloom_filter_fp_chance = 0.01
    AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
    AND comment = ''
    AND compaction = {{{compaction}}}
    AND compression = {{}}
    AND crc_check_chance = 1.0
    AND default_time_to_live = {default_time_to_live}
    AND gc_grace_seconds = {gc_grace_seconds}
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = {speculative_retry};"""
    )


def fill_data_by_cs(  # noqa: PLR0913
    node,
    n_range=None,
    start=0,
    duration_range=None,
    other_opt=None,
    overlap_rate=0,
    flush=True,
):
    """
    fill data by multiple cassandra-stress workloads
    """
    if other_opt is None:
        other_opt = ["-rate", "threads=10", "-col", "size=FIXED(1024)"]
    if duration_range is None:
        duration_range = []
    if n_range is None:
        n_range = [500, 550, 600, 650]
    opts = []
    for num in n_range:
        opts.append([f"n={num}", "-pop", f"seq={start}..{start + num}"])
        start += int(num * (1 - overlap_rate))
    for t in duration_range:
        opts.append([f"duration={t}s"])
    for opt in opts:
        cs_cmdline = ["write", "no-warmup", *opt, *other_opt]
        node.stress(cs_cmdline)
        if flush:
            logger.debug("Flush after writing data .....")
            node.flush()


def _tail_text(text, max_lines):
    """Keep only the last max_lines lines of text."""
    if not text:
        return ""
    lines = text.splitlines(keepends=True)
    return "".join(lines[-max_lines:])


Subprocess_Return = namedtuple("Subprocess_Return", "stdout stderr rc")


def run_stress_with_tailed_output(node, stress_options, max_tail_lines=100):
    """
    Run cassandra-stress and return only the tail of the output to avoid memory exhaustion.

    Useful when running multiple long-duration stress operations in parallel,
    where the full output could consume hundreds of MB per operation.

    Unlike node.stress(), this does not raise on non-zero exit codes — callers
    that run stress for side effects (e.g. background load while stopping nodes)
    should inspect result.rc themselves if needed.
    """
    try:
        full_result = node.stress(stress_options=stress_options)
    except ToolError as e:
        logger.warning("cassandra-stress exited with rc=%s (expected when nodes are stopped during stress)", e.exit_status)
        stdout = e.stdout.decode("utf-8", "replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
        stderr = e.stderr.decode("utf-8", "replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
        return Subprocess_Return(stdout=_tail_text(stdout, max_tail_lines), stderr=_tail_text(stderr, max_tail_lines), rc=e.exit_status)

    if hasattr(full_result, "stdout") and hasattr(full_result, "stderr"):
        return Subprocess_Return(
            stdout=_tail_text(full_result.stdout, max_tail_lines),
            stderr=_tail_text(full_result.stderr, max_tail_lines),
            rc=getattr(full_result, "rc", 0),
        )

    if isinstance(full_result, tuple) and len(full_result) >= 2:
        return (
            _tail_text(full_result[0], max_tail_lines),
            _tail_text(full_result[1], max_tail_lines),
            *full_result[2:],
        )

    return full_result


def format_cs_output(output: tuple):
    if output.__class__.__name__ == "Subprocess_Return":
        return f"stderr:\n{output.stderr}\n\nstdout:\n{output.stdout}"
    elif isinstance(output, tuple):
        return "\n".join(output)
    else:
        return NotImplementedError()


def assert_cs_success(output: tuple):
    stdout = output.stdout if output.__class__.__name__ == "Subprocess_Return" else output[0]
    filtered_stdout = "\n".join(line for line in stdout.splitlines() if not line.startswith(("INFO  [", "WARN  [", "ERROR  [")))
    assert filtered_stdout.strip().endswith(("END", "DONE")), f"Run c-s failed: {format_cs_output(output)}"


@contextlib.contextmanager
def enable_cs_debug(tmp_path: Path) -> Generator[dict]:
    """
    create temporary configuration file for enabling c-s debug
    and yield the enviremnt varibles needed to use it

    :param tmp_path: direcotry where to create configuration,
                     best if using pytest fixture that gives a temporary
                     directory for each test

    Example Usage:
    >>> def test_something(self, tmp_path):
    >>>    node = node1, *_ = self.cluster.nodelist()
    >>>    with enable_cs_debug(tmp_path) as (volumes, env_for_debug):
    >>>        result = node.stress(
    >>>            node,
    >>>            ["write", "n=1000"],
    >>>            env=env_for_debug,
    >>>            volumes=volumes,
    >>>        )

    """
    logback_file = tmp_path / "logback-tools.xml"
    logback_file.write_text(
        dedent(
            """
        <configuration>
          <appender name="STDERR" class="ch.qos.logback.core.ConsoleAppender">
            <target>System.err</target>
            <encoder>
              <pattern>%-5level %date{"HH:mm:ss,SSS"} %msg%n</pattern>
            </encoder>
            <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
              <level>DEBUG</level>
            </filter>
          </appender>

          <root level="DEBUG">
            <appender-ref ref="STDERR" />
          </root>
        </configuration>
        """
        )
    )
    yield ([f"{logback_file}:/etc/cassandra/logback-tools.xml"], {"_JAVA_OPTIONS": f"-Dlogback.configurationFile=/etc/cassandra/logback-tools.xml"})


def run_stress(node, stress_options, **kwargs):
    with CassandraStressDocker(node, stress_cmd=f"cassandra-stress {' '.join(stress_options)}", **kwargs) as cassandra_stress_docker:
        cassandra_stress_docker.run()
        result = cassandra_stress_docker.wait_for_stress_results()
        if result.rc != 0:
            raise ToolError(stress_options, result.rc, result.stdout, result.stderr)
        return result
