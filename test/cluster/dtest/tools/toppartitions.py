import re
import subprocess
import time
from collections import Counter
from threading import Event

from cassandra.cluster import Session
from ccmlib.scylla_node import ScyllaNode

from tools.data import insert_c1c2_no_prepared, query_c1c2_concurrent


class NodetoolToppartitionStartedTimeoutError(Exception):
    pass


def wait_nodetool_toppartitions_start(node: ScyllaNode, cmd: str, timeout: int = 30) -> None:
    """
    Monitoring that process nodetool toppartitions is started

    Search process nodetool toppartitions, validate that
    it has being started

    :param node: node instance on which nodetool will be run
    :type node: ccmlib.Node
    :param cmd: toppartition command with arguments
    :type cmd: str
    :param timeout: time to wait nodetool topparition command start, defaults to 30
    :type timeout: number, optional
    :raises: NodetoolToppartitionStartedTimeoutError
    """
    st = time.time()
    nodetool_cmd_pattern = f"nodetool.*-h.*{node.address()}.*-p.*({node.jmx_port}|10000)"
    toppartition_cmd_pattern = cmd.replace(" ", ".*")
    while True:
        try:
            subprocess.check_output(["pgrep", "-fa", f"{nodetool_cmd_pattern}.*{toppartition_cmd_pattern}"])
            break
        except subprocess.CalledProcessError:
            pass
        ft = time.time()
        if ft - st > timeout:
            raise NodetoolToppartitionStartedTimeoutError("timeout error, nodetool toppartitions not started")


def parse_toppartitions_output(output: str) -> dict:
    """parsing output of toppartitions

    input format stored in output parameter:
    WRITES Sampler:
      Cardinality: ~10 (15 capacity)
      Top 10 partitions:
        Partition     Count       +/-
        9        11         0
        0         1         0
        1         1         0

    READS Sampler:
      Cardinality: ~10 (256 capacity)
      Top 3 partitions:
        Partition     Count       +/-
        0         3         0
        1         3         0
        2         3         0
        3         2         0

    return Dict:
    {
        'READS': {
            'toppartitions': '10',
            'partitions': OrderedDict('0': {'count': '1', 'margin': '0'},
                                      '1': {'count': '1', 'margin': '0'},
                                      '2': {'count': '1', 'margin': '0'}),
            'cardinality': '10',
            'capacity': '256',
        },
        'WRITES': {
            'toppartitions': '10',
            'partitions': OrderedDict('10': {'count': '1', 'margin': '0'},
                                      '11': {'count': '1', 'margin': '0'},
                                      '21': {'count': '1', 'margin': '0'}),
            'cardinality': '10',
            'capacity': '256',
            'sampler': 'WRITES'
        }
    }


    Arguments:
        output {str} -- stdout of nodetool topparitions command

    Returns:
        dict -- result of parsing
    """

    pattern1 = r"(?P<sampler>[A-Z]+)\sSampler:\W+Cardinality:\s~(?P<cardinality>[0-9]+)\s\((?P<capacity>[0-9]+)\scapacity\)\W+Top\s(?P<toppartitions>[0-9]+)\spartitions:"
    pattern2 = r"(?P<partition>\([\w:]+\)\s[\w:]+)\s+(?P<count>[\d]+)\s+(?P<margin>[\d]+)"
    toppartitions = {}
    for out in output.split("\n\n"):
        partition = {}
        sampler_data = re.match(pattern1, out, re.MULTILINE)
        sampler_data = sampler_data.groupdict()
        partitions = re.findall(pattern2, out, re.MULTILINE)
        for v in partitions:
            partition.update({v[0]: {"count": v[1], "margin": v[2]}})
        sampler_data.update({"partitions": partition})
        toppartitions[sampler_data.pop("sampler")] = sampler_data
    return toppartitions


def run_operations_c1c2(  # noqa: PLR0913
    session: Session,
    mode: str = "write",
    keys: list | None = None,
    w_keys: int = 1,
    r_keys: int = 1,
    w_num: int = 1,
    r_num: int = 1,
    ks: str = "ks",
    cf: str = "cf",
    ready_event: Event | None = None,
):
    """Execute operations on cluster for table with c1c2 columns

    Execute operations on cluster in mode

    Arguments:
        session {cassandra.cluster.Session} -- [description]

    Keyword Arguments:
        mode {str} -- Define which operations should be run: Write, Read, Write and Read (default: {"write"})
        keys {list} -- list of specified keys to write/read without prefix "k"
        w_keys {number} -- number of default keys to write (default: {1})
        r_keys {number} -- number of default keys to read (default: {1})
        w_num {number} -- repeat write operations for keys w_num times (default: {1})
        r_num {number} -- repeat read operatiions for keys r_num times (default: {1})
        ks {str} -- name of keyspace (default: {'ks'})
        cf {str} -- name of columnfamily (default: {'cf'})
        ready_event {threading.Event} -- Event object, if is set, start send queries
    """
    if mode == "write":
        if keys:
            keys_list = keys * w_num
            c1_values_list = list(range(w_num)) * len(keys)
            c2_values_list = list(range(w_num)) * len(keys)
        # when exact keys are not provided, write default keys [0-w_keys]
        else:
            keys_list = list(range(w_keys)) * w_num
            c1_values_list = list(range(w_num * w_keys))
            c2_values_list = list(range(w_num * w_keys))

        # time.sleep(delay)
        if ready_event and not ready_event.wait(30):
            raise NodetoolToppartitionStartedTimeoutError("timeout error, nodetool toppartitions not started")
        insert_c1c2_no_prepared(session, keys=keys_list, c1_values=c1_values_list, c2_values=c2_values_list, ks=ks, cf=cf)
    if mode == "read":
        if keys:
            keys_list = keys * r_num
        # when exact keys are not provided, read default keys [0-r_keys]
        else:
            keys_list = list(range(r_keys)) * r_num

        # time.sleep(delay)
        if ready_event and not ready_event.wait(40):
            raise NodetoolToppartitionStartedTimeoutError("timeout error, nodetool toppartitions not started")

        query_c1c2_concurrent(session, keys=keys_list, tolerate_missing=True, ks=ks, cf=cf)


def verify_thread_execution(th):
    exc = th.exception()
    if exc:
        raise exc


def verify_error_message(details):
    error_msg = "toppartitions requires either a keyspace, column family name and duration or no arguments at all"
    assert details.exit_status != 0, f"Command finished succesfully {details.exit_status}"
    output = details.stdout + details.stderr
    assert error_msg in output, f"'{error_msg}' not found in {output}"


def verify_empty_result(out):
    assert not out["WRITES"]["partitions"], f"Result is not empty {out}"
    assert not out["READS"]["partitions"], f"Result is not empty {out}"


def verify_samples_present_in_result(samplers, result):
    assert Counter(sorted(samplers)) == Counter(sorted(result.keys())), f"Samples are not present in the results"


def verify_counters_for_sample(actual_results, expected_results):
    """Verify couters for toppartitions

    Validate length of result lists, and counter of actual result is greater
    or equal the 90% of expected counter

    Arguments:
        actual_results {OrderedDict} -- List with results returned by toppartition
        expected_results {list} -- List of tuples with partition key and expected counter
    """
    result_accurancy = 0.9  # actual result for counter could be less on 10% from expected

    assert len(actual_results["partitions"]) == len(expected_results), f"Expected results: {expected_results}\nActual results: {actual_results['partitions']}"
    for partition, counter in expected_results:
        assert partition in actual_results["partitions"].keys(), f"Partition {partition} not found"
        assert int(actual_results["partitions"][partition]["count"]) >= result_accurancy * int(counter), f"Expected results: {expected_results}\nActual results: {actual_results['partitions']}."


def verify_partition_keys(actual_partition_keys, expected_toppartition_keys):
    for actual_key in actual_partition_keys:
        assert actual_key in expected_toppartition_keys, f"Key {actual_key} not found"
