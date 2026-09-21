import logging
import os
import pprint
import re
import subprocess
import time

import pytest
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.misc import is_coverage

logger = logging.getLogger(__file__)
ALLOW_BALANCE_DIFF = 0.4
PP = pprint.PrettyPrinter(indent=2)


class TestDataDistribution(Tester):
    def prepare(self, num_nodes):
        self.cluster: ScyllaCluster
        self.cluster.populate(nodes=num_nodes)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.ks = "keyspace1"
        self.cf = "standard1"

    @pytest.mark.dtest_full
    @pytest.mark.single_node
    @pytest.mark.parametrize("num_nodes", [3, 4, 6])
    @pytest.mark.parametrize(
        "strategy",
        [
            pytest.param("LeveledCompactionStrategy"),
            pytest.param("SizeTieredCompactionStrategy", marks=pytest.mark.next_gating),
            pytest.param("TimeWindowCompactionStrategy", marks=pytest.mark.next_gating),
            pytest.param("IncrementalCompactionStrategy", marks=pytest.mark.next_gating),
        ],
    )
    @pytest.mark.use_cassandra_stress
    @pytest.mark.high_memory
    def test_data_distribution_balance(self, strategy, num_nodes):
        """Check data distribution between nodes

        Based on issue #6193, verify data distribution
        between nodes by asserting size of dataset with
        nodetool status and filesizes on fs
        """

        debug_mode = type(self.cluster) is ScyllaCluster and self.cluster.scylla_mode == "debug"
        rf = 1
        keys = 21000 if debug_mode or is_coverage(self.cluster.get_install_dir()) else 210000

        self.prepare(generate_cluster_topology(rack_num=1, nodes_per_rack=num_nodes))

        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        tablets = 128 if "tablets" in self.scylla_features else None
        # Create the keyspace in advance of cassandra-stress
        # If tablets are enabled, use a high initial tablets values, so data is split into smaller chuncks
        # Otherwise, the following situation can occur, which makes balancing impossible
        # +-------------------+     +-------------------+     +-------------------+
        # |      Node 1       |     |      Node 2       |     |      Node 3       |
        # +-------------------+     +-------------------+     +-------------------+
        # |   [ 90MB Chunk ]  |     |   [ 90MB Chunk ]  |     |   [ 90MB Chunk ]  |
        # |   [ 90MB Chunk ]  |     |   [ 90MB Chunk ]  |     |   [ 90MB Chunk ]  |
        # |                   |     |   [ 90MB Chunk ]  |     |   [ 90MB Chunk ]  |
        # +-------------------+     +-------------------+     +-------------------+
        create_ks(session, name=self.ks, rf=rf, tablets=tablets)

        logger.info(f"Writing data...")
        stress_cmd = f"write cl=QUORUM n={keys} -schema replication(factor={rf}) compaction(strategy={strategy}) \
                    -mode native cql3 requestTimeout=60000 \
                    -col size=fixed(200) n=FIXED(5) -pop dist=UNIFORM(1..1000000000)"
        logger.debug(f"stress cmd={stress_cmd}")

        self.cluster.stress(stress_cmd.split(" "))
        self.cluster.flush()
        logger.info("Compacting data...")
        self.cluster.nodetool(f"disableautocompaction {self.ks}")
        self.cluster.compact()

        # nodetool status load is updated every 60 seconds
        status_ready_at = time.time() + 60 + 1

        for node in self.cluster.nodelist():
            cf_stats = node.nodetool("cfstats keyspace1", capture_output=True, wait=True)
            logger.info(PP.pformat(cf_stats))

        self.verify_datasize_by_check_filesize()

        now = time.time()
        if now < status_ready_at:
            logger.info(f"sleep for {int(status_ready_at - now + 0.5)} seconds until status.load is ready")
            time.sleep(status_ready_at - now)
        self.verify_datasize_with_nodetool_status()

    def verify_datasize_with_nodetool_status(self):
        node = self.cluster.nodelist()[0]
        result = node.nodetool("status", capture_output=True, wait=True)
        logger.info(result)
        status_result = self.parse_nodetool_status(result[0].splitlines())
        logger.info(PP.pformat(status_result))
        avg_size_dataset = self.get_avg_size(status_result)

        size_dimensions = {res["dimension"] for res in status_result}
        assert 1 == len(size_dimensions), f"Dimension is different {size_dimensions}"

        min_size = min([res["size"] for res in status_result])
        assert min_size >= avg_size_dataset * (1 - ALLOW_BALANCE_DIFF)
        max_size = max([res["size"] for res in status_result])
        assert max_size <= avg_size_dataset * (1 + ALLOW_BALANCE_DIFF)

    def verify_datasize_by_check_filesize(self):
        fs_sizes = self.parse_fs_size()
        avg_size_dataset = self.get_avg_size(fs_sizes)

        size_dimensions = {res["dimension"] for res in fs_sizes}
        assert 1 == len(size_dimensions), f"Dimension is different {size_dimensions}"

        min_size = min([res["size"] for res in fs_sizes])
        assert min_size >= avg_size_dataset * (1 - ALLOW_BALANCE_DIFF)
        max_size = max([res["size"] for res in fs_sizes])
        assert max_size <= avg_size_dataset * (1 + ALLOW_BALANCE_DIFF)

    def parse_nodetool_status(self, lines):
        """parse output of nodetool status

        Nodetool status output:
        Datacenter: eu-west
        ===================
        Status=Up/Down
        |/ State=Normal/Leaving/Joining/Moving
        --  Address      Load        Tokens       Owns    Host ID                               Rack
        UN  10.0.15.114  36.53 GiB   256          ?       98429fc3-1e89-4029-ac1c-325179752142  1a
        UN  10.0.126.57  88.17 GiB   256          ?       aea7e0f2-c2c3-4dc6-8ffd-8eda27f4ab8e  1a
        UN  10.0.74.155  90.62 GiB   256          ?       f2df2267-b8d1-4a1b-a5d8-a6c57a289f44  1a
        UN  10.0.65.254  101.39 GiB  256          ?       20eca592-3eda-478b-b9c8-03266879b8ba  1a

        Older versions label the same (base-2) values with base-10 unit names
        (KB, MB, ...), so both spellings are accepted and normalized to the
        base-2 one.

        Parsed result:
        [
            {"status": "UN", "address": "10.0.15.114", size: "36.53", dimension: KiB|MiB|GiB},
            {"status": "UN", "address": "10.0.126.57", size: "88.17", dimension: KiB|MiB|GiB},
            {"status": "UN", "address": "10.0.74.155", size: "90.62", dimension: KiB|MiB|GiB},
            {"status": "UN", "address": "10.0.65.254", size: "101.39", dimension: KiB|MiB|GiB}

        ]
        """
        keys = ["status", "address", "size", "dimension"]
        nodes_statuses = []
        line_re = re.compile(r"(?P<status>[UND]{2}?)\s+(?P<address>[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}?)\s+(?P<size>[\d]+\.[\d]+?)\s(?P<dimension>[KMGT]i?B)")
        for line in lines:
            node_status = {}
            res = line_re.search(line)
            if res:
                for key in keys:
                    if key == "size":
                        node_status[key] = float(res[key])
                        continue
                    if key == "dimension":
                        # older versions label the same base-2 values with base-10 unit names
                        node_status[key] = res[key][0] + "iB"
                        continue
                    node_status[key] = res[key]
                nodes_statuses.append(node_status)
        return nodes_statuses

    def parse_fs_size(self):
        """Get size of files for ks/table on fs for each node

        output per node:
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/staging
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/0000000000000007.sstable
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/upload
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/0000000000000005.sstable
        412M\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000
        412M\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1

        match the line 412M\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000

        result for all nodes:
        return:
        [
            {"address": "127.0.0.1", size: "412", dimension: KB|MB|GB},
            {"address": "127.0.0.2", size: "412", dimension: KB|MB|GB},
        ]


        """
        size_re = re.compile(rf"^(?P<size>[\d]+\.?\d*)(?P<dimension>[KMGT]?)\s.*\/{self.ks}\/{self.cf}-[0-9a-f]*$")
        node_size = []
        for node in self.cluster.nodelist():
            data_dir = os.path.join(node.get_path(), "data", self.ks)

            res = subprocess.run(["du", "-h", data_dir], capture_output=True, check=False)
            if res.stdout and res.returncode == 0:
                du_output = res.stdout.decode(encoding="utf-8")
                logger.debug("du -h output for node %s:\n%s", node.address(), du_output)
                matched = False
                for line in res.stdout.split(b"\n"):
                    decoded_line = line.decode(encoding="utf-8")
                    size_res = size_re.search(decoded_line)
                    if size_res:
                        matched = True
                        node_size.append({"address": node.address(), "size": float(size_res["size"]), "dimension": size_res["dimension"]})
                if not matched:
                    logger.warning("No regex match found for node %s. Regex: %s", node.address(), size_re.pattern)
            elif res.returncode != 0:
                logger.warning("du command failed for node %s: returncode=%d stderr=%s", node.address(), res.returncode, res.stderr.decode(encoding="utf-8"))
        logger.info(node_size)
        return node_size

    def get_avg_size(self, sizes_data):
        sizes = [float(p["size"]) for p in sizes_data]
        assert sizes, f"No size data collected for {self.ks}.{self.cf}. Check debug logs above for du output and regex match details."
        return sum(sizes) / len(sizes)
