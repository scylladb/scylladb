import logging
import os
import re

import pytest
from cassandra.concurrent import execute_concurrent_with_args

from dtest_class import Tester, create_ks

logger = logging.getLogger(__name__)

known_units = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]


def normalize_units(units):
    """Normalize a unit name to its base-2 (IEC) spelling

    Nodetool divides sizes by 1024, so the values it prints are base-2 ones.
    Older versions label them with base-10 (SI) unit names (KB, MB, ...)
    anyway, so accept both spellings and normalize to the base-2 one.
    """
    if not units or units.lower() == "bytes":
        return known_units[0]
    normalized = units[0].upper() + "iB"
    if normalized not in known_units:
        raise RuntimeError(f"Unknown {units} units")
    return normalized


def human_size(size, units=known_units):
    """Returns a human readable string reprentation of bytes"""
    if size < 1024.0:
        size = float(f"{size:.2f}")
        return f"{size:g} {units[0]}"
    else:
        return human_size(size / 1024.0, units[1:])


def normalize_size(size, units):
    size = float(size)
    units = normalize_units(units)
    return size * 1024 ** known_units.index(units)


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.single_node
class TestNodetoolListSnapshots(Tester):
    """Validate nodetool listshapshot command

    See https://docs.scylladb.com/operating-scylla/nodetool-commands/listsnapshots

    Extends:
        Tester
    """

    def prepare_cluster(self):
        """Create and populate cluster for tests"""

        # create and start cluster with one node
        logger.debug("Create and start cluster")
        self.cluster.set_configuration_options(
            values={
                "tablets_initial_scale_factor": 1,
            }
        )
        self.cluster.populate(1).start()

    def insert_rows(self, session, ks, cf, start, end):
        """Insert data to scylladb

        taken from snapshot_test

        Arguments:
            session {session} -- opened session to scylla cluster
            ks {str} -- keyspace name
            cf {str} -- column family name
            start {int} -- key field value start
            end {int} -- key field value end
        """
        insert_statement = session.prepare(f"INSERT INTO {ks}.{cf} (key, val) VALUES (?, 'asdf')")
        args = [(r,) for r in range(start, end)]
        execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def create_snapshot(self, node, ks, cf=None):
        """Create snapshot on node for ks.cf

        Arguments:
            node {node.Node} -- instance of node
            ks {str} -- keyspace name to build snapshot for

        Keyword Arguments:
            cf {str} -- cf name to build snapshot for (default: {None})
        """
        logger.debug(f"Create snapshot for {ks} on node {node.address()}")
        snapshot_cmd = f"snapshot {ks} -cf {cf}" if cf else f"snapshot {ks}"
        node.nodetool(snapshot_cmd)

    def count_snapshot_disk_size(self, node):
        """Build dictionary with main info about snapshots

        Build the dictionary with main info about snapshots
        on provided node. Return dict has next structure:
        { keyspacename :
            (<column family name>, <snapshot name>): {
                'uuid': <uuid of column family>
                'snapshot_name': {
                    'path': <full path to snapshot dir of cf
                    'size': human readable filesize as sum of *.db hardlink files under path
                }
            }

        Arguments:
            node {node.Node} -- instance of Node in cluster under test

        Returns:
            [dict] -- Structure as dict with info about snapshots on node
        """
        data_dir = os.path.join(node.get_path(), "data")

        # build dict keys of all keyspaces
        snapshots = {ks_dir: {} for ks_dir in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, ks_dir))}

        for ks, value in snapshots.items():
            # build keyspace path name and go through each keyspace
            # and find all cf
            for path, dirs, files in os.walk(os.path.join(data_dir, ks)):
                if "/snapshots/" not in path:
                    continue

                def get_sizes(filename):
                    st = os.lstat(filename)
                    return (st.st_size, st.st_blocks * 512)

                p = path.split("/")
                snapshot_id = p[-1]
                cf_name = p[-3].split("-")
                snapshot_files = [f for f in files if "manifest.json" not in f and "schema.cql" not in f]
                logical_size = 0
                disk_size = 0
                for f in snapshot_files:
                    st = os.lstat(os.path.join(path, f))
                    logical_size += st.st_size
                    disk_size += st.st_blocks * 512
                logger.debug(f"Snapshot ks:{ks} cf:{cf_name[0]} name:{snapshot_id} logical size is {logical_size}, allocated size is {disk_size}, human size is {human_size(disk_size)}")

                # update the dict with data
                value.update({(cf_name[0], snapshot_id): {"uuid": cf_name[1], snapshot_id: {"path": path, "logical_size": float(logical_size), "size": float(disk_size), "human_size": human_size(disk_size)}}})

        return snapshots

    def parse_output_listsnapshots(self, output):
        """parse output of command listsnapshots

        Example of listsnapshots stdout:
        $ nodetool listsnapshots

        Snapshot Details:
        Snapshot Name  Keyspace   Column Family  True Size   Size on Disk

        5487138454987  my_ks1     my_cf1    0 bytes     308.66 MiB
        2157384283120  my_ks2     my_cf2    0 bytes     107.21 MiB
        4824891793663  my_ks3     my_cf3    0 bytes      41.69 MiB

        Older versions label the same (base-2) values with base-10 unit names
        (KB, MB, ...), so both spellings are accepted and normalized to the
        base-2 one.

        Arguments:
            output {string} -- result of command output
        """

        output_regexp = re.compile(r"^(?P<snsh_name>[\w]+)\s+(?P<ks>[\w]+)\s+(?P<cf>[\w]+)\s+(?P<true_size>[0-9.]+)\s(?P<true_size_units>\w+)\s+(?P<size_on_disk>[0-9.]+)\s+(?P<size_on_disk_units>\w+)\s+$", re.MULTILINE)
        logger.debug(f"Output of nodetool listsnapshots:\n{output}")
        parsed = output_regexp.findall(output)
        ret = []
        for snsh_name, ks, cf, true_size, true_size_units, size_on_disk, size_on_disk_units in parsed:
            ret.append((snsh_name, ks, cf, normalize_size(true_size, true_size_units), normalize_size(size_on_disk, size_on_disk_units)))
        logger.debug(f"Parsed and normalized:\n{ret}")
        return ret

    def compare_filesize_and_output(self, node, output):
        """Compare results of counted snapshot size and listsnapshots output

        Get ks, cf, snapshot name, size on disk from stdout of listsnapshots
        and find and compare in dict of snapshot sructure returned by
        count_snapshot_disk_size method and compare counted results with got
        results.

        if size are equal for each snapshot return true, otherwise false

        Arguments:
            node {node.Node} -- instace of node to work on
            output {string} -- stdout of nodetool listsnapshots

        Returns:
            [bool] -- result of validate for each snapshot
        """
        listsnaps = self.parse_output_listsnapshots(output)
        snapshots = self.count_snapshot_disk_size(node)
        return all([snapshots[ks][(cf, snsh_id)][snsh_id]["size"] >= ondisk for snsh_id, ks, cf, _, ondisk in listsnaps]) and all([snapshots[ks][(cf, snsh_id)][snsh_id]["logical_size"] <= ondisk for snsh_id, ks, cf, _, ondisk in listsnaps])

    def populate_keyspaces(self, session, kses, cfes):
        """Fill provided keyspaces with tables and
        simple data

        Create tables from cfes if not exists for each keyspace in kses
        and insert simple value to each table

        Arguments:
            session {session} -- open session to scylladb cluster
            kses {list} -- list of keyspaces to populate
            cfes {list} -- list of column families to create
        """
        for ks in kses:
            create_ks(session, ks, 1)
            for cf in cfes:
                session.execute(f"CREATE TABLE IF NOT EXISTS {ks}.{cf} (key int PRIMARY KEY, val text);")
                session.execute(f"INSERT INTO {ks}.{cf} (key, val) VALUES (1, 'asdf');")

    def test_no_snapshots(self):
        """Validate that after creating,  cluster have
        not any snapshot on node and command is not stopped with exception
        """
        self.prepare_cluster()
        node = self.cluster.nodelist()[0]

        results, errors = node.nodetool("listsnapshots")
        assert not errors, f"Errors: {errors}"
        assert "There are no snapshots" in results, f"There are snapshots on the node {results}"
        assert self.compare_filesize_and_output(node, results), "Not all snapshot size and names are valid"

    def test_one_ks_one_snapshot(self):
        """
        Validate that listsnapshots correclty count and display
        information about snapshot for 1 created ks
        """
        ks = ["my_ks"]
        cf = ["my_cf"]

        self.prepare_cluster()
        node = self.cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.populate_keyspaces(session, ks, cf)
        self.create_snapshot(node, ks[0], cf[0])

        results, errors = node.nodetool("listsnapshots")

        # asserts there is no errors in stderr
        assert not errors, f"Errors: {errors}"
        # assert that all snapshot size and names are valid
        assert self.compare_filesize_and_output(node, results), "Not all snapshot size and names are valid"

    def test_snapshots_of_system_ks(self):
        """
        validate that listsnapshots command correctly display data for
        snapshots of system keyspace only
        """
        self.prepare_cluster()
        node = self.cluster.nodelist()[0]

        self.create_snapshot(node, "system")

        results, errors = node.nodetool("listsnapshots")

        # asserts there is no errors in stderr
        assert not errors, f"Errors: {errors}"
        # assert that all snapshot size and names are valid
        assert self.compare_filesize_and_output(node, results), "Not all snapshot size and names are valid"

    @pytest.mark.dtest_debug
    def test_snapshot_for_several_kses(self):
        """
        Validate the correctness of listsnapshots command if
        snapshots were created for each custom keyspace
        """
        self.prepare_cluster()
        kses = ["my_ks1", "my_ks2", "my_ks3"]
        cfes = ["my_cf1", "my_cf2", "my_cf3"]
        node = self.cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.populate_keyspaces(session, kses, cfes)

        for ks in kses:
            self.create_snapshot(node, ks)

        results, errors = node.nodetool("listsnapshots")

        # asserts there is no errors in stderr
        assert not errors, f"Errors: {errors}"
        # assert that all snapshot size and names are valid
        assert self.compare_filesize_and_output(node, results), "Not all snapshot size and names are valid"

    def test_several_snapshots_for_several_kses(self):
        """
        validate the correctness of stdout for listsnapshots command
        if several snapshots were done for several kses
        """

        self.prepare_cluster()
        kses = ["my_ks1", "my_ks2", "my_ks3"]
        cfes = ["my_cf1", "my_cf2", "my_cf3"]
        node = self.cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.populate_keyspaces(session, kses, cfes)

        for ks in kses:
            self.create_snapshot(node, ks)

        logger.debug("Fill with 10k records")
        for ks in kses:
            for cf in cfes:
                self.insert_rows(session, ks, cf, 2, 10000)

        for ks in kses:
            self.create_snapshot(node, ks)

        results, errors = node.nodetool("listsnapshots")

        # asserts there is no errors in stderr
        assert not errors, f"Errors: {errors}"
        # assert that all snapshot size and names are valid
        assert self.compare_filesize_and_output(node, results), "Not all snapshot size and names are valid"
