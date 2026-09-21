import pytest

from dtest_class import Tester, create_ks


class TestHelper(Tester):
    def get_table_path(self, table):
        """
        Return the path where the table sstables are located
        """
        node1 = self.cluster.nodelist()[0]
        path = ""
        basepath = os.path.join(node1.get_path(), "data", KEYSPACE)
        for x in os.listdir(basepath):
            if x.startswith(table):
                path = os.path.join(basepath, x)
                break
        return path

    def get_index_path(self, index):
        """
        Return the path where the index sstables are located
        """
        node1 = self.cluster.nodelist()[0]
        basepath = os.path.join(node1.get_path(), "data", KEYSPACE)
        index_path = ""
        for x in os.listdir(basepath):
            if x.startswith(index):
                index_path = os.path.join(basepath, x)
                break
        return index_path

    def get_sstable_files(self, path):
        """
        Return the sstable files at a specific location
        """
        ret = []
        logger.debug(f"Checking sstables in {path}")

        for ext in ("*.db", "*.txt", "*.adler32", "*.sha1"):
            for fname in glob.glob(os.path.join(path, ext)):
                bname = os.path.basename(fname)
                if "-scylla." in bname.lower():
                    continue
                ret.append(bname)
        return ret

    def delete_non_essential_sstable_files(self, table):
        """
        Delete all sstable files except for the -Data.db file and the
        -Statistics.db file (only available in >= 3.0)
        """
        # NOTE: TOC file is essential for ScyllaDB, also any component in the
        # TOC is required, so we need to remove deleted components from the TOC too.
        # See https://github.com/scylladb/scylladb/issues/21145

        sstable_re = re.compile(
            r"""(?P<version>la|m[cdes]|n[a-b]|o[a]|d[a])- # the sstable version
                                    (?P<id>[^-]+)-          # sstable identifier
                                    (?P<format>\w+)-        # format: 'big' or 'bti'
                                    (?P<component>.*)       # component: e.g., 'Data'""",
            re.X,
        )
        bti_tocs = list()
        big_tocs = list()
        for fname in self.get_sstable_files(self.get_table_path(table)):
            # Collect removed TOCs, to be restored later.
            fullname = os.path.join(self.get_table_path(table), fname)
            matched = sstable_re.fullmatch(os.path.basename(fname))
            if matched and matched["component"] == "TOC.txt":
                if matched["version"] in ["ms", "da"]:
                    bti_tocs.append(fullname)
                else:
                    big_tocs.append(fullname)
            if not matched or matched["component"] not in ["Data.db", "Index.db", "Statistics.db", "Partitions.db", "Rows.db"]:
                logger.debug(f"Deleting {fullname}")
                os.remove(fullname)
        logger.info(f"TOCS: {bti_tocs + big_tocs}")
        # Restore TOCs
        for toc in big_tocs:
            logger.debug(f"restoring TOC {toc}")
            with open(toc, "w") as f:
                f.write("Data.db\n")
                f.write("Index.db\n")
                f.write("Statistics.db\n")
                f.write("TOC.txt\n")
                f.flush()
        for toc in bti_tocs:
            logger.debug(f"restoring TOC {toc}")
            with open(toc, "w") as f:
                f.write("Data.db\n")
                f.write("Partitions.db\n")
                f.write("Rows.db\n")
                f.write("Statistics.db\n")
                f.write("TOC.txt\n")
                f.flush()

    def get_sstables(self, table, indexes):
        """
        Return the sstables for a table and the specified indexes of this table
        """
        sstables = {}
        table_sstables = self.get_sstable_files(self.get_table_path(table))
        assert len(table_sstables) > 0, f"sstables were not found in {self.get_table_path(table)}"
        sstables[table] = sorted(table_sstables)

        for index in indexes:
            index_sstables = self.get_sstable_files(self.get_index_path(index))
            assert len(index_sstables) > 0, f"No indexes were found by path: {self.get_index_path(index)}"
            sstables[index] = sorted(f"{index}/{sstable}" for sstable in index_sstables)

        return sstables

    def launch_nodetool_cmd(self, cmd):
        """
        Launch a nodetool command and check the result is empty (no error)
        """
        node1 = self.cluster.nodelist()[0]
        response = node1.nodetool(cmd, capture_output=True)[0]
        if not common.is_win():  # nodetool always prints out on windows
            assert len(response) == 0, response  # nodetool does not print anything unless there is an error

    def launch_standalone_scrub(self, ks, cf):
        """
        Launch the standalone scrub
        """
        node1 = self.cluster.nodelist()[0]

        table_path = self.get_table_path(cf)

        with tempfile.TemporaryDirectory() as tmp_dir:
            node1.run_scylla_sstable("scrub", additional_args=["--scrub-mode", "abort", "--output-dir", tmp_dir, "--logger-log-level", "scylla-sstable=debug", "--unsafe-accept-nonempty-output-dir"], keyspace=ks, column_families=[cf])
            # Replace the table's sstables with the scrubbed ones, just like online scrub would do.
            shutil.rmtree(table_path)
            shutil.copytree(tmp_dir, table_path)

    def perform_node_tool_cmd(self, cmd, table, indexes):
        """
        Perform a nodetool command on a table and the indexes specified
        """
        self.launch_nodetool_cmd(f"{cmd} {KEYSPACE} {table}")
        for index in indexes:
            self.launch_nodetool_cmd(f"{cmd} {KEYSPACE} {index}_index")

    def flush(self, table, *indexes):
        """
        Flush table and indexes via nodetool, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.perform_node_tool_cmd("flush", table, indexes)
        return self.get_sstables(table, indexes)

    def scrub(self, table, *indexes):
        """
        Scrub table and indexes via nodetool, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.perform_node_tool_cmd("scrub", table, indexes)
        return self.get_sstables(table, indexes)

    def standalonescrub(self, table, *indexes):
        """
        Launch standalone scrub on table and indexes, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.launch_standalone_scrub(KEYSPACE, table)
        for index in indexes:
            self.launch_standalone_scrub(KEYSPACE, f"{index}_index")
        return self.get_sstables(table, indexes)


@pytest.mark.single_node
@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestCompressionChunkSize(TestHelper):
    @pytest.mark.parametrize("compressor", ["DeflateCompressor", "LZ4Compressor", "SnappyCompressor"])
    def test_sstable_compression_chunk_size_positive(self, compressor):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        # Create negative test
        with pytest.raises(Exception, match="Query invalid because of configuration issue"):
            session.execute(
                f"""
                create table compression_opts_table
                    (id uuid PRIMARY KEY )
                    WITH compression = {{
                        'sstable_compression': '{compressor}',
                        'chunk_length_in_kb': 256
                    }}
                """
            )

        # Create positive test
        session.execute(
            f"""
            create table compression_opts_table
                (id uuid PRIMARY KEY )
                WITH compression = {{
                    'sstable_compression': '{compressor}',
                    'chunk_length_in_kb': 128
                }}
            """
        )

        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces["ks"].tables["compression_opts_table"]
        assert f"org.apache.cassandra.io.compress.{compressor}" == meta.options["compression"]["sstable_compression"]
        assert "128" == meta.options["compression"]["chunk_length_in_kb"]

        # # Alter negative test
        with pytest.raises(Exception, match="Query invalid because of configuration issue"):
            session.execute(
                f"""
                alter table compression_opts_table
                    WITH compression = {{
                        'sstable_compression': '{compressor}',
                        'chunk_length_in_kb': 256
                    }}
                """
            )

        # Positive alter test
        session.execute(
            f"""
                        alter table compression_opts_table
                            WITH compression = {{
                                'sstable_compression': '{compressor}',
                                'chunk_length_in_kb': 64
                            }}
                        """
        )
        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces["ks"].tables["compression_opts_table"]
        assert f"org.apache.cassandra.io.compress.{compressor}" == meta.options["compression"]["sstable_compression"]
        assert "64" == meta.options["compression"]["chunk_length_in_kb"]
