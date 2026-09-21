import pytest
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks, wait_for
from tools.assertions import assert_all, assert_invalid, assert_none, assert_one
from tools.data import create_index, create_local_index, rows_to_list
from tools.paging import PageFetcher

pytestmark = pytest.mark.next_gating


class BaseOperationsHelper:
    special_values = "!@#$%^&*()-_<>?.,/\\ "
    unsupported_column_types_and_values = {
        "bigint": ("10000", "1", "2"),
        "boolean": ("true", "true", "false"),
        "blob": ("textAsBlob('1234567890qwertyuiop')", "textAsBlob('a')", "bigintAsBlob(1)"),
        "date": ("currentDate()", "currentDate()", "currentDate()"),
        "decimal": ("10.1", "11.1", "12.2"),
        "double": ("10.1000001", "11.111111", "22.22222"),
        "duration": ("89h1m48s", "11h11m11s", "22h22m22s"),
        "float": ("10.10001", "33.33", "44.44"),
        "inet": ("'1.1.1.1'", "'1.1.1.1'", "'2.2.2.2'"),
        "int": ("100001", "1", "2"),
        "smallint": ("1", "1", "2"),
        "time": ("currentTime()", "currentTime()", "currentTime()"),
        "timestamp": ("currentTimestamp()", "currentTimestamp()", "currentTimestamp()"),
        "timeuuid": ("currentTimeUUID()", "currentTimeUUID()", "currentTimeUUID()"),
        "tinyint": ("1", "2", "5"),
        "uuid": ("uuid()", "uuid()", "uuid()"),
        "varint": ("1", "4", "5"),
    }

    def prepare_simple_table_with_column_type(self, cl_type="text"):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        create_cf_st = f"""CREATE TABLE test (
            pk {cl_type},
            ck {cl_type},
            test {cl_type},
            PRIMARY KEY (pk, ck)
        )
        """
        session.execute(create_cf_st)

        return session

    def prepare_tables_supported_and_unsupported_types(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        for data_type in self.unsupported_column_types_and_values.keys():
            create_table = f"""CREATE TABLE t_{data_type} (
                pk text,
                cl_{data_type} {data_type},
                cl_text text,
                PRIMARY KEY (pk)
            )
            """
            session.execute(create_table)

        return session

    def prepare_unsupported_column_names_and_types(self):
        names = list(self.unsupported_column_types_and_values.keys())
        columns = [f"pk_{names[0]} {names[0]}"]
        columns.extend([f"cl_{cl_type} {cl_type}" for cl_type in names[1:]])
        return columns

    def generate_unsupported_columns_name(self):
        # Use first type as pk.
        names = list(self.unsupported_column_types_and_values.keys())
        columns = [f"pk_{names[0]}"]
        columns.extend([f"cl_{cl_type}" for cl_type in names[1:]])
        return columns

    def get_unsupported_type_values_single_row(self):
        return [value[0] for value in self.unsupported_column_types_and_values.values()]

    def get_unsupported_type_values_2_rows(self):
        return [(value[1], value[2]) for value in self.unsupported_column_types_and_values.values()]

    def prepare_table_with_unsupported_types(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        columns_names_types = self.prepare_unsupported_column_names_and_types()
        pk_key = self.generate_unsupported_columns_name()[0]
        create_cf_st = f"""CREATE TABLE test (
            {",".join(columns_names_types)},
            PRIMARY KEY ({pk_key})
        )
        """
        session.execute(create_cf_st)

        return session

    def prepare_complex_table(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        create_cf_st_various = """CREATE TABLE various_fields (
            key_txt text,
            key_varchar varchar,
            key_ascii ascii,
            cl_txt text,
            cl_varchar varchar,
            cl_ascii ascii,
            column_txt text,
            column_varchar varchar,
            column_ascii ascii,
            PRIMARY KEY ((key_txt, key_varchar, key_ascii), cl_txt, cl_varchar, cl_ascii)
        )
        """
        session.execute(create_cf_st_various)

        return session

    def prepare_table_with_static_field(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        create_cf_st = """CREATE TABLE test (
            pk ascii,
            ck text,
            test varchar,
            cl_static text STATIC,
            PRIMARY KEY (pk, ck)
        )
        """
        session.execute(create_cf_st)

        return session

    @staticmethod
    def populate_simple_table_with_basic_data(session):
        all_data = [[f"teststring{i}", f"{i}teststring", f"test{i}string"] for i in range(5)]
        for data in all_data:
            session.execute(f"INSERT INTO test (pk, ck, test) VALUES ('{data[0]}', '{data[1]}', '{data[2]}')")
        return all_data

    def populate_unsupported_types_table(self, session):
        """Insert data with unsupported types.

        Unsupported columns are generated in order as they stored in self.unsupported_types list.
        """
        columns = self.generate_unsupported_columns_name()
        column_values = self.get_unsupported_type_values_single_row()
        session.execute(f"""INSERT INTO test ({",".join(columns)}) VALUES ({",".join(column_values)})""")

    def populate_unsupported_supported_tables_with_2_rows(self, session):
        for data_type in self.unsupported_column_types_and_values.keys():
            for i, value in enumerate(self.unsupported_column_types_and_values[data_type][1:]):
                session.execute(f"INSERT INTO t_{data_type} (pk, cl_{data_type}, cl_text) VALUES ('text_{i}', {value}, 'text%{i}')")

    def populate_simple_table_with_specific_data(self, session):
        for i in range(5):
            session.execute(f"INSERT INTO test (pk, ck, test) VALUES ('TEST%STRING{i}', '{i}Test_String', '_{{}}%')")
        for char in self.special_values:
            session.execute(f"INSERT INTO test (pk, ck, test) VALUES ('TEST%STRING7', 'cluster_{char}', '{char}')")
        all_data = rows_to_list(session.execute("SELECT * FROM test"))
        return all_data

    @staticmethod
    def populate_table_with_static_field(session):
        all_data = [[f"teststring{i}", f"{i}teststring", f"test{i}string", "static teststring"] for i in range(5)]
        for data in all_data:
            session.execute(f"INSERT INTO test (pk, ck, test, cl_static) VALUES ('{data[0]}', '{data[1]}', '{data[2]}', '{data[3]}')")
        return all_data

    @staticmethod
    def populate_simple_table_with_several_partitions(session, num_partitions=5, rows_per_partition=2):
        """Populate simple table with several rows per partitions.

        Insert data in num_parititions with rows_per_partitions num of rows.
        Next structure of data will be created by default:
        [
            ['teststring0', '0teststring', 'test0string'],
            ['teststring0', '1teststring', 'test1string'],
            ['teststring1', '0teststring', 'test0string'],
            ['teststring1', '1teststring', 'test1string'],
            ['teststring2', '0teststring', 'test0string'],
            ['teststring2', '1teststring', 'test1string'],
            ['teststring3', '0teststring', 'test0string'],
            ['teststring3', '1teststring', 'test1string'],
            ['teststring4', '0teststring', 'test0string'],
            ['teststring4', '1teststring', 'test1string'],
        ]
        """
        all_data = [[f"teststring{j}", f"{i}teststring", f"test{i}string"] for j in range(num_partitions) for i in range(rows_per_partition)]
        for data in all_data:
            session.execute(f"INSERT INTO test (pk, ck, test) VALUES ('{data[0]}', '{data[1]}', '{data[2]}')")
        return all_data

    def prepare_cluster_with_materialized_views(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        create_cf_st = """CREATE TABLE buildings (
            name text,
            city text,
            PRIMARY KEY (name)
        )
        """
        session.execute(create_cf_st)

        # Create a materialized view.
        session.execute("CREATE MATERIALIZED VIEW building_by_city AS SELECT * FROM buildings WHERE city IS NOT NULL PRIMARY KEY (city, name)")

        for i in range(5):
            session.execute(f"INSERT INTO buildings (name, city) VALUES ('qwerty{i}', 'ytrewq{i}')")

        def check_mv():
            return len(rows_to_list(session.execute("SELECT * FROM building_by_city"))) == 5

        wait_for(check_mv, timeout=5)

        return session

    def prepare_cluster_with_global_index(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        create_cf_st = """CREATE TABLE buildings (
            name text,
            city text,
            PRIMARY KEY (name)
        )
        """
        session.execute(create_cf_st)

        for i in range(5):
            session.execute(f"INSERT INTO buildings (name, city) VALUES ('qwerty{i}', 'ytrewq{i}')")

        create_index(session=session, table_name="buildings", index_column="city", index_name="city_key")
        create_local_index(
            session=session,
            table_name="buildings",
            pk_name="name",
            index_column="city",
            index_name="city_local",
        )

        return session

    def prepare_cluster_with_local_index(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session=session, name="ks1", rf=1)

        create_cf_st = """CREATE TABLE buildings (
            name text,
            city text,
            PRIMARY KEY (name)
        )
        """
        session.execute(create_cf_st)

        for i in range(5):
            session.execute(f"INSERT INTO buildings (name, city) VALUES ('qwerty{i}', 'ytrewq{i}')")

        create_local_index(
            session=session,
            table_name="buildings",
            pk_name="name",
            index_column="city",
            index_name="city_local",
        )

        return session


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestLikeOperatorForBaseTable(Tester, BaseOperationsHelper):
    def test_pk_filtering_of_text_type_with_percent_sign(self):
        """Test filtering with LIKE operator by partition key.

        Filter with LIKE by partition key where partition type is text
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="text")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '%string%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '%string3' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'te%ng3' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 't%1' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%STR%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%strinG3' ALLOW FILTERING")

    def test_ck_filtering_of_text_type_with_percent_sign(self):
        """Test filtering with LIKE operator by clustering key.

        Filter with LIKE by clustering key where clustering type is text.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="text")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%ing' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '3%' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1te%ng' ALLOW FILTERING", expected=all_data[1])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '2%g' ALLOW FILTERING", expected=all_data[2])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%EST%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3%strinG' ALLOW FILTERING")

    def test_cl_filtering_of_text_type_with_percent_sign(self):
        """Test filtering with LIKE operator by column key.

        Filter with LIKE by column key where column type is text.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="text")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%ing' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the middle of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test%string' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'test4%' ALLOW FILTERING", expected=all_data[4])

        # % at the beginning of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '%0string' ALLOW FILTERING", expected=all_data[0])

        # % from both side.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '%2%' ALLOW FILTERING", expected=all_data[2])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'Test%STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'tesT%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '3%strinG' ALLOW FILTERING")

    def test_pk_filtering_of_text_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by partition key.

        Filter with LIKE by partition key where partition type is text.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="text")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '_eststring_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'tes_string_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '_eststring3' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session, query="SELECT * FROM test WHERE pk LIKE 'tes__tring3' ALLOW FILTERING", expected=all_data[3])
        assert_one(session, query="SELECT * FROM test WHERE pk LIKE 't_stst___g1' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'TESTSTRING_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_EstSTRing_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_strinG3' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_teststring3' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring3_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'test_string3' ALLOW FILTERING")

    def test_ck_filtering_of_text_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by clustering key.

        Filter with LIKE by clustering key where clustering type is text.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="text")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_teststring' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_teststrin_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_tes_str_n_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the ending of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '2teststrin_' ALLOW FILTERING", expected=all_data[2])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '3tes__tring' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1t_stst___g' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_TESTSTRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '__EstSTRin_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3_strinG' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_3teststring' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3teststring_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE 'test_string3' ALLOW FILTERING")

    def test_cl_filtering_of_text_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by column.

        Filter with LIKE by column where clustering type is text.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="text")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the middle of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test_string' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '_est_strin_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the ending of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'test0strin_' ALLOW FILTERING", expected=all_data[0])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'te__3_tring' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 't_st1st___g' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'TEST_STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '_Est1STRin_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'Test_strinG' ALLOW FILTERING")

        # assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '_test1string' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'test_string_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'test_1string' ALLOW FILTERING")

    def test_pk_filtering_of_varchar_type_with_percent_sign(self):
        """Test filtering with LIKE operator by partition key.

        Filter with LIKE by partition key where partition type is varchar.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="varchar")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '%string%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '%string3' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'te%ng3' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 't%1' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%STR%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%strinG3' ALLOW FILTERING")

    def test_ck_filtering_of_varchar_type_with_percent_sign(self):
        """Test filtering with LIKE operator by clustering key.

        Filter with LIKE by clustering key where clustering type is varchar.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="varchar")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%ing' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '3%' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1te%ng' ALLOW FILTERING", expected=all_data[1])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '2%g' ALLOW FILTERING", expected=all_data[2])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%EST%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3%strinG' ALLOW FILTERING")

    def test_cl_filtering_of_varchar_type_with_percent_sign(self):
        """Test filtering with LIKE operator by column key.

        Filter with LIKE by column key where column type is varchar.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="varchar")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%ing' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the middle of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test%string' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'test4%' ALLOW FILTERING", expected=all_data[4])

        # % at the beginning of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '%0string' ALLOW FILTERING", expected=all_data[0])

        # % from both side.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '%2%' ALLOW FILTERING", expected=all_data[2])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'Test%STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'tesT%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '3%strinG' ALLOW FILTERING")

    def test_pk_filtering_of_varchar_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by partition key.

        Filter with LIKE by partition key where partition type is varchar.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="varchar")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '_eststring_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'tes_string_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '_eststring3' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'tes__tring3' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 't_stst___g1' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'TESTSTRING_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_EstSTRing_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_strinG3' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_teststring3' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring3_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'test_string3' ALLOW FILTERING")

    def test_ck_filtering_of_varchar_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by clustering key.

        Filter with LIKE by clustering key where clustering type is varchar.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="varchar")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_teststring' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_teststrin_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_tes_str_n_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the ending of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '2teststrin_' ALLOW FILTERING", expected=all_data[2])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '3tes__tring' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1t_stst___g' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_TESTSTRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '__EstSTRin_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3_strinG' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_3teststring' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3teststring_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE 'test_string3' ALLOW FILTERING")

    def test_cl_filtering_of_varchar_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by column.

        Filter with LIKE by column where clustering type is varchar.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="varchar")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the middle of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test_string' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '_est_strin_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the ending of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'test0strin_' ALLOW FILTERING", expected=all_data[0])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'te__3_tring' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 't_st1st___g' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'TEST_STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '_Est1STRin_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'Test_strinG' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '_test1string' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'test_string_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'test_1string' ALLOW FILTERING")

    def test_pk_filtering_of_ascii_type_with_percent_sign(self):
        """Test filtering with LIKE operator by partition key.

        Filter with LIKE by partition key where partition type is ascii.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="ascii")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '%string%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '%string3' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'te%ng3' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 't%1' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%STR%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%strinG3' ALLOW FILTERING")

    def test_ck_filtering_of_ascii_type_with_percent_sign(self):
        """Test filtering with LIKE operator by clustering key.

        Filter with LIKE by clustering key where clustering type is ascii.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="ascii")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%ing' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '3%' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1te%ng' ALLOW FILTERING", expected=all_data[1])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '2%g' ALLOW FILTERING", expected=all_data[2])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%EST%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3%strinG' ALLOW FILTERING")

    def test_cl_filtering_of_ascii_type_with_percent_sign(self):
        """Test filtering with LIKE operator by column key.

        Filter with LIKE by column key where column type is ascii.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="ascii")
        all_data = self.populate_simple_table_with_basic_data(session)

        # % at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%ing' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the middle of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test%string' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % only as pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the end of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'test4%' ALLOW FILTERING", expected=all_data[4])

        # % at the beginning of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '%0string' ALLOW FILTERING", expected=all_data[0])

        # % from both side.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '%2%' ALLOW FILTERING", expected=all_data[2])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'Test%STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'tesT%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '3%strinG' ALLOW FILTERING")

    def test_pk_filtering_of_ascii_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by partition key.

        Filter with LIKE by partition key where partition type is ascii.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="ascii")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '_eststring_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'tes_string_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '_eststring3' ALLOW FILTERING", expected=all_data[3])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'tes__tring3' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 't_stst___g1' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'TESTSTRING_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_EstSTRing_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_strinG3' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '_teststring3' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring3_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'test_string3' ALLOW FILTERING")

    def test_ck_filtering_of_ascii_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by clustering key.

        Filter with LIKE by clustering key where clustering type is ascii.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="ascii")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the beginning of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_teststring' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the beginning and end of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_teststrin_' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_tes_str_n_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the ending of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '2teststrin_' ALLOW FILTERING", expected=all_data[2])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '3tes__tring' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1t_stst___g' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_TESTSTRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '__EstSTRin_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3_strinG' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_3teststring' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '3teststring_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE 'test_string3' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_' and ck = '1teststring' ALLOW FILTERING")

    def test_cl_filtering_of_ascii_type_with_underscore_sign(self):
        """Test filtering with LIKE operator by column.

        Filter with LIKE by column where clustering type is ascii.
        Test and the beginning, end, middle of the pattern.

        Used next data set:
        [
            ["teststring0", "0teststring", "test0string"],
            ["teststring1", "1teststring", "test1string"],
            ["teststring2", "2teststring", "test2string"],
            ["teststring3", "3teststring", "test3string"],
            ["teststring4", "4teststring", "test4string"]
        ]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="ascii")
        all_data = self.populate_simple_table_with_basic_data(session)

        # _ at the middle of pattern.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE 'test_string' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % in several places.
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '_est_strin_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '___________' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # % at the ending of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'test0strin_' ALLOW FILTERING", expected=all_data[0])

        # % at the middle of pattern.
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 'te__3_tring' ALLOW FILTERING", expected=all_data[3])
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE 't_st1st___g' ALLOW FILTERING", expected=all_data[1])

        # Missing results as case sensitive.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'TEST_STRING' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '_Est1STRin_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'Test_strinG' ALLOW FILTERING")

        # Assert that _ match exactly one char.
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '_test1string' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'test_string_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE 'test_1string' ALLOW FILTERING")

    def test_invalid_queries_with_like_operator(self):
        """Test invalid queries with LIKE operator.

        Validate that wrong queries with LIKE operator are invalid and not executed.
        """
        session = self.prepare_simple_table_with_column_type()
        self.populate_simple_table_with_basic_data(session)

        assert_invalid(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring%'")
        assert_invalid(session=session, query="SELECT * FROM test WHERE ck LIKE uuid() ALLOW FILTERING")
        assert_invalid(session=session, query="SELECT * FROM test WHERE ck LIKE currentDate() ALLOW FILTERING")
        assert_invalid(session=session, query="SELECT * FROM test WHERE ck LIKE currentTime() ALLOW FILTERING")

    def test_multiple_like_operator_on_same_column(self):
        """Test query with like operator on same column.

        Validate that query return correct result if filter with like operator by same column:
            - by primary key
            - by cluster key
            - by column
        """
        session = self.prepare_simple_table_with_column_type()
        all_data = self.populate_simple_table_with_basic_data(session)

        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '%4' and pk LIKE '%' ALLOW FILTERING", expected=all_data[4])
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%4%' and ck LIKE '1teststring' ALLOW FILTERING")
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '%4%' and test LIKE '%' ALLOW FILTERING", expected=all_data[4])

    def test_unsupported_data_types(self):
        """Test unsupported types are not filtered by LIKE operator.

        Validate that LIKE operator is not supported by other types than text, varchar, ascii.
        """
        session = self.prepare_table_with_unsupported_types()
        self.populate_unsupported_types_table(session)
        unsupported_types_column_names = self.generate_unsupported_columns_name()

        for column in unsupported_types_column_names:
            assert_invalid(session=session, query=f"SELECT {column} FROM test WHERE {column} LIKE '%'", matching="LIKE is allowed only on string type")
            assert_invalid(session=session, query=f"SELECT {column} FROM test WHERE {column} LIKE '_'", matching="LIKE is allowed only on string type")

    def test_unsupported_data_types_and_supported_data_types(self):
        """Test filtering by supported with LIKE operator and other types.

        Validate that it is possible to filter the data with LIKE operator
        and unsupported types by LIKE operator in same queries.
        """
        session = self.prepare_tables_supported_and_unsupported_types()
        self.populate_unsupported_supported_tables_with_2_rows(session)

        for data_type in self.unsupported_column_types_and_values.keys():
            assert_invalid(session=session, query=f"SELECT * FROM t_{data_type} WHERE cl_{data_type} LIKE '%' ALLOW FILTERING", matching="LIKE is allowed only on string types")
            assert_invalid(session=session, query=f"SELECT * FROM t_{data_type} WHERE cl_{data_type} LIKE '_' ALLOW FILTERING", matching="LIKE is allowed only on string types")
            assert_invalid(session=session, query=f"SELECT * FROM t_{data_type} WHERE cl_{data_type} LIKE '_' AND cl_text LIKE '%1%' ALLOW FILTERING", matching="LIKE is allowed only on string types")
            assert_one(session=session, query=f"SELECT pk FROM t_{data_type} WHERE cl_text LIKE '%1%' ALLOW FILTERING", expected=["text_1"])
            assert_all(session=session, query=f"SELECT cl_text FROM t_{data_type} WHERE pk LIKE 'text%' ALLOW FILTERING", expected=[["text%0"], ["text%1"]], ignore_order=True)

    def test_filtering_with_like_operator_by_static_field(self):
        """Test filtering with LIKE operator by static column.

        Validate that it is possible to filter rows by static fields.
        """
        session = self.prepare_table_with_static_field()
        all_data = self.populate_table_with_static_field(session)

        assert_all(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE cl_static LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE cl_static LIKE 'static%teststring' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE cl_static LIKE 'static teststring' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE cl_static LIKE 'static_teststring' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE cl_static LIKE '% %' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_one(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE test LIKE 'test0string' ALLOW FILTERING", expected=all_data[0])
        assert_none(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE cl_static LIKE 'static\\_teststring' ALLOW FILTERING")
        assert_none(session=session, query="SELECT pk, ck, test, cl_static FROM test WHERE cl_static LIKE 'static\\%teststring' ALLOW FILTERING")

    def test_filtering_query_limiting(self):
        """Validate limitation of result.

        Validate that rows filtered with LIKE operator, correctly returned by LIMIT, PER PARTITION LIMIT.

        Next data set is used:
        [
            ['teststring0', '0teststring', 'test0string'],
            ['teststring0', '1teststring', 'test1string'],
            ['teststring1', '0teststring', 'test0string'],
            ['teststring1', '1teststring', 'test1string'],
            ['teststring2', '0teststring', 'test0string'],
            ['teststring2', '1teststring', 'test1string'],
            ['teststring3', '0teststring', 'test0string'],
            ['teststring3', '1teststring', 'test1string'],
            ['teststring4', '0teststring', 'test0string'],
            ['teststring4', '1teststring', 'test1string'],
        ]
        """
        session = self.prepare_simple_table_with_column_type()
        all_data = self.populate_simple_table_with_several_partitions(session)

        row = rows_to_list(session.execute("SELECT * FROM test WHERE pk LIKE 'test%' LIMIT 1 ALLOW FILTERING"))
        assert len(row) == 1
        assert row[0] in all_data

        row = rows_to_list(session.execute("SELECT * FROM test WHERE ck LIKE '%string' LIMIT 1 ALLOW FILTERING"))
        assert len(row) == 1
        assert row[0] in all_data

        row = rows_to_list(session.execute("SELECT * FROM test WHERE test LIKE 'test_string' LIMIT 1 ALLOW FILTERING"))
        assert len(row) == 1
        assert row[0] in all_data

        rows = rows_to_list(session.execute("SELECT * FROM test WHERE test LIKE 'test%' PER PARTITION LIMIT 1 ALLOW FILTERING"))
        assert len(rows) == 5
        for row in rows:
            assert row in all_data

        rows = rows_to_list(session.execute("SELECT * FROM test WHERE test LIKE 'test%' PER PARTITION LIMIT 2 ALLOW FILTERING"))
        assert len(rows) == 10
        for row in rows:
            assert row in all_data

    def test_advanced_matching_text_type(self):
        """Validate different LIKE operators combinations.

        Validate that LIKE operators %, _, \\ , '' CaseSensitive correctly filters rows
        Used next data set:
        [
            [u'TEST%STRING3', u'3Test_String', u'_{}%'],
            [u'TEST%STRING4', u'4Test_String', u'_{}%'],
            [u'TEST%STRING0', u'0Test_String', u'_{}%'],
            [u'TEST%STRING2', u'2Test_String', u'_{}%'],
            [u'TEST%STRING5', u'cluster_!', u'!'],
            [u'TEST%STRING5', u'cluster_#', u'#'],
            [u'TEST%STRING5', u'cluster_$', u'$'],
            [u'TEST%STRING5', u'cluster_%', u'%'],
            [u'TEST%STRING5', u'cluster_&', u'&'],
            [u'TEST%STRING5', u'cluster_(', u'('],
            [u'TEST%STRING5', u'cluster_)', u')'],
            [u'TEST%STRING5', u'cluster_*', u'*'],
            [u'TEST%STRING5', u'cluster_,', u','],
            [u'TEST%STRING5', u'cluster_-', u'-'],
            [u'TEST%STRING5', u'cluster_.', u'.'],
            [u'TEST%STRING5', u'cluster_/', u'/'],
            [u'TEST%STRING5', u'cluster_<', u'<'],
            [u'TEST%STRING5', u'cluster_>', u'>'],
            [u'TEST%STRING5', u'cluster_?', u'?'],
            [u'TEST%STRING5', u'cluster_@', u'@'],
            [u'TEST%STRING5', u'cluster_\\', u'\\'],
            [u'TEST%STRING5', u'cluster_^', u'^'],
            [u'TEST%STRING5', u'cluster__', u'_'],
            [u'TEST%STRING1', u'1Test_String', u'_{}%']]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="text")
        all_data = self.populate_simple_table_with_specific_data(session)
        special_data = list(filter(lambda x: "cluster" in x[1], all_data))

        # Test special chars as escaped, with LIKE operators.
        for char in self.special_values:
            # Filter by column.
            assert_one(session=session, query=f"SELECT * FROM test WHERE test LIKE '\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '\\_' ALLOW FILTERING", expected=["TEST%STRING7", "cluster__", "_"])
            assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '\\%' ALLOW FILTERING", expected=["TEST%STRING7", "cluster_%", "%"])
            assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '_' ALLOW FILTERING", expected=special_data, ignore_order=True)
            assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

            # Filter by cluster key.
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])

            # Except %, because in this case, query will return all fields.
            if char not in ["%", "_"]:
                assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            if char in ["%", "_"]:
                assert_all(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_{char}' ALLOW FILTERING", expected=special_data)
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster_\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster%\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])

        # Test queries for CaseSensitivity and combinations on partition key.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%STRING%' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%NG_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'test%' ALLOW FILTERING")
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST\\%STRING0' ALLOW FILTERING", expected=["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST_STRING1' ALLOW FILTERING", expected=["TEST%STRING1", "1Test_String", "_{}%"])

        # Test queries for CaseSensitivity and combinations on clustering key.
        expected_rows = list(filter(lambda x: "String" in x[1], all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%Test%Str%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_Test\\_S%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%_\\_String' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%_\\_s%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_test_string' ALLOW FILTERING")
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '0Test\\_String' ALLOW FILTERING", expected=["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1Test_String' ALLOW FILTERING", expected=["TEST%STRING1", "1Test_String", "_{}%"])

        # Validate escaping LIKE operators.
        expected_rows = list(filter(lambda x: "_{}%" in x[2], all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE test like '_{}%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '\\_{}\\%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '\\_\\{\\}\\%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '%' ALLOW FILTERING", expected=all_data)
        assert_none(session=session, query="SELECT * FROM test WHERE test like '' ALLOW FILTERING")

    def test_advanced_matching_ascii_type(self):
        """Validate different LIKE operators combinations.

        Validate that LIKE operators %, _, \\ , '' CaseSensitive correctly filters rows.
        Used next data set:
        [
            [u'TEST%STRING3', u'3Test_String', u'_{}%'],
            [u'TEST%STRING4', u'4Test_String', u'_{}%'],
            [u'TEST%STRING0', u'0Test_String', u'_{}%'],
            [u'TEST%STRING2', u'2Test_String', u'_{}%'],
            [u'TEST%STRING5', u'cluster_!', u'!'],
            [u'TEST%STRING5', u'cluster_#', u'#'],
            [u'TEST%STRING5', u'cluster_$', u'$'],
            [u'TEST%STRING5', u'cluster_%', u'%'],
            [u'TEST%STRING5', u'cluster_&', u'&'],
            [u'TEST%STRING5', u'cluster_(', u'('],
            [u'TEST%STRING5', u'cluster_)', u')'],
            [u'TEST%STRING5', u'cluster_*', u'*'],
            [u'TEST%STRING5', u'cluster_,', u','],
            [u'TEST%STRING5', u'cluster_-', u'-'],
            [u'TEST%STRING5', u'cluster_.', u'.'],
            [u'TEST%STRING5', u'cluster_/', u'/'],
            [u'TEST%STRING5', u'cluster_<', u'<'],
            [u'TEST%STRING5', u'cluster_>', u'>'],
            [u'TEST%STRING5', u'cluster_?', u'?'],
            [u'TEST%STRING5', u'cluster_@', u'@'],
            [u'TEST%STRING5', u'cluster_\\', u'\\'],
            [u'TEST%STRING5', u'cluster_^', u'^'],
            [u'TEST%STRING5', u'cluster__', u'_'],
            [u'TEST%STRING1', u'1Test_String', u'_{}%']]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="ascii")
        all_data = self.populate_simple_table_with_specific_data(session)
        special_data = list(filter(lambda x: "cluster" in x[1], all_data))

        # Test special chars as escaped, with LIKE operators.
        for char in self.special_values:
            # Filter by column.
            assert_one(session=session, query=f"SELECT * FROM test WHERE test LIKE '\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            assert_one(session, query="SELECT * FROM test WHERE test LIKE '\\_' ALLOW FILTERING", expected=["TEST%STRING7", "cluster__", "_"])
            assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '\\%' ALLOW FILTERING", expected=["TEST%STRING7", "cluster_%", "%"])
            assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '_' ALLOW FILTERING", expected=special_data, ignore_order=True)
            assert_all(session, query="SELECT * FROM test WHERE test LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

            # Filter by cluster key.
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])

            # Except %, because in this case, query will return all fields.
            if char not in ["%", "_"]:
                assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            if char in ["%", "_"]:
                assert_all(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_{char}' ALLOW FILTERING", expected=special_data)
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster_\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster%\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])

        # Test queries for CaseSensitivity and combinations on partition key.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%STRING%' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%NG_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'test%' ALLOW FILTERING")
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST\\%STRING0' ALLOW FILTERING", expected=["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST_STRING1' ALLOW FILTERING", expected=["TEST%STRING1", "1Test_String", "_{}%"])

        # Test queries for CaseSensitivity and combinations on clustering key.
        expected_rows = list(filter(lambda x: "String" in x[1], all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%Test%Str%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_Test\\_S%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%_\\_String' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%_\\_s%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_test_string' ALLOW FILTERING")
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '0Test\\_String' ALLOW FILTERING", expected=["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1Test_String' ALLOW FILTERING", expected=["TEST%STRING1", "1Test_String", "_{}%"])

        # Validate escaping LIKE operators.
        expected_rows = list(filter(lambda x: "_{}%" in x[2], all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE test like '_{}%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '\\_{}\\%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '\\_\\{\\}\\%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '%' ALLOW FILTERING", expected=all_data)
        assert_none(session=session, query="SELECT * FROM test WHERE test like '' ALLOW FILTERING")

    def test_advanced_matching_varchar_type(self):
        """Validate different LIKE operators combinations.

        Validate that LIKE operators %, _, \\ , '' CaseSensitive correctly filters rows.
        Used next data set:
        [
            [u'TEST%STRING3', u'3Test_String', u'_{}%'],
            [u'TEST%STRING4', u'4Test_String', u'_{}%'],
            [u'TEST%STRING0', u'0Test_String', u'_{}%'],
            [u'TEST%STRING2', u'2Test_String', u'_{}%'],
            [u'TEST%STRING5', u'cluster_!', u'!'],
            [u'TEST%STRING5', u'cluster_#', u'#'],
            [u'TEST%STRING5', u'cluster_$', u'$'],
            [u'TEST%STRING5', u'cluster_%', u'%'],
            [u'TEST%STRING5', u'cluster_&', u'&'],
            [u'TEST%STRING5', u'cluster_(', u'('],
            [u'TEST%STRING5', u'cluster_)', u')'],
            [u'TEST%STRING5', u'cluster_*', u'*'],
            [u'TEST%STRING5', u'cluster_,', u','],
            [u'TEST%STRING5', u'cluster_-', u'-'],
            [u'TEST%STRING5', u'cluster_.', u'.'],
            [u'TEST%STRING5', u'cluster_/', u'/'],
            [u'TEST%STRING5', u'cluster_<', u'<'],
            [u'TEST%STRING5', u'cluster_>', u'>'],
            [u'TEST%STRING5', u'cluster_?', u'?'],
            [u'TEST%STRING5', u'cluster_@', u'@'],
            [u'TEST%STRING5', u'cluster_\\', u'\\'],
            [u'TEST%STRING5', u'cluster_^', u'^'],
            [u'TEST%STRING5', u'cluster__', u'_'],
            [u'TEST%STRING1', u'1Test_String', u'_{}%']]
        """
        session = self.prepare_simple_table_with_column_type(cl_type="varchar")
        all_data = self.populate_simple_table_with_specific_data(session)
        special_data = list(filter(lambda x: "cluster" in x[1], all_data))

        # Test special chars as escaped, with LIKE operators.
        for char in self.special_values:
            # Filter by column.
            assert_one(session=session, query=f"SELECT * FROM test WHERE test LIKE '\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '\\_' ALLOW FILTERING", expected=["TEST%STRING7", "cluster__", "_"])
            assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '\\%' ALLOW FILTERING", expected=["TEST%STRING7", "cluster_%", "%"])
            assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '_' ALLOW FILTERING", expected=special_data, ignore_order=True)
            assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%' ALLOW FILTERING", expected=all_data, ignore_order=True)

            # Filter by cluster key.
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])

            # Except %, because in this case, query will return all fields.
            if char not in ["%", "_"]:
                assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            if char in ["%", "_"]:
                assert_all(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster\\_{char}' ALLOW FILTERING", expected=special_data)
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster_\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])
            assert_one(session=session, query=f"SELECT * FROM test WHERE ck LIKE 'cluster%\\{char}' ALLOW FILTERING", expected=["TEST%STRING7", f"cluster_{char}", char])

        # Test queries for CaseSensitivity and combinations on partition key.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%STRING%' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST%NG_' ALLOW FILTERING", expected=all_data, ignore_order=True)
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'test%' ALLOW FILTERING")
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST\\%STRING0' ALLOW FILTERING", expected=["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'TEST_STRING1' ALLOW FILTERING", expected=["TEST%STRING1", "1Test_String", "_{}%"])

        # Test queries for CaseSensitivity and combinations on clustering key.
        expected_rows = list(filter(lambda x: "String" in x[1], all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%Test%Str%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '_Test\\_S%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM test WHERE ck LIKE '%_\\_String' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '%_\\_s%' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE ck LIKE '_test_string' ALLOW FILTERING")
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '0Test\\_String' ALLOW FILTERING", expected=["TEST%STRING0", "0Test_String", "_{}%"])
        assert_one(session=session, query="SELECT * FROM test WHERE ck LIKE '1Test_String' ALLOW FILTERING", expected=["TEST%STRING1", "1Test_String", "_{}%"])

        # Validate escaping LIKE operators.
        expected_rows = list(filter(lambda x: "_{}%" in x[2], all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE test like '_{}%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '\\_{}\\%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '\\_\\{\\}\\%' ALLOW FILTERING", expected=expected_rows)
        assert_all(session=session, query="SELECT * FROM test WHERE test like '%' ALLOW FILTERING", expected=all_data)
        assert_none(session=session, query="SELECT * FROM test WHERE test like '' ALLOW FILTERING")

    def test_filtering_combinations_of_fields_and_wc(self):
        """Validate LIKE operator filtering by several columns.

        Validate that LIKE operator could be used to filter results by several columns.
        Used next data set:
        [
            ['teststring0', '0teststring', 'test0string'],
            ['teststring0', '1teststring', 'test1string'],
            ['teststring1', '0teststring', 'test0string'],
            ['teststring1', '1teststring', 'test1string'],
            ['teststring2', '0teststring', 'test0string'],
            ['teststring2', '1teststring', 'test1string'],
            ['teststring3', '0teststring', 'test0string'],
            ['teststring3', '1teststring', 'test1string'],
            ['teststring4', '0teststring', 'test0string'],
            ['teststring4', '1teststring', 'test1string'],
        ]
        """
        session = self.prepare_simple_table_with_column_type()
        all_data = self.populate_simple_table_with_several_partitions(session)
        expected_rows = list(filter(lambda x: "0" in x[1], all_data))

        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE '_e%t%g_' AND ck LIKE '0%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)

        expected_rows = list(filter(lambda x: "1" in x[2], all_data))
        assert_all(session=session, query="SELECT * FROM test where ck LIKE '_test%' and test LIKE '%1%' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring3' AND ck LIKE '1teststring' AND test LIKE 'test1string' ALLOW FILTERING", expected=["teststring3", "1teststring", "test1string"])
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring3' AND ck LIKE '0teststring' AND test LIKE 'test1string' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE 'teststring_' and test LIKE 'test\\_string' ALLOW FILTERING")

        expected_rows = list(filter(lambda x: "1" in x[1] and "1" in x[2], all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE test LIKE '%1%' AND ck = '1teststring' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM test WHERE pk LIKE '%1' AND test LIKE 'tes_1_tring' ALLOW FILTERING", expected=["teststring1", "1teststring", "test1string"])
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%1' AND test = 'tes_1_tring' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM test WHERE pk in ('teststring1', 'teststring0') AND ck > '0teststring' AND test LIKE 'test0string' ALLOW FILTERING")

        expected_rows = list(filter(lambda x: ("0" in x[0] or "1" in x[0]) and "1" in x[1] and ("1" in x[2] or "0" in x[2]), all_data))
        assert_all(session=session, query="SELECT * FROM test WHERE pk in ('teststring1', 'teststring0') AND ck > '0teststring' AND test LIKE 'test_string' ALLOW FILTERING", expected=expected_rows, ignore_order=True)
        assert_none(session=session, query="SELECT * FROM test WHERE pk LIKE '%4' AND ck LIKE '_teststring' AND test LIKE '%Test%String%' ALLOW FILTERING")
        assert_all(
            session=session,
            query="SELECT * FROM test WHERE pk LIKE '%%' and pk in ('teststring1', 'teststring2') ALLOW FILTERING",
            expected=[
                ["teststring1", "0teststring", "test0string"],
                ["teststring1", "1teststring", "test1string"],
                ["teststring2", "0teststring", "test0string"],
                ["teststring2", "1teststring", "test1string"],
            ],
            ignore_order=True,
        )

    def test_filtering_after_update_values(self):
        session = self.prepare_simple_table_with_column_type()
        self.populate_simple_table_with_specific_data(session)

        session.execute("INSERT INTO test (pk, ck, test) VALUES ('TEST%STRING6', ' ', '')")
        all_data = rows_to_list(session.execute("SELECT * FROM test"))
        assert_one(session=session, query="SELECT * FROM test WHERE test LIKE '' ALLOW FILTERING", expected=["TEST%STRING6", " ", ""])

        session.execute("UPDATE test SET test='!@#$%^&*()?:\"\\' WHERE pk = 'TEST%STRING6' and ck = ' '")

        assert_one(session=session, query="SELECT * FROM test WHERE pk = 'TEST%STRING6' and ck = ' ' ALLOW FILTERING", expected=["TEST%STRING6", " ", '!@#$%^&*()?:"\\'])

        assert_none(session=session, query="SELECT * FROM test WHERE test LIKE '' ALLOW FILTERING")

        expected_all_pk_keys = [[pk[0]] for pk in all_data]
        assert_all(session=session, query="SELECT pk FROM test WHERE ck LIKE '%' and test LIKE '%' ALLOW FILTERING", expected=expected_all_pk_keys, ignore_order=True)

        session.execute("DELETE FROM test WHERE pk in ('TEST%STRING0', 'TEST%STRING1', 'TEST%STRING2')")
        all_data = rows_to_list(session.execute("SELECT * FROM test"))
        expected_all_pk_keys = [[pk[0]] for pk in all_data]
        assert_all(session=session, query="SELECT pk FROM test WHERE pk LIKE 'TEST\\%STRING_' ALLOW FILTERING", expected=expected_all_pk_keys, ignore_order=True)

        for i in range(3):
            session.execute(f"INSERT INTO test (pk, ck, test) VALUES ('qwErty{i}', 'ytrewq{i}', '')")
        all_data = rows_to_list(session.execute("SELECT * FROM test"))
        expected_all_pk_keys = [[pk[0]] for pk in all_data]

        assert_all(session=session, query="SELECT pk FROM test WHERE pk LIKE '%E%_' ALLOW FILTERING", expected=expected_all_pk_keys, ignore_order=True)

    def test_filtering_with_long_values(self):
        session = self.prepare_simple_table_with_column_type()
        all_data = self.populate_simple_table_with_basic_data(session)

        # Assert all data without paging.
        assert_all(session=session, query="SELECT * FROM test WHERE pk LIKE 't%ts%g%' ALLOW FILTERING", expected=all_data, ignore_order=True)

        # Update all data with large values size of value is about 1M.
        for i in range(5):
            if i % 2 == 0:
                value = "a" * 65520 + "findme"
                # value = "a" * (1024 * 1024) + "findme"
            else:
                value = "findme" + "a" * 65520
                # value = "findme" + "a" * (1024 * 1024)
            session.execute(f"INSERT INTO test (pk, ck, test) VALUES ('teststring{i}', '{i}teststring', '{value}')")

        # Collect data with LIKE operator using paging.
        future = session.execute_async(SimpleStatement("SELECT pk FROM test WHERE test LIKE '%me' ALLOW FILTERING"))
        pf = PageFetcher(future).request_all()

        # Make sure expected and actual have same data elements (ignoring order.)
        actual_rows = rows_to_list(pf.all_data())
        for expected in [["teststring0"], ["teststring2"], ["teststring4"]]:
            assert expected in actual_rows
        future = session.execute_async(SimpleStatement("SELECT pk FROM test WHERE test LIKE '_indme%' ALLOW FILTERING"))
        pf = PageFetcher(future).request_all()

        # Make sure expected and actual have same data elements (ignoring order.)
        actual_rows = rows_to_list(pf.all_data())
        for expected in [["teststring1"], ["teststring3"]]:
            assert expected in actual_rows


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestLikeOperatorForMV(Tester, BaseOperationsHelper):
    def test_filtering_mv_new_primary(self):
        session = self.prepare_cluster_with_materialized_views()
        expected_result = [[f"ytrewq{i}", f"qwerty{i}"] for i in range(5)]

        assert_all(session=session, query="SELECT * FROM building_by_city WHERE city LIKE 'y%' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM building_by_city WHERE city LIKE '%q%' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM building_by_city WHERE city LIKE '%' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM building_by_city WHERE city LIKE '%2' ALLOW FILTERING", expected=["ytrewq2", "qwerty2"])
        assert_one(session, query="SELECT * FROM building_by_city WHERE city LIKE 'ytrewq3' ALLOW FILTERING", expected=["ytrewq3", "qwerty3"])
        assert_all(session=session, query="SELECT * FROM building_by_city WHERE city LIKE 'ytrewq_' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM building_by_city WHERE city LIKE 'ytr_wq4' ALLOW FILTERING", expected=["ytrewq4", "qwerty4"])
        assert_none(session=session, query="SELECT * FROM building_by_city WHERE city LIKE '' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM building_by_city WHERE city LIKE 'Y%q_' ALLOW FILTERING")

    def test_filtering_mv_new_column(self):
        session = self.prepare_cluster_with_materialized_views()
        expected_result = [[f"ytrewq{i}", f"qwerty{i}"] for i in range(5)]

        assert_all(session=session, query="SELECT * FROM building_by_city WHERE name LIKE 'q%' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM building_by_city WHERE name LIKE '%w%' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM building_by_city WHERE name LIKE '%' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM building_by_city WHERE name LIKE '%2' ALLOW FILTERING", expected=["ytrewq2", "qwerty2"])
        assert_one(session=session, query="SELECT * FROM building_by_city WHERE name LIKE 'qwerty3' ALLOW FILTERING", expected=["ytrewq3", "qwerty3"])
        assert_all(session=session, query="SELECT * FROM building_by_city WHERE name LIKE 'qwerty_' ALLOW FILTERING", expected=expected_result, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM building_by_city WHERE name LIKE 'qw_rty4' ALLOW FILTERING", expected=["ytrewq4", "qwerty4"])
        assert_none(session=session, query="SELECT * FROM building_by_city WHERE name LIKE '' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM building_by_city WHERE name LIKE 'q%T_' ALLOW FILTERING")


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestIndexFilteringWithLike(Tester, BaseOperationsHelper):
    def test_filter_index(self):
        session = self.prepare_cluster_with_global_index()
        expected_all = [[f"qwerty{i}", f"ytrewq{i}"] for i in range(5)]

        assert_one(session=session, query="SELECT * FROM buildings WHERE city LIKE '%2' ALLOW FILTERING", expected=["qwerty2", "ytrewq2"])
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE '%' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE 'y%' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE '%r%' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE '_trewq_' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM buildings WHERE city LIKE '_trewq2' ALLOW FILTERING", expected=["qwerty2", "ytrewq2"])
        assert_one(session=session, query="SELECT * FROM buildings WHERE city LIKE 'ytrewq0' ALLOW FILTERING", expected=["qwerty0", "ytrewq0"])
        assert_none(session=session, query="SELECT * FROM buildings WHERE city LIKE 'Ytrewq_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM buildings WHERE city LIKE '_T%_' ALLOW FILTERING")

    def test_filter_local_index(self):
        session = self.prepare_cluster_with_local_index()
        expected_all = [[f"qwerty{i}", f"ytrewq{i}"] for i in range(5)]

        assert_one(session=session, query="SELECT * FROM buildings WHERE city LIKE '%2' ALLOW FILTERING", expected=["qwerty2", "ytrewq2"])
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE '%' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE 'y%' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE '%r%' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_all(session=session, query="SELECT * FROM buildings WHERE city LIKE '_trewq_' ALLOW FILTERING", expected=expected_all, ignore_order=True)
        assert_one(session=session, query="SELECT * FROM buildings WHERE city LIKE '_trewq2' ALLOW FILTERING", expected=["qwerty2", "ytrewq2"])
        assert_one(session=session, query="SELECT * FROM buildings WHERE city LIKE 'ytrewq0' ALLOW FILTERING", expected=["qwerty0", "ytrewq0"])
        assert_none(session=session, query="SELECT * FROM buildings WHERE city LIKE 'Ytrewq_' ALLOW FILTERING")
        assert_none(session=session, query="SELECT * FROM buildings WHERE city LIKE '_T%_' ALLOW FILTERING")
