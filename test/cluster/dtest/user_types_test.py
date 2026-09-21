import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from textwrap import dedent

import pytest
from cassandra import ConsistencyLevel, Unauthorized
from cassandra.query import SimpleStatement
from packaging.version import Version

from dtest_class import Tester, create_ks
from tools.assertions import assert_all, assert_invalid, assert_row_count
from tools.cluster_topology import generate_cluster_topology
from tools.tables_view_manager import wait_for_view

logger = logging.getLogger(__file__)


def listify(item):
    """
    listify a query result consisting of user types

    returns nested arrays representing user type ordering
    """
    decoded = []

    if isinstance(item, tuple | list):
        if len(item) == 1:
            item = item[0]
        nested = []
        for i in item:
            nested.extend(listify(i))
        decoded.append(nested)
    else:
        decoded.append(item)

    return decoded


def assert_udt_type(session, ks: str, udt: str, expected: bool):
    found_udt = False

    if keyspace := session.cluster.metadata.keyspaces.get(ks, None):
        found_udt = udt in keyspace.user_types

    assert found_udt == expected


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestUserTypes(Tester):
    cluster_topology_1_node = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=1)
    cluster_topology_3_nodes = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)

    def assert_unauthorized(self, session, query, message):
        with pytest.raises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, str(cm.value)), "Expected: %s" % message

    def test_type_dropping(self):
        """
        Tests that a type cannot be dropped when in use, and otherwise can be dropped.
        """
        self.ignore_log_patterns += [
            r"Cannot drop user type .* as it is still used by .*",
        ]
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1)
        create_ks(session, "user_type_dropping", 3)

        stmt = """
              USE user_type_dropping
           """
        session.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int
              )
           """
        session.execute(stmt)
        assert_udt_type(session, "user_type_dropping", "simple_type", expected=True)

        stmt = """
              CREATE TABLE simple_table (
              id uuid PRIMARY KEY,
              number frozen<simple_type>
              )
           """
        session.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        _id = uuid.uuid4()
        stmt = f"""
              INSERT INTO simple_table (id, number)
              VALUES ({_id}, {{user_number: 1}});
           """
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        assert_invalid(session, stmt, "Cannot drop user type user_type_dropping.simple_type as it is still used by table user_type_dropping.simple_table")

        # now that we've confirmed that a user type cannot be dropped while in use
        # let's remove the offending table

        # TODO: uncomment below after CASSANDRA-6472 is resolved
        # and add another check to make sure the table/type drops succeed
        stmt = """
              DROP TABLE simple_table;
           """
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        session.execute(stmt)
        assert_udt_type(session, "user_type_dropping", "simple_type", expected=False)

    def test_nested_type_dropping(self):
        """
        Confirm a user type can't be dropped when being used by another user type.
        """
        self.ignore_log_patterns += [
            r"Cannot drop user type .* as it is still used by .*",
        ]
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1)
        create_ks(session, "nested_user_type_dropping", 3)

        stmt = """
              USE nested_user_type_dropping
           """
        session.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int,
              user_text text
              )
           """
        session.execute(stmt)
        assert_udt_type(session, "nested_user_type_dropping", "simple_type", expected=True)

        stmt = """
              CREATE TYPE another_type (
              somefield frozen<simple_type>
              )
           """
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        assert_invalid(session, stmt, "Cannot drop user type nested_user_type_dropping.simple_type as it is still used by user type another_type")

        # drop the type that's impeding the drop, and then try again
        stmt = """
              DROP TYPE another_type;
           """
        session.execute(stmt)

        stmt = """
              DROP TYPE simple_type;
           """
        session.execute(stmt)
        assert_udt_type(session, "nested_user_type_dropping", "simple_type", expected=False)

    def test_type_enforcement(self):
        """
        Confirm error when incorrect data type used for user type
        """
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        create_ks(session, "user_type_enforcement", 3)

        stmt = """
              USE user_type_enforcement
           """
        session.execute(stmt)

        stmt = """
              CREATE TYPE simple_type (
              user_number int
              )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE simple_table (
              id uuid PRIMARY KEY,
              number frozen<simple_type>
              )
           """
        session.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        # here we will attempt an insert statement which should fail
        # because the user type is an int, but the insert statement is
        # providing text
        _id = uuid.uuid4()
        stmt = f"""
              INSERT INTO simple_table (id, number)
              VALUES ({_id}, {{user_number: 'uh oh....this is not a number'}});
           """
        assert_invalid(session, stmt, "field user_number is not of type int")

        # let's check the rowcount and make sure the data
        # didn't get inserted when the exception asserted above was thrown
        stmt = """
              SELECT * FROM simple_table;
           """
        rows = list(session.execute(stmt))
        assert 0 == len(rows)

    @pytest.mark.dtest_debug
    def test_nested_user_types(self):
        """Tests user types within user types"""
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, node2, _node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_types", 3)

        stmt = """
              USE user_types
           """
        session.execute(stmt)

        # Create a user type to go inside another one:
        stmt = """
              CREATE TYPE item (
              sub_one text,
              sub_two text,
              )
           """
        session.execute(stmt)

        # Create a user type to contain the item:
        stmt = """
              CREATE TYPE container (
              stuff text,
              more_stuff frozen<item>
              )
           """
        session.execute(stmt)

        #  Create a table that holds an item, a container, and a
        #  list of containers:
        stmt = """
              CREATE TABLE bucket (
               id uuid PRIMARY KEY,
               primary_item frozen<item>,
               other_items frozen<container>,
               other_containers list<frozen<container>>
              )
           """
        session.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        #  Insert some data:
        _id = uuid.uuid4()
        stmt = f"""
              INSERT INTO bucket (id, primary_item)
              VALUES ({_id}, {{sub_one: 'test', sub_two: 'test2'}});
           """
        session.execute(stmt)

        stmt = f"""
              UPDATE bucket
              SET other_items = {{stuff: 'stuff', more_stuff: {{sub_one: 'one', sub_two: 'two'}}}}
              WHERE id={_id};
           """
        session.execute(stmt)

        # Use an exclusive session to serialize list updates for a deterministic insertion order
        # See https://github.com/scylladb/scylla/issues/4433
        cs2 = self.patient_cql_cluster_session(node2, keyspace="user_types", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        session2 = cs2.session
        stmt = f"""
              UPDATE bucket
              SET other_containers = other_containers + [{{stuff: 'stuff2', more_stuff: {{sub_one: 'one_other', sub_two: 'two_other'}}}}]
              WHERE id={_id};
           """
        session2.execute(stmt)

        stmt = f"""
              UPDATE bucket
              SET other_containers = other_containers + [{{stuff: 'stuff3', more_stuff: {{sub_one: 'one_2_other', sub_two: 'two_2_other'}}}}, {{stuff: 'stuff4', more_stuff: {{sub_one: 'one_3_other', sub_two: 'two_3_other'}}}}]
              WHERE id={_id};
           """
        session2.execute(stmt)

        stmt = f"""
              SELECT primary_item, other_items, other_containers from bucket where id={_id};
           """
        rows = list(session.execute(stmt))

        primary_item, other_items, other_containers = rows[0]
        assert listify(primary_item) == [["test", "test2"]]
        assert listify(other_items) == [["stuff", ["one", "two"]]]
        assert listify(other_containers) == [[["stuff2", ["one_other", "two_other"]], ["stuff3", ["one_2_other", "two_2_other"]], ["stuff4", ["one_3_other", "two_3_other"]]]]

        #  Generate some repetitive data and check it for it's contents:
        for x in range(50):
            # Create row:
            _id = uuid.uuid4()
            stmt = f"""
              UPDATE bucket
              SET other_containers = other_containers + [{{stuff: 'stuff3', more_stuff: {{sub_one: 'one_2_other', sub_two: 'two_2_other'}}}}, {{stuff: 'stuff4', more_stuff: {{sub_one: 'one_3_other', sub_two: 'two_3_other'}}}}]
              WHERE id={_id};
           """
            session.execute(stmt)

            time.sleep(0.1)

            # Check it:
            stmt = f"""
              SELECT other_containers from bucket WHERE id={_id}
            """
            rows = list(session.execute(stmt))

            items = rows[0][0]
            assert listify(items) == [[["stuff3", ["one_2_other", "two_2_other"]], ["stuff4", ["one_3_other", "two_3_other"]]]]

    def test_type_as_part_of_pkey(self):
        """Tests user types as part of a composite pkey"""
        # make sure we can define a table with a user type as part of the pkey
        # and do a basic insert/query of data in that table.
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_type_pkeys", 3)

        stmt = """
              CREATE TYPE t_person_name (
              first text,
              middle text,
              last text
            )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE person_likes (
              id uuid,
              name frozen<t_person_name>,
              like text,
              PRIMARY KEY ((id, name))
              )
           """
        session.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        _id = uuid.uuid4()

        stmt = f"""
              INSERT INTO person_likes (id, name, like)
              VALUES ({_id}, {{first:'Nero', middle:'Claudius Caesar Augustus', last:'Germanicus'}}, 'arson');
           """
        session.execute(stmt)

        # attempt to query without the user type portion of the pkey and confirm there is an error
        stmt = f"""
              SELECT id, name.first from person_likes where id={_id};
           """

        assert_invalid(session, stmt, "use ALLOW FILTERING")

        stmt = f"""
              SELECT id, name.first, like from person_likes where id={_id} and name = {{first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'}};
           """
        rows = session.execute(stmt)

        _row_uuid, first_name, like = rows[0]
        assert first_name == "Nero"
        assert like == "arson"

    @pytest.mark.skip("Secondary indexes not implemented yet")
    def test_type_secondary_indexing(self):  # noqa: PLR0915
        """
        Confirm that user types are secondary-indexable
        Similar procedure to TestSecondaryIndexesOnCollections.test_list_indexes
        """
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_type_indexing", 3)

        stmt = """
              CREATE TYPE t_person_name (
              first text,
              middle text,
              last text
            )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE person_likes (
              id uuid PRIMARY KEY,
              name frozen<t_person_name>,
              like text
              )
           """
        session.execute(stmt)
        # Make sure the scheam propagate
        time.sleep(2)

        # no index present yet, make sure there's an error trying to query column
        stmt = """
              SELECT * from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """

        if Version(self.cluster.version()) < Version("3.0"):
            assert_invalid(session, stmt, "No secondary indexes on the restricted columns support the provided operators")
        else:
            assert_invalid(session, stmt, "No supported secondary index found for the non primary key columns restrictions")

        # add index and query again (even though there are no rows in the table yet)
        stmt = """
              CREATE INDEX person_likes_name on person_likes (name);
            """
        session.execute(stmt)

        stmt = """
              SELECT * from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """
        rows = list(session.execute(stmt))
        assert 0 == len(rows)

        # add a row which doesn't specify data for the indexed column, and query again
        _id = uuid.uuid4()
        stmt = f"""
              INSERT INTO person_likes (id, like)
              VALUES ({_id}, 'long walks on the beach');
           """
        session.execute(stmt)

        stmt = """
              SELECT * from person_likes where name = {first:'Bob', middle: 'Testy', last: 'McTesterson'};
            """

        rows = list(session.execute(stmt))
        assert 0 == len(rows)

        # finally let's add a queryable row, and get it back using the index
        _id = uuid.uuid4()

        stmt = f"""
              INSERT INTO person_likes (id, name, like)
              VALUES ({_id}, {{first:'Nero', middle:'Claudius Caesar Augustus', last:'Germanicus'}}, 'arson');
           """
        session.execute(stmt)

        stmt = """
              SELECT id, name.first, like from person_likes where name = {first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
           """

        rows = list(session.execute(stmt))

        row_uuid, first_name, like = rows[0]

        assert str(row_uuid) == str(_id)
        assert first_name == "Nero"
        assert like == "arson"

        # rename a field in the type and make sure the index still works
        stmt = """
            ALTER TYPE t_person_name rename first to first_name;
            """
        session.execute(stmt)

        stmt = """
            SELECT id, name.first_name, like from person_likes where name = {first_name:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'};
            """

        rows = list(session.execute(stmt))

        row_uuid, first_name, like = rows[0]

        assert str(row_uuid) == str(_id)
        assert first_name == "Nero"
        assert like == "arson"

        # add another row to be sure the index is still adding new data
        _id = uuid.uuid4()

        stmt = f"""
              INSERT INTO person_likes (id, name, like)
              VALUES ({_id}, {{first_name:'Abraham', middle:'', last:'Lincoln'}}, 'preserving unions');
           """
        session.execute(stmt)

        stmt = """
            SELECT id, name.first_name, like from person_likes where name = {first_name:'Abraham', middle:'', last:'Lincoln'};
            """

        rows = list(session.execute(stmt))

        row_uuid, first_name, like = rows[0]

        assert str(row_uuid) == str(_id)
        assert first_name == "Abraham"
        assert like == "preserving unions"

    def test_type_keyspace_permission_isolation(self):
        """
        Confirm permissions are respected for types in different keyspaces
        """
        self.ignore_log_patterns += [
            # I think this happens when permissions change and a node becomes temporarily unavailable
            # and it's probably ok to ignore on this test, as I can see the schema changes propogating
            # almost immediately after
            r"Can\'t send migration request: node.*is down",
        ]

        cluster = self.cluster
        config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": 0}
        cluster.set_configuration_options(values=config)
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        # need a bit of time for user to be created and propagate
        time.sleep(5)

        # do setup that requires a super user
        superuser_session = self.fixture_dtest_setup.patient_cql_connection(node1, user="cassandra", password="cassandra")
        superuser_session.execute("create user ks1_user with password 'cassandra' nosuperuser;")
        superuser_session.execute("create user ks2_user with password 'cassandra' nosuperuser;")
        create_ks(superuser_session, "ks1", 3)
        create_ks(superuser_session, "ks2", 3)
        superuser_session.execute("grant all permissions on keyspace ks1 to ks1_user;")
        superuser_session.execute("grant all permissions on keyspace ks2 to ks2_user;")

        user1_session = self.fixture_dtest_setup.patient_cql_connection(node1, user="ks1_user", password="cassandra")
        user2_session = self.fixture_dtest_setup.patient_cql_connection(node1, user="ks2_user", password="cassandra")

        # first make sure the users can't create types in each other's ks
        self.assert_unauthorized(user1_session, "CREATE TYPE ks2.simple_type (user_number int, user_text text );", "User ks1_user has no CREATE permission on <keyspace ks2> or any of its parents")

        self.assert_unauthorized(user2_session, "CREATE TYPE ks1.simple_type (user_number int, user_text text );", "User ks2_user has no CREATE permission on <keyspace ks1> or any of its parents")

        # now, actually create the types in the correct keyspaces
        user1_session.execute("CREATE TYPE ks1.simple_type (user_number int, user_text text );")
        user2_session.execute("CREATE TYPE ks2.simple_type (user_number int, user_text text );")

        # each user now has a type belonging to their granted keyspace
        # let's make sure they can't drop each other's types (for which they have no permissions)

        self.assert_unauthorized(user1_session, "DROP TYPE ks2.simple_type;", "User ks1_user has no DROP permission on <keyspace ks2> or any of its parents")

        self.assert_unauthorized(user2_session, "DROP TYPE ks1.simple_type;", "User ks2_user has no DROP permission on <keyspace ks1> or any of its parents")

        # let's make sure they can't rename each other's types (for which they have no permissions)
        self.assert_unauthorized(user1_session, "ALTER TYPE ks2.simple_type RENAME user_number TO user_num;", "User ks1_user has no ALTER permission on <keyspace ks2> or any of its parents")

        self.assert_unauthorized(user2_session, "ALTER TYPE ks1.simple_type RENAME user_number TO user_num;", "User ks2_user has no ALTER permission on <keyspace ks1> or any of its parents")

        # rename the types using the correct user w/permissions to do so
        user1_session.execute("ALTER TYPE ks1.simple_type RENAME user_number TO user_num;")
        user2_session.execute("ALTER TYPE ks2.simple_type RENAME user_number TO user_num;")

        # finally, drop the types using the correct user w/permissions to do so, consistency all avoids using a sleep
        user1_session.execute(SimpleStatement("DROP TYPE ks1.simple_type;", consistency_level=ConsistencyLevel.ALL))
        user2_session.execute(SimpleStatement("DROP TYPE ks2.simple_type;", consistency_level=ConsistencyLevel.ALL))

        time.sleep(5)
        assert_udt_type(superuser_session, "ks1", "simple_type", expected=False)
        assert_udt_type(superuser_session, "ks2", "simple_type", expected=False)

    def test_nulls_in_user_types(self):
        """Tests user types with null values"""
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_types", 3)

        stmt = """
              USE user_types
           """
        session.execute(stmt)

        # Create a user type to go inside another one:
        stmt = """
              CREATE TYPE item (
              sub_one text,
              sub_two text,
              )
           """
        session.execute(stmt)

        # Create a table that holds an item
        stmt = """
              CREATE TABLE bucket (
               id int PRIMARY KEY,
               my_item frozen<item>,
              )
           """
        session.execute(stmt)
        # Make sure the schema propagates
        time.sleep(2)

        # Adds an explicit null
        session.execute("INSERT INTO bucket (id, my_item) VALUES (0, {sub_one: 'test', sub_two: null})")
        # Adds with an implicit null
        session.execute("INSERT INTO bucket (id, my_item) VALUES (1, {sub_one: 'test'})")

        rows = list(session.execute("SELECT my_item FROM bucket WHERE id=0"))
        assert listify(rows[0]) == [["test", None]]

        rows = list(session.execute("SELECT my_item FROM bucket WHERE id=1"))
        assert listify(rows[0]) == [["test", None]]

    @pytest.mark.single_node
    def test_no_counters_in_user_types(self):
        # CASSANDRA-7672
        cluster = self.cluster

        cluster.populate(self.cluster_topology_1_node).start()
        [node1] = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1)
        create_ks(session, "user_types", 1)

        stmt = """
            USE user_types
         """
        session.execute(stmt)

        stmt = """
            CREATE TYPE t_item (
            sub_one COUNTER )
         """

        assert_invalid(session, stmt, "A user type cannot contain counters")

    def test_type_as_clustering_col(self):
        """Tests user types as clustering column"""
        # make sure we can define a table with a user type as a clustering column
        # and do a basic insert/query of data in that table.
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_type_pkeys", 3)

        stmt = """
              CREATE TYPE t_letterpair (
              first text,
              second text
            )
           """
        session.execute(stmt)

        stmt = """
              CREATE TABLE letters (
              id int,
              letterpair frozen<t_letterpair>,
              PRIMARY KEY (id, letterpair)
              )
           """
        session.execute(stmt)

        # create a bit of data and expect a natural order based on clustering user types

        ids = range(1, 10)

        for _id in ids:
            session.execute(f"INSERT INTO letters (id, letterpair) VALUES ({_id}, {{first:'a', second:'z'}})")
            session.execute(f"INSERT INTO letters (id, letterpair) VALUES ({_id}, {{first:'z', second:'a'}})")
            session.execute(f"INSERT INTO letters (id, letterpair) VALUES ({_id}, {{first:'c', second:'f'}})")
            session.execute(f"INSERT INTO letters (id, letterpair) VALUES ({_id}, {{first:'c', second:'a'}})")
            session.execute(f"INSERT INTO letters (id, letterpair) VALUES ({_id}, {{first:'c', second:'z'}})")
            session.execute(f"INSERT INTO letters (id, letterpair) VALUES ({_id}, {{first:'d', second:'e'}})")

        for _id in ids:
            res = list(session.execute(f"SELECT letterpair FROM letters where id = {_id}"))

            assert listify(res) == [[["a", "z"], ["c", "a"], ["c", "f"], ["c", "z"], ["d", "e"], ["z", "a"]]]

    def udt_subfield_test(self):
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_types", 1)

        # Check we can create non-frozen table
        session.execute("CREATE TYPE udt (first text, second int, third int)")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v udt)")

        # Fill in a full UDT across two statements
        # Ensure all subfields are set
        session.execute("INSERT INTO t (id, v) VALUES (0, {third: 2, second: 1})")
        session.execute("UPDATE t set v.first = 'a' WHERE id=0")
        rows = list(session.execute("SELECT * FROM t WHERE id = 0"))
        assert listify(rows[0]) == [[0, ["a", 1, 2]]]

        # Create a full udt
        # Update a subfield on the udt
        # Read back the updated udt
        session.execute("INSERT INTO t (id, v) VALUES (0, {first: 'c', second: 3, third: 33})")
        session.execute("UPDATE t set v.second = 5 where id=0")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        assert listify(rows[0]) == [[0, ["c", 5, 33]]]

        # Rewrite the entire udt
        # Read back
        session.execute("INSERT INTO t (id, v) VALUES (0, {first: 'alpha', second: 111, third: 100})")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        assert listify(rows[0]) == [[0, ["alpha", 111, 100]]]

        # Send three subfield updates to udt
        # Read back
        session.execute("UPDATE t set v.first = 'beta' WHERE id=0")
        session.execute("UPDATE t set v.first = 'delta' WHERE id=0")
        session.execute("UPDATE t set v.second = -10 WHERE id=0")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        assert listify(rows[0]) == [[0, ["delta", -10, 100]]]

        # Send conflicting updates serially to different nodes
        # Read back
        session1 = self.exclusive_cql_connection(node1)
        session2 = self.exclusive_cql_connection(node2)
        session3 = self.exclusive_cql_connection(node3)

        session1.execute("UPDATE user_types.t set v.third = 101 WHERE id=0")
        session2.execute("UPDATE user_types.t set v.third = 102 WHERE id=0")
        session2.execute("UPDATE user_types.t set v.third = 103 WHERE id=0")
        query = SimpleStatement("SELECT * FROM t WHERE id = 0", consistency_level=ConsistencyLevel.ALL)
        rows = list(session.execute(query))
        assert listify(rows[0]) == [[0, ["delta", -10, 103]]]
        session1.shutdown()
        session2.shutdown()
        session3.shutdown()

        # Write full UDT, set one field to null, read back
        session.execute("INSERT INTO t (id, v) VALUES (0, {first:'cass', second:3, third:0})")
        session.execute("UPDATE t SET v.first = null WHERE id = 0")
        rows = list(session.execute("SELECT * FROM t WHERE id=0"))
        assert listify(rows[0]) == [[0, [None, 3, 0]]]

        rows = list(session.execute("SELECT v.first FROM t WHERE id=0"))
        assert listify(rows) == [[None]]
        rows = list(session.execute("SELECT v.second FROM t WHERE id=0"))
        assert listify(rows) == [[3]]
        rows = list(session.execute("SELECT v.third FROM t WHERE id=0"))
        assert listify(rows) == [[0]]

    @pytest.mark.single_node
    def test_user_type_isolation(self):
        """
        Ensure UDT cannot be used from another keyspace
        @jira_ticket CASSANDRA-9409
        @since 2.2
        """

        cluster = self.cluster
        cluster.populate(self.cluster_topology_1_node).start()
        node1 = cluster.nodelist()[0]
        session = self.fixture_dtest_setup.patient_cql_connection(node1)
        create_ks(session, "user_types", 1)

        # create a user defined type in a keyspace
        session.execute("CREATE TYPE udt (first text, second int, third int)")

        # ensure we cannot use a udt from another keyspace
        create_ks(session, "user_ks", 1)
        assert_invalid(session, "CREATE TABLE t (id int PRIMARY KEY, v frozen<user_types.udt>)", "Statement on keyspace user_ks cannot refer to a user type in keyspace user_types")

    @pytest.mark.single_node
    def test_keyspace_drop_with_table_containing_udt(self):
        """
        Test for #3068
        """

        cluster = self.cluster
        cluster.populate(self.cluster_topology_1_node).start()
        node1 = cluster.nodelist()[0]
        session = self.fixture_dtest_setup.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute("CREATE TYPE udt (first text, second int)")
        session.execute("CREATE TABLE table1 (id uuid PRIMARY KEY, x frozen<udt>);")
        session.execute("DROP KEYSPACE IF EXISTS ks;")

    @pytest.mark.single_node
    def test_complex_data_types(self):
        """ "
        This test was adapted from json_test.py (test_complex_data_types).
        Test user defined types, with complex format using the cql driver
        """
        cluster = self.cluster
        cluster.populate(self.cluster_topology_1_node).start()
        node1 = cluster.nodelist()[0]
        session = self.fixture_dtest_setup.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_types", 1)

        stmt_lst = [
            "CREATE TYPE t_todo_item (label text, details text)",
            "CREATE TYPE t_todo_list (name text, todo_list list<frozen<t_todo_item>>)",
            dedent(
                """
            CREATE TYPE t_kitchen_sink
            (
            item1 ascii,
            item2 blob,
            item3 inet,
            item4 text,
            item5 timestamp,
            item6 timeuuid,
            item7 uuid,
            item8 varchar,
            item9 bigint,
            item10 decimal,
            item11 double,
            item12 float,
            item13 int,
            item14 varint,
            item15 boolean,
            item16 list<int>
            )
            """
            ),
            dedent(
                """
            CREATE TABLE complex_types
            (
            key1 text PRIMARY KEY,
            mylist list<text>,
            myset set<uuid>,
            mymap map<text, int>,
            mytuple frozen<tuple<text, int, uuid, boolean>>,
            myudt frozen<t_kitchen_sink>,
            mytodolists list<frozen<t_todo_list>>,
            many_sinks list<frozen<t_kitchen_sink>>,
            named_sinks map<text, frozen<t_kitchen_sink>>
            )
            """
            ),
            dedent(
                """
            INSERT INTO complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
            VALUES (
            'row1', ['five', 'six', 'seven', 'eight'],
            {4b66458a-2a19-41d3-af25-6faef4dea9fe,
            080fdd90-ae74-41d6-9883-635625d3b069,
            6cd7fab5-eacc-45c3-8414-6ad0177651d6
            },
            {'one': 1, 'two': 2, 'three': 3, 'four': 4},
            ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
            {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000',
            item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a,
            item8: 'bleh',  item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222,
            item12: 98712312.5252, item13: -2147483648,
            item14: 2147483647,
            item15: false,
            item16: [1,3,5,7,11,13]
            },
            [{name: 'stuff to do!', todo_list:
            [
            {label: 'buy groceries', details: 'bread and milk'},
            {label: 'pick up car from shop', details: '$325 due'},
            {label: 'call dave', details: 'for some reason'}
            ]},
            {name: 'more stuff to do!', todo_list:
            [
            {label: 'buy new car', details: 'the old one is getting expensive'},
            {label: 'price insurance', details: 'current cost is $95/mo'}
            ]}],
            [
            {
            item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000',
            item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003,
            item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222,
            item12: 40012312.5252, item13: -1147483648, item14: 2047483648,item15: true, item16: [1,1,2,3,5,8]
            },
            {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000',
            item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a,
            item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222,
            item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]
            },
            {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000',
            item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b,
            item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222,
            item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]
            }],
            {
            'namedsink1':
            {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1',item5: '2012-02-03 04:05+0000',
            item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003,
            item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222,
            item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},
            'namedsink2':
            {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000',
            item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a,
            item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222,
            item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},
            'namedsink3':
            {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000',
            item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b,
            item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222,
            item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}
            })
            """
            ),
        ]

        # EXECUTE CREATE + INSERT COMMANDS HERE
        for stmt in stmt_lst:
            session.execute(stmt)

        select_lst = [
            "SELECT mylist from complex_types where key1 = 'row1'",
            "SELECT myset from complex_types where key1 = 'row1'",
            "SELECT mymap from complex_types where key1 = 'row1'",
            "SELECT mytuple from complex_types where key1 = 'row1'",
            "SELECT myudt from complex_types where key1 = 'row1'",
            "SELECT mytodolists from complex_types where key1 = 'row1'",
            "SELECT many_sinks from complex_types where key1 = 'row1'",
            "SELECT named_sinks from complex_types where key1 = 'row1'",
        ]

        expected_lst = [
            "[[['five', 'six', 'seven', 'eight']]]",
            "[[SortedSet([UUID('080fdd90-ae74-41d6-9883-635625d3b069'), UUID('4b66458a-2a19-41d3-af25-6faef4dea9fe'), UUID('6cd7fab5-eacc-45c3-8414-6ad0177651d6')])]]",
            "[[OrderedMapSerializedKey([('four', 4), ('one', 1), ('three', 3), ('two', 2)])]]",
            "[[('hey', 10, UUID('16e69fba-a656-4932-8a01-6782a34505d9'), True)]]",
            "[[t_kitchen_sink(item1='heyimascii', item2=b'\\x00\\x11', item3='127.0.0.1', item4='whatev', "
            "item5=datetime.datetime(2011, 2, 3, 4, 5), item6=UUID('0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f'), "
            "item7=UUID('bdf5e8ac-a75e-4321-9ac8-938fc9576c4a'), item8='bleh', item9=-9223372036854775808, "
            "item10=Decimal('1234.45678'), item11=98712312.1222, item12=98712312.0, item13=-2147483648, "
            "item14=2147483647, item15=False, item16=[1, 3, 5, 7, 11, 13])]]",
            "[[[t_todo_list(name='stuff to do!', todo_list=[t_todo_item(label='buy groceries', "
            "details='bread and milk'), t_todo_item(label='pick up car from shop', details='$325 due'), "
            "t_todo_item(label='call dave', details='for some reason')]), t_todo_list(name='more stuff to do!', "
            "todo_list=[t_todo_item(label='buy new car', details='the old one is getting expensive'), "
            "t_todo_item(label='price insurance', details='current cost is $95/mo')])]]]",
            "[[[t_kitchen_sink(item1='asdf', item2=b'\\x00\\x12', item3='127.0.0.2', item4='whatev1', "
            "item5=datetime.datetime(2012, 2, 3, 4, 5), item6=UUID('d05a10c8-7c12-11e4-949d-b4b6763e9d6f'), "
            "item7=UUID('f90b04b1-f9ad-4ffa-b869-a7d894ce6003'), item8='tyru', item9=-9223372036854771111, "
            "item10=Decimal('4321.45678'), item11=10012312.1222, item12=40012312.0, item13=-1147483648, "
            "item14=2047483648, item15=True, item16=[1, 1, 2, 3, 5, 8]), t_kitchen_sink(item1='fdsa', "
            "item2=b'\\x00\\x13', item3='127.0.0.3', item4='whatev2', item5=datetime.datetime(2013, 2, 3, 4, 5), "
            "item6=UUID('d8ac38c8-7c12-11e4-8955-b4b6763e9d6f'), item7=UUID('e3e84f21-f28c-4e0f-80e0-068a640ae53a'), "
            "item8='uytr', item9=-3333372036854775808, item10=Decimal('1234.12321'), item11=20012312.1222, "
            "item12=50012312.0, item13=-1547483648, item14=1947483648, item15=False, item16=[3, 6, 9, 12, 15]), "
            "t_kitchen_sink(item1='zxcv', item2=b'\\x00\\x14', item3='127.0.0.4', item4='whatev3', "
            "item5=datetime.datetime(2014, 2, 3, 4, 5), item6=UUID('de30838a-7c12-11e4-a907-b4b6763e9d6f'), "
            "item7=UUID('f9381f0e-9467-4d4c-9315-eb9f0232487b'), item8='fghj', item9=-2239372036854775808, "
            "item10=Decimal('5555.55555'), item11=30012312.1222, item12=60012312.0, item13=2147483647, "
            "item14=1347483648, item15=True, item16=[0, 1, 0, 1, 2, 0])]]]",
            "[[OrderedMapSerializedKey([('namedsink1', t_kitchen_sink(item1='asdf', item2=b'\\x00\\x12', "
            "item3='127.0.0.2', item4='whatev1', item5=datetime.datetime(2012, 2, 3, 4, 5), "
            "item6=UUID('d05a10c8-7c12-11e4-949d-b4b6763e9d6f'), item7=UUID('f90b04b1-f9ad-4ffa-b869-a7d894ce6003'), "
            "item8='tyru', item9=-9223372036854771111, item10=Decimal('4321.45678'), item11=10012312.1222, "
            "item12=40012312.0, item13=-1147483648, item14=2047483648, item15=True, item16=[1, 1, 2, 3, 5, 8])), "
            "('namedsink2', t_kitchen_sink(item1='fdsa', item2=b'\\x00\\x13', item3='127.0.0.3', item4='whatev2', "
            "item5=datetime.datetime(2013, 2, 3, 4, 5), item6=UUID('d8ac38c8-7c12-11e4-8955-b4b6763e9d6f'), "
            "item7=UUID('e3e84f21-f28c-4e0f-80e0-068a640ae53a'), item8='uytr', item9=-3333372036854775808, "
            "item10=Decimal('1234.12321'), item11=20012312.1222, item12=50012312.0, item13=-1547483648, "
            "item14=1947483648, item15=False, item16=[3, 6, 9, 12, 15])), "
            "('namedsink3', t_kitchen_sink(item1='zxcv', item2=b'\\x00\\x14', item3='127.0.0.4', item4='whatev3', "
            "item5=datetime.datetime(2014, 2, 3, 4, 5), item6=UUID('de30838a-7c12-11e4-a907-b4b6763e9d6f'), "
            "item7=UUID('f9381f0e-9467-4d4c-9315-eb9f0232487b'), item8='fghj', item9=-2239372036854775808, "
            "item10=Decimal('5555.55555'), item11=30012312.1222, item12=60012312.0, item13=2147483647, "
            "item14=1347483648, item15=True, item16=[0, 1, 0, 1, 2, 0]))])]]",
        ]

        # EXECUTE SELECTS AND COMPARE WITH EXPECTED RESULTS
        for i in range(len(select_lst)):
            assert_all(session=session, query=select_lst[i], expected=expected_lst[i], result_as_string=True)

        # EXECUTE UPDATE AND COMPARE WITH EXPECTED RESULT
        session.execute("UPDATE complex_types SET mylist = ['nine', 'ten', 'eleven'] WHERE key1 = 'row1'")
        expected_res = "[[['nine', 'ten', 'eleven']]]"
        after_update_query = "SELECT mylist from complex_types"
        assert_all(session=session, query=after_update_query, expected=expected_res, result_as_string=True)

    def test_add_udt_to_another_data_types(self):
        logging.getLogger("cassandra").setLevel(logging.DEBUG)
        try:
            self.internal_add_udt_to_another_data_types()
        finally:
            logging.getLogger("cassandra").setLevel(logging.INFO)

    def internal_add_udt_to_another_data_types(self):  # noqa: PLR0915
        """ "
        Test user defined types, with complex format.
        Alter user defined type with another  user defined type
        """
        rows_num = 2000
        self.cluster.populate(self.cluster_topology_3_nodes).start()
        session = self.fixture_dtest_setup.patient_cql_connection(self.cluster.nodelist()[0])
        keyspace_name = "abcinfo"
        create_ks(session, keyspace_name, 1)

        logger.info("Create user_refs type")
        session.execute("create type if not exists user_refs (id text, alt_name text, firstname text,lastname text, email text)")

        logger.info("Create obs_entity type")
        session.execute("create type if not exists obs_entity (entity_id text, version int, entity_type text,type text, service text, user_refs frozen<user_refs>)")

        logger.info("Create other_entity type")
        session.execute("create type if not exists other_entity (other_entity_id text, version int, entities frozen<set<text>>,type text)")

        logger.info("Create entity table")
        session.execute(
            "create table if not exists entity(entity_id text, other_entity_id text,"
            "entity_type text, type text, service text, entity_info frozen < obs_entity >,"
            "import_timestamp timestamp, import_timestamp_day timestamp, primary key(entity_id)"
            ") with "
            "compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}"
            "and bloom_filter_fp_chance = 0.01"
        )

        logger.info("Create index on entity(service)")
        session.execute("create index if not exists on entity(service)")

        logger.info("Create index on entity(type)")
        session.execute("create index if not exists on entity(type)")

        logger.info("Create entity_by_type materialized view")
        session.execute("create materialized view if not exists entity_by_type as select * from entity where type is not null primary key(type, entity_id)")
        wait_for_view(cluster=self.cluster, session=session, ks=keyspace_name, view="entity_by_type")

        logger.info("Create entity_by_unique_id materialized view")
        session.execute("create materialized view if not exists entity_by_unique_id as select * from entity where other_entity_id is not null primary key(other_entity_id, entity_id)")
        wait_for_view(cluster=self.cluster, session=session, ks=keyspace_name, view="entity_by_unique_id")

        logger.info("Create entity_rel table")
        session.execute(
            "create table if not exists entity_rel (src_entity_id text, src_entity frozen<obs_entity>, "
            "dest_entity_id text, dest_entity frozen<obs_entity>, service text, rel_type text, "
            "deleted boolean, primary key ((src_entity_id), dest_entity_id, rel_type, service)) "
            "with compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} "
            "and bloom_filter_fp_chance = 0.01"
        )

        logger.info("Create index on entity_rel(dest_entity_id)")
        session.execute("create index if not exists on entity_rel(dest_entity_id)")

        logger.info("Create index on entity_rel(service)")
        session.execute("create index if not exists on entity_rel(service)")

        logger.info("Create index on entity_rel(rel_type)")
        session.execute("create index if not exists on entity_rel(rel_type)")

        def insert_into_entity(rows=rows_num, altered_type=False):
            for i in range(rows):
                stmt = (
                    "insert into abcinfo.entity (entity_id, entity_info, entity_type, import_timestamp, "
                    "import_timestamp_day, other_entity_id, service, type) values "
                    "('text{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', "
                    "('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), "
                    "'entity_type{i}', 1234568979, 45621313131, 'other_entity_id{i}', 'service{i}', 'type{i}')".format(i=i, new_type=", (%d, {%d, %d})" % (i, i, i) if altered_type else "")
                )
                session.execute(stmt)

        def insert_into_entity_rel(rows=rows_num, altered_type=False):
            for i in range(rows):
                stmt = (
                    "insert into abcinfo.entity_rel (src_entity_id, src_entity, dest_entity_id, dest_entity, "
                    "service, rel_type, deleted) values "
                    "('text{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', "
                    "('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), "
                    "'dest_entity_id{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', "
                    "('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), "
                    "'service{i}', 'rel_type{i}', True)".format(i=i, new_type=", (%d, {%d, %d})" % (i, i, i) if altered_type else "")
                )
                session.execute(stmt)

        runned_thread = []
        logger.info("Start insert into entity")
        entity_run_executer = ThreadPoolExecutor()
        runned_thread.append(entity_run_executer.submit(insert_into_entity))

        logger.info("Start insert into entity_rel")
        entity_rel_run_executer = ThreadPoolExecutor()
        runned_thread.append(entity_rel_run_executer.submit(insert_into_entity_rel))

        for t in runned_thread:
            t.result(60)

        assert_row_count(session=session, table_name="entity", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

        assert_row_count(session=session, table_name="entity_rel", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

        logger.info("Create priority_refs type")
        session.execute("create type if not exists priority_refs (priority int, description set<int>)")

        logger.info("Alter obs_entity type")
        session.execute("alter type obs_entity add priority_refs frozen<priority_refs>")

        logger.info("Alter user_refs type")
        session.execute("alter type user_refs add priority_refs frozen<priority_refs>")

        insert_into_entity(rows=10, altered_type=True)
        insert_into_entity_rel(rows=10, altered_type=True)

        assert_row_count(session=session, table_name="entity", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

        assert_row_count(session=session, table_name="entity_rel", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

        assert_row_count(session=session, table_name="entity_by_type", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

        assert_row_count(session=session, table_name="entity_by_unique_id", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

    def test_alter_inner_udt_by_another_udt(self):
        """ "
        - Create 3 user defined type: type 1 references to type 2 and type 3 references to type 1
        - Create new UDT
        - Alter type 1&2 with new UDT.
        - Create table with column of type 3
        - Insert into table
        """
        rows_num = 10
        self.cluster.populate(self.cluster_topology_3_nodes).start()
        session = self.fixture_dtest_setup.patient_cql_connection(self.cluster.nodelist()[0])
        keyspace_name = "abcinfo"
        create_ks(session, keyspace_name, 1)

        logger.info("Create user_refs type")
        session.execute("create type if not exists user_refs (id text, alt_name text)")

        logger.info("Create obs_entity type")
        session.execute("create type if not exists obs_entity (entity_id text, user_refs frozen<user_refs>)")

        logger.info("Create some_entity type")
        session.execute("create type if not exists some_entity (e_struct frozen<obs_entity>)")

        logger.info("Create priority_refs type")
        session.execute("create type if not exists priority_refs (priority int, description set<int>)")

        logger.info("Alter obs_entity type")
        session.execute("alter type obs_entity add priority_refs frozen<priority_refs>")

        logger.info("Alter user_refs type")
        session.execute("alter type user_refs add priority_refs frozen<priority_refs>")

        logger.info("Create entity table")
        session.execute(
            "create table if not exists entity(entity_id text, type text, entity_info frozen < some_entity >,"
            " primary key(entity_id)) with "
            "compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}"
            "and bloom_filter_fp_chance = 0.01"
        )

        logger.info("Create entity_by_type materialized view")
        session.execute("create materialized view if not exists entity_by_type as select * from entity where type is not null primary key(type, entity_id)")
        wait_for_view(cluster=self.cluster, session=session, ks=keyspace_name, view="entity_by_type")

        for i in range(rows_num):
            new_type = "(%d, {%d, %d})" % (i, i, i)
            stmt = f"insert into entity (entity_id, type, entity_info) values ('text{i}', 'type{i}', (('entity_id{i}', ('id{i}', 'alt_name{i}', {new_type}), {new_type})))"
            session.execute(stmt)

        assert_row_count(session=session, table_name="entity", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

        assert_row_count(session=session, table_name="entity_by_type", expected=rows_num, consistency_level=ConsistencyLevel.QUORUM)

    def test_case_sensitive_type_name(self):
        """Test case sensitive type name"""
        cluster = self.cluster
        cluster.populate(self.cluster_topology_3_nodes).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.fixture_dtest_setup.patient_cql_connection(node1, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        create_ks(session, "user_types", 3)
        session.set_keyspace("user_types")

        logger.info("Test with capital type name - PHone")
        session.execute('CREATE TYPE "PHone" (country_code int, number text)')
        session.execute('CREATE TABLE cf (pk int, pn "PHone", PRIMARY KEY (pk))')
        session.execute('CREATE TABLE cf2 (pk int, pn frozen<"PHone">, PRIMARY KEY (pk))')
        session.execute('CREATE TABLE cf3 (pk int, pn frozen<list<"PHone">>, PRIMARY KEY (pk))')

        # Make sure the schema propagates
        time.sleep(2)

        session.execute("INSERT INTO cf (pk, pn) VALUES (0, {country_code: 86, number: '123'})")
        session.execute("INSERT INTO cf2 (pk, pn) VALUES (0, {country_code: 87, number: '456'})")
        session.execute("INSERT INTO cf3 (pk, pn) VALUES (0, [{country_code: 86, number: '123'}, {country_code: 88, number: '789'}])")

        rows = list(session.execute("SELECT pn FROM cf WHERE pk=0"))
        assert listify(rows[0]) == [[86, "123"]]
        rows = list(session.execute("SELECT pn FROM cf2 WHERE pk=0"))
        assert listify(rows[0]) == [[87, "456"]]
        rows = list(session.execute("SELECT pn FROM cf3 WHERE pk=0"))
        assert listify(rows[0]) == [[[86, "123"], [88, "789"]]]

        logger.info("Test with lower case type name - phone")
        session.execute("CREATE TYPE phone (country_code text, number int)")
        session.execute("CREATE TABLE new_cf (pk int, pn phone, PRIMARY KEY (pk))")
        session.execute("CREATE TABLE new_cf2 (pk int, pn frozen<phone>, PRIMARY KEY (pk))")
        session.execute("CREATE TABLE new_cf3 (pk int, pn frozen<list<phone>>, PRIMARY KEY (pk))")

        # Make sure the schema propagates
        time.sleep(2)

        session.execute("INSERT INTO new_cf (pk, pn) VALUES (0, {country_code: '86', number: 123})")
        session.execute("INSERT INTO new_cf2 (pk, pn) VALUES (0, {country_code: '87', number: 456})")
        session.execute("INSERT INTO new_cf3 (pk, pn) VALUES (0, [{country_code: '86', number: 123}, {country_code: '88', number: 789}])")

        rows = list(session.execute("SELECT pn FROM new_cf WHERE pk=0"))
        assert listify(rows[0]) == [["86", 123]]
        rows = list(session.execute("SELECT pn FROM new_cf2 WHERE pk=0"))
        assert listify(rows[0]) == [["87", 456]]
        rows = list(session.execute("SELECT pn FROM new_cf3 WHERE pk=0"))
        assert listify(rows[0]) == [[["86", 123], ["88", 789]]]

        logger.info("Drop captial type name, and check lower case type still exists")
        session.execute("DROP TABLE cf")
        session.execute("DROP TABLE cf2")
        session.execute("DROP TABLE cf3")
        session.execute('DROP TYPE "PHone"')
        rows = list(session.execute("SELECT pn FROM new_cf3 WHERE pk=0"))
        assert listify(rows[0]) == [[["86", 123], ["88", 789]]]
