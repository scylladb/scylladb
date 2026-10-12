#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# User-defined types on a three-node cluster (each node in a rack of its
# own in datacenter1, like the dtests' generate_cluster_topology(dc_num=1,
# rack_num=3)), ported from scylla-dtest user_types_test.py, one test per
# dtest. As in the dtests, keyspaces use vnodes, the sessions are token aware
# with node1 as their contact point, statements default to consistency level
# ONE unless the dtest asks for another one, and create_ks() USEs the
# keyspace it creates.

import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, Unauthorized
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.dtest_helpers import create_ks as dtest_create_ks
from test.cluster.dtest_helpers import topology
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The dtests' cluster_topology_3_nodes: datacenter1 with rack1, rack2, rack3.
THREE_RACKS = topology({"rack1": 1, "rack2": 1, "rack3": 1})
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled"}


def listify(item):
    """A query result of user types as nested lists in field order."""
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


def assert_udt_type(cql, ks, udt, expected):
    """The dtest's assert_udt_type(): whether the driver's schema metadata
    has the type."""
    found_udt = False
    if keyspace := cql.cluster.metadata.keyspaces.get(ks, None):
        found_udt = udt in keyspace.user_types
    assert found_udt == expected


RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


def connect(manager: ScyllaClusterManager, server, consistency_level=ConsistencyLevel.ONE, exclusive=False,
            keyspace=None, user=None, password=None):
    """A session like the dtests' patient_cql_connection(node): token aware
    (the driver's default policy) with the node as contact point, or only
    to the node with exclusive=True, statements defaulting to the given
    consistency level."""
    if exclusive:
        policy = WhiteListRoundRobinPolicy([server.ip_addr])
    else:
        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy())
    auth_provider = PlainTextAuthProvider(username=user, password=password) if user is not None else None
    cql = manager.con_gen([server.ip_addr], auth_provider=auth_provider, load_balancing_policy=policy).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = consistency_level
    if keyspace:
        cql.set_keyspace(keyspace)
    return cql


def create_ks(cql, name, rf):
    """The dtests' create_ks(): creates the keyspace, waits for schema
    agreement and USEs it."""
    dtest_create_ks(cql, name, rf)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"USE {name}")


async def wait_for_view(manager: ScyllaClusterManager, cql, servers, ks, view, timeout=600):
    """The dtests' wait_for_view(): reads system_distributed.view_build_status
    of the view at QUORUM (ONE with a single live node) until every live
    node reports SUCCESS."""
    cl = ConsistencyLevel.ONE if len(servers) == 1 else ConsistencyLevel.QUORUM
    query = SimpleStatement("SELECT host_id,status FROM system_distributed.view_build_status "
                            f"WHERE keyspace_name = '{ks}' AND view_name = '{view}'", consistency_level=cl)
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]
    deadline = time.time() + timeout
    while time.time() < deadline:
        done = {str(row[0]) for row in cql.execute(query) if row[1] == "SUCCESS"}
        if all(str(h) in done for h in host_ids):
            return
        time.sleep(1)
    raise Exception(f"View {ks}.{view} not built")


def assert_row_count(cql, table_name, expected, consistency_level=ConsistencyLevel.ONE):
    """The dtests' assert_row_count()."""
    count = list(cql.execute(SimpleStatement(f"SELECT count(*) FROM {table_name}", consistency_level=consistency_level),
                             timeout=120))[0][0]
    assert count == expected, f"Expected a row count of {expected} in table '{table_name}', but got {count}"


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_type_dropping.
# A type cannot be dropped while a table uses it, and can be dropped once the
# table is gone.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_type_dropping(manager: ScyllaClusterManager):
    manager.ignore_log_patterns.append(r"Cannot drop user type .* as it is still used by .*")
    # As in the dtest, auto_snapshot is off (test.py's default too), so the
    # DROP TABLE does not flush and snapshot first.
    servers = await manager.servers_add(3, config={**CONFIG, "auto_snapshot": False}, property_file=THREE_RACKS)
    session = connect(manager, servers[0])
    create_ks(session, "user_type_dropping", 3)
    session.execute("USE user_type_dropping")
    session.execute("CREATE TYPE simple_type (user_number int)")
    assert_udt_type(session, "user_type_dropping", "simple_type", expected=True)
    session.execute("CREATE TABLE simple_table (id uuid PRIMARY KEY, number frozen<simple_type>)")
    session.execute(f"INSERT INTO simple_table (id, number) VALUES ({uuid.uuid4()}, {{user_number: 1}});")
    with pytest.raises(InvalidRequest, match=re.escape("Cannot drop user type user_type_dropping.simple_type as it is still used by table user_type_dropping.simple_table")):
        session.execute("DROP TYPE simple_type;")
    session.execute("DROP TABLE simple_table;")
    session.execute("DROP TYPE simple_type;")
    assert_udt_type(session, "user_type_dropping", "simple_type", expected=False)


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_nested_type_dropping.
# A type cannot be dropped while another type uses it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nested_type_dropping(manager: ScyllaClusterManager):
    manager.ignore_log_patterns.append(r"Cannot drop user type .* as it is still used by .*")
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0])
    create_ks(session, "nested_user_type_dropping", 3)
    session.execute("USE nested_user_type_dropping")
    session.execute("CREATE TYPE simple_type (user_number int, user_text text)")
    assert_udt_type(session, "nested_user_type_dropping", "simple_type", expected=True)
    session.execute("CREATE TYPE another_type (somefield frozen<simple_type>)")
    with pytest.raises(InvalidRequest, match=re.escape("Cannot drop user type nested_user_type_dropping.simple_type as it is still used by user type another_type")):
        session.execute("DROP TYPE simple_type;")
    session.execute("DROP TYPE another_type;")
    session.execute("DROP TYPE simple_type;")
    assert_udt_type(session, "nested_user_type_dropping", "simple_type", expected=False)


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_type_enforcement.
# A user type literal with a field of the wrong type is rejected and nothing
# is written.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_type_enforcement(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0])
    create_ks(session, "user_type_enforcement", 3)
    session.execute("USE user_type_enforcement")
    session.execute("CREATE TYPE simple_type (user_number int)")
    session.execute("CREATE TABLE simple_table (id uuid PRIMARY KEY, number frozen<simple_type>)")
    with pytest.raises(InvalidRequest, match="field user_number is not of type int"):
        session.execute(f"INSERT INTO simple_table (id, number) VALUES ({uuid.uuid4()}, {{user_number: 'uh oh....this is not a number'}});")
    rows = list(session.execute("SELECT * FROM simple_table;"))
    assert 0 == len(rows)


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_nested_user_types.
# User types within user types and in a list of them, written and read at
# LOCAL_QUORUM; the list appends go through an exclusive session to node2,
# so that their order is deterministic (#4433).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nested_user_types(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0], consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    create_ks(session, "user_types", 3)
    session.execute("USE user_types")
    session.execute("CREATE TYPE item (sub_one text, sub_two text,)")
    session.execute("CREATE TYPE container (stuff text, more_stuff frozen<item>)")
    session.execute("CREATE TABLE bucket (id uuid PRIMARY KEY, primary_item frozen<item>, other_items frozen<container>, "
                    "other_containers list<frozen<container>>)")
    _id = uuid.uuid4()
    session.execute(f"INSERT INTO bucket (id, primary_item) VALUES ({_id}, {{sub_one: 'test', sub_two: 'test2'}});")
    session.execute(f"UPDATE bucket SET other_items = {{stuff: 'stuff', more_stuff: {{sub_one: 'one', sub_two: 'two'}}}} WHERE id={_id};")
    session2 = connect(manager, servers[1], consistency_level=ConsistencyLevel.LOCAL_QUORUM, exclusive=True, keyspace="user_types")
    session2.execute(f"UPDATE bucket SET other_containers = other_containers + [{{stuff: 'stuff2', more_stuff: {{sub_one: 'one_other', sub_two: 'two_other'}}}}] WHERE id={_id};")
    two = ("[{stuff: 'stuff3', more_stuff: {sub_one: 'one_2_other', sub_two: 'two_2_other'}}, "
           "{stuff: 'stuff4', more_stuff: {sub_one: 'one_3_other', sub_two: 'two_3_other'}}]")
    session2.execute(f"UPDATE bucket SET other_containers = other_containers + {two} WHERE id={_id};")
    rows = list(session.execute(f"SELECT primary_item, other_items, other_containers from bucket where id={_id};"))
    primary_item, other_items, other_containers = rows[0]
    assert listify(primary_item) == [["test", "test2"]]
    assert listify(other_items) == [["stuff", ["one", "two"]]]
    assert listify(other_containers) == [[["stuff2", ["one_other", "two_other"]], ["stuff3", ["one_2_other", "two_2_other"]], ["stuff4", ["one_3_other", "two_3_other"]]]]

    # Generate some repetitive data and check its contents
    for _ in range(50):
        _id = uuid.uuid4()
        session.execute(f"UPDATE bucket SET other_containers = other_containers + {two} WHERE id={_id};")
        time.sleep(0.1)
        rows = list(session.execute(f"SELECT other_containers from bucket WHERE id={_id}"))
        items = rows[0][0]
        assert listify(items) == [[["stuff3", ["one_2_other", "two_2_other"]], ["stuff4", ["one_3_other", "two_3_other"]]]]


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_type_as_part_of_pkey.
# A user type as part of a composite partition key: reading with only the
# other key column needs ALLOW FILTERING, reading with the whole key works.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_type_as_part_of_pkey(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0], consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    create_ks(session, "user_type_pkeys", 3)
    session.execute("CREATE TYPE t_person_name (first text, middle text, last text)")
    session.execute("CREATE TABLE person_likes (id uuid, name frozen<t_person_name>, like text, PRIMARY KEY ((id, name)))")
    _id = uuid.uuid4()
    session.execute(f"INSERT INTO person_likes (id, name, like) VALUES ({_id}, {{first:'Nero', middle:'Claudius Caesar Augustus', last:'Germanicus'}}, 'arson');")
    with pytest.raises(InvalidRequest, match="use ALLOW FILTERING"):
        session.execute(f"SELECT id, name.first from person_likes where id={_id};")
    rows = session.execute(f"SELECT id, name.first, like from person_likes where id={_id} and name = {{first:'Nero', middle: 'Claudius Caesar Augustus', last: 'Germanicus'}};")
    _row_uuid, first_name, like = rows[0]
    assert first_name == "Nero"
    assert like == "arson"


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_type_keyspace_permission_isolation.
# With password authentication and the CassandraAuthorizer, users granted
# all permissions on one keyspace cannot create, drop or alter types in the
# other user's keyspace, and can in their own.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_type_keyspace_permission_isolation(manager: ScyllaClusterManager):
    manager.ignore_log_patterns.append(r"Can\'t send migration request: node.*is down")
    # The harness knows the short names of the dtest's org.apache.cassandra.auth classes.
    config = {**CONFIG, "authenticator": "PasswordAuthenticator",
              "authorizer": "CassandraAuthorizer", "permissions_validity_in_ms": 0}
    servers = await manager.servers_add(3, config=config, property_file=THREE_RACKS,
                                        driver_connect_opts={"auth_provider": PlainTextAuthProvider(username="cassandra", password="cassandra")})
    node1 = servers[0]

    superuser_session = connect(manager, node1, user="cassandra", password="cassandra")
    superuser_session.execute("create user ks1_user with password 'cassandra' nosuperuser;")
    superuser_session.execute("create user ks2_user with password 'cassandra' nosuperuser;")
    create_ks(superuser_session, "ks1", 3)
    create_ks(superuser_session, "ks2", 3)
    superuser_session.execute("grant all permissions on keyspace ks1 to ks1_user;")
    superuser_session.execute("grant all permissions on keyspace ks2 to ks2_user;")

    user1_session = connect(manager, node1, user="ks1_user", password="cassandra")
    user2_session = connect(manager, node1, user="ks2_user", password="cassandra")

    def assert_unauthorized(session, query, message):
        with pytest.raises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, str(cm.value)), "Expected: %s" % message

    # first make sure the users can't create types in each other's ks
    assert_unauthorized(user1_session, "CREATE TYPE ks2.simple_type (user_number int, user_text text );", "User ks1_user has no CREATE permission on <keyspace ks2> or any of its parents")
    assert_unauthorized(user2_session, "CREATE TYPE ks1.simple_type (user_number int, user_text text );", "User ks2_user has no CREATE permission on <keyspace ks1> or any of its parents")

    # now, actually create the types in the correct keyspaces
    user1_session.execute("CREATE TYPE ks1.simple_type (user_number int, user_text text );")
    user2_session.execute("CREATE TYPE ks2.simple_type (user_number int, user_text text );")

    # they can't drop each other's types
    assert_unauthorized(user1_session, "DROP TYPE ks2.simple_type;", "User ks1_user has no DROP permission on <keyspace ks2> or any of its parents")
    assert_unauthorized(user2_session, "DROP TYPE ks1.simple_type;", "User ks2_user has no DROP permission on <keyspace ks1> or any of its parents")

    # they can't rename each other's types
    assert_unauthorized(user1_session, "ALTER TYPE ks2.simple_type RENAME user_number TO user_num;", "User ks1_user has no ALTER permission on <keyspace ks2> or any of its parents")
    assert_unauthorized(user2_session, "ALTER TYPE ks1.simple_type RENAME user_number TO user_num;", "User ks2_user has no ALTER permission on <keyspace ks1> or any of its parents")

    # rename the types using the correct user w/permissions to do so
    user1_session.execute("ALTER TYPE ks1.simple_type RENAME user_number TO user_num;")
    user2_session.execute("ALTER TYPE ks2.simple_type RENAME user_number TO user_num;")

    # finally, drop the types using the correct user w/permissions to do so
    user1_session.execute(SimpleStatement("DROP TYPE ks1.simple_type;", consistency_level=ConsistencyLevel.ALL))
    user2_session.execute(SimpleStatement("DROP TYPE ks2.simple_type;", consistency_level=ConsistencyLevel.ALL))

    # The metadata of this Cluster learns about the drops only from a debounced event; reload it.
    superuser_session.cluster.refresh_schema_metadata()
    assert_udt_type(superuser_session, "ks1", "simple_type", expected=False)
    assert_udt_type(superuser_session, "ks2", "simple_type", expected=False)


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_nulls_in_user_types.
# User type fields left null explicitly or implicitly read back as null.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nulls_in_user_types(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0], consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    create_ks(session, "user_types", 3)
    session.execute("USE user_types")
    session.execute("CREATE TYPE item (sub_one text, sub_two text,)")
    session.execute("CREATE TABLE bucket (id int PRIMARY KEY, my_item frozen<item>,)")
    # Adds an explicit null
    session.execute("INSERT INTO bucket (id, my_item) VALUES (0, {sub_one: 'test', sub_two: null})")
    # Adds with an implicit null
    session.execute("INSERT INTO bucket (id, my_item) VALUES (1, {sub_one: 'test'})")
    rows = list(session.execute("SELECT my_item FROM bucket WHERE id=0"))
    assert listify(rows[0]) == [["test", None]]
    rows = list(session.execute("SELECT my_item FROM bucket WHERE id=1"))
    assert listify(rows[0]) == [["test", None]]


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_type_as_clustering_col.
# Rows with a user type clustering column come back in the order of its
# fields.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_type_as_clustering_col(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0], consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    create_ks(session, "user_type_pkeys", 3)
    session.execute("CREATE TYPE t_letterpair (first text, second text)")
    session.execute("CREATE TABLE letters (id int, letterpair frozen<t_letterpair>, PRIMARY KEY (id, letterpair))")
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


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_add_udt_to_another_data_types.
# Types nested in frozen columns of tables with secondary indexes and
# materialized views: 2000 rows written to each table concurrently, then
# the types get a new user type field and rows using it are written; the
# row counts of the tables and views, read at QUORUM, stay right.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_add_udt_to_another_data_types(manager: ScyllaClusterManager):
    rows_num = 2000
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0])
    keyspace_name = "abcinfo"
    create_ks(session, keyspace_name, 1)

    session.execute("create type if not exists user_refs (id text, alt_name text, firstname text,lastname text, email text)")
    session.execute("create type if not exists obs_entity (entity_id text, version int, entity_type text,type text, service text, user_refs frozen<user_refs>)")
    session.execute("create type if not exists other_entity (other_entity_id text, version int, entities frozen<set<text>>,type text)")
    session.execute(
        "create table if not exists entity(entity_id text, other_entity_id text,"
        "entity_type text, type text, service text, entity_info frozen < obs_entity >,"
        "import_timestamp timestamp, import_timestamp_day timestamp, primary key(entity_id)"
        ") with "
        "compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}"
        "and bloom_filter_fp_chance = 0.01"
    )
    session.execute("create index if not exists on entity(service)")
    session.execute("create index if not exists on entity(type)")
    session.execute("create materialized view if not exists entity_by_type as select * from entity where type is not null primary key(type, entity_id)")
    await wait_for_view(manager, session, servers, keyspace_name, "entity_by_type")
    session.execute("create materialized view if not exists entity_by_unique_id as select * from entity where other_entity_id is not null primary key(other_entity_id, entity_id)")
    await wait_for_view(manager, session, servers, keyspace_name, "entity_by_unique_id")
    session.execute(
        "create table if not exists entity_rel (src_entity_id text, src_entity frozen<obs_entity>, "
        "dest_entity_id text, dest_entity frozen<obs_entity>, service text, rel_type text, "
        "deleted boolean, primary key ((src_entity_id), dest_entity_id, rel_type, service)) "
        "with compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} "
        "and bloom_filter_fp_chance = 0.01"
    )
    session.execute("create index if not exists on entity_rel(dest_entity_id)")
    session.execute("create index if not exists on entity_rel(service)")
    session.execute("create index if not exists on entity_rel(rel_type)")

    def insert_into_entity(rows=rows_num, altered_type=False):
        for i in range(rows):
            new_type = f", ({i}, {{{i}, {i}}})" if altered_type else ""
            session.execute(
                "insert into abcinfo.entity (entity_id, entity_info, entity_type, import_timestamp, "
                "import_timestamp_day, other_entity_id, service, type) values "
                f"('text{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', "
                f"('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), "
                f"'entity_type{i}', 1234568979, 45621313131, 'other_entity_id{i}', 'service{i}', 'type{i}')"
            )

    def insert_into_entity_rel(rows=rows_num, altered_type=False):
        for i in range(rows):
            new_type = f", ({i}, {{{i}, {i}}})" if altered_type else ""
            session.execute(
                "insert into abcinfo.entity_rel (src_entity_id, src_entity, dest_entity_id, dest_entity, "
                "service, rel_type, deleted) values "
                f"('text{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', "
                f"('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), "
                f"'dest_entity_id{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', "
                f"('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), "
                f"'service{i}', 'rel_type{i}', True)"
            )

    # Both tables are written at the same time, from two threads.
    with ThreadPoolExecutor() as entity_run_executer, ThreadPoolExecutor() as entity_rel_run_executer:
        runned_thread = [entity_run_executer.submit(insert_into_entity), entity_rel_run_executer.submit(insert_into_entity_rel)]
        for t in runned_thread:
            t.result(60)

    assert_row_count(session, "entity", rows_num, consistency_level=ConsistencyLevel.QUORUM)
    assert_row_count(session, "entity_rel", rows_num, consistency_level=ConsistencyLevel.QUORUM)

    session.execute("create type if not exists priority_refs (priority int, description set<int>)")
    session.execute("alter type obs_entity add priority_refs frozen<priority_refs>")
    session.execute("alter type user_refs add priority_refs frozen<priority_refs>")

    insert_into_entity(rows=10, altered_type=True)
    insert_into_entity_rel(rows=10, altered_type=True)

    assert_row_count(session, "entity", rows_num, consistency_level=ConsistencyLevel.QUORUM)
    assert_row_count(session, "entity_rel", rows_num, consistency_level=ConsistencyLevel.QUORUM)
    assert_row_count(session, "entity_by_type", rows_num, consistency_level=ConsistencyLevel.QUORUM)
    assert_row_count(session, "entity_by_unique_id", rows_num, consistency_level=ConsistencyLevel.QUORUM)


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_alter_inner_udt_by_another_udt.
# Types nested in types, altered to contain a new type, used in a table with
# a materialized view: rows written through tuple literals of the nested
# types show up in both, counted at QUORUM.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_alter_inner_udt_by_another_udt(manager: ScyllaClusterManager):
    rows_num = 10
    servers = await manager.servers_add(3, config=CONFIG, property_file=THREE_RACKS)
    session = connect(manager, servers[0])
    keyspace_name = "abcinfo"
    create_ks(session, keyspace_name, 1)
    session.execute("create type if not exists user_refs (id text, alt_name text)")
    session.execute("create type if not exists obs_entity (entity_id text, user_refs frozen<user_refs>)")
    session.execute("create type if not exists some_entity (e_struct frozen<obs_entity>)")
    session.execute("create type if not exists priority_refs (priority int, description set<int>)")
    session.execute("alter type obs_entity add priority_refs frozen<priority_refs>")
    session.execute("alter type user_refs add priority_refs frozen<priority_refs>")
    session.execute(
        "create table if not exists entity(entity_id text, type text, entity_info frozen < some_entity >,"
        " primary key(entity_id)) with "
        "compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}"
        "and bloom_filter_fp_chance = 0.01"
    )
    session.execute("create materialized view if not exists entity_by_type as select * from entity where type is not null primary key(type, entity_id)")
    await wait_for_view(manager, session, servers, keyspace_name, "entity_by_type")
    for i in range(rows_num):
        new_type = f"({i}, {{{i}, {i}}})"
        session.execute(f"insert into entity (entity_id, type, entity_info) values ('text{i}', 'type{i}', (('entity_id{i}', ('id{i}', 'alt_name{i}', {new_type}), {new_type})))")
    assert_row_count(session, "entity", rows_num, consistency_level=ConsistencyLevel.QUORUM)
    assert_row_count(session, "entity_by_type", rows_num, consistency_level=ConsistencyLevel.QUORUM)

# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_case_sensitive_type_name.
# A quoted type name is case sensitive: "PHone" and phone are two types, each
# usable as a non-frozen column, a frozen column and in a frozen list;
# dropping "PHone" leaves phone working.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_case_sensitive_type_name(manager: ScyllaClusterManager):
    # Scylla's default, as on the dtest nodes; test.py turns it off.
    servers = await manager.servers_add(3, config={**CONFIG, "auto_snapshot": True}, property_file=THREE_RACKS)
    session = connect(manager, servers[0], consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    create_ks(session, "user_types", 3)
    session.set_keyspace("user_types")

    logger.info("Test with capital type name - PHone")
    session.execute('CREATE TYPE "PHone" (country_code int, number text)')
    session.execute('CREATE TABLE cf (pk int, pn "PHone", PRIMARY KEY (pk))')
    session.execute('CREATE TABLE cf2 (pk int, pn frozen<"PHone">, PRIMARY KEY (pk))')
    session.execute('CREATE TABLE cf3 (pk int, pn frozen<list<"PHone">>, PRIMARY KEY (pk))')
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
    session.execute("INSERT INTO new_cf (pk, pn) VALUES (0, {country_code: '86', number: 123})")
    session.execute("INSERT INTO new_cf2 (pk, pn) VALUES (0, {country_code: '87', number: 456})")
    session.execute("INSERT INTO new_cf3 (pk, pn) VALUES (0, [{country_code: '86', number: 123}, {country_code: '88', number: 789}])")
    rows = list(session.execute("SELECT pn FROM new_cf WHERE pk=0"))
    assert listify(rows[0]) == [["86", 123]]
    rows = list(session.execute("SELECT pn FROM new_cf2 WHERE pk=0"))
    assert listify(rows[0]) == [["87", 456]]
    rows = list(session.execute("SELECT pn FROM new_cf3 WHERE pk=0"))
    assert listify(rows[0]) == [[["86", 123], ["88", 789]]]

    logger.info("Drop capital type name, and check lower case type still exists")
    session.execute("DROP TABLE cf")
    session.execute("DROP TABLE cf2")
    session.execute("DROP TABLE cf3")
    session.execute('DROP TYPE "PHone"')
    rows = list(session.execute("SELECT pn FROM new_cf3 WHERE pk=0"))
    assert listify(rows[0]) == [[["86", 123], ["88", 789]]]
