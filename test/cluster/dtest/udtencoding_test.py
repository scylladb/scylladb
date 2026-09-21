import time

import pytest

from dtest_class import Tester, create_ks
from tools.assertions import assert_invalid
from tools.cluster_topology import generate_cluster_topology

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class TestUDTEncoding(Tester):
    @pytest.fixture(scope="function")
    def node1_session(self):
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)).start()

        time.sleep(0.5)
        with self.patient_cql_connection(self.cluster.nodelist()[0]) as session:
            yield session

    def test_udt(self, node1_session):
        """
        Test (somewhat indirectly) that user queries involving UDT's are properly encoded
        (due to driver not recognizing UDT syntax)
        """
        create_ks(node1_session, "ks", 3)

        # create udt and insert correctly (should be successful)
        node1_session.execute("CREATE TYPE address (city text,zip int);")
        node1_session.execute("CREATE TABLE user_profiles (login text PRIMARY KEY, addresses map<text, frozen<address>>);")
        node1_session.execute("INSERT INTO user_profiles(login, addresses) VALUES ('tsmith', { 'home': {city: 'San Fransisco',zip: 94110 }});")

        # note here address looks likes a map -> which is what the driver thinks it is. udt is encoded server side,
        # we test that if addresses is changed slightly whether encoder recognizes the errors

        # try adding a field - see if will be encoded to a udt (should return error)
        assert_invalid(
            node1_session, "INSERT INTO user_profiles(login, addresses) VALUES ('jsmith', { 'home': {street: 'El Camino Real', city: 'San Fransisco', zip: 94110 }});", "Unknown field 'street' in value of user defined type address"
        )

        # try modifying a field name - see if will be encoded to a udt (should return error)
        assert_invalid(node1_session, "INSERT INTO user_profiles(login, addresses) VALUES ('fsmith', { 'home': {cityname: 'San Fransisco', zip: 94110 }});", "Unknown field 'cityname' in value of user defined type address")

        # try modifying a type within the collection - see if will be encoded to a udt (should return error)
        assert_invalid(node1_session, "INSERT INTO user_profiles(login, addresses) VALUES ('fsmith', { 'home': {city: 'San Fransisco', zip: '94110' }});", "Invalid map literal for addresses")

    def test_udt_change_in_partition_key(self, node1_session):
        """
        This test is meant to verify scylladb/scylla@66e8214
        Scylla will now detect modifications to user-defined data types (UDTs) that are used as partition keys and
        forbid them.
        """
        create_ks(node1_session, "ks", 3)
        node1_session.execute("CREATE TYPE full_name (first_name text, last_name text);")
        node1_session.execute("CREATE TYPE address (city text, zip int);")
        node1_session.execute("CREATE TABLE user_profile (name frozen<full_name>, username text, residential_address address, PRIMARY KEY (name, username));")
        node1_session.execute("INSERT INTO user_profile (name, username, residential_address) VALUES ({first_name: 'John', last_name: 'Smith'}, 'jsmith', {city: 'San Francisco', zip: 93210});")
        assert_invalid(node1_session, "ALTER TYPE full_name RENAME last_name TO surname;")
        node1_session.execute("ALTER TYPE address ADD country text")
