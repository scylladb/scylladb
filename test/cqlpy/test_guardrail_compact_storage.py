import pytest

from cassandra.protocol import InvalidRequest

from .util import config_value_context, new_test_table

# Tests for the enable_create_table_with_compact_storage guardrail.
# Because this feature does not exist in Cassandra , *all* tests in this file are
# Scylla-only. Let's mark them all scylla_only with an autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

def test_create_table_with_compact_storage_default_config(cql, test_keyspace):
    # enable_create_table_with_compact_storage is now disabled in db/config
    try:
        with new_test_table(cql, test_keyspace, schema="p int PRIMARY KEY, v int", extra="WITH COMPACT STORAGE") as test_table:
            pytest.fail
    except InvalidRequest:
        pass

@pytest.mark.parametrize("enable_compact_storage", [False, True])
def test_create_table_with_compact_storage_config(cql, test_keyspace, enable_compact_storage):
    with config_value_context(cql, 'enable_create_table_with_compact_storage', str(enable_compact_storage).lower()):
        try:
            with new_test_table(cql, test_keyspace, schema="p int PRIMARY KEY, v int", extra="WITH COMPACT STORAGE") as test_table:
                if not enable_compact_storage:
                    pytest.fail
        except InvalidRequest:
            # expected to throw InvalidRequest only when
            # enable_create_table_with_compact_storage=false
            if enable_compact_storage:
                pytest.fail
