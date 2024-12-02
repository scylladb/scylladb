import pytest
from contextlib import ExitStack
from util import unique_name, config_value_context, new_test_keyspace
from cassandra.protocol import ConfigurationException

# Tests for the replication_strategy_{warn,fail}_list guardrail. Because
# this feature does not exist in Cassandra , *all* tests in this file are
# Scylla-only. Let's mark them all scylla_only with an autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

def create_ks_and_assert_warnings_and_errors(cql, ks_opts, warnings_count=None, expected_warning=None, unexpected_warning=None, fails_count=0):
    if fails_count:
        with pytest.raises(ConfigurationException):
            with new_test_keyspace(cql, ks_opts):
                pass
    else:
        keyspace = unique_name()
        response_future = cql.execute_async("CREATE KEYSPACE " + keyspace + " " + ks_opts)
        response_future.result()
        cql.execute("DROP KEYSPACE " + keyspace)

        if warnings_count is not None:
            if warnings_count:
                assert response_future.warnings is not None and len(response_future.warnings) == warnings_count
            else:
                assert response_future.warnings is None
        if expected_warning is not None:
            assert response_future.warnings is not None and expected_warning in "\n".join(response_future.warnings)
        if unexpected_warning is not None and response_future.warnings is not None:
            assert not unexpected_warning in "\n".join(response_future.warnings)


def test_given_default_config_when_creating_ks_should_only_produce_warning_for_simple_strategy(cql, this_dc):
    ks_opts = " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }"
    create_ks_and_assert_warnings_and_errors(cql, ks_opts, expected_warning='replication_strategy_warn_list', fails_count=0)

    for key, value in {'NetworkTopologyStrategy': this_dc, 'EverywhereStrategy': 'replication_factor',
                       'LocalStrategy': 'replication_factor'}.items():
        create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : '{key}', '{value}' : 3 }}",
                                                 unexpected_warning='replication_strategy_warn_list', fails_count=0)


def test_given_cleared_guardrails_when_creating_ks_should_not_get_warning_nor_error(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', '[]'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_fail_list', '[]'))

        for key, value in {'SimpleStrategy': 'replication_factor', 'NetworkTopologyStrategy': this_dc,
                           'EverywhereStrategy': 'replication_factor', 'LocalStrategy': 'replication_factor'}.items():
            create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : '{key}', '{value}' : 3 }}",
                                                     unexpected_warning='replication_strategy_warn_list', fails_count=0)


def test_given_non_empty_warn_list_when_creating_ks_should_only_warn_when_listed_strategy_used(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list',
                                                                'SimpleStrategy,LocalStrategy,NetworkTopologyStrategy,EverywhereStrategy'))
        for key, value in {'SimpleStrategy': 'replication_factor', 'NetworkTopologyStrategy': this_dc,
                           'EverywhereStrategy': 'replication_factor', 'LocalStrategy': 'replication_factor'}.items():
            create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : '{key}', '{value}' : 3 }}",
                                                     expected_warning='replication_strategy_warn_list', fails_count=0)


def test_given_non_empty_warn_and_fail_lists_when_creating_ks_should_fail_query_when_listed_strategy_used(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(
            config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy,EverywhereStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_fail_list',
                                                                'SimpleStrategy,LocalStrategy,'
                                                                'NetworkTopologyStrategy,EverywhereStrategy'))
        for key, value in {'SimpleStrategy': 'replication_factor', 'NetworkTopologyStrategy': this_dc,
                           'EverywhereStrategy': 'replication_factor', 'LocalStrategy': 'replication_factor'}.items():
            # note: even though warn list is not empty, no warnings should be generated, because failures come first -
            #  we don't want to issue a warning and also fail the query at the same time
            create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : '{key}', '{value}' : 3 }}",
                                                     warnings_count=0, fails_count=1)


def test_given_already_existing_ks_when_altering_ks_should_validate_against_discouraged_strategies(cql, this_dc, has_tablets):
    with ExitStack() as config_modifications:
        # place 1 strategy on warn list, 1 strategy on fail list and leave remaining strategies unspecified,
        # i.e. let them be allowed
        config_modifications.enter_context(
            config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy'))
        config_modifications.enter_context(
            config_value_context(cql, 'replication_strategy_fail_list', 'EverywhereStrategy'))

        if has_tablets:
            extra_opts = " AND TABLETS = {'enabled': false}"
        else:
            extra_opts = ""

        # create a ks with "allowed" strategy
        with new_test_keyspace(cql, " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 3 }" + extra_opts) as keyspace:
            # alter this ks to use other strategy that is NOT present on any list
            response_future = cql.execute_async(
                "ALTER KEYSPACE " + keyspace + " WITH REPLICATION = { 'class' : 'LocalStrategy', 'replication_factor' : 3 }")
            response_future.result()
            assert response_future.warnings is None

            # alter this ks to use strategy that is present on the warn list
            response_future = cql.execute_async(
                "ALTER KEYSPACE " + keyspace + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }")
            response_future.result()
            assert response_future.warnings is not None and len(response_future.warnings) == 1

            # alter this ks to use strategy that is present on the fail list
            with pytest.raises(ConfigurationException):
                cql.execute_async(
                    "ALTER KEYSPACE " + keyspace + " WITH REPLICATION = { 'class' : 'EverywhereStrategy', 'replication_factor' : 3 }").result()


def test_given_rf_and_strategy_guardrails_when_creating_ks_should_print_2_warnings_if_both_violated(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_warn_threshold', '3'))
        create_ks_and_assert_warnings_and_errors(cql,
                                                 f" WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}",
                                                 warnings_count=2, fails_count=0)


def test_given_rf_and_strategy_guardrails_when_violating_fail_rf_limit_and_warn_strategy_limit_should_fail_the_query_without_warning(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_fail_threshold', '3'))
        create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}",
                                                 warnings_count=0, fails_count=1)


def test_given_rf_and_strategy_guardrails_when_violating_fail_strategy_limit_should_fail_the_query(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_fail_list', 'SimpleStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_fail_threshold', '3'))
        create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}",
                                                 warnings_count=0, fails_count=1)


def test_given_restrict_replication_simplestrategy_when_it_is_set_should_emulate_old_behavior(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'restrict_replication_simplestrategy', 'true'))
        create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }}",
                                                 warnings_count=0, fails_count=1)
        config_modifications.enter_context(config_value_context(cql, 'restrict_replication_simplestrategy', 'warn'))
        create_ks_and_assert_warnings_and_errors(cql, f" WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }}",
                                                 warnings_count=1, fails_count=0)

def test_config_replication_strategy_warn_list_roundtrips_quotes(cql):
    # Use direct SELECT/UPDATE to avoid trippy config_value_context behavior
    value = cql.execute("SELECT value FROM system.config WHERE name = 'replication_strategy_warn_list'").one().value
    assert value == '["SimpleStrategy"]' # our lovely default
    # try without quotes
    cql.execute("UPDATE system.config SET value = '[SimpleStrategy]' WHERE name = 'replication_strategy_warn_list'")
    # reproduces #
    cql.execute("UPDATE system.config SET value = '[\"SimpleStrategy\"]' WHERE name = 'replication_strategy_warn_list'")
