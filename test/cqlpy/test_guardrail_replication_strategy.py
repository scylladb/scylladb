import pytest
from contextlib import ExitStack
import re
from .util import unique_name, config_value_context, new_test_keyspace, ScyllaMetrics
from .conftest import has_tablets
from cassandra.protocol import ConfigurationException

# Tests for the replication_strategy_{warn,fail}_list guardrail. Because
# this feature does not exist in Cassandra , *all* tests in this file are
# Scylla-only. Let's mark them all scylla_only with an autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# Guardrail message regex patterns — check guardrail name and warn/fail, not full text.
STRATEGY_WARN_RE = r"{strategy}.*not recommended.*replication_strategy_warn_list"

STRATEGY_FAIL_RE = r"{strategy}.*forbidden.*replication_strategy_fail_list"

MINIMUM_RF_WARN_RE = r"{dc}={rf}.*minimum_replication_factor_warn_threshold={threshold}.*not recommended"

MINIMUM_RF_FAIL_RE = r"{dc}={rf}.*forbidden.*minimum_replication_factor_fail_threshold={threshold}"

MAXIMUM_RF_WARN_RE = r"{dc}={rf}.*maximum_replication_factor_warn_threshold={threshold}.*not recommended"

MAXIMUM_RF_FAIL_RE = r"{dc}={rf}.*forbidden.*maximum_replication_factor_fail_threshold={threshold}"


def get_metric(cql, name):
    return ScyllaMetrics.query(cql).get(name) or 0


def ks_opts(strategy, rf, dc=None, tablets=True):
    key = dc or 'replication_factor'
    opts = f" WITH REPLICATION = {{ 'class' : '{strategy}', '{key}' : {rf} }}"
    if not tablets:
        opts += " AND TABLETS = {'enabled': false}"
    return opts


def get_replication_strategy_ks_opts(strategy: str, rf: int, dc=None) -> str:
    # If tablets syntax is not supported (e.g. in cassandra) we don't add it to the ks options
    return ks_opts(strategy, rf, dc=dc, tablets=has_tablets and strategy == 'NetworkTopologyStrategy')


def create_ks_and_assert_warnings_and_errors(cql, ks_opts, metric_name=None,
                                              warnings=[], failures=[]):
    before = get_metric(cql, metric_name) if metric_name else None

    if failures:
        with pytest.raises(ConfigurationException, match=failures[0]):
            with new_test_keyspace(cql, ks_opts):
                pass
    else:
        keyspace = unique_name()
        response_future = cql.execute_async("CREATE KEYSPACE " + keyspace + " " + ks_opts)
        response_future.result()
        cql.execute("DROP KEYSPACE " + keyspace)

        if not warnings:
            assert response_future.warnings is None
        else:
            assert response_future.warnings and len(response_future.warnings) == len(warnings)
            for w in response_future.warnings:
                assert any(re.search(p, w) for p in warnings), f"Unexpected warning: {w}"

    if before is not None:
        assert get_metric(cql, metric_name) > before


def test_given_default_config_when_creating_ks_should_only_produce_warning_for_simple_strategy(cql, this_dc):
    create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts('SimpleStrategy', 3),
        metric_name='scylla_cql_replication_strategy_warn_list_violations',
        warnings=[STRATEGY_WARN_RE.format(strategy='SimpleStrategy')])

    for strategy, dc in {'NetworkTopologyStrategy': this_dc, 'EverywhereStrategy': 'replication_factor',
                         'LocalStrategy': 'replication_factor'}.items():
        create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts(strategy, 1, dc=dc),
                        warnings=[MINIMUM_RF_WARN_RE.format(dc=re.escape(dc), rf=1, threshold=3)])


def test_given_cleared_guardrails_when_creating_ks_should_not_get_warning_nor_error(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', '[]'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_fail_list', '[]'))

        for strategy, dc in {'SimpleStrategy': 'replication_factor', 'NetworkTopologyStrategy': this_dc,
                              'EverywhereStrategy': 'replication_factor', 'LocalStrategy': 'replication_factor'}.items():
            create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts(strategy, 1, dc=dc),
                            warnings=[MINIMUM_RF_WARN_RE.format(dc=re.escape(dc), rf=1, threshold=3)])


def test_given_non_empty_warn_list_when_creating_ks_should_only_warn_when_listed_strategy_used(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list',
                                                                'SimpleStrategy,LocalStrategy,NetworkTopologyStrategy,EverywhereStrategy'))
        for strategy, dc in {'SimpleStrategy': 'replication_factor', 'NetworkTopologyStrategy': this_dc,
                              'EverywhereStrategy': 'replication_factor', 'LocalStrategy': 'replication_factor'}.items():
            create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts(strategy, 1, dc=dc),
                            warnings=[STRATEGY_WARN_RE.format(strategy=strategy),
                                      MINIMUM_RF_WARN_RE.format(dc=re.escape(dc), rf=1, threshold=3)])


def test_given_non_empty_warn_and_fail_lists_when_creating_ks_should_fail_query_when_listed_strategy_used(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(
            config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy,EverywhereStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_fail_list',
                                                                'SimpleStrategy,LocalStrategy,'
                                                                'NetworkTopologyStrategy,EverywhereStrategy'))
        for strategy, dc in {'SimpleStrategy': 'replication_factor', 'NetworkTopologyStrategy': this_dc,
                              'EverywhereStrategy': 'replication_factor', 'LocalStrategy': 'replication_factor'}.items():
            # note: even though warn list is not empty, no warnings should be generated, because failures come first -
            #  we don't want to issue a warning and also fail the query at the same time
            create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts(strategy, 1, dc=dc),
                            failures=[STRATEGY_FAIL_RE.format(strategy=strategy)])

        # Verify metric increment and exact error message (docs/cql/guardrails.rst).
        create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts('SimpleStrategy', 3),
            metric_name='scylla_cql_replication_strategy_fail_list_violations',
            failures=[STRATEGY_FAIL_RE.format(strategy='SimpleStrategy')])


def test_given_already_existing_ks_when_altering_ks_should_validate_against_discouraged_strategies(cql, this_dc):
    with ExitStack() as config_modifications:
        # place 1 strategy on warn list, 1 strategy on fail list and leave remaining strategies unspecified,
        # i.e. let them be allowed
        config_modifications.enter_context(
            config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy'))
        config_modifications.enter_context(
            config_value_context(cql, 'replication_strategy_fail_list', 'EverywhereStrategy'))

        # create a ks with "allowed" strategy
        # disable tablets to prevent Replication factor 3 exceeds the number of racks
        with new_test_keyspace(cql, ks_opts('NetworkTopologyStrategy', 3, dc=this_dc, tablets=False)) as keyspace:
            # alter this ks to use other strategy that is NOT present on any list
            response_future = cql.execute_async(
                f"ALTER KEYSPACE {keyspace}" + ks_opts('LocalStrategy', 3))
            response_future.result()
            assert response_future.warnings is None

            # alter this ks to use strategy that is present on the warn list
            response_future = cql.execute_async(
                f"ALTER KEYSPACE {keyspace}" + ks_opts('SimpleStrategy', 3))
            response_future.result()
            assert response_future.warnings is not None and len(response_future.warnings) == 1

            # alter this ks to use strategy that is present on the fail list
            with pytest.raises(ConfigurationException):
                cql.execute_async(
                    f"ALTER KEYSPACE {keyspace}" + ks_opts('EverywhereStrategy', 3)).result()


def test_given_rf_and_strategy_guardrails_when_creating_ks_should_print_2_warnings_if_both_violated(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_warn_threshold', '3'))
        create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts('SimpleStrategy', 1),
                        metric_name='scylla_cql_minimum_replication_factor_warn_violations',
                        warnings=[MINIMUM_RF_WARN_RE.format(
                            dc='replication_factor', rf=1, threshold=3),
                            STRATEGY_WARN_RE.format(strategy='SimpleStrategy')])


def test_given_rf_and_strategy_guardrails_when_violating_fail_rf_limit_and_warn_strategy_limit_should_fail_the_query_without_warning(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', 'SimpleStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_fail_threshold', '3'))
        create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts('SimpleStrategy', 1),
                        metric_name='scylla_cql_minimum_replication_factor_fail_violations',
                        failures=[MINIMUM_RF_FAIL_RE.format(
                            dc='replication_factor', rf=1, threshold=3)])


def test_given_rf_and_strategy_guardrails_when_violating_fail_strategy_limit_should_fail_the_query(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_fail_list', 'SimpleStrategy'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_fail_threshold', '3'))
        create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts('SimpleStrategy', 1),
                        failures=[STRATEGY_FAIL_RE.format(strategy='SimpleStrategy')])


def test_given_restrict_replication_simplestrategy_when_it_is_set_should_emulate_old_behavior(cql):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'restrict_replication_simplestrategy', 'true'))
        create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts('SimpleStrategy', 3),
                        failures=[STRATEGY_FAIL_RE.format(strategy='SimpleStrategy')])
        config_modifications.enter_context(config_value_context(cql, 'restrict_replication_simplestrategy', 'warn'))
        create_ks_and_assert_warnings_and_errors(cql, get_replication_strategy_ks_opts('SimpleStrategy', 3),
                        warnings=[STRATEGY_WARN_RE.format(strategy='SimpleStrategy')])


def test_config_replication_strategy_warn_list_roundtrips_quotes(cql):
    # Use direct SELECT/UPDATE to avoid trippy config_value_context behavior
    value = cql.execute("SELECT value FROM system.config WHERE name = 'replication_strategy_warn_list'").one().value
    assert value == '["SimpleStrategy"]' # our lovely default
    # try without quotes
    cql.execute("UPDATE system.config SET value = '[SimpleStrategy]' WHERE name = 'replication_strategy_warn_list'")
    # reproduces #
    cql.execute("UPDATE system.config SET value = '[\"SimpleStrategy\"]' WHERE name = 'replication_strategy_warn_list'")


def test_rf_zero_always_allowed(cql, this_dc):
    """Maximum RF guardrails fire correctly with high RF, but RF=0
    (meaning 'do not replicate to this data center') must never trigger
    any guardrail — even when both minimum and maximum thresholds are
    active.  Also verifies metric increments and message formats for
    maximum RF guardrails (docs/cql/guardrails.rst)."""
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_warn_threshold', '3'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_fail_threshold', '2'))
        config_modifications.enter_context(config_value_context(cql, 'maximum_replication_factor_warn_threshold', '5'))
        config_modifications.enter_context(config_value_context(cql, 'maximum_replication_factor_fail_threshold', '7'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', ''))
        dc = re.escape(this_dc)

        # max RF warn: RF=6 > warn=5 but < fail=7
        create_ks_and_assert_warnings_and_errors(cql, ks_opts('NetworkTopologyStrategy', 6, dc=this_dc, tablets=False),
            metric_name='scylla_cql_maximum_replication_factor_warn_violations',
            warnings=[MAXIMUM_RF_WARN_RE.format(dc=dc, rf=6, threshold=5)])

        # max RF fail: RF=8 > fail=7
        create_ks_and_assert_warnings_and_errors(cql, ks_opts('NetworkTopologyStrategy', 8, dc=this_dc, tablets=False),
            metric_name='scylla_cql_maximum_replication_factor_fail_violations',
            failures=[MAXIMUM_RF_FAIL_RE.format(dc=dc, rf=8, threshold=7)])

        # RF=0 bypasses all guardrails.
        create_ks_and_assert_warnings_and_errors(cql, ks_opts('NetworkTopologyStrategy', 0, dc=this_dc, tablets=True))


def test_rf_threshold_minus_one_disables_check(cql, this_dc):
    """Setting an RF threshold to -1 disables that guardrail entirely.
    Verify that with all four thresholds set to -1, any RF value (low or
    high) is accepted without warnings or errors."""
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_warn_threshold', '-1'))
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_fail_threshold', '-1'))
        config_modifications.enter_context(config_value_context(cql, 'maximum_replication_factor_warn_threshold', '-1'))
        config_modifications.enter_context(config_value_context(cql, 'maximum_replication_factor_fail_threshold', '-1'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', ''))
        # RF=1 — would normally trigger the default minimum_replication_factor_warn_threshold=3
        create_ks_and_assert_warnings_and_errors(cql, ks_opts('NetworkTopologyStrategy', 1, dc=this_dc, tablets=True))
        # RF=100 — would normally trigger maximum thresholds; disable tablets
        # to avoid the rack count check.
        create_ks_and_assert_warnings_and_errors(cql, ks_opts('NetworkTopologyStrategy', 100, dc=this_dc, tablets=False))


def test_alter_keyspace_minimum_rf_warn(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_warn_threshold', '3'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', ''))
        with new_test_keyspace(cql, ks_opts('NetworkTopologyStrategy', 3, dc=this_dc, tablets=False)) as ks:
            response_future = cql.execute_async(f"ALTER KEYSPACE {ks}" + ks_opts('NetworkTopologyStrategy', 1, dc=this_dc))
            response_future.result()
            assert response_future.warnings is not None


def test_alter_keyspace_minimum_rf_fail(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'minimum_replication_factor_fail_threshold', '3'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', ''))
        with new_test_keyspace(cql, ks_opts('NetworkTopologyStrategy', 3, dc=this_dc, tablets=False)) as ks:
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER KEYSPACE {ks}" + ks_opts('NetworkTopologyStrategy', 1, dc=this_dc))


def test_alter_keyspace_maximum_rf_warn(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'maximum_replication_factor_warn_threshold', '2'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', ''))
        with new_test_keyspace(cql, ks_opts('NetworkTopologyStrategy', 1, dc=this_dc, tablets=False)) as ks:
            response_future = cql.execute_async(f"ALTER KEYSPACE {ks}" + ks_opts('NetworkTopologyStrategy', 3, dc=this_dc))
            response_future.result()
            assert response_future.warnings is not None


def test_alter_keyspace_maximum_rf_fail(cql, this_dc):
    with ExitStack() as config_modifications:
        config_modifications.enter_context(config_value_context(cql, 'maximum_replication_factor_fail_threshold', '2'))
        config_modifications.enter_context(config_value_context(cql, 'replication_strategy_warn_list', ''))
        with new_test_keyspace(cql, ks_opts('NetworkTopologyStrategy', 1, dc=this_dc, tablets=False)) as ks:
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER KEYSPACE {ks}" + ks_opts('NetworkTopologyStrategy', 3, dc=this_dc))
