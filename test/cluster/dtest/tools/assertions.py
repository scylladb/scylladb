#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
The assertion methods in this file are used to structure, execute, and test different queries and scenarios.
Use these anytime you are trying to check the content of a table, the row count of a table, if a query should
raise an exception, etc. These methods handle error messaging well, and will help discovering and treating bugs.

An example:
Imagine some table, test:

    id | name
    1  | John Doe
    2  | Jane Doe

We could assert the row count is 2 by using:
    assert_row_count(session, 'test', 2)

After inserting [3, 'Alex Smith'], we can ensure the table is correct by:
    assert_all(session, "SELECT * FROM test", [[1, 'John Doe'], [2, 'Jane Doe'], [3, 'Alex Smith']])
or we could check the insert was successful:
    assert_one(session, "SELECT * FROM test WHERE id = 3", [3, 'Alex Smith'])

We could remove all rows in test, and assert this was sucessful with:
    assert_none(session, "SELECT * FROM test")

Perhaps we want to assert invalid queries will throw an exception:
    assert_invalid(session, "SELECT FROM test")
or, maybe after shutting down all the nodes, we want to assert an Unavailable exception is raised:
    assert_unavailable(session.execute, "SELECT * FROM test")
    OR
    assert_exception(session, "SELECT * FROM test", expected=Unavailable)

"""

import re
from collections import Counter
from time import sleep
from uuid import UUID

from cassandra import (
    ConsistencyLevel,
    InvalidRequest,
    ReadFailure,
    ReadTimeout,
    Unauthorized,
    Unavailable,
    WriteFailure,
    WriteTimeout,
)
from cassandra.query import SimpleStatement

from test.cluster.dtest.tools.retrying import retrying


def _assert_exception(fun, *args, **kwargs):
    matching = kwargs.pop("matching", None)
    expected = kwargs["expected"]
    try:
        if len(args) == 0:
            fun(None)
        else:
            fun(*args)
    except expected as e:
        if matching is not None:
            msg = str(e)
            assert re.search(matching, msg), f"Raised exception '{msg}' failed to match with '{matching}'"
    except Exception as e:
        raise e
    else:
        assert False, "Expecting query to raise an exception, but nothing was raised."


def assert_exception(session, query, matching=None, expected=None):
    if expected is None:
        assert False, "Expected exception should not be None. Your test code is wrong, please set `expected`."

    _assert_exception(session.execute, query, matching=matching, expected=expected)


def assert_invalid(session, query, matching=None, expected=InvalidRequest):
    """
    Attempt to issue a query and assert that the query is invalid.
    @param session Session to use
    @param query Invalid query to run
    @param matching Optional error message string contained within expected exception
    @param expected Exception expected to be raised by the invalid query

    Examples:
    assert_invalid(session, 'DROP USER nonexistent', "nonexistent doesn't exist")
    """
    assert_exception(session, query, matching=matching, expected=expected)


def assert_one(session, query, expected, cl=None):
    """
    Assert query returns one row.
    @param session Session to use
    @param query Query to run
    @param expected Expected results from query
    @param cl Optional Consistency Level setting. Default ONE

    Examples:
    assert_one(session, "LIST USERS", ['cassandra', True])
    assert_one(session, query, [0, 0])
    """
    from test.cluster.dtest.tools.data import rows_to_list  # to avoid cyclic dependency

    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == [expected], f"Expected {[expected]} from {query}, but got {list_res}"


@retrying(num_attempts=1, sleep_time=10)
def assert_all(
    session,
    query,
    expected,
    cl=ConsistencyLevel.ONE,
    ignore_order=False,
    num_attempts=1,
    sleep_time=10,
    result_as_string=False,
    print_result_on_failure=True,
    timeout=None,
):
    """
    Assert query returns all expected items optionally in the correct order
    @param session Session in use
    @param query Query to run
    @param expected Expected results from query
    @param cl Optional Consistency Level setting. Default ONE
    @param ignore_order Optional boolean flag determining whether response is ordered
    @param timeout Optional query timeout, in seconds
    @param num_attempts: defines how many times to try to assert data in case failure. Used in retrying decorator
    @param sleep_time: defines how many seconds to sleep between attempts. Used in retrying decorator
    @param result_as_string: return result as string
    @param print_result_on_failure print actual result in the error in case failure

    Examples:
    assert_all(session, "LIST USERS", [['aleksey', False], ['cassandra', True]])
    assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
    """
    from test.cluster.dtest.tools.data import get_list_res, rows_to_list  # to avoid cyclic dependency
    from test.cluster.dtest.tools.misc import list_to_hashed_dict  # to avoid cyclic dependency

    if result_as_string:
        list_res = get_list_res(session, query, cl, ignore_order, result_as_string, timeout=timeout)
    else:
        simple_query = SimpleStatement(query, consistency_level=cl)
        res = session.execute(simple_query) if timeout is None else session.execute(simple_query, timeout=timeout)
        list_res = rows_to_list(res)

    if ignore_order:
        expected = list_to_hashed_dict(expected)
        list_res = list_to_hashed_dict(list_res)
    error = f"Expected {expected} from {query}, but got {list_res}" if print_result_on_failure else f"Actual result ({len(list_res)} rows) is not as expected ({len(expected)} rows). Query: {query}"
    assert list_res == expected, error


def assert_almost_equal(*args, **kwargs):
    """
    Assert variable number of arguments all fall within a margin of error.
    @params *args variable number of numerical arguments to check
    @params error Optional margin of error. Default 0.16
    @params error_message Optional error message to print. Default ''

    Examples:
    assert_almost_equal(sizes[2], init_size)
    assert_almost_equal(ttl_session1, ttl_session2[0][0], error=0.005)
    """
    error = kwargs["error"] if "error" in kwargs else 0.16
    vmax = max(args)
    vmin = min(args)
    error_message = "" if "error_message" not in kwargs else kwargs["error_message"]
    assert vmin > vmax * (1.0 - error) or vmin == vmax, f"values not within {error * 100:.2f}% of the max: {args} ({error_message})"


@retrying(num_attempts=1, sleep_time=10)
def assert_row_count(
        session,
        table_name,
        expected,
        consistency_level=ConsistencyLevel.ONE,
        num_attempts=1,
        sleep_time=10,
        timeout=None,
):
    """
    Function to validate the row count expected in table_name
    @param session Session to use
    @param table_name Name of the table to query
    @param expected Number of rows expected to be in table
    @param num_attempts defines how many times to try to assert data in case failure. Used in retrying decorator
    @param sleep_time defines how many seconds to sleep between attempts. Used in retrying decorator
    @param timeout

    Examples:
    assert_row_count(self.session1, 'ttl_table', 1)
    """
    from test.cluster.dtest.tools.data import run_query_with_data_processing  # to avoid cyclic dependency

    query = f"SELECT count(*) FROM {table_name}"
    count = run_query_with_data_processing(session, query, consistency_level=consistency_level, session_timeout=timeout)
    if isinstance(count, list):
        count = count[0][0]
    assert count == expected, f"Expected a row count of {expected} in table '{table_name}', but got {count}"


@retrying(num_attempts=1, sleep_time=10)
def assert_row_count_in_select_less(
        session,
        query,
        max_rows_expected,
        consistency_level=ConsistencyLevel.ONE,
        num_attempts=1,
        timeout=None,
):
    """
    Function to validate the row count are returned by select
    :param num_attempts: defines how many times to try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    from  test.cluster.dtest.tools.data import get_list_res

    count = len(get_list_res(session, query, consistency_level, timeout=timeout))
    assert count < max_rows_expected, f'Expected a row count < of {max_rows_expected} in query "{query}", but got {count}'


def assert_length_equal(object_with_length, expected_length):
    """
    Assert an object has a specific length.
    @param object_with_length The object whose length will be checked
    @param expected_length The expected length of the object

    Examples:
    assert_length_equal(res, nb_counter)
    """
    assert len(object_with_length) == expected_length, f"Expected {object_with_length} to have length {expected_length}, but instead is of length {len(object_with_length)}"


def assert_lists_equal_ignoring_order(list1, list2, sort_key=None):
    """
    asserts that the contents of the two provided lists are equal
    but ignoring the order that the items of the lists are actually in
    :param list1: list to check if it's contents are equal to list2
    :param list2: list to check if it's contents are equal to list1
    :param sort_key: if the contents of the list are of type dict, the
    key to use of each object to sort the overall object with
    """
    normalized_list1 = []
    for obj in list1:
        normalized_list1.append(obj)

    normalized_list2 = []
    for obj in list2:
        normalized_list2.append(obj)

    if not sort_key:
        sorted_list1 = sorted(normalized_list1, key=lambda elm: elm[0])
        sorted_list2 = sorted(normalized_list2, key=lambda elm: elm[0])
    elif not sort_key == "id" and "id" in list1[0].keys():
        # first always sort by "id"
        # that way we get a two factor sort which will increase the chance of ordering lists exactly the same
        sorted_list1 = sorted(sorted(normalized_list1, key=lambda elm: elm["id"]), key=lambda elm: elm[sort_key])
        sorted_list2 = sorted(sorted(normalized_list2, key=lambda elm: elm["id"]), key=lambda elm: elm[sort_key])
    elif isinstance(list1[0]["id"], int | float):
        sorted_list1 = sorted(normalized_list1, key=lambda elm: elm[sort_key])
        sorted_list2 = sorted(normalized_list2, key=lambda elm: elm[sort_key])
    else:
        sorted_list1 = sorted(normalized_list1, key=lambda elm: str(elm[sort_key]))
        sorted_list2 = sorted(normalized_list2, key=lambda elm: str(elm[sort_key]))

    assert sorted_list1 == sorted_list2


# NOTE: the functions below are restored verbatim (imports aside) from
# scylla-dtest's tools/assertions.py; they were trimmed when this module was
# first ported in-tree, but not-yet-adapted dtest/unported test modules still
# import them.


def _rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def assert_unavailable(fun, *args, additional=None):
    """
    Attempt to execute a function, and assert Unavailable, WriteTimeout, WriteFailure,
    ReadTimeout, or ReadFailure exception is raised.
    @param fun Function to be executed
    @param *args Arguments to be passed to the function

    Examples:
    assert_unavailable(session2.execute, "SELECT * FROM ttl_table;")
    assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)
    """
    expected_list = [Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure]
    if additional:
        if isinstance(additional, list):
            expected_list.extend(additional)
        else:
            expected_list.extend(list(additional))
    _assert_exception(fun, *args, expected=tuple(expected_list))


def assert_unauthorized(session, query, message):
    """
    Attempt to issue a query, and assert Unauthorized is raised.
    @param session Session to use
    @param query Unauthorized query to run
    @param message Expected error message

    Examples:
    assert_unauthorized(session, "ALTER USER cassandra NOSUPERUSER",
                        "You aren't allowed to alter your own superuser status")
    assert_unauthorized(cathy, "ALTER TABLE ks.cf ADD val int",
                        "User cathy has no ALTER permission on <table ks.cf> or any of its parents")
    """
    assert_exception(session, query, matching=message, expected=Unauthorized)


def assert_none(session, query, cl=None):
    """
    Assert query returns nothing
    @param session Session to use
    @param query Query to run
    @param cl Optional Consistency Level setting. Default ONE

    Examples:
    assert_none(self.session1, "SELECT * FROM test where key=2;")
    assert_none(cursor, "SELECT * FROM test WHERE k=2", cl=ConsistencyLevel.SERIAL)
    """
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = _rows_to_list(res)
    assert list_res == [], f"Expected nothing from {query}, but got {list_res}"


def assert_some(session, query, cl=None, execution_profile=None):
    """
    Assert query returns something
    @param session Session to use
    @param query Query to run
    @param cl Optional Consistency Level setting. Default ONE
     Examples:
    assert_some(self.session1, "SELECT * FROM test where key=2;")
    assert_some(cursor, "SELECT * FROM test WHERE k=2", cl=ConsistencyLevel.SERIAL)
    """
    res = session.execute(query, cl=cl, execution_profile=execution_profile)
    list_res = _rows_to_list(res)
    assert list_res != [], f"Expected something from {query}, but got {list_res}"


@retrying(num_attempts=1, sleep_time=10)
def assert_one_prepared(session, stmt, expected, parameters, cl=ConsistencyLevel.ONE, timeout=60, num_attempts=1):  # noqa: PLR0913
    res = session.execute(stmt, parameters=parameters, timeout=timeout)
    list_res = _rows_to_list(res)
    assert list_res == [expected], f'Expected {[expected]} from "{stmt}", but got {list_res}'


def assert_crc_check_chance_equal(session, table, expected, ks="ks", view=False):
    """
    Assert crc_check_chance equals expected for a given table or view
    @param session Session to use
    @param table Name of the table or view to check
    @param expected Expected value to assert on that query result matches
    @param ks Optional Name of the keyspace
    @param view Optional Boolean flag indicating if the table is a view

    Examples:
    assert_crc_check_chance_equal(session, "compression_opts_table", 0.25)
    assert_crc_check_chance_equal(session, "t_by_v", 0.5, view=True)

    driver still doesn't support top-level crc_check_chance property,
    so let's fetch directly from system_schema
    """
    if view:
        assert_one(session, f"SELECT crc_check_chance from system_schema.views WHERE keyspace_name = '{ks}' AND view_name = '{table}';", [expected])
    else:
        assert_one(session, f"SELECT crc_check_chance from system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{table}';", [expected])


def assert_not_running(node):
    """
    Assert that a given node is not running
    @param node The node to check status
    """
    attempts = 0
    while node.is_running() and attempts < 10:
        sleep(1)
        attempts = attempts + 1

    assert not node.is_running()


def assert_read_timeout_or_failure(session, query):
    assert_exception(session, query, expected=(ReadTimeout, ReadFailure))


def assert_stderr_clean(err, acceptable_errors=None):
    """
    Assert that stderr is empty or that it only contains harmless messages
    @param err The stderr to clean
    @param acceptable_errors A list that if used, the user chooses what
                             messages are to be acceptable in stderr.
    """
    if acceptable_errors is None:
        acceptable_errors = [
            "WARN.*JNA link failure.*unavailable.",
            "objc.*Class JavaLaunchHelper.*?Which one is undefined.",
            # Stress tool JMX connection failure, see CASSANDRA-12437
            "Failed to connect over JMX; not collecting these stats",
        ]

    regex_str = r"^({}|\s*|\n)*$".format("|".join(acceptable_errors))
    err_str = err.strip()
    # empty string, as good as we can get for a clean stderr output!
    if not err_str:
        return

    match = re.search(regex_str, err_str)

    assert match, f"Attempted to check that stderr was empty. Instead, stderr is {err_str}, but the regex used to check stderr is {regex_str}"


def assert_bootstrap_state(tester, node, expected_bootstrap_state, user=None, password=None):
    """
    Assert that a node is on a given bootstrap state
    @param tester The dtest.Tester object to fetch the exclusive connection to the node
    @param node The node to check bootstrap state
    @param expected_bootstrap_state Bootstrap state to expect
    @param user To connect as for authenticated nodes
    @param password for corresponding user

    Examples:
    assert_bootstrap_state(self, node3, 'COMPLETED')
    """
    session = tester.patient_exclusive_cql_connection(node, user=user, password=password)
    assert_one(session, "SELECT bootstrapped FROM system.local WHERE key='local'", [expected_bootstrap_state])
    session.shutdown()


def assert_lists_of_dicts_equal(list1, list2):
    for adict, bdict in zip(list1, list2):
        assert len(adict) == len(bdict)
        for key, value in adict.items():
            assert key in bdict
            assert bdict[key] == value


@retrying(num_attempts=1, sleep_time=10)
def assert_all_or_none(  # noqa: PLR0913
    session,
    query,
    expected,
    cl=ConsistencyLevel.ONE,
    ignore_order=False,
    num_attempts=1,
    result_as_string=False,
    timeout=None,
):
    """
    :param num_attempts: defines how many time try to assert data in case failure. Used in retrying decorator
    """
    from test.cluster.dtest.tools.data import get_list_res  # to avoid cyclic dependency

    list_res = get_list_res(session, query, cl, ignore_order, result_as_string, timeout=timeout)
    if ignore_order:
        expected = sorted(expected)
    assert list_res in (expected, []), f"Expected {expected} or [] from {query}, but got {list_res}"


@retrying(num_attempts=1, sleep_time=10)
def assert_two_queries_equal(  # noqa: PLR0913
    session1,
    query1,
    session2,
    query2,
    consistency_level=ConsistencyLevel.ONE,
    session_timeout=120,
    group=False,
    groupby_column1=None,
    groupby_column2=None,
    restrict_column1=None,
    restrict_column2=None,
    restrict_value1=None,
    restrict_value2=None,
    num_attempts=1,
):
    from test.cluster.dtest.tools.data import run_query_with_data_processing  # to avoid cyclic dependency

    exp_res = run_query_with_data_processing(
        session1, query1, group=group, consistency_level=consistency_level, session_timeout=session_timeout, groupby_column=groupby_column1, restrict_column=restrict_column1, restrict_value=restrict_value1
    )
    act_res = run_query_with_data_processing(
        session2, query2, group=group, consistency_level=consistency_level, session_timeout=session_timeout, groupby_column=groupby_column2, restrict_column=restrict_column2, restrict_value=restrict_value2
    )
    assert exp_res == act_res, f"Expected {exp_res}, but got {act_res}. Query1: {query1}; Query2: {query2}"


@retrying(num_attempts=1, sleep_time=10)
def assert_two_queries_equal_ignore_order(  # noqa: PLR0913
    session1,
    query1,
    session2,
    query2,
    consistency_level=ConsistencyLevel.ONE,
    session_timeout=120,
    num_attempts=1,
):
    expected = _rows_to_list(session1.execute(query1))
    assert_all(session2, query2, expected, consistency_level, ignore_order=True)


@retrying(num_attempts=1, sleep_time=10)
def assert_row_count_in_select(  # noqa: PLR0913
    session,
    query,
    num_rows_expected,
    consistency_level=ConsistencyLevel.ONE,
    num_attempts=1,
    timeout=None,
):
    """
    Function to validate the row count are returned by select
    :param num_attempts: defines how many time try to assert data in case failure. Used in retrying decorator
    """
    from test.cluster.dtest.tools.data import get_list_res  # to avoid cyclic dependency

    count = len(get_list_res(session, query, consistency_level, timeout=timeout))
    assert count == num_rows_expected, f'Expected a row count of {num_rows_expected} in query "{query}", but got {count}'


def assert_expected_error(func, expected_error, args, kwargs):
    try:
        func(*args, **kwargs)
        assert False, "Expected failure, but function was succeeded"
    except AssertionError:
        raise
    except Exception as e:
        if expected_error in str(e):
            assert True
        else:
            raise


def assert_equal_more_with_deviation(actual, expect, deviation_perc):
    """
    Assert actual is whithin inclusive interval [extected...expected+deviation_perc]
    @param actual Value inspected
    @param expect Begining of expected interval
    @param deviation_perc allowed percent increase
    """
    deviation_high = (expect * (100 + deviation_perc)) / 100
    assert expect <= actual < deviation_high, f"Expect result interval  {expect}..{deviation_high}, received {actual}"


def assert_less_equal_lists(actual_list, expected_list, msg=None):
    """
    Assert actual_list is a subset of the expected list, prints hardcoded or paramertized error message
    @param actual_list Inspected list
    @param expected_list List that supposed to include actual_list
    @param msg Configured message default None.
    """
    standardMsg = msg or f"{actual_list} not less than or equal to {expected_list}"
    assert set(actual_list) <= set(expected_list), standardMsg


class PytestRegex:
    """Assert that a given string meets some expectations."""

    __hash__ = None

    def __init__(self, pattern, flags=0):
        self._regex = re.compile(pattern, flags)

    def __eq__(self, actual):
        return bool(self._regex.match(actual))

    def __repr__(self):
        return self._regex.pattern


class ValidUUID:
    """Assert that the given string is a valid UUID"""

    __hash__ = None

    def __eq__(self, string):
        try:
            UUID(string)
            return True
        except ValueError:
            return False


@retrying(num_attempts=1, sleep_time=10)
def assert_row_count_not_zero(session, table_name, consistency_level=ConsistencyLevel.ONE, timeout=None):
    """
    Function to validate the row count expected in table_name
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    from test.cluster.dtest.tools.data import run_query_with_data_processing  # to avoid cyclic dependency


    query = f"SELECT count(*) FROM {table_name}"
    count = run_query_with_data_processing(session, query, consistency_level=consistency_level, session_timeout=timeout)
    if isinstance(count, list):
        count = count[0][0]
    assert count > 0, "Expected that the table is not empty, but it's empty"


def assert_count_equal(actual, expected):
    """
    Basic implementation for assertCountEqual method.
    Check and verify for lift of list or int values
    """

    def covert_to_hash_type(val):
        return (str(key) if isinstance(key, int) else tuple(key) for key in val)

    assert Counter(covert_to_hash_type(actual)) == Counter(covert_to_hash_type(expected))


def assert_dict_contains_subset(subset_dict, universal_dict):
    """
    Check if $universal contains $subset_dict
    """
    return subset_dict.items() < universal_dict.items()


@retrying(num_attempts=60, sleep_time=0.5, allowed_exceptions=(AssertionError,))
def assert_eventually_raises(attempt, expected_excs, match):
    """Retry until ``attempt()`` raises one of ``expected_excs`` matching ``match``.

    Useful for eventually-consistent checks where ``attempt`` may still
    succeed against stale state.  If ``attempt`` has side effects on
    success (e.g. CREATE KEYSPACE), each invocation should use a distinct
    resource name to avoid collisions.

    ``match`` is a regex; combine alternatives with ``|``.
    """
    try:
        attempt()
    except expected_excs as ex:
        if re.search(match, str(ex)):
            return
        raise AssertionError(f"raised {type(ex).__name__} but message didn't match {match!r}: {ex}")
    raise AssertionError(f"Expected {expected_excs} but no exception was raised")
