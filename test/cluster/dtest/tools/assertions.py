#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

from cassandra import ConsistencyLevel, InvalidRequest
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
