#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import re
from cassandra import (
    InvalidRequest,
)

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


