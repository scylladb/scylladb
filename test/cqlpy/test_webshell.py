# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import collections
import json
import math
import pytest
import requests
import time
import uuid
import util


def check_status(r, expected_status):
    assert r.status_code == expected_status, f"check_status(): expected={expected_status}, got={r.status_code}, response={r.text}"
    return r


def with_retry(func, retries=20, delay=0.1):
    for i in range(0, retries):
        success, result = func()
        if success:
            return result
        time.sleep(delay)
    return None


@pytest.fixture(scope="module")
def webshell_log_level(scylla_only, host):
    url = f'http://{host}:10000'
    requests.post(f'{url}/system/logger/webshell', params={'level' : 'trace'})
    yield
    requests.post(f'{url}/system/logger/webshell', params={'level' : 'info'})


@pytest.fixture(scope="module")
def webshell(host, webshell_log_level):
    yield f"http://{host}:10001"


user_credentials = collections.namedtuple("user_credentials", ["username", "password"])


@pytest.fixture(scope="module")
def credentials(request):
    return user_credentials(request.config.getoption("--auth_username") or "cassandra",
                            request.config.getoption("--auth_password") or "cassandra")


class Session:
    def _reset(self):
        self.cookies = None
        self.paging_state = None
        self.trace_session_id = None

    def __init__(self, webshell):
        self.webshell = webshell
        self._reset()

    def login(self, credentials):
        if self.cookies is not None:
            self.logout()

        login = requests.post(self.webshell + "/login", data=json.dumps(credentials._asdict()))
        check_status(login, 200)
        self.cookies = login.cookies

    def logout(self):
        check_status(requests.post(self.webshell + "/logout", cookies=self.cookies), 200)
        self._reset()

    def check_command(self, command, *args, expected_status=200):
        data = json.dumps({"command": command, "arguments": args})
        res = check_status(requests.post(self.webshell + "/command", cookies=self.cookies, data=data), expected_status)
        self.cookies = res.cookies
        return res.json()["response"]

    def check_option(self, option, *args, expected_status=200):
        data = json.dumps({"option": option, "arguments": args})
        res = check_status(requests.post(self.webshell + "/option", cookies=self.cookies, data=data), expected_status)
        self.cookies = res.cookies
        return res.json()["response"]

    def check_query(self, query, expected_status=200):
        data = json.dumps({"query": query.replace('"', '\\"'), "paging_state": self.paging_state})
        res = check_status(requests.post(self.webshell + "/query", cookies=self.cookies, data=data), expected_status)
        self.cookies = res.cookies
        res_content = res.json()
        self.paging_state = res_content.get("paging_state", None)
        self.trace_session_id = res_content.get("trace_session_id", None)
        return res_content["response"]


class new_session:
    def __init__(self, webshell, credentials):
        self.webshell = webshell
        self.credentials = credentials

    def __enter__(self):
        self.session = Session(self.webshell)
        self.session.login(self.credentials)
        return self.session

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.logout()


@pytest.fixture(scope="function")
def session(webshell, credentials):
    with new_session(webshell, credentials) as session:
        yield session


@pytest.fixture(scope="module")
def test_table(cql, test_keyspace):
    table = util.unique_name()
    full_name = f"{test_keyspace}.{table}"

    cql.execute(f"CREATE TABLE {full_name} (id int PRIMARY KEY, value text)")

    for i in range(0, 10):
        cql.execute(f"INSERT INTO {full_name} (id, value) VALUES ({i}, 'val{i}')")

    data = cql.execute(f"SELECT JSON * FROM {full_name}")
    data = list(map(lambda row: json.loads(row[0]), data))
    yield full_name, data

    cql.execute(f"DROP TABLE {full_name}")


@pytest.mark.parametrize("endpoint", ["query", "option"])
def test_no_session(webshell, endpoint):
    r = requests.post(webshell + f"/{endpoint}")
    check_status(r, 401)


@pytest.mark.parametrize("endpoint", ["query", "option"])
def test_bad_session_id(webshell, endpoint):
    r = requests.post(webshell + f"/{endpoint}", cookies={"session_id": "bad_session_id"})
    check_status(r, 401)


@pytest.mark.parametrize("endpoint", [
    ("query", json.dumps({"query": "select * from system.local"})),
    ("option", json.dumps({"option": "output format", "arguments": ["text"]})),
    ("command", json.dumps({"command": "help", "arguments": []})),
])
def test_inexistent_session_id(webshell, session, endpoint):
    endpoint, data = endpoint
    session_id = session.cookies["session_id"]
    modified_first_digit = str((int(session_id[0], base=16) + 1) % 16)

    check_status(requests.post(webshell + f"/{endpoint}", cookies={"session_id": session_id}, data=data), 200)

    check_status(requests.post(webshell + f"/{endpoint}", cookies={"session_id": "bad_session_id"}, data=data), 401)
    check_status(requests.post(webshell + f"/{endpoint}", cookies={"session_id": modified_first_digit + session_id[1:]}, data=data), 401)


def test_login(webshell, credentials):
    r = requests.post(webshell + "/login", data=json.dumps(credentials._asdict()))
    check_status(r, 200)
    assert "session_id" in r.cookies


def test_login_no_credentials(webshell):
    r = requests.post(webshell + "/login")
    check_status(r, 400)


def test_login_badly_formed_credentials(webshell, credentials):
    username, password = credentials

    r = requests.post(webshell + "/login", data=f'{{"username": {username}')
    check_status(r, 400)

    r = requests.post(webshell + "/login", data=json.dumps({"password": credentials.password}))
    check_status(r, 400)

    r = requests.post(webshell + "/login", data=json.dumps({"username": credentials.username}))
    check_status(r, 400)

    r = requests.post(webshell + "/login", data=json.dumps({"username": 1, "password": credentials.password}))
    check_status(r, 400)

    r = requests.post(webshell + "/login", data=json.dumps({"username": credentials.username, "password": 2}))
    check_status(r, 400)


def test_login_bad_credentials(webshell, credentials):
    check_status(requests.post(webshell + "/login", data=json.dumps(credentials._asdict())), 200)
    check_status(requests.post(webshell + "/login", data=json.dumps({"username": f"{credentials.username}foo", "password": credentials.password})), 400)
    check_status(requests.post(webshell + "/login", data=json.dumps({"username": credentials.username, "password": f"{credentials.password}bar"})), 400)


def test_logout(webshell, credentials):
    login = check_status(requests.post(webshell + "/login", data=json.dumps(credentials._asdict())), 200)

    check_status(requests.post(webshell + "/query", cookies=login.cookies, data=json.dumps({"query": "SELECT * FROM system.local"})), 200)

    check_status(requests.post(webshell + "/logout", cookies=login.cookies), 200)

    check_status(requests.post(webshell + "/query", cookies=login.cookies, data=json.dumps({"query": "SELECT * FROM system.local"})), 401)


def test_logout_no_session(webshell):
    check_status(requests.post(webshell + "/logout"), 200)


def test_command_help(session):
    assert session.check_command("help") == """ScyllaDB WebShell

!!! WebShell is still experimental, things are subject to change and there may be bugs !!!

For more information, see https://docs.scylladb.com/manual/master/operating-scylla/admin-tools/webshell.html.

Available commands:
 * HELP - show this message.
 * SHOW [SESSION <tracing-session-id>] - show tracing session events for the provided tracing session id.

Available options:
 * CONSISTENCY [<level>] - set default consistency level for queries, with no args show current setting (default: ONE).
 * EXPAND [ON|OFF] - enable/disable expanded (vertical) output, with no args show current setting (default: OFF).
 * OUTPUT FORMAT [TEXT|JSON] - set output format, with no args show current setting (default: TEXT).
 * PAGING [ON|OFF|<number>] - enable/disable/limit result paging, with no args show current setting (default: 100).
 * SERIAL CONSISTENCY [<level>] - set default serial consistency level for queries, with no args show current setting (default: SERIAL).
 * TRACING [ON|OFF] - enable/disable query tracing, with no args show current setting (default: OFF).
"""


def test_command_show_session(session):
    session.check_option("tracing", "on")
    session.check_option("output format", "json")

    query = "SELECT * FROM system.local"

    res = session.check_query(query)
    assert len(res) == 1

    trace_session_id = session.trace_session_id
    assert trace_session_id != ""

    def load_tracing_session():
        res = session.check_command("SHOW SESSION", trace_session_id)
        if len(res.split('\n')) <= 3:
            return False, ''
        return True, res

    tracing_events = with_retry(load_tracing_session)
    assert len(tracing_events.split('\n')) > 3

    assert session.check_option("tracing") == "Tracing is currently enabled. Use TRACING OFF to disable."
    assert session.check_option("output format") == "JSON"

    assert session.check_command("show", expected_status=400) == "Unrecognized command: show"
    assert session.check_command("show bar", expected_status=400) == "Unrecognized command: show bar"
    assert session.check_command("show session", trace_session_id, "bar", expected_status=400) == "Invalid SHOW command, expected 'SHOW SESSION <tracing_session_id>'."
    assert session.check_command("show session", "bar", expected_status=400) #FIXME: validate UUID on server side


def test_nonexistent_command(session):
    session.check_command("foo", expected_status=400)
    session.check_command("foo bar", expected_status=400)


def test_option_consistency(session):
    # default
    assert session.check_option("consistency") == "Current consistency level is ONE."

    consistency_levels = ["ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "SERIAL", "LOCAL_SERIAL", "LOCAL_ONE"]
    for level in consistency_levels:
        assert session.check_option("consistency", level.lower()) == f"Consistency level set to {level}."
        assert session.check_option("consistency") == f"Current consistency level is {level}."

    assert session.check_option("consistency", "foo", expected_status=400) == f"Invalid CONSISTENCY argument, expected {', '.join(consistency_levels[:-1])} or {consistency_levels[-1]}."

    assert session.check_option("consistency", "foo", "bar", expected_status=400) == "Invalid CONSISTENCY option, expected 'CONSISTENCY [<consistency_level>]'."


def test_option_expand(session):
    # default
    assert session.check_option("expand") == "Expanded output is currently disabled. Use EXPAND ON to enable."

    assert session.check_option("expand", "on") == "Now Expanded output is enabled."
    assert session.check_option("expand", "on", expected_status=400) == "Expanded output is already enabled. Use EXPAND OFF to disable."
    assert session.check_option("expand") == "Expanded output is currently enabled. Use EXPAND OFF to disable."

    assert session.check_option("expand", "off") == "Disabled Expanded output."
    assert session.check_option("expand", "off", expected_status=400) == "Expanded output is not enabled."
    assert session.check_option("expand") == "Expanded output is currently disabled. Use EXPAND ON to enable."

    assert session.check_option("expand", "foo", expected_status=400) == "Invalid EXPAND argument, expected ON or OFF."
    assert session.check_option("expand", "foo", "bar", expected_status=400) == "Invalid EXPAND option, expected 'EXPAND [ON|OFF]'."


def test_option_paging(session):
    # default
    assert session.check_option("paging") == "Query paging is currently enabled. Use PAGING OFF to disable.\nPage size: 100."

    session.check_option("paging", "off")
    assert session.check_option("paging") == "Query paging is currently disabled. Use PAGING ON to enable."

    assert session.check_option("paging", "off", expected_status=400) == "Query paging is not enabled."

    assert session.check_option("paging", "on") == "Now query paging is enabled.\nPage size: 100."
    assert session.check_option("paging") == "Query paging is currently enabled. Use PAGING OFF to disable.\nPage size: 100."

    assert session.check_option("paging", "on", expected_status=400) == "Query paging is already enabled. Use PAGING OFF to disable."

    assert session.check_option("paging", "200") == "Page size: 200."
    assert session.check_option("paging") == "Query paging is currently enabled. Use PAGING OFF to disable.\nPage size: 200."

    # Not an error to set the already set page size
    assert session.check_option("paging", "200") == "Page size: 200."

    assert session.check_option("paging", "300") == "Page size: 300."
    assert session.check_option("paging") == "Query paging is currently enabled. Use PAGING OFF to disable.\nPage size: 300."

    # Setting page size to <= 0 disables paging
    assert session.check_option("paging", "0") == "Page size: 0."
    assert session.check_option("paging") == "Query paging is currently disabled. Use PAGING ON to enable."

    assert session.check_option("paging", "-1") == "Page size: -1."
    assert session.check_option("paging") == "Query paging is currently disabled. Use PAGING ON to enable."

    assert session.check_option("paging", "-991") == "Page size: -991."
    assert session.check_option("paging") == "Query paging is currently disabled. Use PAGING ON to enable."

    assert session.check_option("paging", "-2147483648") == "Page size: -2147483648."
    assert session.check_option("paging") == "Query paging is currently disabled. Use PAGING ON to enable."

    # Maxint - it is 64 bit signed max int, because of a limitation in std::stoull
    # makes it more convenient to use std:stoll in the implementation.
    # This is inconsequential, such page sizes are not practical anyway.
    assert session.check_option("paging", "2147483647") == "Page size: 2147483647."

    # Not a number
    assert session.check_option("paging", "bar", expected_status=400) == "Page size must be a number."

    # Negative page size disables paging

    # Overflow
    assert session.check_option("paging", "2147483648", expected_status=400) == "Page size must be a 32 bit integer."
    assert session.check_option("paging", "-2147483649", expected_status=400) == "Page size must be a 32 bit integer."

    # Too many args
    assert session.check_option("paging", "10", "20", expected_status=400) == "Invalid PAGING option, expected 'PAGING [ON|OFF|<number>]'."


def test_option_serial_consistency(session):
    # default
    assert session.check_option("serial consistency") == "Current serial consistency level is SERIAL."

    consistency_levels = ["SERIAL", "LOCAL_SERIAL"]
    for level in consistency_levels:
        assert session.check_option("serial consistency", level.lower()) == f"Serial consistency level set to {level}."
        assert session.check_option("serial consistency") == f"Current serial consistency level is {level}."

    assert session.check_option("serial", expected_status=400) == "Unrecognized option: serial"

    assert session.check_option("serial foo", expected_status=400) == "Unrecognized option: serial foo"

    assert session.check_option("serial consistency", "foo", expected_status=400) == f"Invalid SERIAL CONSISTENCY argument, expected {', '.join(consistency_levels[:-1])} or {consistency_levels[-1]}."

    assert session.check_option("serial consistency", "foo", "bar", expected_status=400) == "Invalid SERIAL CONSISTENCY option, expected 'SERIAL CONSISTENCY [<serial_consistency_level>]'."


def test_option_tracing(session):
    # default
    assert session.check_option("tracing") == "Tracing is currently disabled. Use TRACING ON to enable."

    assert session.check_option("tracing", "on") == "Now tracing is enabled."
    assert session.check_option("tracing") == "Tracing is currently enabled. Use TRACING OFF to disable."

    assert session.check_option("tracing", "off") == "Disabled Tracing."
    assert session.check_option("tracing") == "Tracing is currently disabled. Use TRACING ON to enable."

    assert session.check_option("tracing", "off", expected_status=400) == "Tracing is not enabled."

    assert session.check_option("tracing", "on") == "Now tracing is enabled."
    assert session.check_option("tracing", "on", expected_status=400) == "Tracing is already enabled. Use TRACING OFF to disable."

    assert session.check_option("tracing", "on off", expected_status=400) == "Invalid TRACING option, expected 'TRACING [ON|OFF]'."
    assert session.check_option("tracing", "foo", expected_status=400) == "Invalid TRACING option, expected 'TRACING [ON|OFF]'."


def test_option_output_format(session):
    # default
    assert session.check_option("output format") == "TEXT"

    session.check_option("output format", "json") == "Output format set to JSON."
    assert session.check_option("output format") == "JSON"

    session.check_option("output format", "text") == "Output format set to TEXT."
    assert session.check_option("output format") == "TEXT"

    assert session.check_option("output format", "foo", expected_status=400) == "Invalid OUTPUT FORMAT argument, expected TEXT or JSON."
    assert session.check_option("output format", "foo", "bar", expected_status=400) == "Invalid OUTPUT FORMAT option, expected 'OUTPUT FORMAT [TEXT|JSON]'."


def test_nonexistent_option(session):
    session.check_option("foo", expected_status=400)
    session.check_option("foo", "bar", expected_status=400)


def test_query(test_keyspace, session):
    table = util.unique_name()

    session.check_query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY, value text)")
    session.check_query(f"INSERT INTO {test_keyspace}.{table} (id, value) VALUES (1, 'foo')")
    session.check_query(f"INSERT INTO {test_keyspace}.{table} (id, value) VALUES (2, 'bar')")

    session.check_option("output format", "json")

    res = session.check_query(f"SELECT * FROM {test_keyspace}.{table}")
    assert res == [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]

    session.check_query(f"DROP TABLE {test_keyspace}.{table}")


def test_query_expand(session):
    pass # TODO


class json_pager:
    def __init__(self, session, query, page_size):
        self.session = session
        self.query = query
        self.page_size = page_size

        self.pages = 0

    def expected_page_count(self, total_rows):
        # If there is no remainder, there will be an extra empty page at the end
        return total_rows // self.page_size + 1

    def cancel(self):
        self.session.paging_state = None

    def set_page_size(self, page_size):
        self.page_size = page_size
        self.session.check_option("paging", str(self.page_size))

    def __iter__(self):
        self.session.check_option("output format", "json")
        self.session.check_option("paging", str(self.page_size))

        self.pages = 0
        page = []
        has_more_pages = True

        while has_more_pages:
            if page:
                yield page[0]
                del page[0]
                continue

            if self.pages == 0 or self.session.paging_state is not None:
                page = self.session.check_query(self.query)
                assert len(page) <= self.page_size
                self.pages += 1
            else:
                has_more_pages = False


def test_query_paging(session, test_table):
    table_name, table_data = test_table

    for page_size in (1, 2, 3, 4, 5, len(table_data) - 1, len(table_data), len(table_data) * 2):
        pager = json_pager(session, f"SELECT * FROM {table_name}", page_size)
        results = list(pager)
        assert results == table_data
        assert pager.pages == pager.expected_page_count(len(table_data))


def consume_n(it, n):
    data = []
    for _ in range(n):
        data.append(next(it))
    return data

# Clear paging_state between pages
# Unlikely scenario, but should be handled gracefully
def test_query_paging_cancel(session, test_table):
    table_name, table_data = test_table

    pager = json_pager(session, f"SELECT * FROM {table_name}", 2)
    it = iter(pager)
    data = consume_n(it, 2)
    assert data == table_data[0:2]
    assert pager.pages == 1

    # Cancelling paging resets the query to the beginning
    pager.cancel()

    page = session.check_query(pager.query)
    assert list(page) == table_data[0:2]


def test_query_paging_switch_page_size(session, test_table):
    table_name, table_data = test_table

    pager = json_pager(session, f"SELECT * FROM {table_name}", 2)
    original_expected_pages = pager.expected_page_count(len(table_data))
    it = iter(pager)
    data = consume_n(it, 2)
    assert data == table_data[0:2]
    assert pager.pages == 1

    pager.set_page_size(4)

    rest = list(it)
    assert rest == table_data[2:]
    assert pager.pages < original_expected_pages
    assert pager.pages > pager.expected_page_count(len(table_data))


def test_query_paging_switch_output_format(session, test_table):
    table_name, table_data = test_table

    pager = json_pager(session, f"SELECT * FROM {table_name}", 2)
    original_expected_pages = pager.expected_page_count(len(table_data))
    it = iter(pager)
    data = consume_n(it, 2)
    assert data == table_data[0:2]
    assert pager.pages == 1

    session.check_option("output format", "text")

    page = session.check_query(pager.query)
    assert len(page.strip().split("\n")) <= 4 # 2 rows + header + separator


# Switching query while in the middle of paging, should reset paging.
# Client is expected to clear paging_state in this case, but since webshell is
# interactive (so the other side could be a human), they might forget.
def test_query_paging_switch_query(session, test_table):
    table_name, table_data = test_table

    session.check_option("output format", "json")
    session.check_option("paging", "2")

    columns_page = session.check_query("SELECT * FROM system_schema.columns")

    pager = json_pager(session, f"SELECT * FROM {table_name}", 2)
    original_expected_pages = pager.expected_page_count(len(table_data))
    it = iter(pager)
    data = consume_n(it, 2)
    assert data == table_data[0:2]
    assert pager.pages == 1

    assert session.check_query("SELECT * FROM system_schema.columns") == columns_page

    # Attempt to resume previous paged query, should start from the beginning
    page = session.check_query(pager.query)
    assert page == table_data[0:2]


def test_query_tracing(session):
    session.check_option("tracing", "on")
    session.check_option("output format", "json")

    query = "SELECT * FROM system.local"

    res = session.check_query(query)
    assert len(res) == 1

    trace_session_id = session.trace_session_id
    assert trace_session_id != ""

    def load_tracing_session():
        res = session.check_query(f"SELECT * FROM system_traces.sessions WHERE session_id = {trace_session_id}")
        if len(res) == 0:
            return False, []
        return True, res

    tracing_session = with_retry(load_tracing_session)
    assert len(tracing_session) == 1
    assert tracing_session[0]["request"] == "Execute webshell query"
    parameters = tracing_session[0]["parameters"]
    assert parameters['query'] == query
    assert parameters['session_options'] == '{consistency=ONE, expand=false, page_size=100, serial_consistency=SERIAL, tracing=true, output_format=json}'

    tracing_events = session.check_query(f"SELECT * FROM system_traces.events WHERE session_id = {trace_session_id}")
    assert len(tracing_events) > 0


def test_query_output_format(session, test_table):
    table_name, table_data = test_table

    session.check_option("output format", "json")

    res = session.check_query(f"SELECT * FROM {table_name}")
    assert res == table_data

    session.check_option("output format", "text")

    res = session.check_query(f"SELECT * FROM {table_name}")
    lines = res.split("\n")
    if lines[-1] == "":
        del lines[-1]
    assert len(lines) == len(table_data) + 2 # first row is column names, second row is header/body separator
    column_names = list(map(str.strip, lines[0].split("|")))

    for i, line in enumerate(lines[2:]):
        column_values = list(map(str.strip, line.split("|")))
        for col, val in zip(column_names, column_values):
            assert str(table_data[i][col]) == val


# DDL statements (like CREATE TABLE) are bounced to shard 0.
# Test that this is handled correctly by webshell.
def test_ddl_bounce_to_shard0(webshell, credentials, test_keyspace):
    # We need a webshell session on a non-0 shard to test bounce to shard handling.
    # The webshell session is created on the shard which is assigned to the client port.
    # This assignment is random (from the test's point of view), so we need to
    # keep trying until we hit the right shard.
    while True:
        with new_session(webshell, credentials) as session:
            session.check_option("output format", "json")

            data = session.check_query("SELECT address, port, client_type, shard_id FROM system.clients WHERE client_type = 'webshell' ALLOW FILTERING")

            found_shard_non_0 = False
            for entry in data:
                if entry["shard_id"] > 0:
                    found_shard_non_0 = True
                    break

            if not found_shard_non_0:
                continue

            table = util.unique_name()

            # if bounce to shard is not handled, this will return 500
            session.check_query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY)")

            # mix in some non-bounced queries to ensure session is still usable
            for i in range(0, 10):
                session.check_query(f"INSERT INTO {test_keyspace}.{table} (id) VALUES ({i})")
                assert session.check_query(f"SELECT * FROM {test_keyspace}.{table} WHERE id = {i}") == [{"id": i}]

            session.check_query(f"DROP TABLE {test_keyspace}.{table}")

            # test passed
            break


def test_bad_query(test_keyspace, session):
    table = util.unique_name()

    session.check_query("SELECT * FROM system.local")

    session.check_query("SELEKT * FROM system.local", 400)
    session.check_query("SELECT * FROM bar.local", 400)
    session.check_query("SELECT * FROM system.foo", 400)

    session.check_query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY, value text)")
    session.check_query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY, value text)", 400)

    session.check_query(f"DROP TABLE {test_keyspace}.{table}")
    session.check_query(f"DROP TABLE {test_keyspace}.{table}", 400)


def test_query_permissions(test_keyspace, webshell, session):
    session.check_option("output format", "json")

    user = util.unique_name()
    password = "very_secure_password123!!"

    staff_table = util.unique_name()
    secret_table = util.unique_name()

    session.check_query(f"CREATE TABLE {test_keyspace}.{staff_table} (id int PRIMARY KEY, value text)")
    session.check_query(f"INSERT INTO {test_keyspace}.{staff_table} (id, value) VALUES (1, 'foo')")

    session.check_query(f"CREATE TABLE {test_keyspace}.{secret_table} (id int PRIMARY KEY, value text)")
    session.check_query(f"INSERT INTO {test_keyspace}.{secret_table} (id, value) VALUES (9, 'top-secret')")

    session.check_query(f"CREATE ROLE staff")
    session.check_query(f"GRANT SELECT ON {test_keyspace}.{staff_table} TO staff")

    session.check_query(f"CREATE ROLE {user} WITH PASSWORD = '{password}' AND LOGIN = true")
    session.check_query(f"GRANT staff TO {user}")

    # Admin session can read both tables
    assert session.check_query(f"SELECT * FROM {test_keyspace}.{staff_table}") == [{"id": 1, "value": "foo"}]
    assert session.check_query(f"SELECT * FROM {test_keyspace}.{secret_table}") == [{"id": 9, "value": "top-secret"}]

    with new_session(webshell, user_credentials(user, password)) as user_session:
        user_session.check_option("output format", "json")

        assert user_session.check_query(f"SELECT * FROM {test_keyspace}.{staff_table}") == [{"id": 1, "value": "foo"}]

        # Attempt to read table without permissions should result in distinct status code 403
        user_session.check_query(f"SELECT * FROM {test_keyspace}.{secret_table}", 403)

    session.check_query(f"DROP ROLE {user}")
    session.check_query(f"DROP ROLE staff")
    session.check_query(f"DROP TABLE {test_keyspace}.{staff_table}")
    session.check_query(f"DROP TABLE {test_keyspace}.{secret_table}")
