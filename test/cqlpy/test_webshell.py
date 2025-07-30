# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import collections
import json
import pytest
import requests
import uuid
import util


def check_status(r, expected_status):
    assert r.status_code == expected_status, f"check_status(): expected={expected_status}, got={r.status_code}, response={r.text}"
    return r


@pytest.fixture(scope="module")
def webshell(host):
    yield f"http://{host}:10001"


user_credentials = collections.namedtuple("user_credentials", ["username", "password"])


@pytest.fixture(scope="module")
def credentials(request):
    return user_credentials(request.config.getoption("--auth_username") or "cassandra",
                            request.config.getoption("--auth_password") or "cassandra")


class Session:
    def __init__(self, webshell, session):
        self.webshell = webshell
        self.session = session

    def check_option(self, option, expected_status=200):
        if isinstance(option, tuple):
            option = f"{option[0]} {option[1]}"
        return check_status(requests.post(self.webshell + "/option", cookies=self.session, data=f"{option}"), expected_status).text


    def check_query(self, query, expected_status=200):
        return check_status(requests.post(self.webshell + "/query", cookies=self.session, data=query), expected_status).text


class new_session:
    def __init__(self, webshell, credentials):
        self.webshell = webshell
        self.credentials = credentials

    def __enter__(self):
        login = requests.post(self.webshell + "/login", data=f"{self.credentials.username}\n{self.credentials.password}")
        check_status(login, 200)
        return Session(self.webshell, login.cookies)

    def __exit__(self, exc_type, exc_value, traceback):
        requests.post(self.webshell + "/logout")


@pytest.fixture(scope="function")
def session(webshell, credentials):
    with new_session(webshell, credentials) as session:
        yield session


@pytest.mark.parametrize("endpoint", ["query", "option"])
def test_no_session(webshell, endpoint):
    r = requests.post(webshell + f"/{endpoint}")
    check_status(r, 401)


@pytest.mark.parametrize("endpoint", ["query", "option"])
def test_bad_session_id(webshell, endpoint):
    r = requests.post(webshell + f"/{endpoint}", cookies={"session_id": "bad_session_id"})
    check_status(r, 401)


@pytest.mark.parametrize("endpoint", [
    ("query", "select * from system.local"),
    ("option", "output-format text"),
])
def test_inexistent_session_id(webshell, session, endpoint):
    endpoint, data = endpoint
    session_id = session.session["session_id"]
    modified_first_digit = str((int(session_id[0], base=16) + 1) % 16)

    check_status(requests.post(webshell + f"/{endpoint}", cookies={"session_id": session_id}, data=data), 200)

    check_status(requests.post(webshell + f"/{endpoint}", cookies={"session_id": "bad_session_id"}, data=data), 401)
    check_status(requests.post(webshell + f"/{endpoint}", cookies={"session_id": modified_first_digit + session_id[1:]}, data=data), 401)


def test_login(webshell, credentials):
    r = requests.post(webshell + "/login", data=f"{credentials.username}\n{credentials.password}")
    check_status(r, 200)
    assert "session_id" in r.cookies


def test_login_no_credentials(webshell):
    r = requests.post(webshell + "/login")
    check_status(r, 400)


def test_login_badly_formed_credentials(webshell, credentials):
    username, password = credentials

    r = requests.post(webshell + "/login", data=f"{username}")
    check_status(r, 400)

    r = requests.post(webshell + "/login", data=f"{username}\nsomething\n{password}")
    check_status(r, 400)

    r = requests.post(webshell + "/login", data=f"{username}\n{password}\nsomething")
    check_status(r, 400)


def test_login_bad_credentials(webshell, credentials):
    check_status(requests.post(webshell + "/login", data=f"{credentials.username}\n{credentials.password}"), 200)
    check_status(requests.post(webshell + "/login", data=f"{credentials.username}foo\n{credentials.password}"), 400)
    check_status(requests.post(webshell + "/login", data=f"{credentials.username}\n{credentials.password}bar"), 400)


def test_logout(webshell, credentials):
    login = check_status(requests.post(webshell + "/login", data="\n".join(credentials)), 200)

    check_status(requests.post(webshell + "/query", cookies=login.cookies, data="SELECT * FROM system.local"), 200)

    check_status(requests.post(webshell + "/logout", cookies=login.cookies), 200)

    check_status(requests.post(webshell + "/query", cookies=login.cookies, data="SELECT * FROM system.local"), 401)


def test_logout_no_session(webshell):
    check_status(requests.post(webshell + "/logout"), 200)


@pytest.mark.parametrize("option", [
    ("output-format", ("text", "json")),
])
def test_options(session, option):
    option_name, option_values = option
    for option_value in option_values:
        session.check_option((option_name, option_value))


def test_invalid_option(session):
    session.check_option("output-format text", 200)
    session.check_option("output-format json", 200)

    session.check_option("output-format xml", 400)
    session.check_option("output text", 400)
    session.check_option("output-format-text", 400)
    session.check_option("output-format=text", 400)
    session.check_option("output-format text json", 400)


def test_query(test_keyspace, session):
    table = util.unique_name()

    session.check_query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY, value text)")
    session.check_query(f"INSERT INTO {test_keyspace}.{table} (id, value) VALUES (1, 'foo')")
    session.check_query(f"INSERT INTO {test_keyspace}.{table} (id, value) VALUES (2, 'bar')")

    session.check_option("output-format json")

    res = json.loads(session.check_query(f"SELECT * FROM {test_keyspace}.{table}"))
    assert res == [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]

    session.check_query(f"DROP TABLE {test_keyspace}.{table}")


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
    session.check_option("output-format json")

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
    assert json.loads(session.check_query(f"SELECT * FROM {test_keyspace}.{staff_table}")) == [{"id": 1, "value": "foo"}]
    assert json.loads(session.check_query(f"SELECT * FROM {test_keyspace}.{secret_table}")) == [{"id": 9, "value": "top-secret"}]

    with new_session(webshell, user_credentials(user, password)) as user_session:
        user_session.check_option("output-format json")

        assert json.loads(user_session.check_query(f"SELECT * FROM {test_keyspace}.{staff_table}")) == [{"id": 1, "value": "foo"}]

        # Attempt to read table without permissions should result in distinct status code 403
        user_session.check_query(f"SELECT * FROM {test_keyspace}.{secret_table}", 403)

    session.check_query(f"DROP ROLE {user}")
    session.check_query(f"DROP ROLE staff")
    session.check_query(f"DROP TABLE {test_keyspace}.{staff_table}")
    session.check_query(f"DROP TABLE {test_keyspace}.{secret_table}")
