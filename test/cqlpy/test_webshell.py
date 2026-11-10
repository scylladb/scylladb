# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the Web Shell (tools/webshell): an HTTP API for executing CQL
# queries, and the web interface it serves.
#
# Every request is authenticated on its own, and carries all the options of its
# query.

import base64
import collections
import datetime
import json
import time

import pytest
import requests

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from test import TEST_DIR
from test.pylib.skip_types import skip_env
from .util import unique_name
from .test_service_levels import new_service_level

# The Web Shell's HTTPS listeners use the certificate test/pylib installs into
# every test server's conf/ directory - a self-signed certificate for
# "example.com", which cannot be validated against the address the tests
# connect to. Validating the server certificate is not what these tests are
# about, so they don't (verify=False below). The warning urllib3 emits about
# that is filtered out in test/pytest.ini.
#
# The truststore the listeners validate client certificates against is a copy
# of the very same self-signed certificate, which is a CA certificate (see
# ScyllaServer._write_config_file() in test/pylib/scylla_server.py). So the
# client certificates the server accepts are the ones signed with the matching
# private key, which the tests have access to.
ca_certificate_file = TEST_DIR / "pylib" / "resources" / "scylla.crt"
ca_key_file = TEST_DIR / "pylib" / "resources" / "scylla.key"

server_tls_options = {
    "certificate": "conf/scylla.crt",
    "keyfile": "conf/scylla.key",
}

# The Web Shell listeners the tests set up, by name, with their port and TLS
# options - None for plain HTTP. The HTTPS listeners differ in whether they ask
# the client for a certificate.
webshell_listeners = {
    "webshell_http": (10010, None),
    # A client certificate is requested, but not required: clients with one are
    # authenticated by it, the others by the Authorization header.
    "webshell_https": (10011, {**server_tls_options, "truststore": "conf/scyllacadb.pem", "require_client_auth": "optional"}),
    # A client certificate is required, clients without one fail the TLS handshake.
    "webshell_https_mtls": (10012, {**server_tls_options, "truststore": "conf/scyllacadb.pem", "require_client_auth": "true"}),
    # No client certificate is requested.
    "webshell_https_no_client_auth": (10013, server_tls_options),
}


def config_value(cql, name):
    """Return the current value of the given config item.

    The lookup uses a prepared statement on purpose: system.config is a
    shard-aware virtual table, so an unprepared point query is not routed to
    the shard owning the row and finds nothing.
    """
    select = cql.prepare("SELECT value FROM system.config WHERE name=?")
    return json.loads(cql.execute(select, (name,)).one().value)


def as_cql_literal(value):
    """The value as a CQL literal, as ALTER CLUSTER WITH listeners = ... takes it."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k}: {as_cql_literal(v)}" for k, v in value.items()) + "}"
    return "'" + str(value).replace("'", "''") + "'"


def check_status(r, expected_status):
    assert r.status_code == expected_status, f"check_status(): expected={expected_status}, got={r.status_code}, response={r.text}"
    return r


def with_retry(func, retries=100, delay=0.1):
    """Retry func until it reports success.

    Tracing sessions are written out on a timer (tracing::write_period, 2
    seconds), so the default budget comfortably exceeds that.
    """
    for _ in range(retries):
        success, result = func()
        if success:
            return result
        time.sleep(delay)
    pytest.fail(f"with_retry(): {func.__name__} did not succeed in {retries * delay} seconds")


user_credentials = collections.namedtuple("user_credentials", ["username", "password"])


def basic_authorization(credentials):
    token = base64.b64encode(f"{credentials.username}:{credentials.password}".encode()).decode()
    return f"Basic {token}"


class webshell_endpoint:
    """A Web Shell listener, together with what is needed to talk to it.

    query() authenticates with the endpoint's credentials, by default. Pass
    credentials=None to send no Authorization header.
    """
    def __init__(self, url, credentials, client_certificate=None):
        self.url = url
        self.is_https = url.startswith("https://")
        self.credentials = credentials
        self.client_certificate = client_certificate

    def with_client_certificate(self, client_certificate):
        """A copy of this endpoint, presenting the given TLS client certificate."""
        return webshell_endpoint(self.url, self.credentials, client_certificate)

    def request(self, method, path, **kwargs):
        if self.is_https:
            kwargs.setdefault("verify", False)
            if self.client_certificate is not None:
                kwargs.setdefault("cert", self.client_certificate)
        return requests.request(method, self.url + path, **kwargs)

    def post_query(self, body, credentials="default", headers=None, **kwargs):
        """Send a raw /query request, body is sent as JSON, unless it is a string."""
        headers = dict(headers or {})
        if credentials == "default":
            credentials = self.credentials
        if credentials is not None:
            headers["Authorization"] = basic_authorization(credentials)
        # A dict or a list is sent as JSON, anything else (a string, a
        # generator for a chunked body) as it is.
        data = json.dumps(body) if isinstance(body, (dict, list)) else body
        return self.request("POST", "/query", data=data, headers=headers, **kwargs)

    def query(self, query, expected_status=200, credentials="default", headers=None, **options):
        """Execute a query, return the response, parsed."""
        r = check_status(self.post_query({"query": query, **options}, credentials=credentials, headers=headers), expected_status)
        return r.json()

    def rows(self, query, **options):
        """Execute a query, return its rows, following the pages."""
        rows = []
        paging_state = None
        while True:
            if paging_state:
                options["paging_state"] = paging_state
            res = self.query(query, **options)
            rows += res["result_set"]
            paging_state = res.get("paging_state")
            if not paging_state:
                return rows

    def __str__(self):
        return self.url


@pytest.fixture(scope="module")
def credentials(request):
    return user_credentials(request.config.getoption("--auth_username") or "cassandra",
                            request.config.getoption("--auth_password") or "cassandra")


@pytest.fixture(scope="module")
def webshell_log_level(scylla_only, host):
    url = f"http://{host}:10000"
    requests.post(f"{url}/system/logger/webshell", params={"level": "trace"})
    yield
    requests.post(f"{url}/system/logger/webshell", params={"level": "info"})


@pytest.fixture(scope="module")
def webshell_servers(scylla_only, cql, host, webshell_log_level):
    """Set up the Web Shell listeners.

    The Web Shell is served only on the listeners of the "listeners" cluster
    config option. Once set, the option replaces the per-protocol options in
    scylla.yaml for all protocols, so it lists the CQL (and Alternator)
    listeners the server already has, under the names and with the settings
    the server derives from its per-protocol options. Those are left alone, so
    the driver's connections are not disturbed.
    """
    configs = list(cql.execute("SELECT configs FROM system_schema.scylla_clusters"))
    if configs and dict(configs[0].configs or {}).get("listeners"):
        skip_env("The server has its listeners configured already")
    if str(config_value(cql, "client_encryption_options").get("enabled", "false")).lower() == "true":
        skip_env("The server has encrypted CQL listeners, the test only knows how to keep unencrypted ones")

    listeners = {
        "cql": {"protocol": "cql", "port": config_value(cql, "native_transport_port"), "shard_aware_listener": "cql_shard_aware"},
        "cql_shard_aware": {"protocol": "cql", "port": config_value(cql, "native_shard_aware_transport_port"), "shard_aware": True},
    }
    if alternator_port := config_value(cql, "alternator_port"):
        listeners["alternator"] = {"protocol": "alternator", "port": alternator_port}
    if alternator_https_port := config_value(cql, "alternator_https_port"):
        listeners["alternator_https"] = {"protocol": "alternator", "port": alternator_https_port, "tls": True}
    for name, (port, tls_options) in webshell_listeners.items():
        listeners[name] = {"protocol": "webshell", "address": host, "port": port}
        if tls_options:
            listeners[name]["tls"] = tls_options

    cql.execute(f"ALTER CLUSTER WITH listeners = {as_cql_literal(listeners)}")
    try:
        # The listeners are opened asynchronously, after the option is changed.
        def listening():
            try:
                for name, (port, tls_options) in webshell_listeners.items():
                    if name != "webshell_https_mtls":
                        requests.get(f"{'https' if tls_options else 'http'}://{host}:{port}/", verify=False, timeout=5)
                return True, None
            except requests.exceptions.ConnectionError:
                return False, None
        with_retry(listening, retries=600)
        yield
    finally:
        cql.execute("ALTER CLUSTER WITH listeners = null")


def make_endpoint(host, name, credentials):
    port, tls_options = webshell_listeners[name]
    return webshell_endpoint(f"{'https' if tls_options else 'http'}://{host}:{port}", credentials)


@pytest.fixture(scope="module")
def webshell_http(host, credentials, webshell_servers):
    yield make_endpoint(host, "webshell_http", credentials)


@pytest.fixture(scope="module")
def webshell_https(host, credentials, webshell_servers):
    yield make_endpoint(host, "webshell_https", credentials)


@pytest.fixture(scope="module")
def webshell_https_mtls(host, credentials, webshell_servers):
    yield make_endpoint(host, "webshell_https_mtls", credentials)


@pytest.fixture(scope="module")
def webshell_https_no_client_auth(host, credentials, webshell_servers):
    yield make_endpoint(host, "webshell_https_no_client_auth", credentials)


# Plain HTTP and HTTPS differ only in the transport, everything is expected to
# work the same on both, so run the tests against both.
@pytest.fixture(scope="module", params=["http", "https"])
def webshell(request):
    yield request.getfixturevalue(f"webshell_{request.param}")


@pytest.fixture(scope="module")
def test_table(cql, test_keyspace):
    table = unique_name()
    full_name = f"{test_keyspace}.{table}"

    cql.execute(f"CREATE TABLE {full_name} (id int PRIMARY KEY, value text)")
    for i in range(0, 10):
        cql.execute(f"INSERT INTO {full_name} (id, value) VALUES ({i}, 'val{i}')")

    data = cql.execute(f"SELECT JSON * FROM {full_name}")
    data = sorted((json.loads(row[0]) for row in data), key=lambda row: row["id"])
    yield full_name, data

    cql.execute(f"DROP TABLE {full_name}")


@pytest.fixture(scope="function")
def new_user(cql):
    """A role which can log in, with a password. Dropped at the end of the test."""
    user = user_credentials(unique_name(), "very_secure_password123!!")
    cql.execute(f"CREATE ROLE {user.username} WITH PASSWORD = '{user.password}' AND LOGIN = true")
    yield user
    cql.execute(f"DROP ROLE {user.username}")


#
# Endpoints and the web interface
#


def test_web_interface(webshell):
    for path, content_type, needle in [("/", "text/html", b"<title>ScyllaDB Web Shell</title>"),
                                       ("/webshell.html", "text/html", b"<title>ScyllaDB Web Shell</title>"),
                                       ("/webshell.js", "text/javascript", b"'use strict';"),
                                       ("/webshell.css", "text/css", b"}"),
                                       ("/favicon.ico", "image/vnd.microsoft.icon", b"")]:
        r = check_status(webshell.request("GET", path), 200)
        assert r.headers["Content-Type"] == content_type
        assert needle in r.content
        # The web interface cannot be embedded into another page, and uses no
        # resources of other origins.
        assert r.headers["Content-Security-Policy"] == "default-src 'self'; frame-ancestors 'none'"
        assert r.headers["X-Content-Type-Options"] == "nosniff"


def test_web_interface_is_embedded(webshell):
    # The files are served as they are in the source tree: embedded as-is.
    for name in ["webshell.html", "webshell.js", "webshell.css", "favicon.ico"]:
        r = check_status(webshell.request("GET", f"/{name}"), 200)
        assert r.content == (TEST_DIR.parent / "tools" / "webshell" / "resources" / name).read_bytes()


@pytest.mark.parametrize("method,path", [
    ("GET", "/no-such-file.js"),
    ("GET", "/login"),
    ("POST", "/"),
    ("POST", "/login"),
    ("POST", "/webshell.js"),
    ("PUT", "/query"),
    ("DELETE", "/query"),
])
def test_unknown_endpoint(webshell, method, path):
    r = check_status(webshell.request(method, path), 404)
    assert r.headers["Content-Type"] == "application/json"
    assert "error" in r.json()


def test_query_rejects_get(webshell):
    r = check_status(webshell.request("GET", "/query"), 405)
    assert r.headers["Allow"] == "POST"
    assert "error" in r.json()


# Requests made by pages of another origin are rejected, even "simple" ones,
# which browsers send without a CORS preflight. Requests of the web interface
# itself, served by the same origin, are accepted.
def test_cross_origin_request(webshell):
    r = check_status(webshell.post_query('{"query": "SELECT * FROM system.local"}',
                                         headers={"Origin": "https://example.com", "Content-Type": "text/plain"}), 403)
    assert "Cross-origin" in r.json()["error"]
    check_status(webshell.post_query({"query": "SELECT * FROM system.local"}, headers={"Origin": "null"}), 403)
    check_status(webshell.post_query({"query": "SELECT * FROM system.local"}, headers={"Origin": webshell.url}), 200)
    # The scheme is part of the origin.
    other_scheme_url = webshell.url.replace("https://", "http://") if webshell.is_https else webshell.url.replace("http://", "https://")
    check_status(webshell.post_query({"query": "SELECT * FROM system.local"}, headers={"Origin": other_scheme_url}), 403)


# The responses carry no CORS headers, so a page of another origin cannot read
# them - nor send a request with an Authorization header in the first place.
def test_no_cors(webshell):
    r = check_status(webshell.post_query({"query": "SELECT * FROM system.local"}, headers={"Origin": webshell.url}), 200)
    assert not any(h.lower().startswith("access-control-") for h in r.headers)


#
# Authentication
#


def test_authentication(webshell, credentials):
    res = webshell.query("SELECT cluster_name FROM system.local", credentials=credentials)
    assert len(res["result_set"]) == 1


def test_authentication_scheme_is_case_insensitive(webshell, credentials):
    token = basic_authorization(credentials).split(" ")[1]
    check_status(webshell.post_query({"query": "SELECT * FROM system.local"}, credentials=None, headers={"Authorization": f"bAsIc {token}"}), 200)


def test_no_credentials(webshell):
    r = check_status(webshell.post_query({"query": "SELECT * FROM system.local"}, credentials=None), 401)
    assert "error" in r.json()
    # A challenge is sent, as it has to be, but not a Basic one: a browser
    # would answer that with a dialog of its own, then send the credentials it
    # got with every later request to the server.
    challenge = r.headers["WWW-Authenticate"]
    assert not challenge.lower().startswith("basic")


def test_bad_credentials(webshell, credentials):
    r = check_status(webshell.post_query({"query": "SELECT * FROM system.local"},
                                         credentials=user_credentials(credentials.username, "bad-password")), 401)
    assert "error" in r.json()
    check_status(webshell.post_query({"query": "SELECT * FROM system.local"},
                                     credentials=user_credentials(unique_name(), "password")), 401)


@pytest.mark.parametrize("authorization", [
    "Bearer abcd",
    "Basic",
    "Basic !!!not-base64!!!",
    f"Basic {base64.b64encode(b'no-colon').decode()}",
])
def test_malformed_authorization(webshell, authorization):
    r = check_status(webshell.post_query({"query": "SELECT * FROM system.local"}, credentials=None,
                                         headers={"Authorization": authorization}), 401)
    assert "error" in r.json()


def wait_for_status(webshell, credentials, expected_status):
    """Wait until a query authenticated with the given credentials gets the
    expected status: changes to roles reach the authentication cache of the
    node asynchronously."""
    def check():
        r = webshell.post_query({"query": "SELECT * FROM system.local"}, credentials=credentials)
        return r.status_code == expected_status, r
    return with_retry(check)


# A role which cannot log in cannot use the Web Shell either.
def test_role_cannot_login(cql, webshell, new_user):
    webshell.query("SELECT * FROM system.local", credentials=new_user)
    cql.execute(f"ALTER ROLE {new_user.username} WITH LOGIN = false")
    wait_for_status(webshell, new_user, 401)


# Every request is authenticated on its own, so a changed password takes effect
# for the next request.
def test_password_change(cql, webshell, new_user):
    webshell.query("SELECT * FROM system.local", credentials=new_user)
    cql.execute(f"ALTER ROLE {new_user.username} WITH PASSWORD = 'another_password'")
    wait_for_status(webshell, new_user, 401)
    wait_for_status(webshell, user_credentials(new_user.username, "another_password"), 200)


#
# Requests
#


@pytest.mark.parametrize("body,error", [
    ("", "Invalid request"),
    ("not json", "Invalid request"),
    ("[]", "Expected object"),
    ({}, "query"),
    ({"query": 1}, "Expected string at $root.query"),
    ({"query": "SELECT * FROM system.local", "no_such_member": 1}, "Unknown member $root.no_such_member"),
    ({"query": "SELECT * FROM system.local", "consistency": "SOME"}, "Invalid consistency"),
    ({"query": "SELECT * FROM system.local", "consistency": 1}, "Expected string at $root.consistency"),
    ({"query": "SELECT * FROM system.local", "serial_consistency": "ONE"}, "Invalid serial_consistency"),
    ({"query": "SELECT * FROM system.local", "page_size": 0}, "Invalid page_size"),
    ({"query": "SELECT * FROM system.local", "page_size": -1}, "Invalid page_size"),
    ({"query": "SELECT * FROM system.local", "page_size": 1001}, "Invalid page_size"),
    ({"query": "SELECT * FROM system.local", "page_size": "10"}, "Expected integer at $root.page_size"),
    ({"query": "SELECT * FROM system.local", "paging_state": "!!!"}, "Invalid paging_state"),
    ({"query": "SELECT * FROM system.local", "paging_state": "AAAA"}, "Invalid paging_state"),
    ({"query": "SELECT * FROM system.local", "timeout_ms": 0}, "Invalid timeout_ms"),
    ({"query": "SELECT * FROM system.local", "tracing": "yes"}, "Expected boolean at $root.tracing"),
    ({"query": "SELECT * FROM system.local", "keyspace": "no_such_keyspace"}, "does not exist"),
])
def test_bad_request(webshell, body, error):
    r = check_status(webshell.post_query(body), 400)
    assert error in r.json()["error"]


# The size limit of request bodies applies to chunked ones too, which declare no
# Content-Length for the HTTP server to check.
def test_chunked_request_body_too_large(webshell):
    def body():
        chunk = b" " * (1024 * 1024)
        for _ in range(2):
            yield chunk

    check_status(webshell.post_query(body()), 413)


#
# Queries and their results
#


def test_query(webshell, test_table):
    table_name, table_data = test_table

    res = webshell.query(f"SELECT * FROM {table_name}")
    assert res["column_types"] == [{"name": "id", "type": "int"}, {"name": "value", "type": "text"}]
    assert sorted(res["result_set"], key=lambda row: row["id"]) == table_data
    assert "paging_state" not in res
    assert "trace_session_id" not in res


def test_query_types(cql, test_keyspace, webshell):
    table = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"CREATE TABLE {table} (pk int PRIMARY KEY, b blob, l list<int>, m map<text, int>, t tuple<int, text>, ts timestamp, d double)")
    try:
        cql.execute(f"INSERT INTO {table} (pk, b, l, m, t, ts, d) VALUES (0, 0x0102, [1, 2], {{'a': 1}}, (1, 'x'), '2025-01-02 03:04:05+0000', 1.5)")
        res = webshell.query(f"SELECT pk, b, l, m, t, ts, d FROM {table}")
        assert [col["type"] for col in res["column_types"]] == ["int", "blob", "list<int>", "map<text, int>", "frozen<tuple<int, text>>", "timestamp", "double"]
        # Values are formatted as with SELECT JSON.
        expected = json.loads(cql.execute(f"SELECT JSON pk, b, l, m, t, ts, d FROM {table}").one()[0])
        assert res["result_set"] == [expected]
    finally:
        cql.execute(f"DROP TABLE {table}")


# A present but empty value is reported as such, only an absent one as null.
def test_query_empty_value_is_not_null(cql, test_keyspace, webshell):
    table = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"CREATE TABLE {table} (pk int PRIMARY KEY, t text, b blob)")
    try:
        cql.execute(f"INSERT INTO {table} (pk, t, b) VALUES (0, '', 0x)")
        cql.execute(f"INSERT INTO {table} (pk) VALUES (1)")
        res = webshell.query(f"SELECT pk, t, b FROM {table}")
        assert sorted(res["result_set"], key=lambda row: row["pk"]) == [{"pk": 0, "t": "", "b": "0x"}, {"pk": 1, "t": None, "b": None}]
    finally:
        cql.execute(f"DROP TABLE {table}")


def test_query_paging(webshell, test_table):
    table_name, table_data = test_table

    pages = 0
    rows = []
    paging_state = None
    while True:
        options = {"page_size": 3}
        if paging_state:
            options["paging_state"] = paging_state
        res = webshell.query(f"SELECT * FROM {table_name}", **options)
        pages += 1
        assert len(res["result_set"]) <= 3
        rows += res["result_set"]
        paging_state = res.get("paging_state")
        if not paging_state:
            break

    assert pages >= 4
    assert sorted(rows, key=lambda row: row["id"]) == table_data


def test_query_default_page_size(cql, test_keyspace, webshell):
    table = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"CREATE TABLE {table} (pk int, ck int, PRIMARY KEY (pk, ck))")
    try:
        for ck in range(150):
            cql.execute(f"INSERT INTO {table} (pk, ck) VALUES (0, {ck})")
        res = webshell.query(f"SELECT * FROM {table}")
        assert len(res["result_set"]) == 100
        assert "paging_state" in res
        assert len(webshell.rows(f"SELECT * FROM {table}", page_size=1000)) == 150
    finally:
        cql.execute(f"DROP TABLE {table}")


def test_query_tracing(webshell, test_table):
    table_name, _ = test_table
    query = f"SELECT * FROM {table_name}"

    res = webshell.query(query, tracing=True, consistency="LOCAL_ONE", page_size=5)
    trace_session_id = res["trace_session_id"]

    def load_tracing_session():
        rows = webshell.query(f"SELECT * FROM system_traces.sessions WHERE session_id = {trace_session_id}")["result_set"]
        return bool(rows), rows

    tracing_session = with_retry(load_tracing_session)
    assert len(tracing_session) == 1
    assert tracing_session[0]["request"] == "Execute Web Shell query"
    parameters = tracing_session[0]["parameters"]
    assert parameters["query"] == query
    assert parameters["consistency_level"] == "LOCAL_ONE"
    assert parameters["page_size"] == "5"

    assert webshell.rows(f"SELECT * FROM system_traces.events WHERE session_id = {trace_session_id}")


# The tracing session id is reported for every traced query, not only for those
# returning rows.
def test_tracing_query_without_rows(webshell, test_table):
    table_name, _ = test_table
    # Writes the value the row already has, leaving the table as it was.
    res = webshell.query(f"UPDATE {table_name} SET value = 'val0' WHERE id = 0", tracing=True)
    assert "result_set" not in res
    assert res["trace_session_id"]


def test_use_keyspace(webshell, test_table):
    full_name, data = test_table
    keyspace, table = full_name.split(".")

    # USE reports the keyspace it switched to, ...
    assert webshell.query(f"USE {keyspace}") == {"set_keyspace": keyspace}

    # ... which doesn't apply to later requests: each request names its own
    # keyspace.
    webshell.query(f"SELECT * FROM {table}", expected_status=400)
    res = webshell.query(f"SELECT * FROM {table}", keyspace=keyspace)
    assert "set_keyspace" not in res
    assert sorted(res["result_set"], key=lambda row: row["id"]) == data

    # The keyspace USE switches to is reported, whatever the keyspace of the
    # request.
    assert webshell.query("USE system", keyspace=keyspace) == {"set_keyspace": "system"}


def test_schema_change(test_keyspace, webshell):
    table = unique_name()
    res = webshell.query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY)")
    assert res["schema_change"] == {"change": "CREATED", "target": "TABLE", "keyspace": test_keyspace, "arguments": [table]}
    assert "result_set" not in res
    res = webshell.query(f"ALTER TABLE {table} ADD v int", keyspace=test_keyspace)
    assert res["schema_change"] == {"change": "UPDATED", "target": "TABLE", "keyspace": test_keyspace, "arguments": [table]}
    res = webshell.query(f"DROP TABLE {test_keyspace}.{table}")
    assert res["schema_change"] == {"change": "DROPPED", "target": "TABLE", "keyspace": test_keyspace, "arguments": [table]}


def test_bad_query(test_keyspace, webshell):
    table = unique_name()

    webshell.query("SELEKT * FROM system.local", expected_status=400)
    webshell.query("SELECT * FROM bar.local", expected_status=400)
    webshell.query("SELECT * FROM system.foo", expected_status=400)

    webshell.query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY, value text)")
    try:
        r = webshell.query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY, value text)", expected_status=400)
        assert "already exist" in r["error"]
    finally:
        webshell.query(f"DROP TABLE {test_keyspace}.{table}")


# A query the coordinator cannot execute is reported as such, with the reason,
# not as an internal error of the Web Shell: here, the single node of the test
# cannot provide consistency TWO.
def test_query_unavailable(webshell, test_table):
    table_name, _ = test_table
    res = webshell.query(f"SELECT * FROM {table_name}", consistency="TWO", expected_status=503)
    assert "consistency" in res["error"].lower()


def test_query_timeout(webshell, test_table):
    table_name, _ = test_table
    # A timeout long enough for the query to succeed is accepted. Whether the
    # timeout is applied is hard to tell from the outside, without a query slow
    # enough to time out reliably.
    assert len(webshell.query(f"SELECT * FROM {table_name}", timeout_ms=10000)["result_set"]) == 10


def test_query_permissions(cql, test_keyspace, webshell, new_user):
    staff_table = f"{test_keyspace}.{unique_name()}"
    secret_table = f"{test_keyspace}.{unique_name()}"

    cql.execute(f"CREATE TABLE {staff_table} (id int PRIMARY KEY, value text)")
    cql.execute(f"INSERT INTO {staff_table} (id, value) VALUES (1, 'foo')")
    cql.execute(f"CREATE TABLE {secret_table} (id int PRIMARY KEY, value text)")
    cql.execute(f"INSERT INTO {secret_table} (id, value) VALUES (9, 'top-secret')")
    cql.execute(f"GRANT SELECT ON {staff_table} TO {new_user.username}")

    try:
        assert webshell.query(f"SELECT * FROM {staff_table}", credentials=new_user)["result_set"] == [{"id": 1, "value": "foo"}]
        # Reading a table without permission is rejected with the dedicated
        # status code 403.
        webshell.query(f"SELECT * FROM {secret_table}", credentials=new_user, expected_status=403)
    finally:
        cql.execute(f"DROP TABLE {staff_table}")
        cql.execute(f"DROP TABLE {secret_table}")


# Requests run in the scheduling group of the user's service level, which is
# looked up for each request. The query traces the scheduling group it is
# executed in.
def test_query_scheduling_group(cql, webshell):
    if not cql.cluster.auth_provider:
        skip_env("Service levels are attached to roles, the test needs authentication")

    prefix = "Executing Web Shell query in scheduling group "

    def query_scheduling_group():
        trace_session_id = webshell.query("SELECT * FROM system.local", tracing=True)["trace_session_id"]

        def load_events():
            events = webshell.rows(f"SELECT activity FROM system_traces.events WHERE session_id = {trace_session_id}")
            groups = [e["activity"].removeprefix(prefix) for e in events if e["activity"].startswith(prefix)]
            return bool(groups), groups

        return with_retry(load_events)[0]

    # The service level cache of the node is updated asynchronously.
    def wait_for_scheduling_group(pred):
        groups = []
        for _ in range(30):
            groups.append(query_scheduling_group())
            if pred(groups[-1]):
                return
            time.sleep(0.5)
        pytest.fail(f"Unexpected scheduling groups: {groups}")

    with new_service_level(cql) as sl:
        wait_for_scheduling_group(lambda group: group == f"sl:{sl}")
    wait_for_scheduling_group(lambda group: group != f"sl:{sl}")


# Requests are reported in system.clients, while they wait for their turn and
# while they are processed. A query reading system.clients through the Web Shell
# sees itself.
def test_system_clients(cql, webshell, new_user):
    cql.execute(f"GRANT SELECT ON system.clients TO {new_user.username}")
    rows = webshell.query("SELECT client_type, username, connection_stage, driver_name, ssl_enabled FROM system.clients",
                          credentials=new_user, headers={"User-Agent": "test-agent"})["result_set"]
    mine = [r for r in rows if r["client_type"] == "webshell" and r["username"] == new_user.username]
    assert len(mine) == 1, rows
    assert mine[0]["connection_stage"] == "READY"
    # Prefixed, so a client cannot pass itself off as another kind of client.
    assert mine[0]["driver_name"] == "webshell: test-agent"
    assert mine[0]["ssl_enabled"] == webshell.is_https


# DDL statements are bounced to shard 0, a request arriving on another shard has
# to follow them there. Each request opens a new connection, which lands on a
# shard picked by the client's port, so enough requests arrive on other shards.
def test_ddl_bounce_to_shard0(test_keyspace, webshell):
    for _ in range(8):
        table = unique_name()
        res = webshell.query(f"CREATE TABLE {test_keyspace}.{table} (id int PRIMARY KEY)")
        assert res["schema_change"]["change"] == "CREATED"
        webshell.query(f"INSERT INTO {test_keyspace}.{table} (id) VALUES (1)")
        assert webshell.query(f"SELECT * FROM {test_keyspace}.{table}")["result_set"] == [{"id": 1}]
        webshell.query(f"DROP TABLE {test_keyspace}.{table}")


# A conditional update is executed on the shard owning its partition, which is
# often not the shard the request arrived on. Its result is rows, which have to
# make it back from that shard all the same, and be rendered there.
def test_lwt(cql, test_keyspace, webshell):
    table = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
    try:
        # Enough partitions to be sure some are owned by another shard.
        for pk in range(16):
            res = webshell.query(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 0) IF NOT EXISTS")
            assert res["column_types"][0]["name"] == "[applied]"
            assert len(res["result_set"]) == 1 and res["result_set"][0]["[applied]"] is True
    finally:
        cql.execute(f"DROP TABLE {table}")


# A partition key computed by a function is computed once: when the conditional
# update is bounced to the shard owning the partition, it is executed there with
# the same key, instead of a freshly computed one, which may belong to yet
# another shard.
def test_lwt_function_partition_key(cql, test_keyspace, webshell):
    table = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"CREATE TABLE {table} (pk uuid PRIMARY KEY, v int)")
    try:
        for _ in range(16):
            res = webshell.query(f"INSERT INTO {table} (pk, v) VALUES (uuid(), 0) IF NOT EXISTS")
            assert len(res["result_set"]) == 1 and res["result_set"][0]["[applied]"] is True
        assert len(list(cql.execute(f"SELECT pk FROM {table}"))) == 16
    finally:
        cql.execute(f"DROP TABLE {table}")


#
# TLS client certificates (mutual TLS)
#


def sign_client_certificate(dir, name, subject):
    """Create a client certificate with the given subject, signed by the CA the server trusts.

    Returns the (certificate, key) file name pair, in the form the requests
    module's cert= parameter expects.
    """
    ca_certificate = x509.load_pem_x509_certificate(ca_certificate_file.read_bytes())
    ca_key = serialization.load_pem_private_key(ca_key_file.read_bytes(), password=None)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_certificate.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=30))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))

    certificate_file = dir / f"{name}.crt"
    key_file = dir / f"{name}.key"
    certificate_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(key.private_bytes(encoding=serialization.Encoding.PEM,
                                           format=serialization.PrivateFormat.TraditionalOpenSSL,
                                           encryption_algorithm=serialization.NoEncryption()))
    return (str(certificate_file), str(key_file))


@pytest.fixture(scope="module")
def client_certificates_dir(cql, tmp_path_factory):
    # Mapping a client certificate to a role is the authenticator's job, so
    # these tests need one which can do it.
    authenticator = config_value(cql, "authenticator")
    if "Certificate" not in authenticator:
        skip_env(f"Client-certificate authentication needs a certificate authenticator, the server has {authenticator}")
    yield tmp_path_factory.mktemp("webshell_client_certificates")


@pytest.fixture(scope="module")
def certificate_role(cql, client_certificates_dir):
    """A role which can log in, and a client certificate identifying it.

    The default auth_certificate_role_queries extracts the role name from the
    certificate subject's CN, so a certificate whose CN is the role's name
    authenticates as that role - no password involved.
    """
    role = unique_name()
    cql.execute(f"CREATE ROLE {role} WITH LOGIN = true")
    yield role, sign_client_certificate(client_certificates_dir, role,
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, role)]))
    cql.execute(f"DROP ROLE {role}")


@pytest.fixture(scope="module")
def unknown_role_certificate(client_certificates_dir):
    """A trusted client certificate whose CN names a role which doesn't exist."""
    yield sign_client_certificate(client_certificates_dir, "unknown_role",
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, unique_name())]))


@pytest.fixture(scope="module")
def no_role_certificate(client_certificates_dir):
    """A trusted client certificate with no CN, so no role name can be extracted from it."""
    yield sign_client_certificate(client_certificates_dir, "no_role",
            x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ScyllaDB")]))


def query_user(endpoint, admin, **kwargs):
    """The user a request is authenticated as, according to the tracing session.

    The tracing session is read through the admin endpoint: the user may have
    no permission to read it.
    """
    trace_session_id = endpoint.query("SELECT * FROM system.local", tracing=True, **kwargs)["trace_session_id"]

    def load_session():
        rows = admin.query(f"SELECT username FROM system_traces.sessions WHERE session_id = {trace_session_id}")["result_set"]
        return bool(rows), rows

    return with_retry(load_session)[0]["username"]


# A client certificate authenticates the request on its own, with no
# Authorization header, on the listeners which ask for one.
@pytest.mark.parametrize("listener", ["webshell_https", "webshell_https_mtls"])
def test_client_certificate(request, webshell_http, listener, certificate_role):
    role, certificate = certificate_role
    endpoint = request.getfixturevalue(listener).with_client_certificate(certificate)
    assert query_user(endpoint, webshell_http, credentials=None) == role


# A listener which doesn't ask for a client certificate doesn't get one, so
# requests have to be authenticated by the Authorization header.
def test_client_certificate_not_requested(webshell_https_no_client_auth, credentials, certificate_role):
    _, certificate = certificate_role
    endpoint = webshell_https_no_client_auth.with_client_certificate(certificate)
    check_status(endpoint.post_query({"query": "SELECT * FROM system.local"}, credentials=None), 401)
    assert query_user(endpoint, endpoint) == credentials.username


# On a listener which requests a certificate, but doesn't require one, a client
# without a certificate is authenticated by the Authorization header.
def test_client_certificate_optional(webshell_https, credentials):
    assert query_user(webshell_https, webshell_https) == credentials.username


# A listener which requires a client certificate rejects clients without one in
# the TLS handshake, their requests never reach the Web Shell.
def test_client_certificate_required(webshell_https_mtls):
    with pytest.raises((requests.exceptions.SSLError, requests.exceptions.ConnectionError)):
        webshell_https_mtls.post_query({"query": "SELECT * FROM system.local"})


# A certificate the server trusts, but which names a role that doesn't exist,
# is rejected - there is no falling back to the Authorization header.
@pytest.mark.parametrize("listener", ["webshell_https", "webshell_https_mtls"])
def test_client_certificate_of_unknown_role(request, listener, unknown_role_certificate):
    endpoint = request.getfixturevalue(listener).with_client_certificate(unknown_role_certificate)
    r = check_status(endpoint.post_query({"query": "SELECT * FROM system.local"}, credentials=None), 401)
    assert "doesn't exist" in r.json()["error"]
    # Not even with valid credentials in the Authorization header.
    r = check_status(endpoint.post_query({"query": "SELECT * FROM system.local"}), 401)
    assert "doesn't exist" in r.json()["error"]


# A certificate no role name can be extracted from is rejected too.
@pytest.mark.parametrize("listener", ["webshell_https", "webshell_https_mtls"])
def test_client_certificate_without_role_name(request, listener, no_role_certificate):
    endpoint = request.getfixturevalue(listener).with_client_certificate(no_role_certificate)
    r = check_status(endpoint.post_query({"query": "SELECT * FROM system.local"}, credentials=None), 401)
    assert "does not match any query expression" in r.json()["error"]


# A request authenticated by a certificate has exactly the permissions of the
# role the certificate names.
def test_client_certificate_permissions(cql, webshell_https, test_keyspace, certificate_role):
    role, certificate = certificate_role
    endpoint = webshell_https.with_client_certificate(certificate)

    granted_table = f"{test_keyspace}.{unique_name()}"
    denied_table = f"{test_keyspace}.{unique_name()}"

    cql.execute(f"CREATE TABLE {granted_table} (id int PRIMARY KEY, value text)")
    cql.execute(f"INSERT INTO {granted_table} (id, value) VALUES (1, 'foo')")
    cql.execute(f"CREATE TABLE {denied_table} (id int PRIMARY KEY, value text)")
    cql.execute(f"GRANT SELECT ON {granted_table} TO {role}")

    try:
        assert endpoint.query(f"SELECT * FROM {granted_table}", credentials=None)["result_set"] == [{"id": 1, "value": "foo"}]
        endpoint.query(f"SELECT * FROM {denied_table}", credentials=None, expected_status=403)
    finally:
        cql.execute(f"DROP TABLE {granted_table}")
        cql.execute(f"DROP TABLE {denied_table}")
