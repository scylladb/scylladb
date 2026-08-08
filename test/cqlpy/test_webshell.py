# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import collections
import datetime
import json
import math
import pytest
import requests
import time
import uuid

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from test import TEST_DIR
from test.pylib.skip_types import skip_env
from .util import unique_name

# The Web Shell's HTTPS listener uses the certificate test/pylib installs into
# every test server's conf/ directory - a self-signed certificate for
# "example.com", which cannot be validated against the address the tests
# connect to. Validating the server certificate is not what these tests are
# about, so they don't (verify=False below). The warning urllib3 emits about
# that is filtered out in test/pytest.ini.
#
# The truststore the test server validates client certificates against is a
# copy of the very same self-signed certificate, which is a CA certificate (see
# ScyllaServer._write_config_file() in test/pylib/scylla_cluster.py). So the
# client certificates the server will accept are the ones signed with the
# matching private key, which the tests have access to.
ca_certificate_file = TEST_DIR / "pylib" / "resources" / "scylla.crt"
ca_key_file = TEST_DIR / "pylib" / "resources" / "scylla.key"

# Live-updatable config item selecting whether the HTTPS listener requests a
# client certificate: none, request or require.
client_auth_config = "webshell_https_client_auth"


def check_status(r, expected_status):
    assert r.status_code == expected_status, f"check_status(): expected={expected_status}, got={r.status_code}, response={r.text}"
    return r


# Every current caller waits for a tracing session to become readable, and the
# tracing subsystem writes those out on a timer of its own (tracing::write_period,
# 2 seconds). The default budget below has to comfortably exceed that period, or
# the wait expires just as the data is about to land.
def with_retry(func, retries=100, delay=0.1):
    for i in range(0, retries):
        success, result = func()
        if success:
            return result
        time.sleep(delay)
    pytest.fail(f"with_retry(): {func.__name__} did not succeed in {retries * delay} seconds")


def config_value(cql, name):
    """Return the current value of the given config item.

    The lookup uses a prepared statement on purpose: system.config is a
    shard-aware virtual table, so an unprepared point query is not routed to
    the shard owning the row and finds nothing.
    """
    select = cql.prepare("SELECT value FROM system.config WHERE name=?")
    return json.loads(cql.execute(select, (name,)).one().value)


class client_auth_context:
    """Set the HTTPS client-authentication mode while the context is active.

    The mode is live-updatable, the new value applies to connections
    established after the update.

    This does what util.config_value_context does, except that it decodes the
    value it reads from system.config, so that restoring a string-valued config
    item doesn't store it with its JSON quotes included.
    """
    def __init__(self, cql, mode):
        self._cql = cql
        self._mode = mode
        self._original_mode = None
        self._update = cql.prepare("UPDATE system.config SET value=? WHERE name=?")

    def _set(self, mode):
        self._cql.execute(self._update, (mode, client_auth_config))

    def __enter__(self):
        self._original_mode = config_value(self._cql, client_auth_config)
        self._set(self._mode)
        return self._mode

    def __exit__(self, exc_type, exc_value, traceback):
        self._set(self._original_mode)


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


class webshell_endpoint:
    """A Web Shell base URL, together with the TLS options needed to talk to it."""
    def __init__(self, url, client_certificate=None):
        self.url = url
        self.is_https = url.startswith("https://")
        self.client_certificate = client_certificate

    def with_client_certificate(self, client_certificate):
        """A copy of this endpoint, authenticating with the given TLS client certificate."""
        return webshell_endpoint(self.url, client_certificate)

    def _tls_kwargs(self, kwargs):
        if self.is_https:
            kwargs.setdefault("verify", False)
            if self.client_certificate is not None:
                kwargs.setdefault("cert", self.client_certificate)
        return kwargs

    def post(self, path, **kwargs):
        return requests.post(self.url + path, **self._tls_kwargs(kwargs))

    def request(self, method, path, **kwargs):
        return requests.request(method, self.url + path, **self._tls_kwargs(kwargs))

    def __str__(self):
        return self.url


@pytest.fixture(scope="module")
def webshell_log_level(scylla_only, host):
    url = f'http://{host}:10000'
    requests.post(f'{url}/system/logger/webshell', params={'level' : 'trace'})
    yield
    requests.post(f'{url}/system/logger/webshell', params={'level' : 'info'})


@pytest.fixture(scope="module")
def webshell_http(host, webshell_log_level):
    yield webshell_endpoint(f"http://{host}:10001")


@pytest.fixture(scope="module")
def webshell_https(cql, host, webshell_log_level):
    port = config_value(cql, "webshell_https_port")
    if not port:
        skip_env("Web Shell HTTPS listener is disabled (webshell_https_port is 0)")
    yield webshell_endpoint(f"https://{host}:{port}")


# The HTTP and the HTTPS listener differ only in the transport, everything is
# expected to work the same on both, so run the tests against both of them.
@pytest.fixture(scope="module", params=["http", "https"])
def webshell(request):
    yield request.getfixturevalue(f"webshell_{request.param}")


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
        self.login_response = None

    def __init__(self, webshell):
        self.webshell = webshell
        self._reset()

    def login(self, credentials=None):
        """Log in with the given credentials, or with no request body at all.

        The latter is how one logs in with a TLS client certificate: the
        certificate identifies the role, so no credentials are needed.
        """
        if self.cookies is not None:
            self.logout()

        login = self.webshell.post("/login", data=None if credentials is None else json.dumps(credentials._asdict()))
        check_status(login, 200)
        self.cookies = login.cookies
        self.login_response = login.json()["response"]
        return self.login_response

    def logout(self):
        check_status(self.webshell.post("/logout", cookies=self.cookies), 200)
        self._reset()

    def check_command(self, command, *args, expected_status=200):
        data = json.dumps({"command": command, "arguments": args})
        res = check_status(self.webshell.post("/command", cookies=self.cookies, data=data), expected_status)
        self.cookies = res.cookies
        return res.json()["response"]

    def get_option(self, option=None, expected_status=200):
        """GET /option: report one option, or every option if none is named.

        A successful response is an object of option values, so this returns a
        dict; an error response is a message, so it returns a string.
        """
        params = None if option is None else {"option": option}
        res = check_status(self.webshell.request("GET", "/option", cookies=self.cookies, params=params), expected_status)
        self.cookies = res.cookies
        return res.json()["response"]

    def set_option(self, option, *args, expected_status=200):
        """POST /option: change one option and report its new value."""
        data = json.dumps({"option": option, "arguments": args})
        res = check_status(self.webshell.post("/option", cookies=self.cookies, data=data), expected_status)
        self.cookies = res.cookies
        return res.json()["response"]

    def check_option(self, option=None, *args, expected_status=200):
        """Read or change an option, whichever the arguments call for.

        Reading is GET /option and changing is POST /option, so this dispatches
        on the arguments exactly as a client has to.
        """
        if args:
            return self.set_option(option, *args, expected_status=expected_status)
        return self.get_option(option, expected_status=expected_status)

    def check_query(self, query, expected_status=200, options=None):
        """Send a /query request, optionally with per-request option overrides.

        options, if given, is sent as the request's "options" member and applies
        to this query only, leaving the session's own options alone.
        """
        body = {"query": query.replace('"', '\\"'), "paging_state": self.paging_state}
        if options is not None:
            body["options"] = options
        data = json.dumps(body)
        res = check_status(self.webshell.post("/query", cookies=self.cookies, data=data), expected_status)
        self.cookies = res.cookies
        res_content = res.json()
        self.paging_state = res_content.get("paging_state", None)
        self.trace_session_id = res_content.get("trace_session_id", None)
        return res_content["response"]


class new_session:
    def __init__(self, webshell, credentials=None):
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
    table = unique_name()
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
    r = webshell.post(f"/{endpoint}")
    check_status(r, 401)


@pytest.mark.parametrize("endpoint", ["query", "option"])
def test_bad_session_id(webshell, endpoint):
    r = webshell.post(f"/{endpoint}", cookies={"session_id": "bad_session_id"})
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

    check_status(webshell.post(f"/{endpoint}", cookies={"session_id": session_id}, data=data), 200)

    check_status(webshell.post(f"/{endpoint}", cookies={"session_id": "bad_session_id"}, data=data), 401)
    check_status(webshell.post(f"/{endpoint}", cookies={"session_id": modified_first_digit + session_id[1:]}, data=data), 401)


QUERY_SYSTEM_LOCAL = json.dumps({"query": "select * from system.local"})


def test_session_id_shard_field_is_parsed_as_hex(webshell, session):
    """The shard field of a session_id is hex, the way it is written.

    It used to be written with "{:08x}" but read back with a base-10 stoul, so
    every id created on shard 10 or above parsed back to a different shard
    ("0000000a" -> 0), its session could never be found again, and the client was
    locked out immediately after logging in. The test cluster has too few shards
    to create such an id, so this goes the other way: appending a hex digit to
    the shard field leaves a string that a decimal parse still truncates to the
    session's real shard - which would find the session and answer 200 - but that
    a hex parse reads as a shard far out of range.
    """
    msb, lsb, shard = session.cookies["session_id"].split("-")

    spoofed = f"{msb}-{lsb}-{int(shard, base=16):07d}a"
    assert len(spoofed.split('-')[2]) == len(shard)

    check_status(webshell.post("/query", cookies={"session_id": spoofed}, data=QUERY_SYSTEM_LOCAL), 401)


def test_session_id_out_of_range_shard(webshell, session):
    """A session_id naming a shard the machine does not have is rejected.

    The shard field routes the request with smp::submit_to(), which indexes its
    queue array without checking, so an unvalidated shard here was an out of
    bounds access rather than a lookup miss. No session is needed to reach it,
    which is why this one is worth its own test.
    """
    msb, lsb, _ = session.cookies["session_id"].split("-")

    # What /login answers when it is given no session at all, which is what a
    # rejected session_id has to amount to.
    no_cookie_status = webshell.post("/login").status_code

    for shard in ["ffffffff", "7fffffff", "000000ff"]:
        spoofed = {"session_id": f"{msb}-{lsb}-{shard}"}

        check_status(webshell.post("/query", cookies=spoofed, data=QUERY_SYSTEM_LOCAL), 401)

        # /login routes on the shard field before authenticating anyone, so it is
        # the endpoint where an unchecked shard was reachable unauthenticated.
        # The id is rejected while parsing, so login sees no session and carries
        # on as usual - answering at all is the point of the check.
        check_status(webshell.post("/login", cookies=spoofed), no_cookie_status)


# A copy of the snippet in the comment above fmt::formatter<session_id> in
# tools/webshell/webshell.cc, which is what a developer reaches for to find a
# live session in the log. Keep the two identical: reproducing the server's own
# digest here is the whole point of test_session_digest() below, and a snippet
# that has drifted from the implementation is worse than none at all.
def session_id_log_tag(session_id):
    msb, lsb, shard = (int(field, 16) for field in session_id.split("-"))
    mask = (1 << 64) - 1
    x = (msb * 0x9e3779b97f4a7c15 + lsb) & mask
    x = ((x ^ (x >> 30)) * 0xbf58476d1ce4e5b9) & mask
    x = ((x ^ (x >> 27)) * 0x94d049bb133111eb) & mask
    return f"{((x ^ (x >> 31)) >> 32):08x}@{shard}"


def test_session_id_log_tag_snippet():
    """The snippet reproduces the worked example documented beside it.

    A typo in either copy of the snippet shows up here, without needing a server.
    """
    assert session_id_log_tag("cec3fa9d6f2a1b04-a71e5d3c98460f21-0000000b") == "869d78de@11"


def test_session_digest(webshell, session):
    """session_digest reports the digest the logs name this session by.

    A session_id is a credential, so it is never logged; formatting one yields
    this digest instead. The cookie is what lets a client match its own session
    against the log - and computing the same value here, with the snippet
    documented beside the formatter, is what keeps that snippet honest.
    """
    session_id = session.cookies["session_id"]
    expected = session_id_log_tag(session_id)

    assert session.cookies["session_digest"] == expected

    # The shard is carried through as it is, so a log line points at a shard.
    # Only the two random halves are folded away.
    digest, _, shard = expected.partition("@")
    assert shard == str(int(session_id.split("-")[2], base=16))
    assert digest not in session_id

    # It comes back on every response, not only on the login that created it.
    for endpoint, data in [("query", QUERY_SYSTEM_LOCAL), ("command", json.dumps({"command": "help", "arguments": []}))]:
        res = check_status(webshell.post(f"/{endpoint}", cookies={"session_id": session_id}, data=data), 200)
        assert res.cookies["session_digest"] == expected

    # And it is the server's own view of the session: a client cannot assert it,
    # so whatever one sends is replaced rather than echoed back.
    res = check_status(webshell.post("/query", cookies={"session_id": session_id, "session_digest": "deadbeef@9"},
            data=QUERY_SYSTEM_LOCAL), 200)
    assert res.cookies["session_digest"] == expected

    # A session_id that does not parse has no session, so there is nothing to
    # report a digest for.
    res = check_status(webshell.post("/query", cookies={"session_id": "bad_session_id"}, data=QUERY_SYSTEM_LOCAL), 401)
    assert "session_digest" not in res.cookies


def test_session_cookie_attributes(webshell, credentials):
    """Session cookies carry the attributes that keep them from leaking.

    A session_id is a bearer credential for as long as the session lives, so:
    HttpOnly keeps it away from scripts, SameSite=Strict keeps it off cross-site
    requests - a page the operator happens to be visiting must not be able to
    drive the shell - and Secure keeps a session established over TLS from being
    replayed in the clear.

    Secure is set on the HTTPS listener only. A cookie marked Secure is not sent
    over plain HTTP at all, so setting it on the HTTP listener would stop that
    listener working rather than protect anything.
    """
    login = check_status(webshell.post("/login", data=json.dumps(credentials._asdict())), 200)
    try:
        # requests joins repeated headers with ", ", which is ambiguous here, so
        # take the Set-Cookie headers apart individually.
        headers = {h.split("=", 1)[0].strip(): h for h in login.raw.headers.getlist("Set-Cookie")}
        assert set(headers) == {"session_id", "session_digest", "user_name", "cluster_name"}

        for name, header in headers.items():
            attributes = [attribute.strip() for attribute in header.split(";")[1:]]
            where = f"{name}: {header}"

            assert "SameSite=Strict" in attributes, where
            assert any(attribute.startswith("Max-Age=") for attribute in attributes), where
            assert ("Secure" in attributes) == webshell.is_https, where
            # Only session_id is HttpOnly; the web interface displays the other
            # two, so they have to stay readable.
            assert ("HttpOnly" in attributes) == (name == "session_id"), where
    finally:
        check_status(webshell.post("/logout", cookies=login.cookies), 200)


def test_session_id_is_not_aliased(webshell, session):
    """Only the id itself names the session, not a family of look-alikes.

    std::stoull() skipped leading whitespace, took a sign, and stopped at the
    first character it could not use without reporting it, so every string below
    parsed to the very same numbers as the real id and could be presented in its
    place. Each of them is built from a live session id, so a parser that still
    accepted them would answer 200 here.
    """
    session_id = session.cookies["session_id"]
    msb, lsb, shard = session_id.split("-")

    check_status(webshell.post("/query", cookies={"session_id": session_id}, data=QUERY_SYSTEM_LOCAL), 200)

    look_alikes = [
        f" {msb}-{lsb}-{shard}",        # leading whitespace, skipped by stoull
        f"{msb}- {lsb}-{shard}",        # ... in any field
        f"{msb}-{lsb}-{shard}junk",     # trailing garbage, silently ignored
        f"{msb}-{lsb}-+{shard[1:]}",    # a sign, accepted for unsigned fields
    ]
    # Upper case hex reads as the same number but is not what the formatter
    # writes. Only worth asserting if this id happens to contain a hex letter.
    if msb.upper() != msb:
        look_alikes.append(f"{msb.upper()}-{lsb}-{shard}")

    for look_alike in look_alikes:
        check_status(webshell.post("/query", cookies={"session_id": look_alike}, data=QUERY_SYSTEM_LOCAL), 401)

    # None of that disturbed the session it was imitating.
    check_status(webshell.post("/query", cookies={"session_id": session_id}, data=QUERY_SYSTEM_LOCAL), 200)


# Strings std::stoull()/std::stoul() used to accept for the fields of a
# session_id, so that a whole family of them aliased onto one session: leading
# whitespace, a sign, trailing garbage, and short or over-long fields. The parser
# now takes only what it writes - exactly 16, 16 and 8 lower case hex digits.
@pytest.mark.parametrize("session_id", [
    "-1-0-0",
    "0-0-0",
    "ffffffffffffffff-ffffffffffffffff-0",
    " 0000000000000000-0000000000000000-00000000",
    "0000000000000000-0000000000000000-00000000 ",
    "0000000000000000-0000000000000000-00000000junk",
    "+000000000000000-0000000000000000-00000000",
    "-000000000000000-0000000000000000-00000000",
    "0x00000000000000-0000000000000000-00000000",
    "0000000000000000-0000000000000000-0000000",
    "0000000000000000-0000000000000000-000000000",
    "0000000000000000-0000000000000000",
    "0000000000000000-0000000000000000-00000000-0",
    "0000000000000000_0000000000000000_00000000",
])
def test_session_id_malformed(webshell, session_id):
    check_status(webshell.post("/query", cookies={"session_id": session_id}, data=QUERY_SYSTEM_LOCAL), 401)


# Endpoints that only accept POST. /option is not one of them: reading its
# options is a GET, see test_option_read_and_write_are_separate.
POST_ONLY_ENDPOINTS = ["/login", "/logout", "/query", "/command"]


def check_json_response(r):
    """Assert the response is JSON carrying a "response" member, and return the body.

    Every endpoint answers this way whatever the status code, so that a client
    never has to guess whether it got JSON or something else. Only the resource
    handler, which serves the files of the web interface, is exempt.
    """
    where = f"{r.request.method} {r.request.path_url} [{r.status_code}]"
    content_type = r.headers.get("Content-Type", "")
    assert content_type.startswith("application/json"), f"{where}: Content-Type is {content_type!r}"
    body = r.json()
    assert isinstance(body, dict) and "response" in body, f"{where}: no response member in {body}"
    return body


@pytest.mark.parametrize("endpoint", POST_ONLY_ENDPOINTS)
def test_endpoint_rejects_get(webshell, endpoint):
    # These endpoints are POST-only. A GET has to be answered here, or it falls
    # through to the resource handler, gets looked up as a file and comes back as
    # a bodyless 404.
    r = webshell.request("GET", endpoint)
    check_status(r, 405)
    assert r.headers.get("Allow") == "POST"
    assert "use POST" in check_json_response(r)["response"]


@pytest.mark.parametrize("method", ["PUT", "DELETE", "PATCH"])
def test_endpoint_rejects_other_methods(webshell, method):
    r = webshell.request(method, "/query")
    check_status(r, 404)
    assert "No such endpoint" in check_json_response(r)["response"]


def test_unknown_endpoint(webshell):
    r = webshell.post("/nonesuch")
    check_status(r, 404)
    assert "No such endpoint" in check_json_response(r)["response"]


def test_all_endpoints_answer_json(webshell, session):
    """Sweep the endpoints, in success and in failure, and check every answer is JSON."""
    cookies = session.cookies
    cases = [
        # No session: 401, except /logout which is idempotent.
        ("POST", "/logout", {}),
        ("POST", "/query", {}),
        ("POST", "/command", {}),
        ("POST", "/option", {}),
        ("GET", "/option", {}),
        # Bad request bodies.
        ("POST", "/query", {"cookies": cookies, "data": "not json at all"}),
        ("POST", "/query", {"cookies": cookies, "data": json.dumps({"paging_state": None})}),
        ("POST", "/command", {"cookies": cookies, "data": json.dumps({"command": "nonesuch", "arguments": []})}),
        ("POST", "/option", {"cookies": cookies, "data": json.dumps({"option": "nonesuch", "arguments": ["on"]})}),
        ("POST", "/option", {"cookies": cookies, "data": json.dumps({})}),
        ("GET", "/option", {"cookies": cookies, "params": {"option": "nonesuch"}}),
        # Successful requests, including the ones whose response is not a string.
        ("POST", "/query", {"cookies": cookies, "data": json.dumps({"query": "SELECT * FROM system.local"})}),
        ("POST", "/command", {"cookies": cookies, "data": json.dumps({"command": "help", "arguments": []})}),
        ("POST", "/option", {"cookies": cookies, "data": json.dumps({"option": "tracing", "arguments": ["on"]})}),
        ("GET", "/option", {"cookies": cookies}),
        ("GET", "/option", {"cookies": cookies, "params": {"option": "serial consistency"}}),
        # Wrong method, and no endpoint at all.
        ("GET", "/query", {}),
        ("PUT", "/query", {}),
        ("PUT", "/option", {}),
        ("POST", "/nonesuch", {}),
    ]

    for method, path, kwargs in cases:
        r = webshell.request(method, path, **kwargs)
        assert r.status_code != 500, f"{method} {path} returned 500: {r.text}"
        check_json_response(r)


def test_login(webshell, credentials):
    r = webshell.post("/login", data=json.dumps(credentials._asdict()))
    check_status(r, 200)
    assert "session_id" in r.cookies


def test_login_no_credentials(webshell):
    r = webshell.post("/login")
    check_status(r, 400)


def test_login_badly_formed_credentials(webshell, credentials):
    username, password = credentials

    r = webshell.post("/login", data=f'{{"username": {username}')
    check_status(r, 400)

    r = webshell.post("/login", data=json.dumps({"password": credentials.password}))
    check_status(r, 400)

    r = webshell.post("/login", data=json.dumps({"username": credentials.username}))
    check_status(r, 400)

    r = webshell.post("/login", data=json.dumps({"username": 1, "password": credentials.password}))
    check_status(r, 400)

    r = webshell.post("/login", data=json.dumps({"username": credentials.username, "password": 2}))
    check_status(r, 400)


def test_login_bad_credentials(webshell, credentials):
    check_status(webshell.post("/login", data=json.dumps(credentials._asdict())), 200)
    check_status(webshell.post("/login", data=json.dumps({"username": f"{credentials.username}foo", "password": credentials.password})), 400)
    check_status(webshell.post("/login", data=json.dumps({"username": credentials.username, "password": f"{credentials.password}bar"})), 400)


def test_logout(webshell, credentials):
    login = check_status(webshell.post("/login", data=json.dumps(credentials._asdict())), 200)

    check_status(webshell.post("/query", cookies=login.cookies, data=json.dumps({"query": "SELECT * FROM system.local"})), 200)

    check_status(webshell.post("/logout", cookies=login.cookies), 200)

    check_status(webshell.post("/query", cookies=login.cookies, data=json.dumps({"query": "SELECT * FROM system.local"})), 401)


def test_logout_no_session(webshell):
    check_status(webshell.post("/logout"), 200)


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

    assert session.check_option("tracing") == {"tracing": True}
    assert session.check_option("output format") == {"output_format": "JSON"}

    assert session.check_command("show", expected_status=400) == "Unrecognized command: show"
    assert session.check_command("show bar", expected_status=400) == "Unrecognized command: show bar"
    assert session.check_command("show session", trace_session_id, "bar", expected_status=400) == "Invalid SHOW command, expected 'SHOW SESSION <tracing_session_id>'."
    assert session.check_command("show session", "bar", expected_status=400) #FIXME: validate UUID on server side


def test_nonexistent_command(session):
    session.check_command("foo", expected_status=400)
    session.check_command("foo bar", expected_status=400)


# The session options and their default values, as reported by /option.
DEFAULT_OPTIONS = {
    "consistency": "ONE",
    "expand": False,
    "output_format": "TEXT",
    "page_size": 100,
    "serial_consistency": "SERIAL",
    "tracing": False,
}


def test_option_all(session):
    # With no option named, all of them are reported.
    assert session.get_option() == DEFAULT_OPTIONS

    session.set_option("consistency", "quorum")
    session.set_option("expand", "on")
    session.set_option("paging", "7")

    assert session.get_option() == DEFAULT_OPTIONS | {"consistency": "QUORUM", "expand": True, "page_size": 7}


def test_option_read_and_write_are_separate(session):
    """Reading an option is a GET, changing it a POST, and neither does the other's job."""
    # Reading reports without changing.
    assert session.get_option("paging") == {"page_size": 100}
    assert session.get_option("paging") == {"page_size": 100}

    # Changing reports the new value.
    assert session.set_option("paging", "7") == {"page_size": 7}
    assert session.get_option("paging") == {"page_size": 7}

    # POST is for changing only, and says where to go for a read.
    assert session.set_option("paging", expected_status=400) == \
            "No arguments given for option paging. Use GET /option to query the current value."

    # An option name is required by POST, and validated by both.
    res = check_status(session.webshell.post("/option", cookies=session.cookies,
                                            data=json.dumps({"arguments": ["on"]})), 400)
    assert "option" in res.json()["response"]
    assert session.get_option("nonesuch", expected_status=400) == "Unrecognized option: nonesuch"
    assert session.set_option("nonesuch", "on", expected_status=400) == "Unrecognized option: nonesuch"

    # An empty body is no longer a way to ask for every option.
    check_status(session.webshell.post("/option", cookies=session.cookies, data=None), 400)


@pytest.mark.parametrize("option,member", [
    ("consistency", "consistency"),
    ("expand", "expand"),
    ("output format", "output_format"),
    ("paging", "page_size"),
    ("serial consistency", "serial_consistency"),
    ("tracing", "tracing"),
])
def test_option_read_by_name(session, option, member):
    """Every option is readable by name, spaces and all - they have to survive the query string."""
    assert session.get_option(option) == {member: DEFAULT_OPTIONS[member]}


def test_option_consistency(session):
    # default
    assert session.check_option("consistency") == {"consistency": "ONE"}

    consistency_levels = ["ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "SERIAL", "LOCAL_SERIAL", "LOCAL_ONE"]
    for level in consistency_levels:
        assert session.check_option("consistency", level.lower()) == {"consistency": level}
        assert session.check_option("consistency") == {"consistency": level}

    assert session.check_option("consistency", "foo", expected_status=400) == f"Invalid CONSISTENCY argument, expected {', '.join(consistency_levels[:-1])} or {consistency_levels[-1]}."

    assert session.check_option("consistency", "foo", "bar", expected_status=400) == "Invalid CONSISTENCY option, expected 'CONSISTENCY [<consistency_level>]'."


def test_option_expand(session):
    # default
    assert session.check_option("expand") == {"expand": False}

    assert session.check_option("expand", "on") == {"expand": True}
    # Setting an option to the value it already has is accepted, not an error
    assert session.check_option("expand", "on") == {"expand": True}
    assert session.check_option("expand") == {"expand": True}

    assert session.check_option("expand", "off") == {"expand": False}
    assert session.check_option("expand", "off") == {"expand": False}
    assert session.check_option("expand") == {"expand": False}

    assert session.check_option("expand", "foo", expected_status=400) == "Invalid EXPAND argument, expected ON or OFF."
    assert session.check_option("expand", "foo", "bar", expected_status=400) == "Invalid EXPAND option, expected 'EXPAND [ON|OFF]'."


def test_option_paging(session):
    # default
    assert session.check_option("paging") == {"page_size": 100}

    # PAGING OFF is reported as a page size of 0, which is what disables paging
    session.check_option("paging", "off")
    assert session.check_option("paging") == {"page_size": 0}

    # Setting an option to the value it already has is accepted, not an error
    assert session.check_option("paging", "off") == {"page_size": 0}

    # PAGING ON resolves to the server-side default page size, which the client
    # cannot know from its own request - hence reporting the resulting value
    assert session.check_option("paging", "on") == {"page_size": 100}
    assert session.check_option("paging") == {"page_size": 100}

    assert session.check_option("paging", "on") == {"page_size": 100}

    assert session.check_option("paging", "200") == {"page_size": 200}
    assert session.check_option("paging") == {"page_size": 200}

    # Not an error to set the already set page size
    assert session.check_option("paging", "200") == {"page_size": 200}

    assert session.check_option("paging", "300") == {"page_size": 300}
    assert session.check_option("paging") == {"page_size": 300}

    # PAGING ON means the default page size, so it resets a page size the
    # client picked earlier
    assert session.check_option("paging", "on") == {"page_size": 100}

    # Setting page size to <= 0 disables paging
    assert session.check_option("paging", "0") == {"page_size": 0}

    assert session.check_option("paging", "-1") == {"page_size": -1}

    assert session.check_option("paging", "-991") == {"page_size": -991}

    assert session.check_option("paging", "-2147483648") == {"page_size": -2147483648}

    # Maxint - it is 64 bit signed max int, because of a limitation in std::stoull
    # makes it more convenient to use std:stoll in the implementation.
    # This is inconsequential, such page sizes are not practical anyway.
    assert session.check_option("paging", "2147483647") == {"page_size": 2147483647}

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
    assert session.check_option("serial consistency") == {"serial_consistency": "SERIAL"}

    consistency_levels = ["SERIAL", "LOCAL_SERIAL"]
    for level in consistency_levels:
        assert session.check_option("serial consistency", level.lower()) == {"serial_consistency": level}
        assert session.check_option("serial consistency") == {"serial_consistency": level}

    assert session.check_option("serial", expected_status=400) == "Unrecognized option: serial"

    assert session.check_option("serial foo", expected_status=400) == "Unrecognized option: serial foo"

    assert session.check_option("serial consistency", "foo", expected_status=400) == f"Invalid SERIAL CONSISTENCY argument, expected {', '.join(consistency_levels[:-1])} or {consistency_levels[-1]}."

    assert session.check_option("serial consistency", "foo", "bar", expected_status=400) == "Invalid SERIAL CONSISTENCY option, expected 'SERIAL CONSISTENCY [<serial_consistency_level>]'."


def test_option_tracing(session):
    # default
    assert session.check_option("tracing") == {"tracing": False}

    assert session.check_option("tracing", "on") == {"tracing": True}
    assert session.check_option("tracing") == {"tracing": True}

    assert session.check_option("tracing", "off") == {"tracing": False}
    assert session.check_option("tracing") == {"tracing": False}

    # Setting an option to the value it already has is accepted, not an error
    assert session.check_option("tracing", "off") == {"tracing": False}

    assert session.check_option("tracing", "on") == {"tracing": True}
    assert session.check_option("tracing", "on") == {"tracing": True}

    assert session.check_option("tracing", "on off", expected_status=400) == "Invalid TRACING option, expected 'TRACING [ON|OFF]'."
    assert session.check_option("tracing", "foo", expected_status=400) == "Invalid TRACING option, expected 'TRACING [ON|OFF]'."


def test_option_output_format(session):
    # default
    assert session.check_option("output format") == {"output_format": "TEXT"}

    assert session.check_option("output format", "json") == {"output_format": "JSON"}
    assert session.check_option("output format") == {"output_format": "JSON"}

    assert session.check_option("output format", "text") == {"output_format": "TEXT"}
    assert session.check_option("output format") == {"output_format": "TEXT"}

    assert session.check_option("output format", "foo", expected_status=400) == "Invalid OUTPUT FORMAT argument, expected TEXT or JSON."
    assert session.check_option("output format", "foo", "bar", expected_status=400) == "Invalid OUTPUT FORMAT option, expected 'OUTPUT FORMAT [TEXT|JSON]'."


def test_nonexistent_option(session):
    session.check_option("foo", expected_status=400)
    session.check_option("foo", "bar", expected_status=400)


def test_query(test_keyspace, session):
    table = unique_name()

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


def test_query_option_overrides(session, test_table):
    table_name, table_data = test_table
    query = f"SELECT * FROM {table_name}"

    # The session stays on the defaults throughout: each override below applies
    # to the request carrying it and to nothing else.
    assert session.check_option() == DEFAULT_OPTIONS

    # output_format is the override this exists for: it gets a machine-readable
    # result out of a session that a human is typing TEXT queries into.
    assert session.check_query(query, options={"output_format": "json"}) == table_data
    assert isinstance(session.check_query(query), str)

    # Values are case-insensitive, like the arguments of /option
    assert session.check_query(query, options={"output_format": "JSON"}) == table_data

    # expand prints one table per row
    res = session.check_query(query, options={"expand": True})
    assert "@ Row 1" in res
    assert f"@ Row {len(table_data)}" in res
    assert "@ Row" not in session.check_query(query)

    # page_size, with output_format so that the page is countable
    page = session.check_query(query, options={"output_format": "json", "page_size": 2})
    assert len(page) == 2
    assert session.paging_state is not None
    session.paging_state = None

    # A member set to null counts as not mentioned, leaving the session's TEXT
    assert isinstance(session.check_query(query, options={"output_format": None}), str)

    # An empty override object is not an error
    assert isinstance(session.check_query(query, options={}), str)

    assert session.check_option() == DEFAULT_OPTIONS


def test_query_option_overrides_tracing(session, test_table):
    table_name, _ = test_table

    # A trace records the options its query actually ran with, which is how the
    # overrides can be observed reaching query execution.
    session.check_query(f"SELECT * FROM {table_name}", options={"tracing": True, "consistency": "quorum", "output_format": "json"})

    trace_session_id = session.trace_session_id
    assert trace_session_id is not None

    # Tracing was never switched on in the session itself
    assert session.check_option() == DEFAULT_OPTIONS

    def load_tracing_session():
        res = session.check_query(f"SELECT * FROM system_traces.sessions WHERE session_id = {trace_session_id}",
                options={"output_format": "json"})
        if len(res) == 0:
            return False, []
        return True, res

    tracing_session = with_retry(load_tracing_session)
    assert tracing_session[0]["parameters"]["session_options"] == \
            "{consistency=QUORUM, expand=false, page_size=100, serial_consistency=SERIAL, tracing=true, output_format=json}"


def test_query_option_overrides_bad(session, test_table):
    table_name, _ = test_table
    query = f"SELECT * FROM {table_name}"

    # A misspelled option is rejected, not silently ignored
    assert session.check_query(query, 400, options={"output_fromat": "json"}) == "Unrecognized option: output_fromat"

    # Bad values
    assert session.check_query(query, 400, options={"output_format": "yaml"}) == "Invalid output_format argument, expected TEXT or JSON."
    assert session.check_query(query, 400, options={"consistency": "foo"}).startswith("Invalid consistency argument, expected ")
    assert session.check_query(query, 400, options={"serial_consistency": "quorum"}).startswith("Invalid serial_consistency argument, expected ")

    # Wrong types are caught by the request schema
    assert session.check_query(query, 400, options={"expand": "on"}) == "Expected boolean at $root.options.expand"
    assert session.check_query(query, 400, options={"page_size": "100"}) == "Expected integer at $root.options.page_size"
    assert session.check_query(query, 400, options="json") == "Expected object at $root.options"

    # A rejected override leaves the session alone
    assert session.check_option() == DEFAULT_OPTIONS


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

            table = unique_name()

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
    table = unique_name()

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

    user = unique_name()
    password = "very_secure_password123!!"

    staff_table = unique_name()
    secret_table = unique_name()

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


# The Web Shell reports its sessions in system.clients, telling encrypted
# (HTTPS) sessions apart from plain (HTTP) ones.
def test_session_is_reported_as_encrypted_over_https(cql, webshell):
    user = unique_name()
    password = "very_secure_password123!!"

    cql.execute(f"CREATE ROLE {user} WITH PASSWORD = '{password}' AND LOGIN = true")
    try:
        with new_session(webshell, user_credentials(user, password)):
            clients = list(cql.execute("SELECT ssl_enabled FROM system.clients WHERE "
                                       f"client_type = 'webshell' AND username = '{user}' ALLOW FILTERING"))
            assert len(clients) == 1
            assert clients[0].ssl_enabled == webshell.is_https
    finally:
        cql.execute(f"DROP ROLE {user}")


# Below: TLS client-certificate authentication (mutual TLS) on the HTTPS
# listener. The mode is selected by webshell_https_client_auth, which is
# live-updatable, so the tests switch between its three values - none, request
# and require - at runtime, with client_auth_context().


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


# With client authentication disabled, no certificate is requested from the
# client, so even a client which has one has to log in with a password.
def test_client_auth_none(cql, webshell_https, credentials, certificate_role):
    _, certificate = certificate_role
    endpoint = webshell_https.with_client_certificate(certificate)

    with client_auth_context(cql, "none"):
        check_status(endpoint.post("/login"), 400)

        with new_session(endpoint, credentials) as session:
            assert session.login_response == f"Successfully logged in as user {credentials.username}"


# A client certificate authenticates the session on its own, in both modes
# which request one.
@pytest.mark.parametrize("mode", ["request", "require"])
def test_client_certificate_login(cql, webshell_https, mode, certificate_role):
    role, certificate = certificate_role

    with client_auth_context(cql, mode):
        with new_session(webshell_https.with_client_certificate(certificate)) as session:
            assert session.login_response == f"Successfully logged in as user {role}"
            session.check_option("output format", "json")
            assert len(session.check_query("SELECT * FROM system.local")) == 1


# In request mode the certificate is optional: a client without one falls back
# to logging in with a username and password.
def test_client_auth_request_without_certificate(cql, webshell_https, credentials):
    with client_auth_context(cql, "request"):
        with new_session(webshell_https, credentials) as session:
            assert session.login_response == f"Successfully logged in as user {credentials.username}"


# In require mode a client without a certificate is rejected by the TLS
# handshake, its request never reaches the Web Shell.
def test_client_auth_require_without_certificate(cql, webshell_https, webshell_http, credentials, certificate_role):
    role, certificate = certificate_role

    with client_auth_context(cql, "require"):
        with pytest.raises((requests.exceptions.SSLError, requests.exceptions.ConnectionError)):
            webshell_https.post("/login", data=json.dumps(credentials._asdict()))

        # A client presenting a certificate is let in, ...
        with new_session(webshell_https.with_client_certificate(certificate)) as session:
            assert session.login_response == f"Successfully logged in as user {role}"

        # ... and the plain HTTP listener is not affected by the mode at all.
        with new_session(webshell_http, credentials) as session:
            assert session.login_response == f"Successfully logged in as user {credentials.username}"

    # Leaving the context restored the mode, live: a certificate is no longer
    # required.
    with new_session(webshell_https, credentials) as session:
        assert session.login_response == f"Successfully logged in as user {credentials.username}"


# A certificate the server trusts, but which names a role that doesn't exist,
# is rejected - there is no fall back to password authentication.
@pytest.mark.parametrize("mode", ["request", "require"])
def test_client_certificate_of_unknown_role(cql, webshell_https, mode, unknown_role_certificate, credentials):
    with client_auth_context(cql, mode):
        endpoint = webshell_https.with_client_certificate(unknown_role_certificate)

        r = check_status(endpoint.post("/login"), 400)
        assert "doesn't exist" in r.json()["response"]

        # Not even with valid credentials in the request body.
        r = check_status(endpoint.post("/login", data=json.dumps(credentials._asdict())), 400)
        assert "doesn't exist" in r.json()["response"]


# A certificate no role name can be extracted from is rejected too.
@pytest.mark.parametrize("mode", ["request", "require"])
def test_client_certificate_without_role_name(cql, webshell_https, mode, no_role_certificate):
    with client_auth_context(cql, mode):
        r = check_status(webshell_https.with_client_certificate(no_role_certificate).post("/login"), 400)
        assert "does not match any query expression" in r.json()["response"]


# The session a certificate logs in has exactly the permissions of the role the
# certificate names.
def test_client_certificate_session_permissions(cql, webshell_https, test_keyspace, certificate_role):
    role, certificate = certificate_role

    granted_table = unique_name()
    denied_table = unique_name()

    cql.execute(f"CREATE TABLE {test_keyspace}.{granted_table} (id int PRIMARY KEY, value text)")
    cql.execute(f"INSERT INTO {test_keyspace}.{granted_table} (id, value) VALUES (1, 'foo')")
    cql.execute(f"CREATE TABLE {test_keyspace}.{denied_table} (id int PRIMARY KEY, value text)")
    cql.execute(f"GRANT SELECT ON {test_keyspace}.{granted_table} TO {role}")

    try:
        with client_auth_context(cql, "request"):
            with new_session(webshell_https.with_client_certificate(certificate)) as session:
                session.check_option("output format", "json")

                assert session.check_query(f"SELECT * FROM {test_keyspace}.{granted_table}") == [{"id": 1, "value": "foo"}]

                # Reading a table the role has no permissions for is rejected
                # with the dedicated status code 403.
                session.check_query(f"SELECT * FROM {test_keyspace}.{denied_table}", 403)
    finally:
        cql.execute(f"DROP TABLE {test_keyspace}.{granted_table}")
        cql.execute(f"DROP TABLE {test_keyspace}.{denied_table}")
