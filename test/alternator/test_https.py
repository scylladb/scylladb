# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for Alternator (DynamoDB API) requests over HTTPS (a.k.a. SSL, TLS).
# These tests are skipped when the tests are *not* running over https, so
# run the tests with the "--https" flag to enable them. The "--https" flag,
# among other things, uses an https:// endpoint.
#############################################################################

import pytest
import urllib3
import urllib.parse
import ssl

@pytest.fixture(scope="module")
def https_url(dynamodb):
    url = dynamodb.meta.client._endpoint.host
    if not url.startswith('https://'):
        pytest.skip("HTTPS-specific tests are skipped without the '--https' option")
    yield url

# Test which TLS versions are supported. We require that both TLS 1.2 and 1.3
# must be supported, and the older TLS 1.1 version may either work or be
# rejected by the server with the proper error ("no protocols available"),
# and not a broken connection as happened in issue #8827.
# To make it easier to understand what TLS setup the client-side stack is
# using, we avoid boto3 and instead create requests manually using Python's
# "urllib3" (which boto3 also uses in its implementation). Moreover, to
# avoid having to build and sign complex requests (see examples of that in
# test_manual_requests.py), we used the generic health-check request (the "/"
# request). This request does not need to be signed - but still needs to use
# TLS properly, so we can use it to test TLS.
@pytest.mark.parametrize("tls_version_and_support_required", [
    ('TLSv1_1', False),
    ('TLSv1_2', True),
    ('TLSv1_3', True)])
def test_tls_versions(https_url, tls_version_and_support_required):
    tls_version, support_required = tls_version_and_support_required
    context = ssl.create_default_context()
    context.minimum_version = getattr(ssl.TLSVersion, tls_version)
    context.maximum_version = context.minimum_version
    # check_hostname and verify_mode is needed when we use self-signed
    # certificates in tests.
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with urllib3.PoolManager(ssl_context=context, retries=0) as pool:
        try:
            # preload_content=False tells the library not to read the content
            # and not release the connection back to the pool, so we can
            # still inspect this connection and its negoatiated TLS version.
            res = pool.request('GET', https_url, preload_content=False)
        except urllib3.exceptions.MaxRetryError as e:
            if support_required or 'NO_PROTOCOLS_AVAILABLE' not in str(e):
                # An error in a TLS version we are required to support,
                # or an unexpected type of error in any version - this is
                # a failure.
                raise
            return
        # Check that the negotiated TLS version, res.connection.sock.version(),
        # is the one we tried to set. Note that version() returns a string
        # like TLSv1.2, while the ssl.TLSVersion attribute name we used for
        # tls_version uses a underscore.
        assert res.connection
        assert res.connection.sock.version().replace('.', '_') == tls_version
        # Finally read the response
        res.read()
        assert res.status == 200

# Test that if we send an unencrypted (HTTP) request to an HTTPS port,
# it doesn't work.
# Although in theory it is possible to implement a server that serves both
# HTTP and HTTPS on the same port (by looking at the first byte of the
# request) - neither DynamoDB nor Scylla do this, and it's not a good idea:
# A deployment that wants to allow only HTTPS, not HTTP, probably has a
# security reason for doing this, and doesn't want us to allow an unencrypted
# HTTP request to be sent over the HTTPS port. So let's verify that an HTTP
# request on an HTTPS port indeed does not work.
def test_http_on_https(https_url):
    # Take an https URL, replace the https by http. If the port is not
    # explicitly specified we need to assume it is the default https port
    # (443) and need to add it explicitly, because it's not the default for
    # http.
    p = urllib.parse.urlparse(https_url)
    assert p.scheme == 'https'
    p = p._replace(scheme='http')
    if p.port is None:
        p = p._replace(netloc=p.netloc+':443')
    http_url = p.geturl()

    # CERT_NONE causes the client to not verify the server certificate,
    # and is needed because we use self-signed certificates in tests.
    with urllib3.PoolManager(retries=0, cert_reqs=ssl.CERT_NONE) as pool:
        # Sanity check: the original https:// URL works. This is useful to
        # know so that if the http:// URL does *not* work - as we expect -
        # we can be sure this is a real success of the test and not some
        # silly error causing every request to fail and not just the http one.
        res = pool.request('GET', https_url)
        assert res.status == 200
        # In DynamoDB, sending an unencrypted request to the SSL port results
        # in nice clean HTTP 400 Bad Request error, with a body explaining
        # that "The plain HTTP request was sent to HTTPS port". In Scylla,
        # the situation is not as clean - it replies with an SSL error which
        # confuses urllib3 because doesn't look like an HTTP reply. Let's
        # accept both. The important part is that the unencrypted request is
        # not accepted.
        try:
            res = pool.request('GET', http_url)
            assert res.status == 400
        except urllib3.exceptions.MaxRetryError as e:
            # This happens when the SSL server returns an error in SSL
            # form, which doesn't look like an HTTP status line.
            assert 'BadStatusLine' in str(e)
