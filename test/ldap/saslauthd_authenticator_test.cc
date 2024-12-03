/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdlib>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>

#include <fmt/ranges.h>
#include "utils/to_string.hh"

#include "auth/saslauthd_authenticator.hh"
#include "db/config.hh"
#include "test/ldap/ldap_common.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/test_utils.hh"
#include "seastarx.hh"

const auto sockpath = std::getenv("SASLAUTHD_MUX_PATH");

using exceptions::authentication_exception;
using exception_predicate::message_contains;

SEASTAR_THREAD_TEST_CASE(simple_password_checking) {
    BOOST_REQUIRE(!auth::authenticate_with_saslauthd(sockpath, {"jdoe", "xxxxxxxx", "", ""}).get());
    BOOST_REQUIRE(auth::authenticate_with_saslauthd(sockpath, {"jdoe", "pa55w0rd", "", ""}).get());
    BOOST_REQUIRE(!auth::authenticate_with_saslauthd(sockpath, {"", "", "", ""}).get());
    BOOST_REQUIRE(!auth::authenticate_with_saslauthd(sockpath, {"", "", ".", "."}).get());
    BOOST_REQUIRE_EXCEPTION(
            auth::authenticate_with_saslauthd("/a/nonexistent/path", {"jdoe", "pa55w0rd", "", ""}).get(),
            authentication_exception, message_contains("socket connection error"));
}

namespace {

void fail_test(std::exception_ptr ex) {
    BOOST_FAIL(format("{}", ex));
}

/// Creates a network response that saslauthd would send to convey this payload.  If lie_size is provided,
/// force-write it into the response's first two bytes, even if that results in an invalid response.
temporary_buffer<char> make_saslauthd_response(std::string_view payload, std::optional<uint16_t> lie_size = std::nullopt) {
    const uint16_t sz = payload.size();
    temporary_buffer<char> resp(sz + 2);
    auto p = resp.get_write();
    produce_be(p, lie_size.value_or(sz));
    std::copy_n(payload.begin(), sz, p);
    return resp;
}

/// Invokes authenticate_with_saslauthd against a mock saslauthd instance that sends this response through this
/// domain socket.
///
/// authenticate_with_saslauthd is invoked with correct credentials, and its result is returned.
///
/// Must be invoked inside a Seastar thread.
bool authorize_against_this_response(temporary_buffer<char> resp, sstring socket_path) {
    auto socket = seastar::listen(socket_address(unix_domain_addr(socket_path)));
    auto [result, closing] = when_all(
            auth::authenticate_with_saslauthd(socket_path, {"jdoe", "pa55w0rd", "", ""}),
            socket.accept().then([resp = std::move(resp), socket_path] (accept_result ar) mutable {
                return do_with(
                        ar.connection.input(), ar.connection.output(), socket_path,
                        [resp = std::move(resp)] (input_stream<char>& in, output_stream<char>& out, sstring& socket_path) mutable {
                            return in.read().then(
                                    [&out, resp=std::move(resp)] (temporary_buffer<char>) mutable {
                                        return out.write(std::move(resp)).finally([&out] { return out.close(); });
                                    }).handle_exception(fail_test).finally([&] {
                                        return in.close().finally([&] { return remove_file(socket_path); });
                                    });
                        });
            })).get();
    return result.get();
}

/// Temp file name unique to this test run and this suffix.
sstring tmpfile(const sstring& suffix) {
    return seastar::format("saslauthd_authenticator_test.tmpfile.{}.{}", ldap_port, suffix);
}

shared_ptr<db::config> make_config() {
    auto p = make_shared<db::config>();
    p->authenticator("com.scylladb.auth.SaslauthdAuthenticator");
    p->saslauthd_socket_path(sockpath);
    return p;
}

auth::authenticator& authenticator(cql_test_env& env) {
    return env.local_auth_service().underlying_authenticator();
}

/// Creates a cql_test_env with saslauthd_authenticator in a Seastar thread, then invokes func with the env's
/// authenticator.
future<> do_with_authenticator_thread(std::function<void(auth::authenticator&, service::group0_batch& b)> func) {
    return do_with_cql_env_thread([func = std::move(func)] (cql_test_env& env) {
        return do_with_mc(env, [&] (service::group0_batch& b) {
            return func(authenticator(env), b);
        });
    }, make_config());
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(empty_response) {
    BOOST_REQUIRE_EXCEPTION(authorize_against_this_response(temporary_buffer<char>(0), tmpfile("0")),
                            authentication_exception, message_contains("closed connection"));
}

SEASTAR_THREAD_TEST_CASE(single_byte_response) {
    BOOST_REQUIRE_EXCEPTION(
            authorize_against_this_response(temporary_buffer<char>(1), tmpfile("1")),
            authentication_exception, message_contains("closed connection"));
}

SEASTAR_THREAD_TEST_CASE(two_byte_response) {
    BOOST_REQUIRE(!authorize_against_this_response(make_saslauthd_response(""), tmpfile("2")));
    BOOST_REQUIRE_EXCEPTION(
            authorize_against_this_response(make_saslauthd_response("", 1), tmpfile("2")),
            authentication_exception, message_contains("response length different"));
    BOOST_REQUIRE_EXCEPTION(
            authorize_against_this_response(make_saslauthd_response("", 100), tmpfile("2")),
            authentication_exception, message_contains("response length different"));
}

SEASTAR_THREAD_TEST_CASE(three_byte_response) {
    BOOST_REQUIRE(!authorize_against_this_response(make_saslauthd_response("O"), tmpfile("3")));
    // If advertised size is 0, the payload isn't read even if sent.  No exception is expected:
    BOOST_REQUIRE(!authorize_against_this_response(make_saslauthd_response("O", 0), tmpfile("3")));
    BOOST_REQUIRE_EXCEPTION(
            authorize_against_this_response(make_saslauthd_response("O", 100), tmpfile("3")),
            authentication_exception, message_contains("response length different"));
}

SEASTAR_THREAD_TEST_CASE(ok_response_wrong_length) {
    BOOST_REQUIRE_EXCEPTION(
            authorize_against_this_response(make_saslauthd_response("OK", 100), tmpfile("3")),
            authentication_exception, message_contains("response length different"));
    // Extra payload beyond advertised size is not read.  No exception is expected:
    BOOST_REQUIRE(!authorize_against_this_response(make_saslauthd_response("OK", 1), tmpfile("3")));
}

SEASTAR_TEST_CASE(require_authentication) {
    return do_with_authenticator_thread([] (auth::authenticator& authr, service::group0_batch& b) {
        BOOST_REQUIRE(authr.require_authentication());
    });
}

SEASTAR_TEST_CASE(authenticate) {
    return do_with_authenticator_thread([] (auth::authenticator& authr, service::group0_batch& b) {
        const auto user = auth::authenticator::USERNAME_KEY, pwd = auth::authenticator::PASSWORD_KEY;
        BOOST_REQUIRE_EQUAL(authr.authenticate({{user, "jdoe"}, {pwd, "pa55w0rd"}}).get().name, "jdoe");
        BOOST_REQUIRE_EXCEPTION(
                authr.authenticate({{user, "jdoe"}, {pwd, ""}}).get(),
                authentication_exception, message_contains("Incorrect credentials"));
        BOOST_REQUIRE_EXCEPTION(
                authr.authenticate({{user, "jdoe"}}).get(),
                authentication_exception, message_contains("password' is missing"));
        BOOST_REQUIRE_EXCEPTION(
                authr.authenticate({{pwd, "pwd"}}).get(),
                authentication_exception, message_contains("username' is missing"));
    });
}

SEASTAR_TEST_CASE(create) {
    return do_with_authenticator_thread([] (auth::authenticator& authr, service::group0_batch& b) {
        BOOST_REQUIRE_EXCEPTION(
                authr.create("new-role", {auth::password_option{"password"}}, b).get(),
                authentication_exception, message_contains("Cannot create"));
    });
}

SEASTAR_TEST_CASE(alter) {
    return do_with_authenticator_thread([] (auth::authenticator& authr, service::group0_batch& b) {
        BOOST_REQUIRE_EXCEPTION(
                authr.alter("jdoe", {auth::password_option{"password"}}, b).get(),
                authentication_exception, message_contains("Cannot modify"));
    });
}

SEASTAR_TEST_CASE(drop) {
    return do_with_authenticator_thread([] (auth::authenticator& authr, service::group0_batch& b) {
        BOOST_REQUIRE_EXCEPTION(authr.drop("jdoe", b).get(), authentication_exception, message_contains("Cannot delete"));
    });
}

SEASTAR_TEST_CASE(sasl_challenge) {
    return do_with_authenticator_thread([] (auth::authenticator& authr, service::group0_batch& b) {
        constexpr char creds[] = "\0jdoe\0pa55w0rd";
        const auto ch = authr.new_sasl_challenge();
        BOOST_REQUIRE(ch->evaluate_response(bytes(creds, creds + 14)).empty());
        BOOST_REQUIRE_EQUAL("jdoe", ch->get_authenticated_user().get().name);
        BOOST_REQUIRE(ch->evaluate_response(bytes(creds, creds + 13)).empty());
        BOOST_REQUIRE_EXCEPTION(
                ch->get_authenticated_user().get(),
                authentication_exception, message_contains("Incorrect credentials"));
    });
}
