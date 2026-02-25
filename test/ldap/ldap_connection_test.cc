/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define LDAP_DEPRECATED 1

#include <seastar/testing/thread_test_case.hh>

#include <cassert>
#include <exception>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>
#include <set>
#include <string>

#include "ent/ldap/ldap_connection.hh"
#include "test/lib/exception_utils.hh"
#include "ldap_common.hh"
#include "seastarx.hh"

extern "C" {
#include <ldap.h>
}

namespace {

logger mylog{"ldap_connection_test"}; // `log` is taken by math.

void set_defbase() {
    ldap_set_option(nullptr, LDAP_OPT_DEFBASE, base_dn);
}

const std::set<std::string> results_expected_from_search_base_dn{
    "dc=example,dc=com",
    "cn=root,dc=example,dc=com",
    "ou=People,dc=example,dc=com",
    "uid=cassandra,ou=People,dc=example,dc=com",
    "uid=jsmith,ou=People,dc=example,dc=com",
    "uid=jdoe,ou=People,dc=example,dc=com",
    "cn=role1,dc=example,dc=com",
    "cn=role2,dc=example,dc=com",
    "cn=role3,dc=example,dc=com",
};

/// Ignores exceptions from LDAP operations.  Useful for failure-injection tests, which must
/// tolerate aborted communication.
ldap_msg_ptr ignore(std::exception_ptr) {
    return ldap_msg_ptr();
}

/// Extracts entries from an ldap_search result.
std::set<std::string> entries(LDAP* ld, LDAPMessage* res) {
    BOOST_REQUIRE_EQUAL(LDAP_RES_SEARCH_ENTRY, ldap_msgtype(res));
    std::set<std::string> entry_set;
    for (auto e = ldap_first_entry(ld, res); e; e = ldap_next_entry(ld, e)) {
        char* dn = ldap_get_dn(ld, e);
        entry_set.insert(dn);
        ldap_memfree(dn);
    }
    return entry_set;
}

future<ldap_msg_ptr> search(ldap_connection& conn, const char* term) {
    return conn.search(
            const_cast<char*>(term),
            LDAP_SCOPE_SUBTREE,
            /*filter=*/nullptr,
            /*attrs=*/nullptr,
            /*attrsonly=*/0,
            /*serverctrls=*/nullptr,
            /*clientctrls=*/nullptr,
            /*timeout=*/nullptr,
            /*sizelimit=*/0);
}

future<ldap_msg_ptr> bind(ldap_connection& conn) {
    return conn.simple_bind(manager_dn, manager_password);
}


/// Creates an ldap_connection, invokes a function on it, then waits for its closing.  Must be
/// invoked from Seastar thread.
void with_ldap_connection(seastar::connected_socket&& socket, std::function<void(ldap_connection&)> f) {
    mylog.trace("with_ldap_connection");
    ldap_connection c(std::move(socket));
    auto do_close = defer([&] { c.close().get(); });
    mylog.trace("with_ldap_connection: invoking f");
    f(c);
    mylog.trace("with_ldap_connection done");
}

/// Connects to an address, then invokes with_ldap_connection on the resulting socket.  Must be
/// invoked from Seastar thread.
void with_ldap_connection(const seastar::socket_address& a, std::function<void(ldap_connection&)> f) {
    with_ldap_connection(connect(a).get(), f);
}

} // anonymous namespace

// Tests default (non-custom) libber networking.  Failure here indicates a likely bug in test.py's
// LDAP setup.
SEASTAR_THREAD_TEST_CASE(bind_with_default_io) {
    set_defbase();
    const auto server_uri = "ldap://localhost:" + ldap_port;
    LDAP *manager_client_state{nullptr};
    BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, ldap_initialize(&manager_client_state, server_uri.c_str()));
    static constexpr int v3 = LDAP_VERSION3;
    BOOST_REQUIRE_EQUAL(LDAP_OPT_SUCCESS, ldap_set_option(manager_client_state, LDAP_OPT_PROTOCOL_VERSION, &v3));
    // Retry on EINTR, rather than return error:
    BOOST_REQUIRE_EQUAL(LDAP_OPT_SUCCESS, ldap_set_option(manager_client_state, LDAP_OPT_RESTART, LDAP_OPT_ON));
    BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, ldap_simple_bind_s(manager_client_state, manager_dn, manager_password));
    BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, ldap_unbind(manager_client_state));
}

SEASTAR_THREAD_TEST_CASE(bind_with_custom_sockbuf_io) {
    set_defbase();
    mylog.trace("bind_with_custom_sockbuf_io");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        mylog.trace("bind_with_custom_sockbuf_io invoking bind");
        const auto res = bind(c).get();
        BOOST_REQUIRE_EQUAL(LDAP_RES_BIND, ldap_msgtype(res.get()));
    });
    mylog.trace("bind_with_custom_sockbuf_io done");
}

SEASTAR_THREAD_TEST_CASE(search_with_custom_sockbuf_io) {
    set_defbase();
    mylog.trace("search_with_custom_sockbuf_io");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        mylog.trace("search_with_custom_sockbuf_io: invoking search");
        const auto res = search(c, base_dn).get();
        mylog.trace("search_with_custom_sockbuf_io: got result");
        const auto actual = entries(c.get_ldap(), res.get());
        const std::set<std::string>& expected = results_expected_from_search_base_dn;
        BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.cbegin(), actual.cend(), expected.cbegin(), expected.cend());
    });
    mylog.trace("search_with_custom_sockbuf_io done");
}

SEASTAR_THREAD_TEST_CASE(multiple_outstanding_operations) {
    set_defbase();
    mylog.trace("multiple_outstanding_operations");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        mylog.trace("multiple_outstanding_operations: bind");
        BOOST_REQUIRE_EQUAL(LDAP_RES_BIND, ldap_msgtype(bind(c).get().get()));

        std::vector<future<ldap_msg_ptr>> results_base;
        for (size_t i = 0; i < 30; ++i) {
            mylog.trace("multiple_outstanding_operations: invoking search base #{}", i);
            results_base.push_back(search(c, base_dn));
            mylog.trace("multiple_outstanding_operations: search base #{} got future {}", i, static_cast<const void*>(&results_base.back()));
        }

        std::vector<future<ldap_msg_ptr>> results_jsmith;
        for (size_t i = 0; i < 30; ++i) {
            mylog.trace("multiple_outstanding_operations: invoking search jsmith #{}", i);
            results_jsmith.push_back(search(c, "uid=jsmith,ou=People,dc=example,dc=com"));
            mylog.trace("multiple_outstanding_operations: search jsmith #{} got future {}", i, static_cast<const void*>(&results_jsmith.back()));
        }

        using boost::test_tools::per_element;
        mylog.trace("multiple_outstanding_operations: check base results");
        future<> base_result_fut = parallel_for_each(results_base, [&c] (future<ldap_msg_ptr>& res) {
            const auto actual_base = entries(c.get_ldap(), res.get().get());
            BOOST_TEST_INFO("result for " << &res);
            BOOST_TEST(actual_base == results_expected_from_search_base_dn, per_element());
            return make_ready_future();
        });

        mylog.trace("multiple_outstanding_operations: check jsmith result");
        static const std::set<std::string> expected_jsmith{"uid=jsmith,ou=People,dc=example,dc=com"};
        future<> jsmith_result_fut = parallel_for_each(results_jsmith, [&c] (future<ldap_msg_ptr>& res) {
            const auto actual_jsmith = entries(c.get_ldap(), res.get().get());
            BOOST_TEST_INFO("result for " << &res);
            BOOST_TEST(actual_jsmith == expected_jsmith, per_element());
            return make_ready_future();
        });

        when_all(std::move(base_result_fut), std::move(jsmith_result_fut)).get();
    });
    mylog.trace("multiple_outstanding_operations done");
}

SEASTAR_THREAD_TEST_CASE(early_shutdown) {
    set_defbase();
    mylog.trace("early_shutdown: noop");
    with_ldap_connection(local_ldap_address, [] (ldap_connection&) {});
    mylog.trace("early_shutdown: bind");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) { bind(c).handle_exception(&ignore).get(); });
    mylog.trace("early_shutdown: search");
    with_ldap_connection(
            local_ldap_address, [] (ldap_connection& c) { search(c, base_dn).handle_exception(&ignore).get(); });
}

SEASTAR_THREAD_TEST_CASE(bind_after_fail) {
    set_defbase();
    mylog.trace("bind_after_fail: wonky connection");
    with_ldap_connection(local_fail_inject_address, [] (ldap_connection& wonky_conn) {
        bind(wonky_conn).handle_exception(&ignore).get();
    });
    mylog.trace("bind_after_fail: solid connection");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        const auto res = bind(c).get();
        BOOST_REQUIRE_EQUAL(LDAP_RES_BIND, ldap_msgtype(res.get()));
    });
    mylog.trace("bind_after_fail done");
}

SEASTAR_THREAD_TEST_CASE(search_after_fail) {
    set_defbase();
    mylog.trace("search_after_fail: wonky connection");
    with_ldap_connection(local_fail_inject_address, [] (ldap_connection& wonky_conn) {
        search(wonky_conn, base_dn).handle_exception(&ignore).get();
    });
    mylog.trace("search_after_fail: solid connection");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        const auto res = search(c, base_dn).get();
        mylog.trace("search_after_fail: got search result");
        const auto actual = entries(c.get_ldap(), res.get());
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
                actual.cbegin(), actual.cend(),
                results_expected_from_search_base_dn.cbegin(), results_expected_from_search_base_dn.cend());
    });
    mylog.trace("search_after_fail done");
}

SEASTAR_THREAD_TEST_CASE(multiple_outstanding_operations_on_failing_connection) {
    set_defbase();
    mylog.trace("multiple_outstanding_operations_on_failing_connection");
    with_ldap_connection(local_fail_inject_address, [] (ldap_connection& c) {
        mylog.trace("multiple_outstanding_operations_on_failing_connection: invoking bind");
        bind(c).handle_exception(&ignore).get();;

        std::vector<future<ldap_msg_ptr>> results_base;
        for (size_t i = 0; i < 10; ++i) {
            mylog.trace("multiple_outstanding_operations_on_failing_connection: invoking search base");
            results_base.push_back(search(c, base_dn).handle_exception(&ignore));
        }

        std::vector<future<ldap_msg_ptr>> results_jsmith;
        for (size_t i = 0; i < 10; ++i) {
            mylog.trace("multiple_outstanding_operations_on_failing_connection: invoking search jsmith");
            results_jsmith.push_back(search(c, "uid=jsmith,ou=People,dc=example,dc=com").handle_exception(&ignore));
        }

        mylog.trace("multiple_outstanding_operations_on_failing_connection: getting base results");
        when_all_succeed(results_base.begin(), results_base.end()).get();
        mylog.trace("multiple_outstanding_operations_on_failing_connection: getting jsmith results");
        when_all_succeed(results_jsmith.begin(), results_jsmith.end()).get();
    });
    mylog.trace("multiple_outstanding_operations_on_failing_connection done");
}

using exception_predicate::message_contains;

SEASTAR_THREAD_TEST_CASE(bind_after_close) {
    set_defbase();
    ldap_connection c(connect(local_ldap_address).get());
    c.close().get();
    BOOST_REQUIRE_EXCEPTION(bind(c).get(), std::runtime_error, message_contains("ldap_connection"));
}

SEASTAR_THREAD_TEST_CASE(search_after_close) {
    set_defbase();
    ldap_connection c(connect(local_ldap_address).get());
    c.close().get();
    BOOST_REQUIRE_EXCEPTION(search(c, base_dn).get(), std::runtime_error, message_contains("ldap_connection"));
}

SEASTAR_THREAD_TEST_CASE(close_after_close) {
    set_defbase();
    ldap_connection c(connect(local_ldap_address).get());
    c.close().get();
    BOOST_REQUIRE_EXCEPTION(c.close().get(), std::runtime_error, message_contains("ldap_connection"));
}

SEASTAR_THREAD_TEST_CASE(severed_connection_yields_exceptional_future) {
    set_defbase();
    with_ldap_connection(local_fail_inject_address, [] (ldap_connection& c) {
        int up = 1;
        while (up) {
            search(c, base_dn)
                    .handle_exception_type([&] (std::runtime_error&) { up = 0; return ldap_msg_ptr(); })
                    .get();
        }
    });
}
