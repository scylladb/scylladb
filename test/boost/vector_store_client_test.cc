/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/vector_store_client.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/short_streams.hh>


namespace {

using namespace seastar;

using vector_store_client = service::vector_store_client;
using vector_store_client_tester = service::vector_store_client_tester;
using config = vector_store_client::config;
using configuration_exception = exceptions::configuration_exception;

} // namespace

BOOST_AUTO_TEST_CASE(vector_store_client_test_ctor) {
    {
        auto cfg = config();
        auto vs = vector_store_client{cfg};
        BOOST_CHECK(vs.is_disabled());
        BOOST_CHECK(!vs.host());
        BOOST_CHECK(!vs.port());
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://good.authority.com:6080");
        auto vs = vector_store_client{cfg};
        BOOST_CHECK(!vs.is_disabled());
        BOOST_CHECK_EQUAL(*vs.host(), "good.authority.com");
        BOOST_CHECK_EQUAL(*vs.port(), 6080);
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://bad,authority.com:6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("bad-schema://authority.com:6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:a6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:60806080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.format.com:60:80");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://authority.com:6080/bad/path");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    }
}

