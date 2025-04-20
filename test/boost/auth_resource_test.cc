/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core

#include "auth/resource.hh"
#include "test/lib/test_utils.hh"

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

BOOST_AUTO_TEST_CASE(root_of) {
    //
    // data
    //

    const auto dr = auth::resource(auth::resource_kind::data);
    BOOST_REQUIRE_EQUAL(dr.kind(), auth::resource_kind::data);

    const auto dv = auth::data_resource_view(dr);
    BOOST_REQUIRE(!dv.keyspace());
    BOOST_REQUIRE(!dv.table());

    //
    // role
    //

    const auto rr = auth::resource(auth::resource_kind::role);
    BOOST_REQUIRE_EQUAL(rr.kind(), auth::resource_kind::role);

    const auto rv = auth::role_resource_view(rr);
    BOOST_REQUIRE(!rv.role());

    //
    // service_level
    //

    const auto slr = auth::resource(auth::resource_kind::service_level);
    BOOST_REQUIRE_EQUAL(slr.kind(), auth::resource_kind::service_level);
}

BOOST_AUTO_TEST_CASE(data) {
    const auto r1 = auth::make_data_resource("my_keyspace");
    BOOST_REQUIRE_EQUAL(r1.kind(), auth::resource_kind::data);
    const auto v1 = auth::data_resource_view(r1);

    BOOST_REQUIRE_EQUAL(*v1.keyspace(), "my_keyspace");
    BOOST_REQUIRE(!v1.table());

    const auto r2 = auth::make_data_resource("my_keyspace", "my_table");
    BOOST_REQUIRE_EQUAL(r2.kind(), auth::resource_kind::data);
    const auto v2 = auth::data_resource_view(r2);

    BOOST_REQUIRE_EQUAL(*v2.keyspace(), "my_keyspace");
    BOOST_REQUIRE_EQUAL(*v2.table(), "my_table");
}

BOOST_AUTO_TEST_CASE(role) {
    const auto r = auth::make_role_resource("joe");
    BOOST_REQUIRE_EQUAL(r.kind(), auth::resource_kind::role);

    const auto v = auth::role_resource_view(r);
    BOOST_REQUIRE_EQUAL(*v.role(), "joe");
}

BOOST_AUTO_TEST_CASE(service_level) {
    const auto r = auth::make_service_level_resource();
    BOOST_REQUIRE_EQUAL(r.kind(), auth::resource_kind::service_level);
}

BOOST_AUTO_TEST_CASE(from_name) {
    //
    // data
    //

    const auto dr1 = auth::parse_resource("data");
    BOOST_REQUIRE_EQUAL(dr1, auth::root_data_resource());

    const auto dr2 = auth::parse_resource("data/my_keyspace");
    BOOST_REQUIRE_EQUAL(dr2, auth::make_data_resource("my_keyspace"));

    const auto dr3 = auth::parse_resource("data/my_keyspace/my_table");
    BOOST_REQUIRE_EQUAL(dr3, auth::make_data_resource("my_keyspace", "my_table"));

    BOOST_REQUIRE_THROW(auth::parse_resource("data/foo/bar/baz"), auth::invalid_resource_name);

    //
    // role
    //

    const auto rr1 = auth::parse_resource("roles");
    BOOST_REQUIRE_EQUAL(rr1, auth::root_role_resource());

    const auto rr2 = auth::parse_resource("roles/joe");
    BOOST_REQUIRE_EQUAL(rr2, auth::make_role_resource("joe"));

    BOOST_REQUIRE_THROW(auth::parse_resource("roles/joe/smith"), auth::invalid_resource_name);

    //
    //service_level
    //

    const auto slr1 = auth::parse_resource("service_levels");
    BOOST_REQUIRE_EQUAL(slr1, auth::root_service_level_resource());

    BOOST_REQUIRE_THROW(auth::parse_resource("service_levels/low_priority"), auth::invalid_resource_name);

    //
    // Generic errors.
    //

    BOOST_REQUIRE_THROW(auth::parse_resource("animal/horse"), auth::invalid_resource_name);
    BOOST_REQUIRE_THROW(auth::parse_resource(""), auth::invalid_resource_name);
}

BOOST_AUTO_TEST_CASE(name) {
    //
    // data
    //

    BOOST_REQUIRE_EQUAL(auth::root_data_resource().name(), "data");
    BOOST_REQUIRE_EQUAL(auth::make_data_resource("my_keyspace").name(), "data/my_keyspace");
    BOOST_REQUIRE_EQUAL(auth::make_data_resource("my_keyspace", "my_table").name(), "data/my_keyspace/my_table");

    //
    // role
    //

    BOOST_REQUIRE_EQUAL(auth::root_role_resource().name(), "roles");
    BOOST_REQUIRE_EQUAL(auth::make_role_resource("joe").name(), "roles/joe");

    //
    // service_level
    //

    BOOST_REQUIRE_EQUAL(auth::root_service_level_resource().name(), "service_levels");
}

BOOST_AUTO_TEST_CASE(parent) {
    const auto r1 = auth::make_data_resource("my_keyspace", "my_table");

    const auto r2 = r1.parent();
    BOOST_REQUIRE(r2);
    BOOST_REQUIRE_EQUAL(*r2, auth::make_data_resource("my_keyspace"));

    const auto r3 = r2->parent();
    BOOST_REQUIRE(r3);
    BOOST_REQUIRE_EQUAL(*r3, auth::root_data_resource());

    const auto r4 = r3->parent();
    BOOST_REQUIRE(!r4);
}

BOOST_AUTO_TEST_CASE(output) {
    //
    // data
    //

    BOOST_REQUIRE_EQUAL(format("{}", auth::root_data_resource()), "<all keyspaces>");
    BOOST_REQUIRE_EQUAL(format("{}", auth::make_data_resource("my_keyspace")), "<keyspace my_keyspace>");

    BOOST_REQUIRE_EQUAL(
            format("{}", auth::make_data_resource("my_keyspace", "my_table")),
            "<table my_keyspace.my_table>");

    //
    // role
    //

    BOOST_REQUIRE_EQUAL(format("{}", auth::root_role_resource()), "<all roles>");
    BOOST_REQUIRE_EQUAL(format("{}", auth::make_role_resource("joe")), "<role joe>");

    //
    // service_level
    //

    BOOST_REQUIRE_EQUAL(format("{}", auth::root_service_level_resource()), "<all service levels>");
}

BOOST_AUTO_TEST_CASE(expand) {
    const auto r1 = auth::root_data_resource();
    const auto r2 = auth::make_data_resource("ks");
    const auto r3 = auth::make_data_resource("ks", "tab");

    BOOST_REQUIRE_EQUAL(auth::expand_resource_family(r1), (auth::resource_set{r1}));
    BOOST_REQUIRE_EQUAL(auth::expand_resource_family(r2), (auth::resource_set{r1, r2}));
    BOOST_REQUIRE_EQUAL(auth::expand_resource_family(r3), (auth::resource_set{r1, r2, r3}));
}
