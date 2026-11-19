/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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

    //
    // object_storage
    //

    const auto osr = auth::resource(auth::resource_kind::object_storage);
    BOOST_REQUIRE_EQUAL(osr.kind(), auth::resource_kind::object_storage);
    BOOST_REQUIRE(!osr.parent());
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
    // object_storage
    //

    const auto osr1 = auth::parse_resource("object_storage");
    BOOST_REQUIRE_EQUAL(osr1, auth::root_object_storage_resource());

    BOOST_REQUIRE_THROW(auth::parse_resource("object_storage/my_bucket"), auth::invalid_resource_name);

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

    //
    // object_storage
    //

    BOOST_REQUIRE_EQUAL(auth::root_object_storage_resource().name(), "object_storage");
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

    //
    // object_storage
    //

    BOOST_REQUIRE_EQUAL(format("{}", auth::root_object_storage_resource()), "<all object storage>");
}

BOOST_AUTO_TEST_CASE(expand) {
    const auto r1 = auth::root_data_resource();
    const auto r2 = auth::make_data_resource("ks");
    const auto r3 = auth::make_data_resource("ks", "tab");

    BOOST_REQUIRE_EQUAL(auth::expand_resource_family(r1), (auth::resource_set{r1}));
    BOOST_REQUIRE_EQUAL(auth::expand_resource_family(r2), (auth::resource_set{r1, r2}));
    BOOST_REQUIRE_EQUAL(auth::expand_resource_family(r3), (auth::resource_set{r1, r2, r3}));
}

BOOST_AUTO_TEST_CASE(cluster_resources) {
    const auto cluster = auth::root_cluster_resource();
    const auto dc = auth::make_cluster_resource("dc1");
    const auto rack = auth::make_cluster_resource("dc1", "rack1");
    const auto node = auth::make_cluster_resource("dc1", "rack1", "7f4d8b1e-1c2a-4f3b-9e6d-0a1b2c3d4e5f");

    BOOST_REQUIRE_EQUAL(cluster.name(), "cluster");
    BOOST_REQUIRE_EQUAL(dc.name(), "cluster/dc1");
    BOOST_REQUIRE_EQUAL(rack.name(), "cluster/dc1/rack1");
    BOOST_REQUIRE_EQUAL(node.name(), "cluster/dc1/rack1/7f4d8b1e-1c2a-4f3b-9e6d-0a1b2c3d4e5f");

    for (const auto& r : {cluster, dc, rack, node}) {
        BOOST_REQUIRE_EQUAL(auth::parse_resource(r.name()), r);
        BOOST_REQUIRE_EQUAL(r.applicable_permissions().mask(),
                (auth::permission_set::of<auth::permission::CONFIGURE, auth::permission::AUTHORIZE>().mask()));
    }

    BOOST_REQUIRE_EQUAL(*node.parent(), rack);
    BOOST_REQUIRE_EQUAL(*rack.parent(), dc);
    BOOST_REQUIRE_EQUAL(*dc.parent(), cluster);
    BOOST_REQUIRE(!cluster.parent());

    BOOST_REQUIRE_EQUAL(format("{}", cluster), "<all scopes>");
    BOOST_REQUIRE_EQUAL(format("{}", dc), "<datacenter dc1>");
    BOOST_REQUIRE_EQUAL(format("{}", rack), "<rack dc1 rack1>");
    BOOST_REQUIRE_EQUAL(format("{}", node), "<node 7f4d8b1e-1c2a-4f3b-9e6d-0a1b2c3d4e5f>");

    BOOST_REQUIRE_THROW(auth::parse_resource("cluster/dc1/rack1/node/extra"), auth::invalid_resource_name);
}

// Datacenter and rack names are free-form; the name separator and the config option
// marker must survive a round trip.
BOOST_AUTO_TEST_CASE(cluster_resource_name_encoding) {
    const auto rack = auth::make_cluster_resource("us-east/1", "@config:x y%");
    BOOST_REQUIRE_EQUAL(rack.name(), "cluster/us-east%2F1/%40config%3Ax%20y%25");

    const auto parsed = auth::parse_resource(rack.name());
    BOOST_REQUIRE_EQUAL(parsed, rack);
    BOOST_REQUIRE_EQUAL(*auth::cluster_resource_view(parsed).datacenter(), "us-east/1");
    BOOST_REQUIRE_EQUAL(*auth::cluster_resource_view(parsed).rack(), "@config:x y%");
    BOOST_REQUIRE(!parsed.config_option());

    BOOST_REQUIRE_THROW(auth::parse_resource("cluster/dc%2"), auth::invalid_resource_name);
    BOOST_REQUIRE_THROW(auth::parse_resource("cluster/dc%zz"), auth::invalid_resource_name);
}

BOOST_AUTO_TEST_CASE(config_option_resources) {
    const auto table = auth::make_data_resource("ks", "t");
    const auto ks = auth::make_data_resource("ks");
    const auto dc = auth::make_cluster_resource("dc1");

    const auto table_opt = table.with_config_option("auto_repair_enabled");
    BOOST_REQUIRE_EQUAL(table_opt.name(), "data/ks/t/@config:auto_repair_enabled");
    BOOST_REQUIRE_EQUAL(ks.with_config_option("x").name(), "data/ks/@config:x");
    BOOST_REQUIRE_EQUAL(auth::root_data_resource().with_config_option("x").name(), "data/@config:x");
    BOOST_REQUIRE_EQUAL(auth::root_cluster_resource().with_config_option("x").name(), "cluster/@config:x");
    BOOST_REQUIRE_EQUAL(dc.with_config_option("x").name(), "cluster/dc1/@config:x");

    for (const auto& r : {table_opt, ks.with_config_option("x"), auth::root_data_resource().with_config_option("x"),
            auth::root_cluster_resource().with_config_option("x"), dc.with_config_option("x")}) {
        const auto parsed = auth::parse_resource(r.name());
        BOOST_REQUIRE_EQUAL(parsed, r);
        BOOST_REQUIRE(parsed.config_option() == r.config_option());
        BOOST_REQUIRE_EQUAL(r.applicable_permissions().mask(), auth::permission_set::of<auth::permission::CONFIGURE>().mask());
    }

    // The sub-resource is distinct from the resource and from another option's sub-resource.
    BOOST_REQUIRE(table_opt != table);
    BOOST_REQUIRE(table_opt != table.with_config_option("other"));
    BOOST_REQUIRE(std::hash<auth::resource>()(table_opt) != std::hash<auth::resource>()(table));
    BOOST_REQUIRE_EQUAL(*table_opt.parent(), table);
    BOOST_REQUIRE_EQUAL(table_opt.without_config_option(), table);
    BOOST_REQUIRE_EQUAL(*auth::data_resource_view(table_opt).table(), "t");

    // An Alternator view is named <table>:<index>, so a table can be called "config:idx".
    const auto view = auth::parse_resource("data/alternator_config/config:idx");
    BOOST_REQUIRE_EQUAL(view, auth::make_data_resource("alternator_config", "config:idx"));
    BOOST_REQUIRE(!view.config_option());

    // Only data and cluster resources have config options. For the other kinds the part
    // is an ordinary name.
    const auto role = auth::parse_resource("roles/@config:x");
    BOOST_REQUIRE_EQUAL(role, auth::make_role_resource("@config:x"));
    BOOST_REQUIRE(!role.config_option());
}

BOOST_AUTO_TEST_CASE(config_scope_chain) {
    const auto table = auth::make_data_resource("ks", "t");
    const auto p1 = auth::config_scope_parent(table);
    BOOST_REQUIRE_EQUAL(*p1, auth::make_data_resource("ks"));
    const auto p2 = auth::config_scope_parent(*p1);
    BOOST_REQUIRE_EQUAL(*p2, auth::root_data_resource());
    const auto p3 = auth::config_scope_parent(*p2);
    BOOST_REQUIRE_EQUAL(*p3, auth::root_cluster_resource());
    BOOST_REQUIRE(!auth::config_scope_parent(*p3));

    const auto node = auth::make_cluster_resource("dc1", "rack1", "7f4d8b1e-1c2a-4f3b-9e6d-0a1b2c3d4e5f");
    const auto n1 = auth::config_scope_parent(node);
    BOOST_REQUIRE_EQUAL(*n1, auth::make_cluster_resource("dc1", "rack1"));
    const auto n2 = auth::config_scope_parent(*n1);
    BOOST_REQUIRE_EQUAL(*n2, auth::make_cluster_resource("dc1"));
    BOOST_REQUIRE_EQUAL(*auth::config_scope_parent(*n2), auth::root_cluster_resource());

    // The roots of the other kinds are not below the cluster.
    BOOST_REQUIRE(!auth::config_scope_parent(auth::root_role_resource()));
}
