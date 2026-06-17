/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Test for alternator::scan_table() - full table scan functionality.
// Tables and items are created through the Alternator executor API
// (CreateTable / PutItem) rather than raw CQL.
// See SCYLLADB-1888.

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"

#include "alternator/export.hh"
#include "alternator/executor.hh"
#include "alternator/error.hh"
#include "alternator/rmw_operation.hh"
#include "cdc/metadata.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "service_permit.hh"
#include "utils/rjson.hh"

#include <set>
#include <seastar/core/smp.hh>
#include <seastar/util/defer.hh>

namespace {

// Wraps a sharded<alternator::executor> for use in tests. Provides
// create_table() and put_item() methods that accept DynamoDB-style
// JSON requests, just like the real Alternator HTTP API would.
class alternator_test_executor {
    cdc::metadata _cdc_md;
    sharded<alternator::executor> _exec;
    alternator::rmw_operation::write_isolation _saved_write_isolation;
public:
    void start(cql_test_env& e) {
        // Save and override the default write isolation. The default
        // LWT_ALWAYS uses proxy.cas() which requires need_remote_proxy
        // in the test env. FORBID_RMW uses direct quorum writes which
        // work without a remote proxy — sufficient for our unconditional
        // PutItem calls.
        _saved_write_isolation = alternator::rmw_operation::default_write_isolation;
        alternator::rmw_operation::set_default_write_isolation("forbid_rmw");
        _exec.start(
            std::ref(e.gossiper().local()),
            std::ref(e.get_storage_proxy().local()),
            std::ref(e.get_storage_service().local()),
            std::ref(e.migration_manager().local()),
            std::ref(e.get_system_distributed_keyspace().local()),
            std::ref(e.get_system_keyspace().local()),
            std::ref(_cdc_md),
            std::ref(e.vector_store_client().local()),
            default_smp_service_group(),
            utils::updateable_value<uint32_t>(10000)).get();
    }

    void stop() {
        _exec.stop().get();
        alternator::rmw_operation::default_write_isolation = _saved_write_isolation;
    }

    // Call CreateTable with a DynamoDB-style JSON request.
    void create_table(rjson::value request) {
        service::client_state cs(service::client_state::internal_tag{});
        tracing::trace_state_ptr ts;
        std::unique_ptr<audit::audit_info_alternator> ai;
        auto result = _exec.local().create_table(cs, ts, empty_service_permit(),
            std::move(request), ai).get();
        if (auto* err = std::get_if<alternator::api_error>(&result)) {
            BOOST_FAIL(fmt::format("CreateTable failed: {}", err->what()));
        }
    }

    // Call PutItem with a DynamoDB-style JSON request.
    void put_item(rjson::value request) {
        service::client_state cs(service::client_state::internal_tag{});
        tracing::trace_state_ptr ts;
        std::unique_ptr<audit::audit_info_alternator> ai;
        auto result = _exec.local().put_item(cs, ts, empty_service_permit(),
            std::move(request), ai).get();
        if (auto* err = std::get_if<alternator::api_error>(&result)) {
            BOOST_FAIL(fmt::format("PutItem failed: {}", err->what()));
        }
    }
};

// Build a PutItem request JSON for a table with hash key "p" (string),
// sort key "c" (string), and a single extra string attribute.
rjson::value make_put_item_request(std::string_view table_name,
    std::string_view pk, std::string_view ck,
    std::string_view attr_name, std::string_view attr_value)
{
    rjson::value req = rjson::empty_object();
    rjson::add(req, "TableName", rjson::from_string(table_name));

    rjson::value item = rjson::empty_object();

    rjson::value pk_val = rjson::empty_object();
    rjson::add(pk_val, "S", rjson::from_string(pk));
    rjson::add(item, "p", std::move(pk_val));

    rjson::value ck_val = rjson::empty_object();
    rjson::add(ck_val, "S", rjson::from_string(ck));
    rjson::add(item, "c", std::move(ck_val));

    rjson::value attr_val = rjson::empty_object();
    rjson::add(attr_val, "S", rjson::from_string(attr_value));
    rjson::add_with_string_name(item, attr_name, std::move(attr_val));

    rjson::add(req, "Item", std::move(item));
    return req;
}

// Build a PutItem request for a hash-only table with arbitrary typed attributes.
// `attrs` is a vector of (name, typed_value) pairs already in DynamoDB format.
rjson::value make_put_item_hash_only(std::string_view table_name,
    std::string_view pk, rjson::value extra_attrs)
{
    rjson::value req = rjson::empty_object();
    rjson::add(req, "TableName", rjson::from_string(table_name));

    rjson::value item = rjson::empty_object();
    rjson::value pk_val = rjson::empty_object();
    rjson::add(pk_val, "S", rjson::from_string(pk));
    rjson::add(item, "p", std::move(pk_val));

    // Merge extra_attrs into item.
    for (auto it = extra_attrs.MemberBegin(); it != extra_attrs.MemberEnd(); ++it) {
        rjson::add_with_string_name(item, rjson::to_string_view(it->name),
            std::move(it->value));
    }

    rjson::add(req, "Item", std::move(item));
    return req;
}

// Extract a string attribute value from a DynamoDB-style JSON item.
// Item format: {"attr_name": {"S": "value"}}
std::string get_string_attr(const rjson::value& item, const char* attr_name) {
    if (!item.HasMember(attr_name)) {
        return "";
    }
    const auto& typed_val = item[attr_name];
    if (!typed_val.HasMember("S")) {
        return "";
    }
    return std::string(rjson::to_string_view(typed_val["S"]));
}

// Extract a numeric attribute value (as string) from a DynamoDB-style JSON item.
std::string get_number_attr(const rjson::value& item, const char* attr_name) {
    if (!item.HasMember(attr_name)) {
        return "";
    }
    const auto& typed_val = item[attr_name];
    if (!typed_val.HasMember("N")) {
        return "";
    }
    return std::string(rjson::to_string_view(typed_val["N"]));
}

} // anonymous namespace

// Basic test: create an Alternator table with a hash key and sort key via
// the Alternator API, insert a few items via PutItem, then verify that
// scan_table visits all items exactly once.
SEASTAR_TEST_CASE(test_scan_table_basic) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        // CreateTable with hash key "p" and sort key "c", both strings.
        exec.create_table(rjson::parse(R"({
            "TableName": "testtbl",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"},
                {"AttributeName": "c", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"},
                {"AttributeName": "c", "KeyType": "RANGE"}
            ]
        })"));

        struct test_item {
            std::string_view pk, ck, data;
        };
        std::vector<test_item> expected_items = {
            {"user1", "order1", "item_a"},
            {"user1", "order2", "item_b"},
            {"user2", "order1", "item_c"},
            {"user3", "order1", "item_d"},
            {"user3", "order2", "item_e"},
        };

        // PutItem for each item.
        for (const auto& ti : expected_items) {
            exec.put_item(make_put_item_request("testtbl", ti.pk, ti.ck,
                "data", ti.data));
        }

        // Run the full table scan.
        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_testtbl", "testtbl");

        std::vector<rjson::value> scanned_items;
        alternator::scan_table(proxy, schema,
            [&scanned_items] (rjson::value item) -> future<> {
                scanned_items.push_back(std::move(item));
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(scanned_items.size(), expected_items.size());

        // Compare as sets since scan order is not guaranteed.
        std::set<std::tuple<std::string, std::string, std::string>> scanned_set;
        for (const auto& item : scanned_items) {
            scanned_set.emplace(get_string_attr(item, "p"),
                                get_string_attr(item, "c"),
                                get_string_attr(item, "data"));
        }

        std::set<std::tuple<std::string, std::string, std::string>> expected_set;
        for (const auto& ti : expected_items) {
            expected_set.emplace(ti.pk, ti.ck, ti.data);
        }

        BOOST_REQUIRE(scanned_set == expected_set);
    });
}

// Test: scan an empty table returns no items.
SEASTAR_TEST_CASE(test_scan_table_empty) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        exec.create_table(rjson::parse(R"({
            "TableName": "emptytbl",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_emptytbl", "emptytbl");

        size_t count = 0;
        alternator::scan_table(proxy, schema,
            [&count] (rjson::value) -> future<> {
                ++count;
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(count, 0);
    });
}

// Test: scan a table with only a hash key (no sort key).
SEASTAR_TEST_CASE(test_scan_table_hash_only) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        exec.create_table(rjson::parse(R"({
            "TableName": "hashonly",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        // Insert 10 items with numeric "score" attributes.
        for (int i = 0; i < 10; i++) {
            rjson::value attrs = rjson::empty_object();
            rjson::value score = rjson::empty_object();
            rjson::add(score, "N", rjson::from_string(format("{}", i * 10)));
            rjson::add(attrs, "score", std::move(score));

            exec.put_item(make_put_item_hash_only("hashonly",
                format("key{}", i), std::move(attrs)));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_hashonly", "hashonly");

        std::set<std::string> scanned_keys;
        alternator::scan_table(proxy, schema,
            [&scanned_keys] (rjson::value item) -> future<> {
                scanned_keys.insert(get_string_attr(item, "p"));
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(scanned_keys.size(), 10);
        for (int i = 0; i < 10; i++) {
            BOOST_REQUIRE(scanned_keys.contains(format("key{}", i)));
        }
    });
}

// Test: scan with multiple attributes (including various types).
SEASTAR_TEST_CASE(test_scan_table_multiple_attrs) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        exec.create_table(rjson::parse(R"({
            "TableName": "multiattr",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        // Insert an item with multiple attributes of different types.
        rjson::value attrs = rjson::empty_object();
        rjson::value name_v = rjson::empty_object();
        rjson::add(name_v, "S", rjson::from_string("Alice"));
        rjson::add(attrs, "name", std::move(name_v));
        rjson::value age_v = rjson::empty_object();
        rjson::add(age_v, "N", rjson::from_string("30"));
        rjson::add(attrs, "age", std::move(age_v));

        exec.put_item(make_put_item_hash_only("multiattr", "pk1",
            std::move(attrs)));

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_multiattr", "multiattr");

        std::vector<rjson::value> items;
        alternator::scan_table(proxy, schema,
            [&items] (rjson::value item) -> future<> {
                items.push_back(std::move(item));
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(items.size(), 1);
        const auto& item = items[0];
        BOOST_REQUIRE_EQUAL(get_string_attr(item, "p"), "pk1");
        BOOST_REQUIRE_EQUAL(get_string_attr(item, "name"), "Alice");
        BOOST_REQUIRE_EQUAL(get_number_attr(item, "age"), "30");
    });
}
