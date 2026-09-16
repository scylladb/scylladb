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

#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/defer.hh>

#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

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
        // work without a remote proxy - sufficient for our unconditional
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
// sort key "c" (string - optional), and a single extra string attribute (optional).
rjson::value make_put_item_request(std::string_view table_name,
    std::string_view pk, std::optional<std::string_view> ck,
    std::optional<std::string_view> attr_name = std::nullopt, std::optional<std::string_view> attr_value = std::nullopt)
{
    rjson::value req = rjson::empty_object();
    rjson::add(req, "TableName", rjson::from_string(table_name));

    rjson::value item = rjson::empty_object();

    rjson::value pk_val = rjson::empty_object();
    rjson::add(pk_val, "S", rjson::from_string(pk));
    rjson::add(item, "p", std::move(pk_val));
    if (ck) {
        rjson::value ck_val = rjson::empty_object();
        rjson::add(ck_val, "S", rjson::from_string(*ck));
        rjson::add(item, "c", std::move(ck_val));
    }

    if (attr_name && attr_value) {
        rjson::value attr_val = rjson::empty_object();
        rjson::add(attr_val, "S", rjson::from_string(*attr_value));
        rjson::add_with_string_name(item, *attr_name, std::move(attr_val));
    }

    rjson::add(req, "Item", std::move(item));
    return req;
}

// Build a PutItem request JSON for a table with hash key "p" (string) and extra attributes (optional) -
// those will be moved into the "Item" object of the request.
rjson::value make_put_item_request(std::string_view table_name, std::string_view pk, rjson::value attrs)
{
    rjson::value req = rjson::empty_object();
    rjson::add(req, "TableName", rjson::from_string(table_name));

    rjson::value item = rjson::empty_object();
    rjson::value pk_val = rjson::empty_object();
    rjson::add(pk_val, "S", rjson::from_string(pk));
    rjson::add(item, "p", std::move(pk_val));

    for (auto it = attrs.MemberBegin(); it != attrs.MemberEnd(); ++it) {
        rjson::add_with_string_name(item, rjson::to_string_view(it->name), std::move(it->value));
    }

    rjson::add(req, "Item", std::move(item));
    return req;
}

// Extract a string attribute value from a DynamoDB-style JSON item.
// Item format: {"attr_name": {"S": "value"}}
std::string get_string_attr(const rjson::value& item, const char* attr_name) {
    BOOST_REQUIRE(item.IsObject());
    BOOST_REQUIRE(item.HasMember(attr_name));
    const auto& typed_val = item[attr_name];
    BOOST_REQUIRE(typed_val.IsObject());
    BOOST_REQUIRE(typed_val.HasMember("S"));
    return std::string(rjson::to_string_view(typed_val["S"]));
}

service_permit make_test_service_permit(seastar::semaphore& sem) {
    return make_service_permit(seastar::get_units(sem, 1).get());
}

struct executor_fixture {
  executor_fixture() {
      BOOST_TEST_MESSAGE( "ctor fixture" );
  }
  void setup() {
  }
  void teardown() {
  }
  ~executor_fixture() {
      BOOST_TEST_MESSAGE( "dtor fixture" );
  }
  static int i;
};
int executor_fixture::i = 0;

BOOST_TEST_GLOBAL_FIXTURE( executor_fixture );
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

        std::multiset<std::tuple<std::string, std::string, std::string>> scanned_set, expected_set;
        expected_set = {
            {"user1", "order1", "item_a"},
            {"user1", "order2", "item_b"},
            {"user2", "order1", "item_c"},
            {"user3", "order1", "item_d"},
            {"user3", "order2", "item_e"},
        };

        // PutItem for each item.
        for (const auto& ti : expected_set) {
            exec.put_item(make_put_item_request("testtbl", std::get<0>(ti), std::get<1>(ti), "data", std::get<2>(ti)));
        }

        // Run the full table scan.
        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_testtbl", "testtbl");

        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&scanned_set] (rjson::value item) -> future<> {
                BOOST_REQUIRE(item.IsObject());
                std::multiset<std::string_view> expected_keys = {"p", "c", "data"};
                std::multiset<std::string_view> actual_keys;
                for (auto it = item.MemberBegin(); it != item.MemberEnd(); ++it) {
                    actual_keys.insert(rjson::to_string_view(it->name));
                }
                BOOST_REQUIRE_EQUAL_COLLECTIONS(expected_keys.begin(), expected_keys.end(),
                    actual_keys.begin(), actual_keys.end());
                scanned_set.emplace(get_string_attr(item, "p"),
                                    get_string_attr(item, "c"),
                                    get_string_attr(item, "data"));
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(scanned_set.size(), expected_set.size());
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
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
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

        std::multiset<std::tuple<std::string, std::string>> scanned_set, expected_set;

        // Insert 10 items with numeric "score" attributes.
        for (int i = 0; i < 10; i++) {
            exec.put_item(make_put_item_request("hashonly", format("key{}", i), std::nullopt, "score", format("{}", i * 10)));
            expected_set.emplace(format("key{}", i), format("{}", i * 10));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_hashonly", "hashonly");

        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&scanned_set] (rjson::value item) -> future<> {
                BOOST_REQUIRE(item.IsObject());
                std::multiset<std::string_view> expected_keys = {"p", "score"};
                std::multiset<std::string_view> actual_keys;
                for (auto it = item.MemberBegin(); it != item.MemberEnd(); ++it) {
                    actual_keys.insert(rjson::to_string_view(it->name));
                }
                BOOST_REQUIRE_EQUAL_COLLECTIONS(expected_keys.begin(), expected_keys.end(), actual_keys.begin(), actual_keys.end());
                scanned_set.emplace(get_string_attr(item, "p"),
                                    get_string_attr(item, "score"));
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(scanned_set.size(), expected_set.size());
        BOOST_REQUIRE(scanned_set == expected_set);
    });
}

// Test: scan a table with only a hash key (no sort key) and without any additional attributes whatsoever.
SEASTAR_TEST_CASE(test_scan_table_hash_only_no_additional_attrs) {
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

        std::multiset<std::string> scanned_set, expected_set;

        // Insert 10 items with no additional attributes.
        for (int i = 0; i < 10; i++) {
            exec.put_item(make_put_item_request("hashonly", format("key{}", i), std::nullopt));
            expected_set.insert(format("key{}", i));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_hashonly", "hashonly");

        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&scanned_set] (rjson::value item) -> future<> {
                BOOST_REQUIRE(item.IsObject());
                std::multiset<std::string_view> expected_keys = { "p" };
                std::multiset<std::string_view> actual_keys;
                for (auto it = item.MemberBegin(); it != item.MemberEnd(); ++it) {
                    actual_keys.insert(rjson::to_string_view(it->name));
                }
                BOOST_REQUIRE(actual_keys == expected_keys);
                scanned_set.insert(get_string_attr(item, "p"));
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(scanned_set.size(), expected_set.size());
        BOOST_REQUIRE(scanned_set == expected_set);
    });
}

// Test: non-string DynamoDB attribute values survive the row-to-item conversion.
SEASTAR_TEST_CASE(test_scan_table_non_string_attributes) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        exec.create_table(rjson::parse(R"({
            "TableName": "typedattrs",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        exec.put_item(make_put_item_request("typedattrs", "key", rjson::parse(R"({
            "score": {"N": "42"},
            "enabled": {"BOOL": true},
            "payload": {"B": "aGVsbG8="},
            "missing": {"NULL": true},
            "string_set": {"SS": ["blue", "green"]},
            "number_set": {"NS": ["1", "2.5"]},
            "binary_set": {"BS": ["Zmlyc3Q=", "c2Vjb25k"]},
            "nested": {"M": {"child": {"S": "value"}}},
            "items": {"L": [{"S": "first"}, {"N": "7"}]}
        })")));

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_typedattrs", "typedattrs");

        size_t callbacks = 0;
        abort_source as;
        auto compare_set = [](std::set<std::string_view> expected, const rjson::value& actual_array) {
            BOOST_REQUIRE(actual_array.IsArray());
            std::set<std::string_view> actual;
            for (const auto& val : actual_array.GetArray()) {
                actual.insert(rjson::to_string_view(val));
            }
            BOOST_REQUIRE_EQUAL_COLLECTIONS(expected.begin(), expected.end(), actual.begin(), actual.end());
        };
        seastar::semaphore scan_permit_sem{1};
        alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&] (rjson::value item) -> future<> {
                ++callbacks;
                BOOST_REQUIRE_EQUAL(get_string_attr(item, "p"), "key");

                BOOST_REQUIRE(item.HasMember("score"));
                BOOST_REQUIRE(item["score"].HasMember("N"));
                BOOST_REQUIRE_EQUAL(rjson::to_string_view(item["score"]["N"]), "42");

                BOOST_REQUIRE(item.HasMember("enabled"));
                BOOST_REQUIRE(item["enabled"].HasMember("BOOL"));
                BOOST_REQUIRE(item["enabled"]["BOOL"].GetBool());

                BOOST_REQUIRE(item.HasMember("payload"));
                BOOST_REQUIRE(item["payload"].HasMember("B"));
                BOOST_REQUIRE_EQUAL(rjson::to_string_view(item["payload"]["B"]), "aGVsbG8=");

                BOOST_REQUIRE(item.HasMember("missing"));
                BOOST_REQUIRE(item["missing"].HasMember("NULL"));
                BOOST_REQUIRE(item["missing"]["NULL"].GetBool());

                BOOST_REQUIRE(item.HasMember("string_set"));
                BOOST_REQUIRE(item["string_set"].HasMember("SS"));
                const auto& string_set = item["string_set"]["SS"];
                BOOST_REQUIRE(string_set.IsArray());
                compare_set({"blue", "green"}, string_set);

                BOOST_REQUIRE(item.HasMember("number_set"));
                BOOST_REQUIRE(item["number_set"].HasMember("NS"));
                const auto& number_set = item["number_set"]["NS"];
                BOOST_REQUIRE(number_set.IsArray());
                compare_set({"1", "2.5"}, number_set);

                BOOST_REQUIRE(item.HasMember("binary_set"));
                BOOST_REQUIRE(item["binary_set"].HasMember("BS"));
                const auto& binary_set = item["binary_set"]["BS"];
                BOOST_REQUIRE(binary_set.IsArray());
                compare_set({"Zmlyc3Q=", "c2Vjb25k"}, binary_set);

                BOOST_REQUIRE(item.HasMember("nested"));
                BOOST_REQUIRE(item["nested"].HasMember("M"));
                const auto& nested = item["nested"]["M"];
                BOOST_REQUIRE(nested.HasMember("child"));
                BOOST_REQUIRE(nested["child"].HasMember("S"));
                BOOST_REQUIRE_EQUAL(rjson::to_string_view(nested["child"]["S"]), "value");

                BOOST_REQUIRE(item.HasMember("items"));
                BOOST_REQUIRE(item["items"].HasMember("L"));
                const auto& items = item["items"]["L"];
                BOOST_REQUIRE(items.IsArray());
                BOOST_REQUIRE_EQUAL(items.Size(), 2);
                BOOST_REQUIRE(items[0].HasMember("S"));
                BOOST_REQUIRE_EQUAL(rjson::to_string_view(items[0]["S"]), "first");
                BOOST_REQUIRE(items[1].HasMember("N"));
                BOOST_REQUIRE_EQUAL(rjson::to_string_view(items[1]["N"]), "7");

                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(callbacks, 1);
    });
}

// Test: callback failures are propagated and stop the scan.
SEASTAR_TEST_CASE(test_scan_table_callback_exception) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        exec.create_table(rjson::parse(R"({
            "TableName": "failtbl",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        for (int i = 0; i < 3; ++i) {
            exec.put_item(make_put_item_request("failtbl", format("key{}", i), std::nullopt));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_failtbl", "failtbl");

        size_t callbacks = 0;
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        BOOST_REQUIRE_THROW(alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&callbacks] (rjson::value) -> future<> {
                ++callbacks;
                return make_exception_future<>(std::runtime_error("expected scan callback failure"));
            }).get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(callbacks, 1);
    });
}

// Test: abort requests are propagated and stop the scan.
SEASTAR_TEST_CASE(test_scan_table_abort) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        exec.create_table(rjson::parse(R"({
            "TableName": "aborttbl",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        for (int i = 0; i < 3; ++i) {
            exec.put_item(make_put_item_request("aborttbl", format("key{}", i), std::nullopt));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_aborttbl", "aborttbl");

        seastar::semaphore scan_permit_sem{1};
        abort_source already_aborted;
        already_aborted.request_abort();
        BOOST_REQUIRE_THROW(alternator::scan_table(proxy, schema, already_aborted,
            make_test_service_permit(scan_permit_sem),
            [] (rjson::value) -> future<> {
                BOOST_FAIL("callback should not be called after aborting before scan start");
                return make_ready_future<>();
            }).get(), abort_requested_exception);

        size_t callbacks = 0;
        abort_source abort_during_scan;
        BOOST_REQUIRE_THROW(alternator::scan_table(proxy, schema, abort_during_scan,
            make_test_service_permit(scan_permit_sem),
            [&callbacks, &abort_during_scan] (rjson::value) -> future<> {
                ++callbacks;
                abort_during_scan.request_abort();
                return make_ready_future<>();
            }).get(), abort_requested_exception);
        BOOST_REQUIRE_EQUAL(callbacks, 1);
    });
}

// Test: scan_table waits for each callback before invoking the next one.
SEASTAR_TEST_CASE(test_scan_table_callback_is_awaited_sequentially) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] { exec.stop(); });

        exec.create_table(rjson::parse(R"({
            "TableName": "sequencedtbl",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        for (int i = 0; i < 3; ++i) {
            exec.put_item(make_put_item_request("sequencedtbl", format("key{}", i), std::nullopt));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_sequencedtbl", "sequencedtbl");

        bool callback_in_progress = false;
        size_t callbacks = 0;
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&callback_in_progress, &callbacks] (rjson::value) -> future<> {
                BOOST_REQUIRE(!callback_in_progress);
                callback_in_progress = true;
                ++callbacks;
                return seastar::yield().then([&callback_in_progress] {
                    BOOST_REQUIRE(callback_in_progress);
                    callback_in_progress = false;
                });
            }).get();

        BOOST_REQUIRE_EQUAL(callbacks, 3);
        BOOST_REQUIRE(!callback_in_progress);
    });
}

// Test: scan a table with lot of items to ensure paging works correctly. We insert 10000 items and verify that all are scanned.
// Nothing special about 10000 itself, the number must be bigger than 4096, as scan_table() explicitly sets 4096 page size limit.
SEASTAR_TEST_CASE(test_scan_table_lot_of_items) {
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

        // Insert items with "score" attribute, so we could force paging. We know the page size
        // so insert enough items to require 4 pages just to be safe.
        constexpr size_t item_count = alternator::scan_table_page_size * 3 + 1;
        for (size_t i = 0; i < item_count; i++) {
            exec.put_item(make_put_item_request("hashonly", format("key{}", i), std::nullopt, "score", format("{}", i * 10)));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_hashonly", "hashonly");

        std::multiset<std::string> scanned_keys;
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&scanned_keys] (rjson::value item) -> future<> {
                scanned_keys.insert(get_string_attr(item, "p"));
                return make_ready_future<>();
            }).get();

        BOOST_REQUIRE_EQUAL(scanned_keys.size(), item_count);
        for (size_t i = 0; i < item_count; i++) {
            BOOST_REQUIRE(scanned_keys.count(format("key{}", i)) == 1);
        }
    });
}
