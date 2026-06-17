/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Test for alternator::export_scan_table() - the internal table-reading helper
// used by S3 export. It is *not* the implementation of Alternator's Scan
// request, and nothing here says anything about how Scan behaves; the two share
// only the word "scan".
// Tables and items are created through the Alternator executor API
// (CreateTable / PutItem) rather than raw CQL.
// See SCYLLADB-1888.

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/exception_utils.hh"

#include "alternator/export.hh"
#include "alternator/executor.hh"
#include "alternator/error.hh"
#include "alternator/rmw_operation.hh"
#include "cdc/metadata.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "service_permit.hh"
#include "utils/error_injection.hh"
#include "utils/rjson.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace {
// Wraps a sharded<alternator::executor> for use in tests. Provides
// create_table() and put_item() methods that accept DynamoDB-style
// JSON requests, just like the real Alternator HTTP API would.
class alternator_test_executor {
    sharded<cdc::metadata> _cdc_md;
    sharded<alternator::executor> _exec;
    alternator::rmw_operation::write_isolation _saved_write_isolation;
public:
    void start(cql_test_env& e) {
        // Save and override the default write isolation. The default
        // LWT_ALWAYS uses proxy.cas() which requires need_remote_proxy
        // in the test env. FORBID_RMW uses direct quorum writes which
        // work without a remote proxy - sufficient for our unconditional
        // PutItem calls.
        // LWT_RMW_ONLY and UNSAFE_RMW would also write unconditional items
        // without proxy.cas(), but FORBID_RMW is preferred because it rejects
        // read-modify-write operations outright: a test that accidentally
        // issues one fails with a clear error, instead of silently running it
        // unisolated (UNSAFE_RMW) or tripping over the missing remote proxy
        // (LWT_RMW_ONLY).
        _saved_write_isolation = alternator::rmw_operation::default_write_isolation;
        alternator::rmw_operation::set_default_write_isolation("forbid_rmw");
        // Pass the sharded<> services themselves, not their shard-0 instances:
        // seastar only unwraps std::reference_wrapper<sharded<T>> into the
        // shard-local instance, so a plain reference would be copied verbatim
        // to every shard, leaving executors on shards other than 0 using
        // shard 0's services.
        _cdc_md.start().get();
        _exec.start(
            std::ref(e.gossiper()),
            std::ref(e.get_storage_proxy()),
            std::ref(e.get_storage_service()),
            std::ref(e.migration_manager()),
            std::ref(e.get_system_distributed_keyspace()),
            std::ref(e.get_system_keyspace()),
            std::ref(_cdc_md),
            std::ref(e.vector_store_client()),
            default_smp_service_group(),
            utils::updateable_value<uint32_t>(10000)).get();
    }

    void stop() {
        _exec.stop().get();
        _cdc_md.stop().get();
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
        put_item_async(std::move(request)).get();
    }

    // PutItem / DeleteItem returning a future instead of blocking. Needed to write to the
    // table from inside an export_scan_table() callback, which runs in a continuation and
    // not in a seastar thread, so it cannot call get().
    future<> put_item_async(rjson::value request) {
        service::client_state cs(service::client_state::internal_tag{});
        tracing::trace_state_ptr ts;
        std::unique_ptr<audit::audit_info_alternator> ai;
        auto result = co_await _exec.local().put_item(cs, ts, empty_service_permit(),
            std::move(request), ai);
        if (auto* err = std::get_if<alternator::api_error>(&result)) {
            BOOST_FAIL(fmt::format("PutItem failed: {}", err->what()));
        }
    }

    future<> delete_item_async(rjson::value request) {
        service::client_state cs(service::client_state::internal_tag{});
        tracing::trace_state_ptr ts;
        std::unique_ptr<audit::audit_info_alternator> ai;
        auto result = co_await _exec.local().delete_item(cs, ts, empty_service_permit(),
            std::move(request), ai);
        if (auto* err = std::get_if<alternator::api_error>(&result)) {
            BOOST_FAIL(fmt::format("DeleteItem failed: {}", err->what()));
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

// Build a PutItem request JSON for a table with hash key "p" (string), sort key "c"
// (string - optional) and extra attributes - those will be moved into the "Item" object
// of the request.
rjson::value make_put_item_request(std::string_view table_name, std::string_view pk,
    std::optional<std::string_view> ck, rjson::value attrs)
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

    for (auto it = attrs.MemberBegin(); it != attrs.MemberEnd(); ++it) {
        rjson::add_with_string_name(item, rjson::to_string_view(it->name), std::move(it->value));
    }

    rjson::add(req, "Item", std::move(item));
    return req;
}

// Build a DeleteItem request JSON for a table whose only key is the hash key "p" (string).
rjson::value make_delete_item_request(std::string_view table_name, std::string_view pk)
{
    rjson::value req = rjson::empty_object();
    rjson::add(req, "TableName", rjson::from_string(table_name));

    rjson::value pk_val = rjson::empty_object();
    rjson::add(pk_val, "S", rjson::from_string(pk));
    rjson::value key = rjson::empty_object();
    rjson::add(key, "p", std::move(pk_val));

    rjson::add(req, "Key", std::move(key));
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

// Compare an item the scan returned against the item which was written into the table.
// The elements of a set attribute (SS, NS, BS) are compared disregarding their order - a
// set is unordered, and nothing promises the scan returns its elements in the order they
// were written in. Everything else, the set of attribute names included, has to match
// exactly.
void require_item_equal(const rjson::value& expected, const rjson::value& actual) {
    BOOST_REQUIRE(expected.IsObject());
    BOOST_REQUIRE(actual.IsObject());
    BOOST_REQUIRE_EQUAL(expected.MemberCount(), actual.MemberCount());
    for (auto it = expected.MemberBegin(); it != expected.MemberEnd(); ++it) {
        auto attr_name = rjson::to_string_view(it->name);
        BOOST_REQUIRE_MESSAGE(actual.HasMember(it->name), fmt::format("missing attribute \"{}\"", attr_name));
        // A DynamoDB attribute value is a one-member object: {"<type>": <value>}.
        const auto& expected_value = it->value;
        const auto& actual_value = actual[it->name];
        BOOST_REQUIRE_EQUAL(expected_value.MemberCount(), 1u);
        BOOST_REQUIRE_EQUAL(actual_value.MemberCount(), 1u);
        auto type = rjson::to_string_view(expected_value.MemberBegin()->name);
        if (type != "SS" && type != "NS" && type != "BS") {
            BOOST_REQUIRE_MESSAGE(expected_value == actual_value,
                fmt::format("attribute \"{}\": expected {}, got {}", attr_name, expected_value, actual_value));
            continue;
        }
        BOOST_REQUIRE_MESSAGE(actual_value.HasMember(expected_value.MemberBegin()->name),
            fmt::format("attribute \"{}\": expected type \"{}\", got {}", attr_name, type, actual_value));
        auto elements_of = [] (const rjson::value& array) {
            BOOST_REQUIRE(array.IsArray());
            std::multiset<std::string_view> elements;
            for (const auto& element : array.GetArray()) {
                elements.insert(rjson::to_string_view(element));
            }
            return elements;
        };
        auto expected_elements = elements_of(expected_value.MemberBegin()->value);
        auto actual_elements = elements_of(actual_value.MemberBegin()->value);
        BOOST_REQUIRE_EQUAL_COLLECTIONS(expected_elements.begin(), expected_elements.end(),
            actual_elements.begin(), actual_elements.end());
    }
}

service_permit make_test_service_permit(seastar::semaphore& sem) {
    return make_service_permit(seastar::get_units(sem, 1).get());
}

// Error injection which makes a read of the given table (the "table_name" parameter)
// fail with the error named by its "error" parameter - see storage_proxy::query_result().
constexpr const char* read_error_injection = "alternator_query_result_timeout";

// A page read by the scan is limited by its size in bytes and not by the number of rows it
// holds. The scan runs in the streaming scheduling group, which makes its reads maintenance
// requests, and those get the hard-coded `query::result_memory_limiter::maximum_result_size`
// (1 MiB) rather than the configurable `query_page_size_in_bytes`. So a test which wants the
// scan to read more than one page has to write more than a megabyte into the table. Few
// but large items are the cheapest way to get there: every item of such a test carries a
// filler attribute of this size, and this many of them add up to several pages.
constexpr size_t page_filler_size = 16 * 1024;
constexpr size_t items_spanning_several_pages = 256;

std::string make_page_filler() {
    return std::string(page_filler_size, 'x');
}

// A DynamoDB-style string attribute value: {"S": <value>}.
rjson::value make_string_attr(std::string_view value) {
    rjson::value attr = rjson::empty_object();
    rjson::add(attr, "S", rjson::from_string(value));
    return attr;
}

} // anonymous namespace

// Happy-path test: create an Alternator table through the Alternator API, check that
// scanning it before anything has been written into it visits nothing, insert items with
// PutItem, and verify that export_scan_table then visits every one of them exactly once
// and rebuilds it faithfully. All of that runs over both key schemas an Alternator table
// can have - hash key only, and hash key plus sort key - and over items carrying no
// attributes beyond the key, a plain string attribute, and an attribute of each of the
// DynamoDB types.
SEASTAR_TEST_CASE(test_export_scan_table_basic) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        auto& proxy = e.get_storage_proxy().local();
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        // Run a full table scan and wait for it to finish.
        auto scan = [&] (const sstring& table_name, seastar::noncopyable_function<future<>(rjson::value)> cb) {
            auto schema = e.local_db().find_schema(format("alternator_{}", table_name), table_name);
            alternator::export_scan_table(proxy, schema, as,
                make_test_service_permit(scan_permit_sem), std::move(cb)).get();
        };

        for (bool with_sort_key : {false, true}) {
            // CreateTable with hash key "p" and - in the second round - sort key "c" as well,
            // both strings.
            const sstring table_name = with_sort_key ? "hashandsort" : "hashonly";
            exec.create_table(rjson::parse(with_sort_key ? R"({
                "TableName": "hashandsort",
                "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [
                    {"AttributeName": "p", "AttributeType": "S"},
                    {"AttributeName": "c", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "p", "KeyType": "HASH"},
                    {"AttributeName": "c", "KeyType": "RANGE"}
                ]
            })" : R"({
                "TableName": "hashonly",
                "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [
                    {"AttributeName": "p", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "p", "KeyType": "HASH"}
                ]
            })"));

            // Scanning the table before a single item was written into it visits nothing.
            size_t callbacks = 0;
            scan(table_name, [&callbacks] (rjson::value) -> future<> {
                ++callbacks;
                return make_ready_future<>();
            });
            BOOST_REQUIRE_EQUAL(callbacks, 0);

            // Every item written below, keyed by (hash key, sort key), with all the
            // attributes it was written with. The sort key part is empty in the round
            // which has no sort key.
            std::map<std::pair<std::string, std::string>, rjson::value> expected_items;
            auto put = [&] (std::string_view pk, std::string_view ck, rjson::value attrs) {
                std::optional<std::string_view> sort_key;
                if (with_sort_key) {
                    sort_key = ck;
                }
                auto request = make_put_item_request(table_name, pk, sort_key, std::move(attrs));
                expected_items.emplace(
                    std::pair(std::string(pk), std::string(sort_key.value_or(std::string_view()))),
                    rjson::copy(request["Item"]));
                exec.put_item(std::move(request));
            };

            // An item with no attributes beyond the key.
            put("user1", "order1", rjson::empty_object());
            // An item with a plain string attribute.
            put("user2", "order1", rjson::parse(R"({"data": {"S": "item_b"}})"));
            // An item with an attribute of every DynamoDB type, to check that none of them
            // is lost or mangled by the row-to-item conversion.
            put("user3", "order1", rjson::parse(R"({
                "score": {"N": "42"},
                "enabled": {"BOOL": true},
                "payload": {"B": "aGVsbG8="},
                "missing": {"NULL": true},
                "string_set": {"SS": ["blue", "green"]},
                "number_set": {"NS": ["1", "2.5"]},
                "binary_set": {"BS": ["Zmlyc3Q=", "c2Vjb25k"]},
                "nested": {"M": {"child": {"S": "value"}}},
                "items": {"L": [{"S": "first"}, {"N": "7"}]}
            })"));
            if (with_sort_key) {
                // A second item in a partition which already holds one, so that a partition
                // of more than one row is scanned too.
                put("user2", "order2", rjson::parse(R"({"data": {"S": "item_c"}})"));
            }

            std::map<std::pair<std::string, std::string>, rjson::value> scanned_items;
            scan(table_name, [&] (rjson::value item) -> future<> {
                BOOST_REQUIRE(item.IsObject());
                auto key = std::pair(get_string_attr(item, "p"),
                    with_sort_key ? get_string_attr(item, "c") : std::string());
                // An item visited twice would fail right here.
                BOOST_REQUIRE(scanned_items.emplace(std::move(key), std::move(item)).second);
                return make_ready_future<>();
            });

            BOOST_REQUIRE_EQUAL(scanned_items.size(), expected_items.size());
            for (const auto& [key, expected_item] : expected_items) {
                auto it = scanned_items.find(key);
                BOOST_REQUIRE_MESSAGE(it != scanned_items.end(),
                    format("item (\"{}\", \"{}\") was not visited", key.first, key.second));
                require_item_equal(expected_item, it->second);
            }
        }
    });
}

// Test: callback failures are propagated and stop the scan.
SEASTAR_TEST_CASE(test_export_scan_table_callback_exception) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

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
        BOOST_REQUIRE_EXCEPTION(alternator::export_scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&callbacks] (rjson::value) -> future<> {
                ++callbacks;
                return make_exception_future<>(std::runtime_error("expected scan callback failure"));
            }).get(), std::runtime_error,
            exception_predicate::message_equals("expected scan callback failure"));
        BOOST_REQUIRE_EQUAL(callbacks, 1);
        // A failed scan must not leave anything of ours behind. Here it is the callback
        // which failed, no page read did, so nothing else can be holding the permit by
        // the time the scan's future resolves: the one thing which may outlive that
        // future is a replica request of a page read which failed shortly before, which
        // keeps a copy of the permit until it answers or times out (see the comment on
        // export_scan_table()). `scan_permit_sem` lives on this stack, so a permit
        // outliving the test would be a dangling reference.
        BOOST_REQUIRE_EQUAL(scan_permit_sem.available_units(), 1);
    });
}

// Test: abort requests are propagated and stop the scan.
SEASTAR_TEST_CASE(test_export_scan_table_abort) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

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
        BOOST_REQUIRE_THROW(alternator::export_scan_table(proxy, schema, already_aborted,
            make_test_service_permit(scan_permit_sem),
            [] (rjson::value) -> future<> {
                BOOST_FAIL("callback should not be called after aborting before scan start");
                return make_ready_future<>();
            }).get(), abort_requested_exception);
        // Same as for a failed scan, and even more plainly so: this scan aborted before
        // issuing a single read, so there is nothing left which could hold the permit
        // once its future resolves.
        BOOST_REQUIRE_EQUAL(scan_permit_sem.available_units(), 1);

        size_t callbacks = 0;
        abort_source abort_during_scan;
        BOOST_REQUIRE_THROW(alternator::export_scan_table(proxy, schema, abort_during_scan,
            make_test_service_permit(scan_permit_sem),
            [&callbacks, &abort_during_scan] (rjson::value) -> future<> {
                ++callbacks;
                abort_during_scan.request_abort();
                return make_ready_future<>();
            }).get(), abort_requested_exception);
        BOOST_REQUIRE_EQUAL(callbacks, 1);
        BOOST_REQUIRE_EQUAL(scan_permit_sem.available_units(), 1);
    });
}

// Test: export_scan_table waits for each callback before invoking the next one.
SEASTAR_TEST_CASE(test_export_scan_table_callback_is_awaited_sequentially) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

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
        alternator::export_scan_table(proxy, schema, as,
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

// Scan a table with more items than fit in a single page, to ensure paging works correctly,
// and verify that all of them are scanned. The read of the second page is made to fail once
// with `injected_error`, so this also covers the retry path: a failed page must be retried on
// a brand new pager resumed from the paging state saved after the last successful page,
// without losing or repeating items. Each of the errors the scan retries on gets its own test
// case below, so that dropping the catch block of any one of them fails a test.
static future<> do_test_export_scan_table_lot_of_items(const char* injected_error) {
    return do_with_cql_env_thread([injected_error] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

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

        // Insert items with a "score" attribute, which is checked below together with the
        // key, and a filler attribute which makes the items big enough for the whole set to
        // span several pages - that is what forces the scan to page.
        constexpr size_t item_count = items_spanning_several_pages;
        const auto filler = make_page_filler();
        std::multiset<std::tuple<std::string, std::string>> scanned_set, expected_set;
        for (size_t i = 0; i < item_count; i++) {
            rjson::value attrs = rjson::empty_object();
            rjson::add(attrs, "score", make_string_attr(format("{}", i * 10)));
            rjson::add(attrs, "filler", make_string_attr(filler));
            exec.put_item(make_put_item_request("hashonly", format("key{}", i), std::nullopt, std::move(attrs)));
            expected_set.emplace(format("key{}", i), format("{}", i * 10));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_hashonly", "hashonly");

        // Arming the injection below is a no-op in builds without error injection compiled
        // in (release) - there the scan simply reads both pages, and the assertions at the
        // end still hold. Disable it at the end of the test in case it never fired.
        auto disable_injection = defer([] noexcept {
            utils::get_local_injector().disable(read_error_injection);
        });

        bool injection_armed = false;
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::export_scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&scanned_set, &injection_armed, injected_error] (rjson::value item) -> future<> {
                if (scanned_set.empty()) {
                    // The first page has already been read - make the read of the next one
                    // fail once. The scan is expected to back off, build a fresh pager from
                    // the paging state of the first page, and read the rest of the table.
                    auto& injector = utils::get_local_injector();
                    injector.enable(read_error_injection, true /* one_shot */,
                        {{"table_name", "hashonly"}, {"error", injected_error}});
                    injection_armed = injector.is_enabled(read_error_injection);
                }
                scanned_set.emplace(get_string_attr(item, "p"), get_string_attr(item, "score"));
                return make_ready_future<>();
            }).get();

        // A one-shot injection disables itself when it fires, so an injection which is still
        // enabled means the read we wanted to fail never happened.
        BOOST_REQUIRE(!injection_armed || !utils::get_local_injector().is_enabled(read_error_injection));
        BOOST_REQUIRE_EQUAL(scanned_set.size(), expected_set.size());
        BOOST_REQUIRE(scanned_set == expected_set);
        // The failed read must not leak the permit either: the pager it was made on is
        // dropped and a new one built, and neither outlives the scan's future. Note that
        // the injected error is returned by `storage_proxy::query_result()` before any
        // replica request is issued, so - unlike a page read which fails for real - it
        // leaves behind no replica request keeping a copy of the permit alive past that
        // future (see the comment on export_scan_table()).
        BOOST_REQUIRE_EQUAL(scan_permit_sem.available_units(), 1);
    });
}

SEASTAR_TEST_CASE(test_export_scan_table_lot_of_items, *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_lot_of_items("read_timeout");
}

SEASTAR_TEST_CASE(test_export_scan_table_lot_of_items_read_failure, *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_lot_of_items("read_failure");
}

SEASTAR_TEST_CASE(test_export_scan_table_lot_of_items_unavailable, *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_lot_of_items("unavailable");
}

SEASTAR_TEST_CASE(test_export_scan_table_lot_of_items_overloaded, *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_lot_of_items("overloaded");
}

// Test: an abort which arrives while the scan is waiting between two attempts at the same
// page stops the scan with abort_requested_exception, like an abort arriving anywhere else
// does. Unlike in the test above the injected read error is not one-shot: once armed, every
// attempt at the second page fails, so the scan retries that page forever and spends nearly
// all of its time in the backoff sleep (the first one is a second long). The abort is
// requested from a timer armed when the first page has been read, firing well inside that
// first sleep.
// The whole point of the test is the injected error, so it only runs where error injection
// is compiled in - with the reads succeeding the scan would simply read the table to its end
// and never wait for anything.
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
SEASTAR_TEST_CASE(test_export_scan_table_abort_between_attempts, *boost::unit_test::label("tier2")) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        exec.create_table(rjson::parse(R"({
            "TableName": "abortretrytbl",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        // Items big enough for the whole set to span several pages - the scan has to ask for
        // a second page, which is the read the injection below makes fail.
        const auto filler = make_page_filler();
        for (size_t i = 0; i < items_spanning_several_pages; i++) {
            exec.put_item(make_put_item_request("abortretrytbl", format("key{}", i), std::nullopt,
                "filler", filler));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_abortretrytbl", "abortretrytbl");

        // The injection is not one-shot, so nothing disables it on its own.
        auto disable_injection = defer([] noexcept {
            utils::get_local_injector().disable(read_error_injection);
        });

        abort_source as;
        seastar::timer<> abort_timer([&as] { as.request_abort(); });
        bool injection_armed = false;
        seastar::semaphore scan_permit_sem{1};
        BOOST_REQUIRE_THROW(alternator::export_scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&] (rjson::value) -> future<> {
                if (!injection_armed) {
                    // The first page has been read - make every read from now on fail, and
                    // arm the abort to arrive while the scan waits before retrying.
                    utils::get_local_injector().enable(read_error_injection, false /* one_shot */,
                        {{"table_name", "abortretrytbl"}, {"error", "read_timeout"}});
                    injection_armed = true;
                    abort_timer.arm(std::chrono::milliseconds(300));
                }
                return make_ready_future<>();
            }).get(), abort_requested_exception);
        BOOST_REQUIRE(injection_armed);
        // Same as in the test above: the page reads here failed by injection, before any
        // replica request was issued, so no such request is left holding a copy of the
        // permit once the scan's future resolves.
        BOOST_REQUIRE_EQUAL(scan_permit_sem.available_units(), 1);
    });
}
#endif // SCYLLA_ENABLE_ERROR_INJECTION

// Test: the table is written to while the scan is running. Every item which exists for the
// whole duration of the scan has to be visited exactly once, and neither deleting nor adding
// items is allowed to stop the iteration early or to make it repeat an item.
// The test deletes each item from inside the callback which was given it, which means that by
// the time the scan asks for the next page, every item of the page it has just finished is gone
// - including the very last one of that page, which is the position the next page is resumed
// from. It also adds items while the scan is running. The table holds more than one page, so
// the scan really does have to resume a page over the deleted items.
SEASTAR_TEST_CASE(test_export_scan_table_modified_during_scan, *boost::unit_test::label("tier2")) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        exec.create_table(rjson::parse(R"({
            "TableName": "modifiedtbl",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"}
            ]
        })"));

        // Items big enough for the whole set to span several pages, so the scan really has
        // to resume a page over the items deleted below.
        constexpr size_t item_count = items_spanning_several_pages;
        const auto filler = make_page_filler();
        // Every key which has ever been put into the table, so that a visited item can be
        // checked to be one which was really put there.
        std::set<std::string> put_keys;
        for (size_t i = 0; i < item_count; i++) {
            put_keys.insert(format("key{}", i));
            exec.put_item(make_put_item_request("modifiedtbl", format("key{}", i), std::nullopt, "filler", filler));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_modifiedtbl", "modifiedtbl");

        // How many brand new items are added while the scan runs. Bounded, because an added
        // item may itself be visited (and then deleted, and then trigger another addition),
        // and we do want this scan to end.
        constexpr size_t items_added_during_scan = 100;
        size_t items_added = 0;

        std::set<std::string> scanned_keys;
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::export_scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&] (rjson::value item) -> future<> {
                // The items put here have no attributes beyond the key and the filler.
                BOOST_REQUIRE_EQUAL(item.MemberCount(), 2u);
                auto key = get_string_attr(item, "p");
                BOOST_REQUIRE(put_keys.contains(key));
                // An item visited twice would fail right here.
                BOOST_REQUIRE(scanned_keys.insert(key).second);
                co_await exec.delete_item_async(make_delete_item_request("modifiedtbl", key));
                if (items_added < items_added_during_scan) {
                    auto added_key = format("added{}", items_added++);
                    put_keys.insert(added_key);
                    // The same shape as the items written before the scan, so that the
                    // member count checked above holds for a visited added item too.
                    co_await exec.put_item_async(make_put_item_request("modifiedtbl", added_key, std::nullopt,
                        "filler", filler));
                }
            }).get();

        // Each item was deleted only after it had been visited, so each of them existed for
        // the whole part of the scan which preceded its own visit - all of them have to be
        // there. The added ones may or may not have been visited, which is why they are not
        // checked here; that none of them was visited twice has been checked above.
        for (size_t i = 0; i < item_count; i++) {
            BOOST_REQUIRE(scanned_keys.contains(format("key{}", i)));
        }
        BOOST_REQUIRE_EQUAL(items_added, items_added_during_scan);
    });
}

