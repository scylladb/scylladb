/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/exception_utils.hh"

#include "alternator/export.hh"
#include "alternator/executor.hh"
#include "alternator/error.hh"
#include "alternator/rmw_operation.hh"
#include "cdc/metadata.hh"
#include "exceptions/exceptions.hh"
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

#include "utils/base64.hh"
#include "utils/hashers.hh"

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

// Tests of the export (sink) and import (source) pipelines from alternator/export.hh.
// They all use the in-memory storage backend, so they exercise the serialization,
// framing and flushing logic of the pipelines themselves, without involving S3.
// The pipelines write JSON lines: one serialized item per line, each terminated by '\n'.
// A test usually writes items through a sink pipeline and reads them back with a source
// pipeline over the same storage, and expects to get exactly the items it wrote, in order.

// The basic happy path: a single item written through the sink comes back identical from the
// source. Also verifies that flush_and_close() (sink) and close() (source) reach down to the
// storage - a pipeline that never flushes its backend would silently lose the tail of the data,
// and with a single small item everything still sits in the pipeline's buffers until the flush.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_single_item) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage });

    auto item = rjson::parse("{\"key\": \"value\"}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    BOOST_CHECK(storage->is_write_flushed());

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read_all();
    co_await source->close();

    BOOST_CHECK(storage->is_read_flushed());
    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item));
}

// The source must accept a last line that is not terminated by a newline. Our own sink always
// emits a trailing '\n', but a file produced by another writer (e.g. DynamoDB's export) or a
// truncated one may lack it, so the parser has to emit the pending line when the source is
// exhausted instead of waiting for a separator that never arrives. The storage is filled by hand
// here, since the sink cannot produce such a file.
SEASTAR_TEST_CASE(test_in_memory_line_without_newline) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    std::string line = "{\"key\": \"value\"}"; // Note: no newline at the end
    storage->append(std::as_bytes(std::span<const char>(line)));

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read_all();
    co_await source->close();

    BOOST_CHECK(storage->is_read_flushed());
    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(rjson::parse(line)));
}

// Several items in one file: checks the JSON-lines framing, i.e. that consecutive items are
// separated rather than concatenated or merged, and that the source returns them in the order
// they were passed to process().
SEASTAR_TEST_CASE(test_in_memory_roundtrip_multiple_items) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage });

    auto item1 = rjson::parse("{\"id\": 1, \"name\": \"alice\"}");
    auto item2 = rjson::parse("{\"id\": 2, \"name\": \"bob\"}");
    auto item3 = rjson::parse("{\"id\": 3, \"name\": \"charlie\"}");
    co_await sink->process(item1);
    co_await sink->process(item2);
    co_await sink->process(item3);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read_all();
    co_await source->close();

    BOOST_REQUIRE_EQUAL(received.size(), 3u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item1));
    BOOST_CHECK_EQUAL(rjson::print(received[1]), rjson::print(item2));
    BOOST_CHECK_EQUAL(rjson::print(received[2]), rjson::print(item3));
}

// The item is serialized as-is, with no flattening or type coercion: nested objects, arrays and
// booleans have to survive the roundtrip unchanged. Exported items are arbitrary DynamoDB JSON,
// which is nested by nature, so anything else would corrupt the exported data.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_nested_json) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage });

    auto item = rjson::parse("{\"nested\": {\"array\": [1, 2, 3], \"obj\": {\"a\": true}}}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read_all();
    co_await source->close();

    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item));
}

// Reading an empty file is a valid outcome, not an error - exporting an empty table produces one.
// read_all() is expected to complete normally without ever invoking the item callback, and close()
// still has to flush the storage.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_empty_storage) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();

    std::vector<rjson::value> received;
    auto source = co_await  alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read_all();
    co_await source->close();

    BOOST_CHECK(storage->is_read_flushed());
    BOOST_CHECK(received.empty());
}

// Test that special characters in JSON strings are properly escaped and unescaped during export/import roundtrip,
// especially end of line character, which has additional meaning as item separator.
// Two items are written on purpose: if an embedded newline were emitted literally, it would split
// its item into two lines, so the failure shows up as a wrong item count and not merely as a
// mangled string value.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_special_characters) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage });

    auto item1 = rjson::parse("{\"msg\": \"hello\\nworld\\t\\\"quoted1\\\"\"}");
    auto item2 = rjson::parse("{\"msg\": \"hello\\nworld\\t\\\"quoted2\\\"\"}");

    co_await sink->process(item1);
    co_await sink->process(item2);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read_all();
    co_await source->close();

    BOOST_REQUIRE_EQUAL(received.size(), 2u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item1));
    BOOST_CHECK_EQUAL(rjson::print(received[1]), rjson::print(item2));
}

// Verify the summary `flush_and_close()` reports - size, etag and md5 - describes the bytes the
// pipeline actually wrote to the storage.
SEASTAR_TEST_CASE(test_in_memory_flush_and_close_result) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage });

    std::vector<rjson::value> items;
    items.push_back(rjson::parse("{\"id\": 1, \"name\": \"alice\"}"));
    items.push_back(rjson::parse("{\"id\": 2, \"nested\": {\"array\": [1, 2, 3]}}"));
    items.push_back(rjson::parse("{\"id\": 3, \"msg\": \"hello\\nworld\"}"));

    // The pipeline writes one JSON line per item, so this is exactly the payload we expect.
    std::string expected;
    for (auto& item : items) {
        co_await sink->process(item);
        expected += rjson::print(item);
        expected += "\n";
    }
    auto result = co_await sink->flush_and_close();

    auto data = storage->data();
    BOOST_REQUIRE_EQUAL(std::string_view(reinterpret_cast<const char*>(data.data()), data.size()), expected);

    BOOST_CHECK_EQUAL(result.size_in_bytes, expected.size());

    md5_hasher hasher;
    hasher.update(expected.data(), expected.size());
    auto md5_bytes = hasher.finalize();
    auto etag = to_hex(md5_bytes);

    BOOST_CHECK_EQUAL(result.md5, base64_encode(md5_bytes));
    // For a single part upload the etag is the object's md5 hash.
    BOOST_CHECK_EQUAL(result.etag, etag);
}

// Tests of alternator::export_scan_table() - the S3 export's table scan helper (not the
// Alternator Scan request). See SCYLLADB-1888.

namespace {
// Wraps a sharded<alternator::executor>, taking DynamoDB-style JSON requests.
class alternator_test_executor {
    sharded<cdc::metadata> _cdc_md;
    sharded<alternator::executor> _exec;
    alternator::rmw_operation::write_isolation _saved_write_isolation;
public:
    void start(cql_test_env& e) {
        // The default LWT_ALWAYS needs need_remote_proxy in the test env. FORBID_RMW is
        // enough for unconditional writes and fails loudly on an accidental RMW.
        _saved_write_isolation = alternator::rmw_operation::default_write_isolation;
        alternator::rmw_operation::set_default_write_isolation("forbid_rmw");
        // Pass sharded<> services, so each shard gets its local instances.
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

    void put_item(rjson::value request) {
        put_item_async(std::move(request)).get();
    }

    // Non-blocking variants, for use in export_scan_table() callbacks (not a seastar thread).
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

// PutItem request with hash key "p", optional sort key "c" and optional string attribute.
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

// PutItem request with hash key "p", optional sort key "c" and given extra attributes.
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

// DeleteItem request for a table with hash key "p" only.
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

// Get string attribute from a DynamoDB-style item ({"attr_name": {"S": "value"}}).
std::string get_string_attr(const rjson::value& item, const char* attr_name) {
    BOOST_REQUIRE(item.IsObject());
    BOOST_REQUIRE(item.HasMember(attr_name));
    const auto& typed_val = item[attr_name];
    BOOST_REQUIRE(typed_val.IsObject());
    BOOST_REQUIRE(typed_val.HasMember("S"));
    return std::string(rjson::to_string_view(typed_val["S"]));
}

// Compare a scanned item with the written one. Sets (SS, NS, BS) are compared ignoring order.
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

// Fails reads of "table_name" with "error" - see storage_proxy::query_result().
constexpr const char* read_error_injection = "alternator_query_result_timeout";

// Scan pages are limited to 1 MiB (maintenance requests ignore `query_page_size_in_bytes`),
// so these many items with a filler this large span several pages.
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

// Happy path: an empty table scans to nothing, then every written item is visited once and
// unchanged. Covers hash-only and hash+sort keys, and all DynamoDB attribute types.
SEASTAR_TEST_CASE(test_export_scan_table_basic) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        auto& proxy = e.get_storage_proxy().local();
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        auto scan = [&] (const sstring& table_name, seastar::noncopyable_function<future<>(rjson::value)> cb) {
            auto schema = e.local_db().find_schema(format("alternator_{}", table_name), table_name);
            alternator::export_scan_table(proxy, schema, as,
                make_test_service_permit(scan_permit_sem), std::move(cb)).get();
        };

        for (bool with_sort_key : {false, true}) {
            // Hash key "p", plus sort key "c" in the second round.
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

            // Empty table.
            size_t callbacks = 0;
            scan(table_name, [&callbacks] (rjson::value) -> future<> {
                ++callbacks;
                return make_ready_future<>();
            });
            BOOST_REQUIRE_EQUAL(callbacks, 0);

            // Keyed by (hash key, sort key or empty).
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
            // An item with an attribute of every DynamoDB type.
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
                // A partition with more than one row.
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
        // No read failed, so nothing may still hold the permit (see export_scan_table()).
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
        // No read was issued, so nothing holds the permit.
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

// Multi-page scan where the second page read fails once with `injected_error` - the retry
// must lose or repeat no item. One test case per retried error type.
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

        // "score" is checked together with the key, "filler" makes the set span several pages.
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

        // In release builds the injection is a no-op and the test still passes. Disable it
        // in case it never fired.
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
                    // First page read - fail the next read once.
                    auto& injector = utils::get_local_injector();
                    injector.enable(read_error_injection, true /* one_shot */,
                        {{"table_name", "hashonly"}, {"error", injected_error}});
                    injection_armed = injector.is_enabled(read_error_injection);
                }
                scanned_set.emplace(get_string_attr(item, "p"), get_string_attr(item, "score"));
                return make_ready_future<>();
            }).get();

        // Check the one-shot injection fired.
        BOOST_REQUIRE(!injection_armed || !utils::get_local_injector().is_enabled(read_error_injection));
        BOOST_REQUIRE_EQUAL(scanned_set.size(), expected_set.size());
        BOOST_REQUIRE(scanned_set == expected_set);
        // Injected errors issue no replica requests, so nothing holds the permit.
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

// A page boundary inside a partition (`has_ck` in query_pager::do_fetch_page()), with the
// read of the next page failing once. The scan must resume inside the partition without
// skipping or repeating rows.
SEASTAR_TEST_CASE(test_export_scan_table_page_boundary_inside_partition, *boost::unit_test::label("tier2")) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        exec.create_table(rjson::parse(R"({
            "TableName": "widepartition",
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

        // A single partition spanning several pages.
        constexpr size_t item_count = items_spanning_several_pages;
        const auto filler = make_page_filler();
        // Zero-padded for readable failure messages.
        auto sort_key = [] (size_t i) { return fmt::format("row{:04d}", i); };
        for (size_t i = 0; i < item_count; i++) {
            exec.put_item(make_put_item_request("widepartition", "thepartition",
                sort_key(i), "filler", filler));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_widepartition", "widepartition");

        // In release builds the injection is a no-op and the test still passes. Disable it
        // in case it never fired.
        auto disable_injection = defer([] noexcept {
            utils::get_local_injector().disable(read_error_injection);
        });

        std::set<std::string> scanned_keys;
        bool injection_armed = false;
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::export_scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&scanned_keys, &injection_armed] (rjson::value item) -> future<> {
                if (scanned_keys.empty()) {
                    // First page read - fail the next read once.
                    auto& injector = utils::get_local_injector();
                    injector.enable(read_error_injection, true /* one_shot */,
                        {{"table_name", "widepartition"}, {"error", "read_timeout"}});
                    injection_armed = injector.is_enabled(read_error_injection);
                }
                BOOST_REQUIRE_EQUAL(get_string_attr(item, "p"), "thepartition");
                // An item visited twice would fail right here.
                BOOST_REQUIRE(scanned_keys.insert(get_string_attr(item, "c")).second);
                return make_ready_future<>();
            }).get();

        // Check the one-shot injection fired.
        BOOST_REQUIRE(!injection_armed || !utils::get_local_injector().is_enabled(read_error_injection));
        // No row skipped (the callback checked for duplicates).
        BOOST_REQUIRE_EQUAL(scanned_keys.size(), item_count);
        for (size_t i = 0; i < item_count; i++) {
            BOOST_REQUIRE(scanned_keys.contains(sort_key(i)));
        }
        BOOST_REQUIRE_EQUAL(scan_permit_sem.available_units(), 1);
    });
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
// Shortens the backoff between attempts - see export_scan_table().
constexpr const char* retry_delay_injection = "export_scan_retry_delay_ms";

// All reads after the first page fail with `injected_error`, and a timer requests an abort
// while the scan waits between two attempts, so the scan always ends - with
// `Expected`. `retry_delay_ms` shortens the backoff, so that a scan which gives up gets
// there well before the abort. Requires error injection.
template <typename Expected>
static future<> do_test_export_scan_table_failing_reads(const char* injected_error,
        std::optional<uint32_t> retry_delay_ms) {
    return do_with_cql_env_thread([injected_error, retry_delay_ms] (cql_test_env& e) {
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

        // Span several pages, so there is a second page read to fail.
        const auto filler = make_page_filler();
        for (size_t i = 0; i < items_spanning_several_pages; i++) {
            exec.put_item(make_put_item_request("abortretrytbl", format("key{}", i), std::nullopt,
                "filler", filler));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_abortretrytbl", "abortretrytbl");

        // Not one-shot, so disable them explicitly.
        auto disable_injection = defer([] noexcept {
            utils::get_local_injector().disable(read_error_injection);
            utils::get_local_injector().disable(retry_delay_injection);
        });
        if (retry_delay_ms) {
            utils::get_local_injector().enable(retry_delay_injection, false /* one_shot */,
                {{"value", format("{}", *retry_delay_ms)}});
        }

        abort_source as;
        seastar::timer<> abort_timer([&as] { as.request_abort(); });
        bool injection_armed = false;
        seastar::semaphore scan_permit_sem{1};
        BOOST_REQUIRE_THROW(alternator::export_scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&] (rjson::value) -> future<> {
                if (!injection_armed) {
                    // First page read - fail all further reads, abort during the backoff.
                    utils::get_local_injector().enable(read_error_injection, false /* one_shot */,
                        {{"table_name", "abortretrytbl"}, {"error", injected_error}});
                    injection_armed = true;
                    abort_timer.arm(std::chrono::milliseconds(300));
                }
                return make_ready_future<>();
            }).get(), Expected);
        BOOST_REQUIRE(injection_armed);
        // Injected errors issue no replica requests, so nothing holds the permit.
        BOOST_REQUIRE_EQUAL(scan_permit_sem.available_units(), 1);
    });
}

// An abort during the backoff sleep between retries stops the scan with
// abort_requested_exception - the abort fires inside the (1 s) sleep.
SEASTAR_TEST_CASE(test_export_scan_table_abort_between_attempts, *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_failing_reads<abort_requested_exception>(
        "read_timeout", std::nullopt);
}

// A read failure which keeps coming back once the page shrank to a single row makes the scan
// give up - with the real backoff that takes minutes, so shorten it. The abort timer only
// bounds the test: reaching it would mean the scan did not give up.
SEASTAR_TEST_CASE(test_export_scan_table_read_failure_gives_up, *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_failing_reads<exceptions::read_failure_exception>(
        "read_failure", 1);
}

// A read failure carrying a timeout says the connection to a replica closed, not that the page
// is too big, so it is retried like a timeout: unlike the case above, this scan never gives up
// and only the abort stops it.
SEASTAR_TEST_CASE(test_export_scan_table_read_failure_with_timeout_never_gives_up,
        *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_failing_reads<abort_requested_exception>(
        "read_failure_with_timeout", 1);
}
#endif // SCYLLA_ENABLE_ERROR_INJECTION

// The table is modified during the scan: the callback deletes each visited item (so pages
// resume from deleted positions) and adds new ones. Every pre-existing item must be visited
// exactly once.
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

        // Span several pages, so pages resume over deleted items.
        constexpr size_t item_count = items_spanning_several_pages;
        const auto filler = make_page_filler();
        std::set<std::string> put_keys;
        for (size_t i = 0; i < item_count; i++) {
            put_keys.insert(format("key{}", i));
            exec.put_item(make_put_item_request("modifiedtbl", format("key{}", i), std::nullopt, "filler", filler));
        }

        auto& proxy = e.get_storage_proxy().local();
        auto schema = e.local_db().find_schema("alternator_modifiedtbl", "modifiedtbl");

        // Bounded, as added items can be visited and cause more additions.
        constexpr size_t items_added_during_scan = 100;
        size_t items_added = 0;

        std::set<std::string> scanned_keys;
        abort_source as;
        seastar::semaphore scan_permit_sem{1};
        alternator::export_scan_table(proxy, schema, as,
            make_test_service_permit(scan_permit_sem),
            [&] (rjson::value item) -> future<> {
                // The items here have no attribute beyond the key and the filler.
                BOOST_REQUIRE_EQUAL(item.MemberCount(), 2u);
                auto key = get_string_attr(item, "p");
                BOOST_REQUIRE(put_keys.contains(key));
                // An item visited twice would fail right here.
                BOOST_REQUIRE(scanned_keys.insert(key).second);
                co_await exec.delete_item_async(make_delete_item_request("modifiedtbl", key));
                if (items_added < items_added_during_scan) {
                    auto added_key = format("added{}", items_added++);
                    put_keys.insert(added_key);
                    // Same shape as the initial items (see the member count check).
                    co_await exec.put_item_async(make_put_item_request("modifiedtbl", added_key, std::nullopt,
                        "filler", filler));
                }
            }).get();

        // Every initial item must be visited. Added items may or may not be.
        for (size_t i = 0; i < item_count; i++) {
            BOOST_REQUIRE(scanned_keys.contains(format("key{}", i)));
        }
        BOOST_REQUIRE_EQUAL(items_added, items_added_during_scan);
    });
}
