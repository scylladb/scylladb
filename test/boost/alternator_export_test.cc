/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"

#include <seastar/core/coroutine.hh>
#include "alternator/export.hh"
#include "utils/rjson.hh"
#include "utils/base64.hh"
#include "utils/hashers.hh"
#include <string>
#include <string_view>
#include <vector>
#include <span>

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
