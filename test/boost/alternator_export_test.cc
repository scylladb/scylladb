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
#include "test/lib/s3_fixture.hh"
#include "test/lib/test_utils.hh"
#include "utils/base64.hh"
#include "utils/error_injection.hh"
#include "utils/hashers.hh"
#include "utils/rjson.hh"
#include "utils/s3/aws_error.hh"
#include "utils/s3/client.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/defer.hh>
#include <seastar/testing/test_fixture.hh>

#include "utils/base64.hh"
#include "bytes.hh"
#include "utils/hashers.hh"

#include <chrono>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "replica/database.hh"

// Tests of the export (sink) and import (source) pipelines from alternator/export.hh.
// Except for the test_s3_* ones, which need an S3 server, they use the in-memory storage backend,
// so they exercise the serialization, framing and flushing logic of the pipelines themselves,
// without involving S3.
// The pipelines write JSON lines: one serialized item per line, each terminated by '\n'.
// A test usually writes items through a sink pipeline and reads them back with a source
// pipeline over the same storage, and expects to get exactly the items it wrote, in order.

// The basic happy path: a single item written through the sink comes back identical from the
// source. Also verifies that flush_and_close() (sink) and close() (source) reach down to the
// storage - a pipeline that never flushes its backend would silently lose the tail of the data,
// and with a single small item everything still sits in the pipeline's buffers until the flush.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_single_item) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::no_compression{});

    auto item = rjson::parse("{\"key\": \"value\"}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    BOOST_CHECK(storage->is_write_flushed());

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::no_compression{});
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
    }, alternator::no_compression{});
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
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::no_compression{});

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
    }, alternator::no_compression{});
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
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::no_compression{});

    auto item = rjson::parse("{\"nested\": {\"array\": [1, 2, 3], \"obj\": {\"a\": true}}}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::no_compression{});
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
    }, alternator::no_compression{});
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
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::no_compression{});

    auto item1 = rjson::parse("{\"msg\": \"hello\\nworld\\t\\\"quoted1\\\"\"}");
    auto item2 = rjson::parse("{\"msg\": \"hello\\nworld\\t\\\"quoted2\\\"\"}");

    co_await sink->process(item1);
    co_await sink->process(item2);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::no_compression{});
    co_await source->read_all();
    co_await source->close();

    BOOST_REQUIRE_EQUAL(received.size(), 2u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item1));
    BOOST_CHECK_EQUAL(rjson::print(received[1]), rjson::print(item2));
}

// Writes a few items through `sink`, closes it and checks the size and md5 `flush_and_close()`
// reports against the payload it was given, one JSON line per item. Returns that payload and the
// reported etag, which each storage checks its own way.
static future<std::pair<std::string, sstring>> write_items_and_check_result(alternator::export_pipeline_interface& sink) {
    std::vector<rjson::value> items;
    items.push_back(rjson::parse("{\"id\": 1, \"name\": \"alice\"}"));
    items.push_back(rjson::parse("{\"id\": 2, \"nested\": {\"array\": [1, 2, 3]}}"));
    items.push_back(rjson::parse("{\"id\": 3, \"msg\": \"hello\\nworld\"}"));

    // The pipeline writes one JSON line per item, so this is exactly the payload we expect.
    std::string expected;
    for (auto& item : items) {
        co_await sink.process(item);
        expected += rjson::print(item);
        expected += "\n";
    }
    auto result = co_await sink.flush_and_close();

    BOOST_CHECK_EQUAL(result.size_in_bytes, expected.size());

    md5_hasher hasher;
    hasher.update(expected.data(), expected.size());
    BOOST_CHECK_EQUAL(result.md5, base64_encode(hasher.finalize()));

    co_return std::make_pair(std::move(expected), std::move(result.etag));
}

// Verify the summary `flush_and_close()` reports - size, etag and md5 - describes the bytes the
// pipeline actually wrote to the storage.
SEASTAR_TEST_CASE(test_in_memory_flush_and_close_result) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::no_compression{});

    auto [expected, etag] = co_await write_items_and_check_result(*sink);

    auto data = storage->data();
    BOOST_REQUIRE_EQUAL(std::string_view(reinterpret_cast<const char*>(data.data()), data.size()), expected);

    md5_hasher hasher;
    hasher.update(expected.data(), expected.size());
    // For a single part upload the etag is the object's md5 hash.
    BOOST_CHECK_EQUAL(etag, to_hex(hasher.finalize()));
}

static shared_ptr<s3::client> make_test_s3_client() {
    s3::endpoint_config cfg = {
        .port = static_cast<unsigned>(std::stoul(tests::getenv_safe("S3_SERVER_PORT_FOR_TEST"))),
        .use_https = ::getenv("AWS_DEFAULT_REGION") != nullptr,
        .region = ::getenv("AWS_DEFAULT_REGION") ? : "local",
    };
    return s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_lw_shared<s3::endpoint_config>(std::move(cfg)));
}

// The bucket belongs to the fixture and is dropped with its objects, so no test cleans up the
// objects it puts there.
static sstring test_s3_object_name(std::string_view key) {
    return seastar::format("/{}/{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), key);
}

// Runs `test` with a client of its own, and closes that client before it returns. The fixture's
// client must be the only one on this endpoint when it deletes the bucket: a second client on the
// same endpoint and shard fails to register its metrics.
static future<> with_test_s3_client(std::function<future<>(shared_ptr<s3::client>)> test) {
    auto client = make_test_s3_client();
    std::exception_ptr ex;
    try {
        co_await test(client);
    } catch (...) {
        ex = std::current_exception();
    }
    try {
        co_await client->close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }
    if (ex) {
        std::rethrow_exception(ex);
    }
}

// The same, against a real S3 object: the etag the S3 sink reports is the one S3 assigned to the
// object, so it has to match what S3 says about that object afterwards - the point being that the
// sink passes the storage's etag through rather than computing one of its own, which for a
// multipart upload would not be the object's etag at all. Needs the S3 server test.py sets up, so
// the precondition skips the test when it runs outside test.py.
SEASTAR_TEST_CASE(test_s3_flush_and_close_result, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    co_await with_test_s3_client([](shared_ptr<s3::client> client) -> future<> {
        const sstring object_name = test_s3_object_name("alternator-export-test");

        auto sink = co_await alternator::create_sink_pipeline(alternator::s3_target_config{ client, object_name }, alternator::no_compression{});
        auto etag = (co_await write_items_and_check_result(*sink)).second;
        // S3 reports an etag as an RFC 9110 entity-tag, i.e. in double quotes, which the pipeline strips.
        BOOST_CHECK_EQUAL(seastar::format("\"{}\"", etag), (co_await client->get_object_info(object_name)).etag);
    });
}

// The gzip variant of the basic happy path: the item survives the roundtrip, and the bytes left in
// the storage are a real gzip stream rather than the plain JSON line.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_gzip_single_item) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::gzip_compression{});

    auto item = rjson::parse("{\"key\": \"value\"}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    BOOST_CHECK(storage->is_write_flushed());

    // What the pipeline would have written without compression: one JSON line per item.
    auto uncompressed = rjson::print(item) + "\n";

    auto data = storage->data();
    auto bytes = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    // A gzip member is at least a 10 byte header plus an 8 byte trailer.
    BOOST_REQUIRE_GE(bytes.size(), 18u);
    // The gzip magic number (RFC 1952: ID1, ID2) followed by the only compression method defined
    // for gzip, CM = 8 (deflate).
    BOOST_CHECK_EQUAL(unsigned(bytes[0]), 0x1fu);
    BOOST_CHECK_EQUAL(unsigned(bytes[1]), 0x8bu);
    BOOST_CHECK_EQUAL(unsigned(bytes[2]), 0x08u);
    // The stored bytes must not be the plain payload with a header glued in front of it.
    BOOST_CHECK(std::string_view(reinterpret_cast<const char*>(data.data()), data.size()).find(uncompressed) == std::string_view::npos);
    // The gzip trailer ends with ISIZE, the size of the uncompressed data, little endian.
    uint32_t isize = uint32_t(bytes[bytes.size() - 4])
            | (uint32_t(bytes[bytes.size() - 3]) << 8)
            | (uint32_t(bytes[bytes.size() - 2]) << 16)
            | (uint32_t(bytes[bytes.size() - 1]) << 24);
    BOOST_CHECK_EQUAL(isize, uncompressed.size());

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::gzip_compression{});
    co_await source->read_all();
    co_await source->close();

    BOOST_CHECK(storage->is_read_flushed());
    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item));
}

// Same as test_in_memory_roundtrip_gzip_single_item, just multiple items.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_gzip_multiple_items) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::gzip_compression{});

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
    }, alternator::gzip_compression{});
    co_await source->read_all();
    co_await source->close();

    BOOST_REQUIRE_EQUAL(received.size(), 3u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item1));
    BOOST_CHECK_EQUAL(rjson::print(received[1]), rjson::print(item2));
    BOOST_CHECK_EQUAL(rjson::print(received[2]), rjson::print(item3));
}

// Pseudo-random text made of characters that JSON does not escape. The LCG (the constants are the
// ones from Knuth's MMIX) keeps it reproducible, and drawing from a 64 character alphabet leaves
// it with 6 bits of entropy per byte, so no compressor can squeeze it below three quarters of its
// size. That is what makes it useful both as padding that moves a member's compressed size by a
// predictable amount, and as the payload that drives the compressor's output buffer further below.
static std::string make_incompressible_text(size_t size) {
    static constexpr std::string_view alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
    std::string result;
    result.reserve(size);
    uint64_t state = 0x2545f4914f6cdd1dull;
    for (size_t i = 0; i < size; ++i) {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        result += alphabet[(state >> 33) % alphabet.size()];
    }
    return result;
}

// A gzip file is allowed to hold several members concatenated (RFC 1952 2.2), and that is what a
// producer that compresses in chunks emits - pigz, `cat a.gz b.gz`, or an exporter that restarts
// its compressor partway through. zlib stops at the end of each member and has to be reset before
// it will look at the next one, so a decompressor that simply returns on Z_STREAM_END imports the
// first member and silently drops the rest, with read_all() and close() both succeeding. The two
// members are built with two separate sinks and their bytes glued together, which is exactly how
// such a file comes about.
//
// The decompressor reaches that reset by two different routes, depending on where the boundary
// falls in the chunks the source hands it. A boundary inside a chunk leaves zlib reporting
// Z_STREAM_END with input still unconsumed, and is reset on the spot; a boundary on the last byte
// of a chunk has to be remembered and acted on only once the next chunk arrives, because resetting
// it right away would restart into a member that may never come and turn a complete file into a
// truncated one. Getting the second route wrong drops everything past the first member of any file
// whose member happens to end on a chunk boundary - a one-in-sixteen file, not an exotic one.
// in_memory_source reads 16 bytes at a time, so which route is taken is decided by the compressed
// size of the first member, and the test pads the item in it until it has a size for each.
SEASTAR_TEST_CASE(test_in_memory_gzip_concatenated_members) {
    constexpr size_t source_chunk_size = 16; // what in_memory_source reads at a time

    // One standalone gzip member holding `items`, exactly as a complete exporter run would have
    // left it behind.
    auto make_member = [](const std::vector<rjson::value>& items) -> future<std::vector<std::byte>> {
        auto storage = std::make_shared<alternator::in_memory_test_storage>();
        auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::gzip_compression{});
        for (const auto& item : items) {
            co_await sink->process(item);
        }
        co_await sink->flush_and_close();
        auto data = storage->data();
        co_return std::vector<std::byte>(data.begin(), data.end());
    };

    std::vector<rjson::value> second_items;
    second_items.push_back(rjson::parse("{\"id\": 2, \"name\": \"bob\"}"));
    second_items.push_back(rjson::parse("{\"id\": 3, \"name\": \"charlie\"}"));
    auto second_member = co_await make_member(second_items);

    // A byte of padding does not always add a byte to the compressed form, which is why the two
    // first-member sizes are searched for rather than calculated.
    bool boundary_on_chunk_end = false;
    bool boundary_inside_chunk = false;
    for (size_t padding = 0; padding <= 4 * source_chunk_size && !(boundary_on_chunk_end && boundary_inside_chunk); ++padding) {
        std::vector<rjson::value> first_items;
        first_items.push_back(rjson::parse("{\"id\": 1, \"name\": \"alice\", \"pad\": \"" + make_incompressible_text(padding) + "\"}"));
        auto first_member = co_await make_member(first_items);

        bool& covered = first_member.size() % source_chunk_size == 0 ? boundary_on_chunk_end : boundary_inside_chunk;
        if (covered) {
            continue;
        }
        covered = true;

        auto storage = std::make_shared<alternator::in_memory_test_storage>();
        storage->append(first_member);
        storage->append(second_member);

        std::vector<rjson::value> received;
        auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
            received.push_back(std::move(v));
            co_return;
        }, alternator::gzip_compression{});
        co_await source->read_all();
        co_await source->close();

        BOOST_REQUIRE_EQUAL(received.size(), 3u);
        BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(first_items[0]));
        BOOST_CHECK_EQUAL(rjson::print(received[1]), rjson::print(second_items[0]));
        BOOST_CHECK_EQUAL(rjson::print(received[2]), rjson::print(second_items[1]));
    }
    // Both routes have to have been taken: a change to the chunk size or to the compressor that
    // leaves one of them untested has to be noticed rather than silently tolerated.
    BOOST_REQUIRE(boundary_on_chunk_end);
    BOOST_REQUIRE(boundary_inside_chunk);
}

// An object that ends before its gzip trailer must be rejected rather than imported as whatever
// decoded successfully. gzip's only integrity check is the CRC32 and ISIZE trailer, which zlib
// verifies at the end of a member and nowhere else, so a partial download or an interrupted export
// decompresses cleanly up to the cut. Here the cut is placed after the last complete line, which is
// the dangerous case: the parser has nothing pending to choke on, so without an explicit
// end-of-stream check the import silently returns short data and reports success.
SEASTAR_TEST_CASE(test_in_memory_gzip_truncated_stream_is_rejected) {
    auto complete_storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ complete_storage }, alternator::gzip_compression{});

    auto item1 = rjson::parse("{\"id\": 1, \"name\": \"alice\"}");
    auto item2 = rjson::parse("{\"id\": 2, \"name\": \"bob\"}");
    co_await sink->process(item1);
    co_await sink->process(item2);
    co_await sink->flush_and_close();

    // Drop the 8 byte gzip trailer (CRC32 and ISIZE). Everything before it is a valid deflate
    // stream holding both complete lines, so the data itself still decodes without error.
    auto complete = complete_storage->data();
    BOOST_REQUIRE_GT(complete.size(), 8u);
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    storage->append(complete.subspan(0, complete.size() - 8));

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::gzip_compression{});
    // Reading succeeds - the bytes that are there are valid - the truncation is only detectable
    // once the source is exhausted without the stream having ended.
    co_await source->read_all();
    BOOST_CHECK_THROW(co_await source->close(), alternator::api_error);
}

// The complement of the truncation test: here the object is complete but its deflate data is
// corrupt, so inflate() fails in the middle of read_all() rather than at the end of the stream.
// The import has to surface that as an api_error, and close() still has to run cleanly afterwards -
// read_all() cancels the decompressor on the way out, so close() must not report the aborted
// stream as a truncated one on top of the error the caller already got.
SEASTAR_TEST_CASE(test_in_memory_gzip_corrupt_stream_is_rejected) {
    auto complete_storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ complete_storage }, alternator::gzip_compression{});

    auto item = rjson::parse("{\"id\": 1, \"name\": \"alice\"}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    // Byte 10 is the first byte of the deflate stream, right past the fixed 10 byte gzip header:
    // BFINAL in bit 0 and BTYPE in bits 1-2. Setting both BTYPE bits makes it 11, the reserved
    // value (RFC 1951 3.2.3), so inflate() rejects the very first block it looks at.
    auto complete = complete_storage->data();
    BOOST_REQUIRE_GT(complete.size(), 10u);
    std::vector<std::byte> corrupt(complete.begin(), complete.end());
    corrupt[10] |= std::byte{0x06};

    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    storage->append(corrupt);

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::gzip_compression{});
    BOOST_CHECK_THROW(co_await source->read_all(), alternator::api_error);
    co_await source->close();

    BOOST_CHECK(received.empty());
}

// Thrown by the on_item callback of the test below. Any type distinct from api_error does, the
// point being that whatever the callback failed with is what the caller gets back from read_all().
struct on_item_error : public std::runtime_error {
    on_item_error() : std::runtime_error("on_item failed") {}
};

// The other way the decompressor gets cancelled in the middle of a member: the error comes from
// above it instead of from zlib. The object here is complete and valid - it is read_all()'s caller
// that fails, by throwing from the on_item callback for the very first item. The gzip member
// therefore never reaches its end, so without read_all() cancelling the decompressor close() would
// report a truncated stream on top of the callback's exception, hiding the real error behind a
// bogus complaint about the object.
SEASTAR_TEST_CASE(test_in_memory_gzip_throwing_callback_is_not_reported_as_truncation) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::gzip_compression{});

    auto item1 = rjson::parse("{\"id\": 1, \"name\": \"alice\"}");
    auto item2 = rjson::parse("{\"id\": 2, \"name\": \"bob\"}");
    co_await sink->process(item1);
    co_await sink->process(item2);
    co_await sink->flush_and_close();

    size_t calls = 0;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value) -> seastar::future<> {
        ++calls;
        return seastar::make_exception_future<>(on_item_error());
    }, alternator::gzip_compression{});
    BOOST_CHECK_THROW(co_await source->read_all(), on_item_error);
    co_await source->close();

    // The first failure stops the read - the second item is never handed to the callback.
    BOOST_CHECK_EQUAL(calls, 1u);
    BOOST_CHECK(storage->is_read_flushed());
}

SEASTAR_TEST_CASE(test_in_memory_roundtrip_gzip_empty_storage) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::gzip_compression{});
    co_await sink->flush_and_close();

    BOOST_CHECK(storage->is_write_flushed());

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::gzip_compression{});
    co_await source->read_all();
    co_await source->close();

    BOOST_CHECK(storage->is_read_flushed());
    BOOST_CHECK(received.empty());
}

// The test above goes through the gzip sink, so its object is a complete, if empty, gzip stream -
// a header plus a trailer. This one reads an object with no bytes at all, which is what AWS leaves
// behind for an empty partition. It is the one degenerate case that is accepted without a gzip
// member ever being seen, so it must not be mistaken for a truncated stream by close().
SEASTAR_TEST_CASE(test_in_memory_gzip_zero_byte_object_is_an_empty_stream) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    BOOST_REQUIRE(storage->data().empty());

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::gzip_compression{});
    co_await source->read_all();
    co_await source->close();

    BOOST_CHECK(storage->is_read_flushed());
    BOOST_CHECK(received.empty());
}

// Every gzip test above uses items of at most a few hundred bytes, which both the compressor and the
// decompressor turn into far less than the 4 KB buffer they hand to zlib: their do/while loops run
// a single iteration and never refill that buffer. With the compressor that also means the only
// bytes compress() writes to the storage are the gzip header, which deflate() emits on its first
// call whatever its input is - all the compressed data follows from flush_and_close().
// The two tests below force the loops to go around several times, which is where the delicate part
// of the code sits: the co_await in the middle of the loop suspends while zlib's next_in/next_out
// still point into the loop-local output buffer and into the caller's data. Covering both
// directions takes two payloads, since the compressor's loop needs data that stays large once
// compressed, and the decompressor's loop needs data that grows enormously when inflated.

// An item whose compressed form is far bigger than the compressor's 4 KB output buffer. zlib can
// hold back only its window and its pending output - some 128 KB together, with the window bits
// and memory level the compressor is initialized with - so of the ~384 KB this half megabyte of
// incompressible text has to compress to, the bulk must leave zlib while process() is still
// running. The storage is therefore inspected before flush_and_close(): every byte in it at that
// point was written from inside compress()'s loop, and since one iteration can write at most the
// 4 KB the buffer holds, more than 4 KB means the loop refilled it and came back for more.
// The same argument applied to how much the storage grows across flush_and_close() pins down the
// flush loop too, but there the margin is not something zlib guarantees: all it still holds at
// Z_FINISH is the partly filled deflate block plus the unprocessed lookahead, which is anywhere
// between a few hundred bytes and some kilobytes depending on where the last block boundary
// happened to fall - a function of the payload size and of the zlib version and build. So instead
// of trusting one payload size, the test walks a handful of them and keeps the first whose
// flush_and_close() writes more than one bufferful, failing if none of them does.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_gzip_item_larger_than_buffer) {
    constexpr size_t compressor_buf_size = 4096; // the compressor's output buffer
    static constexpr size_t payload_sizes[] = {
        512 * 1024, 516 * 1024, 520 * 1024, 524 * 1024, 528 * 1024, 532 * 1024, 536 * 1024, 540 * 1024,
    };

    std::shared_ptr<alternator::in_memory_test_storage> storage;
    auto item = rjson::empty_object();

    for (size_t payload_size : payload_sizes) {
        auto candidate_storage = std::make_shared<alternator::in_memory_test_storage>();
        auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ candidate_storage }, alternator::gzip_compression{});

        auto candidate = rjson::empty_object();
        rjson::add(candidate, "payload", rjson::from_string(make_incompressible_text(payload_size)));

        co_await sink->process(candidate);
        auto size_before_flush = candidate_storage->data().size();
        BOOST_REQUIRE_GT(size_before_flush, compressor_buf_size);
        co_await sink->flush_and_close();
        if (candidate_storage->data().size() - size_before_flush > compressor_buf_size) {
            storage = std::move(candidate_storage);
            item = std::move(candidate);
            break;
        }
    }
    // No payload size left zlib with more than one bufferful to emit on Z_FINISH, so the flush loop
    // would not be exercised - the test has lost its point and has to be given sizes that work.
    BOOST_REQUIRE(storage != nullptr);

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::gzip_compression{});
    co_await source->read_all();
    co_await source->close();

    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    // Compared without BOOST_CHECK_EQUAL on purpose: a mismatch would print half a megabyte of JSON.
    BOOST_CHECK(rjson::print(received[0]) == rjson::print(item));
}

// The decompressor's mirror case. in_memory_source hands the pipeline 16 byte chunks, so reaching
// the loop's avail_out == 0 continuation needs a chunk that inflates to more than the 4 KB output
// buffer, i.e. data that expands by more than 256:1. A long run of one character does that: deflate
// encodes it as back references of up to 258 bytes apiece and shrinks it by roughly a thousand to
// one. The size check below makes that a guarantee instead of an expectation - had no chunk ever
// refilled the output buffer, the object could not have decoded to more than 4 KB per 16 bytes of
// it - and fails loudly if a future change to either buffer size takes the coverage away.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_gzip_decompressed_larger_than_buffer) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::gzip_compression{});

    auto item = rjson::empty_object();
    rjson::add(item, "payload", rjson::from_string(std::string(1024 * 1024, 'a')));
    // What the decompressor has to produce: the JSON line plus the newline the formatter appends.
    auto decompressed_size = rjson::print(item).size() + 1;

    co_await sink->process(item);
    co_await sink->flush_and_close();

    constexpr size_t source_chunk_size = 16; // what in_memory_source reads at a time
    constexpr size_t decompressor_buf_size = 4096; // the decompressor's output buffer
    auto chunks = (storage->data().size() + source_chunk_size - 1) / source_chunk_size;
    BOOST_REQUIRE_GT(decompressed_size, decompressor_buf_size * chunks);

    std::vector<rjson::value> received;
    auto source = co_await alternator::create_source_pipeline(alternator::in_memory_target_config{ storage }, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    }, alternator::gzip_compression{});
    co_await source->read_all();
    co_await source->close();

    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    // Compared without BOOST_CHECK_EQUAL on purpose: a mismatch would print a megabyte of JSON.
    BOOST_CHECK(rjson::print(received[0]) == rjson::print(item));
}

// Closes the sink also when process() throws: a sink that is destroyed without flush_and_close()
// is an internal error, and leaves its upload behind in S3.
static future<> write_s3_object(shared_ptr<s3::client> client, sstring object_name, const std::vector<rjson::value>& items, alternator::compression_type compression) {
    auto sink = co_await alternator::create_sink_pipeline(alternator::s3_target_config{ std::move(client), std::move(object_name) }, compression);
    std::exception_ptr ex;
    try {
        for (const auto& item : items) {
            co_await sink->process(item);
        }
    } catch (...) {
        ex = std::current_exception();
    }
    try {
        co_await sink->flush_and_close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }
    if (ex) {
        std::rethrow_exception(ex);
    }
}

// Runs `test` on a source pipeline over the object, and closes the pipeline also when `test`
// throws: a pipeline that is destroyed without close() asserts in its destructor.
static future<> with_s3_source(shared_ptr<s3::client> client, sstring object_name, alternator::compression_type compression,
        std::function<future<>(rjson::value)> on_item,
        std::function<future<>(alternator::import_pipeline_interface&)> test,
        abort_source* as = nullptr) {
    auto source = co_await alternator::create_source_pipeline(alternator::s3_target_config{ std::move(client), std::move(object_name), as }, std::move(on_item), compression);
    std::exception_ptr ex;
    try {
        co_await test(*source);
    } catch (...) {
        ex = std::current_exception();
    }
    try {
        co_await source->close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }
    if (ex) {
        std::rethrow_exception(ex);
    }
}

static future<std::vector<rjson::value>> read_s3_object(shared_ptr<s3::client> client, sstring object_name, alternator::compression_type compression) {
    std::vector<rjson::value> items;
    co_await with_s3_source(std::move(client), std::move(object_name), compression, [&items](rjson::value item) -> future<> {
        items.push_back(std::move(item));
        co_return;
    }, [](alternator::import_pipeline_interface& source) -> future<> {
        co_await source.read_all();
    });
    co_return items;
}

static std::vector<rjson::value> parse_items(std::initializer_list<std::string_view> lines) {
    std::vector<rjson::value> items;
    for (auto line : lines) {
        items.push_back(rjson::parse(line));
    }
    return items;
}

// Compared without BOOST_CHECK_EQUAL on purpose: some tests use items of a megabyte each, and a
// mismatch would print megabytes of JSON.
static void require_same_items(const std::vector<rjson::value>& actual, const std::vector<rjson::value>& expected) {
    BOOST_REQUIRE_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        BOOST_CHECK(rjson::print(actual[i]) == rjson::print(expected[i]));
    }
}

static future<> check_s3_roundtrip(const std::vector<rjson::value>& items, alternator::compression_type compression) {
    co_await with_test_s3_client([&](shared_ptr<s3::client> client) -> future<> {
        auto object_name = test_s3_object_name("roundtrip");
        co_await write_s3_object(client, object_name, items, compression);
        require_same_items(co_await read_s3_object(client, object_name, compression), items);
    });
}

// The in-memory roundtrips again, now through the S3 sink and source, without and with gzip.
SEASTAR_TEST_CASE(test_s3_roundtrip_multiple_items, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    auto items = parse_items({
        R"({"id": 1, "name": "alice"})",
        R"({"id": 2, "name": "bob"})",
        R"({"id": 3, "name": "charlie"})",
    });
    co_await check_s3_roundtrip(items, alternator::no_compression{});
}

SEASTAR_TEST_CASE(test_s3_roundtrip_gzip_multiple_items, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    auto items = parse_items({
        R"({"id": 1, "name": "alice"})",
        R"({"id": 2, "name": "bob"})",
        R"({"id": 3, "name": "charlie"})",
    });
    co_await check_s3_roundtrip(items, alternator::gzip_compression{});
}

// The most the download source buffers ahead of its reader. An object larger than this is handed
// out while its download is under way, and when the reader falls behind, in more than one request.
constexpr size_t s3_download_chunk_size = s3::max_download_buffers_size;

// Items of a megabyte each: a few of them make an object larger than two download chunks, with
// lines longer than any buffer the source hands out.
static std::vector<rjson::value> make_items_spanning_several_download_chunks() {
    constexpr size_t item_payload_size = 1024 * 1024;
    constexpr size_t item_count = 2 * s3_download_chunk_size / item_payload_size + 1;

    std::vector<rjson::value> items;
    for (size_t i = 0; i < item_count; ++i) {
        auto item = rjson::empty_object();
        rjson::add(item, "id", i);
        rjson::add(item, "payload", rjson::from_string(make_incompressible_text(item_payload_size)));
        items.push_back(std::move(item));
    }
    return items;
}

// An object larger than the download buffers cannot be held whole: the reader drains it while the
// download is under way, and the source stops and resumes the download when the reader falls
// behind. The source must join the pieces with no byte lost or repeated. The size check fails
// loudly if a change to the chunk size or to the items takes that coverage away.
SEASTAR_TEST_CASE(test_s3_roundtrip_object_spanning_several_download_chunks, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    auto items = make_items_spanning_several_download_chunks();

    co_await with_test_s3_client([&](shared_ptr<s3::client> client) -> future<> {
        auto object_name = test_s3_object_name("several-download-chunks");
        co_await write_s3_object(client, object_name, items, alternator::no_compression{});
        auto object_size = co_await client->get_object_size(object_name);
        BOOST_REQUIRE_GT(object_size, 2 * s3_download_chunk_size);
        require_same_items(co_await read_s3_object(client, object_name, alternator::no_compression{}), items);
    });
}

// test_in_memory_line_without_newline against S3. The object is put by hand, since the sink
// cannot produce one without a trailing newline.
SEASTAR_TEST_CASE(test_s3_line_without_newline, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    co_await with_test_s3_client([](shared_ptr<s3::client> client) -> future<> {
        auto object_name = test_s3_object_name("line-without-newline");
        std::string_view content = "{\"id\": 1}\n{\"id\": 2}";
        co_await client->put_object(object_name, temporary_buffer<char>(content.data(), content.size()));

        auto items = co_await read_s3_object(client, object_name, alternator::no_compression{});

        require_same_items(items, parse_items({ R"({"id": 1})", R"({"id": 2})" }));
    });
}

// test_in_memory_gzip_zero_byte_object_is_an_empty_stream against S3, where such an object is
// what AWS leaves behind for an empty partition. Also covers the download source, whose first
// request has no range for this very case: S3 answers a byte range of a zero-length object
// with 416.
SEASTAR_TEST_CASE(test_s3_gzip_zero_byte_object_is_an_empty_stream, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    co_await with_test_s3_client([](shared_ptr<s3::client> client) -> future<> {
        auto object_name = test_s3_object_name("zero-byte.gz");
        co_await client->put_object(object_name, temporary_buffer<char>());

        auto items = co_await read_s3_object(client, object_name, alternator::gzip_compression{});

        BOOST_CHECK(items.empty());
    });
}

// test_in_memory_gzip_truncated_stream_is_rejected against S3. The sink cannot produce a truncated
// object, so a complete one is cut and put again under another name. The cut takes off exactly
// the 8 byte CRC32/ISIZE trailer, so all the deflate data is still there: every line decodes,
// read_all() succeeds, and only close() can report the truncation.
SEASTAR_TEST_CASE(test_s3_gzip_truncated_object_is_rejected, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    co_await with_test_s3_client([](shared_ptr<s3::client> client) -> future<> {
        constexpr size_t gzip_trailer_size = 8;
        auto complete_object_name = test_s3_object_name("complete.gz");
        auto truncated_object_name = test_s3_object_name("truncated.gz");
        co_await write_s3_object(client, complete_object_name, parse_items({ R"({"id": 1})", R"({"id": 2})" }), alternator::gzip_compression{});

        auto complete = co_await client->get_object_contiguous(complete_object_name);
        BOOST_REQUIRE_GT(complete.size(), gzip_trailer_size);
        complete.trim(complete.size() - gzip_trailer_size);
        co_await client->put_object(truncated_object_name, std::move(complete));

        co_await with_s3_source(client, truncated_object_name, alternator::gzip_compression{}, [](rjson::value) -> future<> {
            co_return;
        }, [](alternator::import_pipeline_interface& source) -> future<> {
            co_await source.read_all();
            BOOST_CHECK_THROW(co_await source.close(), alternator::api_error);
        });
    });
}

// A GET of a missing key fails with a 404. AWS puts a NoSuchKey error body in it, which the client
// maps to NO_SUCH_KEY; a 404 without such a body maps to HTTP_NOT_FOUND. Both mean that the object
// is not there, and which one comes depends on the server.
SEASTAR_TEST_CASE(test_s3_missing_object_is_rejected, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    co_await with_test_s3_client([](shared_ptr<s3::client> client) -> future<> {
        BOOST_REQUIRE_EXCEPTION(
            co_await read_s3_object(client, test_s3_object_name("missing"), alternator::no_compression{}),
            aws::aws_exception, [](const aws::aws_exception& e) {
                auto type = e.error().get_error_type();
                return type == aws::aws_error_type::NO_SUCH_KEY || type == aws::aws_error_type::HTTP_NOT_FOUND;
            });
    });
}

// Passed to request_abort_ex() by the test below, so that read_all() is seen to fail with the
// abort itself and not with some other error.
struct s3_read_aborted : public std::runtime_error {
    s3_read_aborted() : std::runtime_error("s3 read aborted") {}
};

// An abort requested during the download must stop read_all() with the abort's exception, before
// the end of the object. The abort source only stops requests, and does nothing to data that is
// already buffered. The object therefore spans several download chunks, so that the download is
// still under way when the first item arrives and the abort comes.
SEASTAR_TEST_CASE(test_s3_aborted_read_is_rejected, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    co_await with_test_s3_client([](shared_ptr<s3::client> client) -> future<> {
        auto items = make_items_spanning_several_download_chunks();

        auto object_name = test_s3_object_name("aborted");
        co_await write_s3_object(client, object_name, items, alternator::no_compression{});

        abort_source as;
        size_t received = 0;
        co_await with_s3_source(client, object_name, alternator::no_compression{}, [&](rjson::value) -> future<> {
            if (++received == 1) {
                as.request_abort_ex(s3_read_aborted{});
            }
            co_return;
        }, [](alternator::import_pipeline_interface& source) -> future<> {
            BOOST_CHECK_THROW(co_await source.read_all(), s3_read_aborted);
        }, &as);
        BOOST_CHECK_LT(received, items.size());
    });
}

// close() without read_all(). The download source starts its fetching fiber when it is
// constructed, so close() has to stop a fiber that is still at work, and nothing may reach the
// callback. The object spans several download chunks, so that the fiber has not finished by then.
SEASTAR_TEST_CASE(test_s3_close_without_read_all, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    co_await with_test_s3_client([](shared_ptr<s3::client> client) -> future<> {
        auto object_name = test_s3_object_name("closed-without-reading");
        co_await write_s3_object(client, object_name, make_items_spanning_several_download_chunks(), alternator::no_compression{});

        size_t received = 0;
        co_await with_s3_source(client, object_name, alternator::no_compression{}, [&](rjson::value) -> future<> {
            ++received;
            co_return;
        }, [](alternator::import_pipeline_interface&) -> future<> {
            co_return;
        });
        BOOST_CHECK_EQUAL(received, 0u);
    });
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
// `abort_after` later, so the scan always ends - with `Expected`. `retry_delay_ms` shortens
// the backoff, so that a scan which gives up gets there well before the abort. Requires error
// injection.
template <typename Expected>
static future<> do_test_export_scan_table_failing_reads(const char* injected_error,
        std::optional<uint32_t> retry_delay_ms, std::chrono::milliseconds abort_after) {
    return do_with_cql_env_thread([injected_error, retry_delay_ms, abort_after] (cql_test_env& e) {
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
                    // First page read - fail all further reads.
                    utils::get_local_injector().enable(read_error_injection, false /* one_shot */,
                        {{"table_name", "abortretrytbl"}, {"error", injected_error}});
                    injection_armed = true;
                    abort_timer.arm(abort_after);
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
        "read_timeout", std::nullopt, std::chrono::milliseconds(300));
}

// A read failure which keeps coming back once the page shrank to a single row makes the scan
// give up - with the real backoff that takes minutes, so shorten it. The abort timer only
// bounds the test: reaching it would mean the scan did not give up.
SEASTAR_TEST_CASE(test_export_scan_table_read_failure_gives_up, *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_failing_reads<exceptions::read_failure_exception>(
        "read_failure", 10, std::chrono::seconds(60));
}

// A read failure carrying a timeout says the connection to a replica closed, not that the page
// is too big, so it is retried like a timeout: unlike the case above, this scan never gives up
// and only the abort stops it.
SEASTAR_TEST_CASE(test_export_scan_table_read_failure_with_timeout_never_gives_up,
        *boost::unit_test::label("tier2")) {
    return do_test_export_scan_table_failing_reads<abort_requested_exception>(
        "read_failure_with_timeout", 10, std::chrono::seconds(1));
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
