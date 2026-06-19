/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"

#include <seastar/core/coroutine.hh>
#include "alternator/export.hh"
#include "alternator/error.hh"
#include "utils/rjson.hh"
#include "utils/base64.hh"
#include "utils/hashers.hh"
#include <string>
#include <string_view>
#include <vector>
#include <span>
#include <cstdint>
#include <stdexcept>

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

// Verify the summary `flush_and_close()` reports - size, etag and md5 - describes the bytes the
// pipeline actually wrote to the storage.
SEASTAR_TEST_CASE(test_in_memory_flush_and_close_result) {
    auto storage = std::make_shared<alternator::in_memory_test_storage>();
    auto sink = co_await alternator::create_sink_pipeline(alternator::in_memory_target_config{ storage }, alternator::no_compression{});

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
