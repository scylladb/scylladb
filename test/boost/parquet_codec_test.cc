/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the Parquet codec in sstables/parquet.
//
// The Thrift compact protocol is checked against hand-written byte vectors.
// The metadata structures are checked by round trips, and against Arrow C++,
// which serves as the reference Parquet implementation. Arrow's low-level
// Parquet reader starts no threads when it reads from an in-memory buffer,
// so it can run inside a Seastar test.

#include <cstring>
#include <string>
#include <vector>

#include <zlib.h>

#include <seastar/core/iostream.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/memory-data-source.hh>

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/page_index.h>
#include <parquet/statistics.h>

#include "sstables/exceptions.hh"
#include "sstables/file_writer.hh"
#include "sstables/parquet/metadata.hh"
#include "sstables/parquet/thrift.hh"
#include "sstables/parquet/writer.hh"
#include "utils/memory_data_sink.hh"

namespace pq = sstables::parquet;
namespace thrift = sstables::parquet::thrift;
using sstables::malformed_sstable_exception;

namespace {

std::string bytes_of(std::initializer_list<uint8_t> b) {
    return std::string(b.begin(), b.end());
}

std::string to_string(const bytes_ostream& b) {
    std::string s;
    for (bytes_view frag : b.fragments()) {
        s.append(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    return s;
}

std::span<const char> as_span(const std::string& s) {
    return {s.data(), s.size()};
}

template <typename Fn>
std::string encode(Fn&& fn) {
    bytes_ostream out;
    thrift::encoder e(out);
    fn(e);
    return to_string(out);
}

// Serves the input in fragments of a fixed size, to exercise reads that
// span fragment boundaries.
class chunked_source final : public thrift::byte_source {
    std::string_view _data;
    size_t _chunk;
public:
    chunked_source(std::string_view data, size_t chunk) : _data(data), _chunk(chunk) {}
    std::span<const char> next() override {
        auto n = std::min(_chunk, _data.size());
        std::span<const char> r(_data.data(), n);
        _data.remove_prefix(n);
        return r;
    }
};

} // anonymous namespace

// Thrift compact protocol

SEASTAR_THREAD_TEST_CASE(test_thrift_encode_integers) {
    auto i32 = [] (int32_t v) { return encode([v] (thrift::encoder& e) { e.write_i32(v); }); };
    BOOST_REQUIRE_EQUAL(i32(0), bytes_of({0x00}));
    BOOST_REQUIRE_EQUAL(i32(-1), bytes_of({0x01}));
    BOOST_REQUIRE_EQUAL(i32(1), bytes_of({0x02}));
    BOOST_REQUIRE_EQUAL(i32(63), bytes_of({0x7e}));
    BOOST_REQUIRE_EQUAL(i32(-64), bytes_of({0x7f}));
    BOOST_REQUIRE_EQUAL(i32(64), bytes_of({0x80, 0x01}));
    BOOST_REQUIRE_EQUAL(i32(std::numeric_limits<int32_t>::max()), bytes_of({0xfe, 0xff, 0xff, 0xff, 0x0f}));
    BOOST_REQUIRE_EQUAL(i32(std::numeric_limits<int32_t>::min()), bytes_of({0xff, 0xff, 0xff, 0xff, 0x0f}));

    auto i64 = [] (int64_t v) { return encode([v] (thrift::encoder& e) { e.write_i64(v); }); };
    BOOST_REQUIRE_EQUAL(i64(std::numeric_limits<int64_t>::max()), bytes_of({0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}));
    BOOST_REQUIRE_EQUAL(i64(std::numeric_limits<int64_t>::min()), bytes_of({0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}));
    BOOST_REQUIRE_EQUAL(i64(-3), bytes_of({0x05}));

    BOOST_REQUIRE_EQUAL(encode([] (thrift::encoder& e) { e.write_i16(-300); }), bytes_of({0xd7, 0x04}));
    BOOST_REQUIRE_EQUAL(encode([] (thrift::encoder& e) { e.write_i8(-1); }), bytes_of({0xff}));
}

// struct { 1: i32 5, 17: i32 0, 16: bool true }
static const std::string struct_with_long_field_ids = bytes_of({
    0x15, 0x0a,       // short form: delta 1, i32; value 5
    0x05, 0x22, 0x00, // long form: i32, id 17; value 0
    0x01, 0x20,       // long form, because the id decreases: true, id 16
    0x00,
});

// struct { 2: list<i32> [1, -1, 2], 3: binary "ab", 4: list<bool> [true, false], 5: list<i8> of 15 zeros }
static const std::string struct_with_containers = [] {
    auto s = bytes_of({
        0x29, 0x35, 0x02, 0x01, 0x04,
        0x18, 0x02, 'a', 'b',
        0x19, 0x21, 0x01, 0x02,
        0x19, 0xf3, 0x0f,
    });
    s.append(15, '\0');
    s.push_back(0x00);
    return s;
}();

SEASTAR_THREAD_TEST_CASE(test_thrift_encode_structs) {
    BOOST_REQUIRE_EQUAL(encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.write_i32_field(1, 5);
        e.write_i32_field(17, 0);
        e.write_bool_field(16, true);
        e.end_struct();
    }), struct_with_long_field_ids);

    BOOST_REQUIRE_EQUAL(encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.write_list_field(2, thrift::type::i32, 3);
        e.write_i32(1);
        e.write_i32(-1);
        e.write_i32(2);
        e.write_binary_field(3, "ab");
        e.write_list_field(4, thrift::type::bool_true, 2);
        e.write_bool(true);
        e.write_bool(false);
        e.write_list_field(5, thrift::type::i8, 15);
        for (int i = 0; i < 15; ++i) {
            e.write_i8(0);
        }
        e.end_struct();
    }), struct_with_containers);

    // A nested struct restarts the field id sequence, and the outer one resumes it.
    BOOST_REQUIRE_EQUAL(encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.begin_struct_field(3);
        e.write_i32_field(1, 1);
        e.end_struct();
        e.write_i32_field(4, 1);
        e.end_struct();
    }), bytes_of({0x3c, 0x15, 0x02, 0x00, 0x15, 0x02, 0x00}));
}

static void decode_struct_with_long_field_ids(thrift::decoder& d) {
    std::vector<std::pair<int16_t, int64_t>> fields;
    d.read_struct([&] (const thrift::field_header& f) {
        if (f.t == thrift::type::i32) {
            fields.emplace_back(f.id, d.read_i32(f));
        } else {
            fields.emplace_back(f.id, d.read_bool(f));
        }
    });
    BOOST_REQUIRE_EQUAL(d.remaining(), 0);
    BOOST_REQUIRE((fields == std::vector<std::pair<int16_t, int64_t>>{{1, 5}, {17, 0}, {16, 1}}));
}

static void decode_struct_with_containers(thrift::decoder& d) {
    std::vector<int32_t> ints;
    sstring str;
    std::vector<bool> bools;
    uint32_t zeros = 0;
    d.read_struct([&] (const thrift::field_header& f) {
        switch (f.id) {
        case 2: {
            auto h = d.read_list_header(f, thrift::type::i32);
            for (uint32_t i = 0; i < h.size; ++i) {
                ints.push_back(d.read_i32());
            }
            break;
        }
        case 3: str = d.read_string(f); break;
        case 4: {
            auto h = d.read_list_header(f, thrift::type::bool_true);
            for (uint32_t i = 0; i < h.size; ++i) {
                bools.push_back(d.read_bool());
            }
            break;
        }
        case 5: {
            auto h = d.read_list_header(f, thrift::type::i8);
            for (uint32_t i = 0; i < h.size; ++i) {
                BOOST_REQUIRE_EQUAL(d.read_i8(), 0);
                ++zeros;
            }
            break;
        }
        default: BOOST_FAIL("unexpected field");
        }
    });
    BOOST_REQUIRE_EQUAL(d.remaining(), 0);
    BOOST_REQUIRE((ints == std::vector<int32_t>{1, -1, 2}));
    BOOST_REQUIRE_EQUAL(str, "ab");
    BOOST_REQUIRE((bools == std::vector<bool>{true, false}));
    BOOST_REQUIRE_EQUAL(zeros, 15);
}

SEASTAR_THREAD_TEST_CASE(test_thrift_decode) {
    {
        thrift::decoder d(as_span(struct_with_long_field_ids));
        decode_struct_with_long_field_ids(d);
    }
    {
        thrift::decoder d(as_span(struct_with_containers));
        decode_struct_with_containers(d);
    }
    // The same input, served in fragments of every size.
    for (size_t chunk = 1; chunk <= struct_with_containers.size(); ++chunk) {
        chunked_source src(struct_with_containers, chunk);
        thrift::decoder d(src, struct_with_containers.size());
        decode_struct_with_containers(d);
    }
    // The decoder stops at the declared length, even if the source has more.
    {
        auto input = struct_with_long_field_ids + "garbage";
        chunked_source src(input, 3);
        thrift::decoder d(src, struct_with_long_field_ids.size());
        decode_struct_with_long_field_ids(d);
    }
}

SEASTAR_THREAD_TEST_CASE(test_thrift_skip_unknown_fields) {
    const auto input = bytes_of({
        0x17, 1, 2, 3, 4, 5, 6, 7, 8,                                // 1: double
        0x1d, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,  // 2: uuid
        0x1b, 0x01, 0x58, 0x02, 0x01, 'x',                           // 3: map<i32, binary> {1: "x"}
        0x1a, 0x16, 0x0a,                                            // 4: set<i64> {5}
        0x1c, 0x19, 0x1c, 0x00, 0x12, 0x00,                          // 5: struct {1: list<struct> [{}], 2: false}
        0x1b, 0x00,                                                  // 6: empty map
        0x13, 0x7f,                                                  // 7: i8
        0x14, 0xd7, 0x04,                                            // 8: i16
        0x11,                                                        // 9: true
        0x19, 0x1b, 0x01, 0x11, 0x01, 0x02,                          // 10: list<map<bool, bool>> [{true: false}]
        0x19, 0x23, 0x01, 0x02,                                      // 11: list<i8> [1, 2]
        0x19, 0x17, 1, 2, 3, 4, 5, 6, 7, 8,                          // 12: list<double> [x]
        0x19, 0x1d, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,  // 13: list<uuid> [x]
        0x19, 0x31, 0x01, 0x02, 0x01,                                // 14: list<bool> [true, false, true]
        0x05, 0xc8, 0x01, 0x0e,                                      // 100: i32 7
        0x00,
    });
    std::optional<int32_t> known;
    thrift::decoder d(as_span(input));
    d.read_struct([&] (const thrift::field_header& f) {
        if (f.id == 100) {
            known = d.read_i32(f);
        } else {
            d.skip(f);
        }
    });
    BOOST_REQUIRE_EQUAL(d.remaining(), 0);
    BOOST_REQUIRE(known == 7);
}

static void require_malformed(const std::string& input) {
    thrift::decoder d(as_span(input));
    BOOST_REQUIRE_THROW(d.read_struct([&] (const thrift::field_header& f) { d.skip(f); }), malformed_sstable_exception);
}

SEASTAR_THREAD_TEST_CASE(test_thrift_malformed) {
    // Every proper prefix of a valid struct is truncated.
    for (size_t n = 0; n < struct_with_containers.size(); ++n) {
        require_malformed(struct_with_containers.substr(0, n));
    }
    // A varint longer than 64 bits.
    require_malformed(bytes_of({0x16, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00, 0x00}));
    require_malformed(bytes_of({0x16, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02, 0x00}));
    // An i32 that does not fit in 32 bits.
    {
        const auto input = bytes_of({0x15, 0x80, 0x80, 0x80, 0x80, 0x10, 0x00});
        thrift::decoder d(as_span(input));
        BOOST_REQUIRE_THROW(d.read_struct([&] (const thrift::field_header& f) { d.read_i32(f); }), malformed_sstable_exception);
    }
    // Invalid field types.
    require_malformed(bytes_of({0x1e, 0x00}));
    require_malformed(bytes_of({0x1f, 0x00}));
    // A stop field with a delta.
    require_malformed(bytes_of({0x10, 0x00}));
    // An invalid list element type.
    require_malformed(bytes_of({0x19, 0x10, 0x00}));
    // A list of doubles longer than the input, though not longer in elements than in bytes.
    require_malformed(bytes_of({0x19, 0x27, 1, 2, 3, 4, 5, 6, 7, 8, 0x00}));
    // A list, a map, and a binary value that claim more than the input holds.
    // The decoder must reject them before it allocates or loops.
    require_malformed(bytes_of({0x19, 0xf3, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00}));
    require_malformed(bytes_of({0x1b, 0xff, 0xff, 0xff, 0xff, 0x07, 0x33, 0x00}));
    require_malformed(bytes_of({0x18, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x00}));
    {
        const auto input = bytes_of({0x18, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x00});
        thrift::decoder d(as_span(input));
        BOOST_REQUIRE_THROW(d.read_struct([&] (const thrift::field_header& f) { d.read_string(f); }), malformed_sstable_exception);
    }
    // A string above the limit, which the input holds, is rejected. A
    // string at the limit is read.
    for (const uint32_t n : {thrift::decoder::max_string_size, thrift::decoder::max_string_size + 1}) {
        bytes_ostream out;
        thrift::encoder e(out);
        e.begin_struct();
        e.write_binary_field(1, std::string(n, 'x'));
        e.end_struct();
        const auto input = to_string(out);
        thrift::decoder d(as_span(input));
        auto read = [&] { d.read_struct([&] (const thrift::field_header& f) { d.read_string(f); }); };
        if (n > thrift::decoder::max_string_size) {
            BOOST_REQUIRE_THROW(read(), malformed_sstable_exception);
        } else {
            read();
        }
    }
    // A field of a known id with the wrong type.
    {
        thrift::decoder d(as_span(struct_with_long_field_ids));
        BOOST_REQUIRE_THROW(d.read_struct([&] (const thrift::field_header& f) { d.read_i64(f); }), malformed_sstable_exception);
    }
    // Nesting deeper than the limit.
    {
        std::string input(thrift::decoder::max_depth + 1, 0x1c);
        input.append(thrift::decoder::max_depth + 2, 0x00);
        require_malformed(input);
        // One level less is accepted.
        std::string ok(thrift::decoder::max_depth - 1, 0x1c);
        ok.append(thrift::decoder::max_depth, 0x00);
        thrift::decoder d(as_span(ok));
        d.read_struct([&] (const thrift::field_header& f) { d.skip(f); });
        BOOST_REQUIRE_EQUAL(d.remaining(), 0);
    }
    // A source that ends before the declared length.
    {
        chunked_source src(struct_with_containers, 4);
        thrift::decoder d(src, struct_with_containers.size() + 1);
        d.read_struct([&] (const thrift::field_header& f) { d.skip(f); });
        BOOST_REQUIRE_EQUAL(d.remaining(), 1);
        BOOST_REQUIRE_THROW(d.read_byte(), malformed_sstable_exception);
    }
}

// Metadata structures

static pq::page_header make_data_page_header() {
    return pq::page_header{
        .type = pq::page_type::data_page_v2,
        .uncompressed_page_size = 1000,
        .compressed_page_size = 400,
        .crc = -123456789,
        .data_page_v2 = pq::data_page_header_v2{
            .num_values = 100,
            .num_nulls = 10,
            .num_rows = 90,
            .enc = pq::encoding::delta_binary_packed,
            .definition_levels_byte_length = 12,
            .repetition_levels_byte_length = 3,
            .is_compressed = false,
        },
    };
}

static pq::page_header make_dictionary_page_header() {
    return pq::page_header{
        .type = pq::page_type::dictionary_page,
        .uncompressed_page_size = 64,
        .compressed_page_size = 64,
        .dictionary_page = pq::dictionary_page_header{
            .num_values = 8,
            .enc = pq::encoding::plain,
            .is_sorted = false,
        },
    };
}

SEASTAR_THREAD_TEST_CASE(test_page_header_round_trip) {
    for (const auto& h : {make_data_page_header(), make_dictionary_page_header()}) {
        bytes_ostream out;
        pq::write_page_header(h, out);
        // The page follows the header.
        auto input = to_string(out) + "page";
        thrift::decoder d(as_span(input));
        BOOST_REQUIRE(pq::parse_page_header(d) == h);
        BOOST_REQUIRE_EQUAL(d.remaining(), 4);
    }
}

SEASTAR_THREAD_TEST_CASE(test_page_header_unknown_fields) {
    // A V1 data page header and a future field are skipped.
    auto input = encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.write_i32_field(1, int32_t(pq::page_type::data_page));
        e.write_i32_field(2, 10);
        e.write_i32_field(3, 10);
        e.begin_struct_field(5);
        e.write_i32_field(1, 3);
        e.write_i32_field(2, 0);
        e.write_i32_field(3, 3);
        e.write_i32_field(4, 3);
        e.end_struct();
        e.write_binary_field(20, "future");
        e.end_struct();
    });
    thrift::decoder d(as_span(input));
    auto h = pq::parse_page_header(d);
    BOOST_REQUIRE_EQUAL(d.remaining(), 0);
    BOOST_REQUIRE(h.type == pq::page_type::data_page);
    BOOST_REQUIRE_EQUAL(h.compressed_page_size, 10);
    BOOST_REQUIRE(!h.data_page_v2 && !h.dictionary_page && !h.crc);

    // A missing required field.
    auto missing = encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.write_i32_field(1, int32_t(pq::page_type::data_page_v2));
        e.write_i32_field(3, 10);
        e.end_struct();
    });
    thrift::decoder d2(as_span(missing));
    BOOST_REQUIRE_THROW(pq::parse_page_header(d2), malformed_sstable_exception);

    // A duplicate field.
    auto duplicate = encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.write_i32_field(1, int32_t(pq::page_type::data_page_v2));
        e.write_i32_field(2, 10);
        e.write_i32_field(3, 10);
        e.write_i32_field(2, 10);
        e.end_struct();
    });
    thrift::decoder d3(as_span(duplicate));
    BOOST_REQUIRE_THROW(pq::parse_page_header(d3), malformed_sstable_exception);
}

SEASTAR_THREAD_TEST_CASE(test_offset_index_round_trip) {
    pq::offset_index idx;
    for (int i = 0; i < 1000; ++i) {
        idx.page_locations.push_back({.offset = 4 + 1000 * i, .compressed_page_size = 1000 - i, .first_row_index = 17 * i});
    }
    bytes_ostream out;
    pq::write_offset_index(idx, out);
    auto input = to_string(out);
    {
        thrift::decoder d(as_span(input));
        BOOST_REQUIRE(pq::parse_offset_index(d) == idx);
        BOOST_REQUIRE_EQUAL(d.remaining(), 0);
    }
    for (size_t n = 0; n < input.size(); n += 7) {
        auto truncated = input.substr(0, n);
        thrift::decoder d(as_span(truncated));
        BOOST_REQUIRE_THROW(pq::parse_offset_index(d), malformed_sstable_exception);
    }
}

namespace {

// Builds a footer with a nested schema and `n_row_groups` row groups.
pq::file_metadata make_file_metadata(int n_row_groups) {
    pq::file_metadata md;
    using pq::schema_element;
    md.schema.push_back(schema_element{.name = "schema", .num_children = 3});
    md.schema.push_back(schema_element{.type = pq::physical_type::int64, .repetition = pq::repetition_type::required, .name = "pk"});
    md.schema.push_back(schema_element{.repetition = pq::repetition_type::required, .name = "_scylla", .num_children = 2});
    md.schema.push_back(schema_element{.type = pq::physical_type::int32, .repetition = pq::repetition_type::required, .name = "kind",
            .logical = pq::logical_type{.k = pq::logical_type::kind::integer, .bit_width = 8, .is_signed = true}});
    md.schema.push_back(schema_element{.repetition = pq::repetition_type::optional, .name = "values", .num_children = 1,
            .converted = pq::converted_type::list, .logical = pq::logical_type{.k = pq::logical_type::kind::list}});
    md.schema.push_back(schema_element{.repetition = pq::repetition_type::repeated, .name = "list", .num_children = 1});
    md.schema.push_back(schema_element{.type = pq::physical_type::byte_array, .repetition = pq::repetition_type::optional, .name = "element",
            .logical = pq::logical_type{.k = pq::logical_type::kind::string}});
    md.schema.push_back(schema_element{.type = pq::physical_type::fixed_len_byte_array, .type_length = 16,
            .repetition = pq::repetition_type::optional, .name = "id", .logical = pq::logical_type{.k = pq::logical_type::kind::uuid}});
    int64_t offset = 4;
    for (int rg = 0; rg < n_row_groups; ++rg) {
        pq::row_group g{.total_byte_size = 1000 + rg, .num_rows = 100 + rg};
        if (rg % 2) {
            g.file_offset = offset;
            g.total_compressed_size = 500;
            g.ordinal = rg;
        }
        for (int c = 0; c < 4; ++c) {
            pq::column_chunk chunk{
                .meta_data = pq::column_metadata{
                    .type = pq::physical_type::int32,
                    .encodings = {pq::encoding::rle, pq::encoding::plain},
                    .codec = pq::compression_codec::zstd,
                    .num_values = 100 + rg,
                    .total_uncompressed_size = 300,
                    .total_compressed_size = 125,
                    .data_page_offset = offset + 20,
                    .stats = pq::statistics{.null_count = c},
                },
                .offset_index_offset = offset + 1000,
                .offset_index_length = 30,
            };
            if (c == 1) {
                chunk.meta_data.dictionary_page_offset = offset;
                chunk.meta_data.encodings.push_back(pq::encoding::rle_dictionary);
            }
            if (c == 2) {
                chunk.meta_data.stats.reset();
                chunk.offset_index_offset.reset();
                chunk.offset_index_length.reset();
            }
            g.columns.push_back(std::move(chunk));
            offset += 125;
        }
        md.row_groups.push_back(std::move(g));
    }
    md.num_rows = 12345;
    md.key_value_metadata.push_back({.key = "scylla.layout", .value = "1"});
    md.key_value_metadata.push_back({.key = "no value"});
    md.created_by = "scylla test";
    return md;
}

const std::vector<std::vector<sstring>> expected_paths = {
    {"pk"}, {"_scylla", "kind"}, {"_scylla", "values", "list", "element"}, {"id"},
};

// Collects the whole footer, and checks the paths of the column chunks.
class checking_builder : public pq::file_metadata_builder {
public:
    size_t chunks = 0;
    void on_column_chunk(size_t rg, size_t col, std::span<const sstring> path, pq::column_chunk&& c) override {
        BOOST_REQUIRE_LT(col, expected_paths.size());
        BOOST_REQUIRE(std::ranges::equal(path, expected_paths[col]));
        ++chunks;
        pq::file_metadata_builder::on_column_chunk(rg, col, path, std::move(c));
    }
};

pq::file_metadata parse_footer(const std::string& input, size_t chunk) {
    chunked_source src(input, chunk);
    pq::file_metadata_builder b;
    pq::parse_file_metadata(src, input.size(), b);
    return std::move(b).get();
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_file_metadata_round_trip) {
    const auto md = make_file_metadata(50);
    const auto serialized = pq::serialize_file_metadata(md);
    const auto input = to_string(serialized);

    // From fragments of several sizes.
    for (size_t chunk : {size_t(1), size_t(7), size_t(4096), input.size()}) {
        chunked_source src(input, chunk);
        checking_builder b;
        pq::parse_file_metadata(src, input.size(), b);
        BOOST_REQUIRE_EQUAL(b.chunks, 50 * 4);
        BOOST_REQUIRE(std::move(b).get() == md);
    }

    // From the fragments of the serialized buffer.
    {
        thrift::fragment_range_source src(serialized.fragments());
        pq::file_metadata_builder b;
        pq::parse_file_metadata(src, serialized.size(), b);
        BOOST_REQUIRE(std::move(b).get() == md);
    }

    // From an input stream.
    {
        std::vector<temporary_buffer<char>> bufs;
        for (size_t pos = 0; pos < input.size(); pos += 100) {
            auto n = std::min<size_t>(100, input.size() - pos);
            bufs.emplace_back(input.data() + pos, n);
        }
        auto in = seastar::util::as_input_stream(std::move(bufs));
        pq::file_metadata_builder b;
        pq::parse_file_metadata(in, input.size(), b).get();
        in.close().get();
        BOOST_REQUIRE(std::move(b).get() == md);
    }

    // Through a file_writer.
    {
        memory_data_sink_buffers bufs;
        sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
        auto written = pq::write_file_metadata(md, fw);
        fw.close();
        BOOST_REQUIRE_EQUAL(written.size, input.size());
        BOOST_REQUIRE_EQUAL(written.crc32, ::crc32(0, reinterpret_cast<const Bytef*>(input.data()), input.size()));
        std::string out;
        for (const auto& b : bufs.buffers()) {
            out.append(b.get(), b.size());
        }
        BOOST_REQUIRE_EQUAL(out, input);
    }

    // Through a file_writer, with the row groups from a source.
    {
        auto header = md;
        header.row_groups.clear();
        size_t next = 0;
        memory_data_sink_buffers bufs;
        sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
        auto written = pq::write_file_metadata(header, md.row_groups.size(), [&] (size_t i) {
            BOOST_REQUIRE_EQUAL(i, next++);
            return md.row_groups[i];
        }, fw);
        fw.close();
        BOOST_REQUIRE_EQUAL(next, md.row_groups.size());
        BOOST_REQUIRE_EQUAL(written.size, input.size());
        BOOST_REQUIRE_EQUAL(written.crc32, ::crc32(0, reinterpret_cast<const Bytef*>(input.data()), input.size()));
        std::string out;
        for (const auto& b : bufs.buffers()) {
            out.append(b.get(), b.size());
        }
        BOOST_REQUIRE_EQUAL(out, input);
    }
}

// A key-value value above max_key_value_size is skipped without an
// allocation: its key is kept without a value. A key above the limit is
// malformed.
SEASTAR_THREAD_TEST_CASE(test_key_value_size_limit) {
    auto md = make_file_metadata(2);
    const sstring at_limit(pq::max_key_value_size, 'a');
    const sstring above_limit(pq::max_key_value_size + 1, 'b');
    md.key_value_metadata.push_back({.key = "scylla.zstd_dictionary.0", .value = at_limit});
    md.key_value_metadata.push_back({.key = "large", .value = above_limit});
    md.key_value_metadata.push_back({.key = "after", .value = "x"});
    auto parsed = parse_footer(to_string(pq::serialize_file_metadata(md)), 4096);
    const auto& kv = parsed.key_value_metadata;
    BOOST_REQUIRE_EQUAL(kv.size(), md.key_value_metadata.size());
    BOOST_REQUIRE(kv[kv.size() - 3] == md.key_value_metadata[kv.size() - 3]);
    BOOST_REQUIRE_EQUAL(kv[kv.size() - 2].key, "large");
    BOOST_REQUIRE(!kv[kv.size() - 2].value);
    BOOST_REQUIRE(kv.back() == md.key_value_metadata.back());

    md.key_value_metadata.push_back({.key = above_limit, .value = "x"});
    BOOST_REQUIRE_THROW(parse_footer(to_string(pq::serialize_file_metadata(md)), 4096), sstables::malformed_sstable_exception);
}

SEASTAR_THREAD_TEST_CASE(test_file_metadata_large) {
    // Large enough that write_file_metadata() flushes several times.
    const auto md = make_file_metadata(5000);
    const auto input = to_string(pq::serialize_file_metadata(md));
    BOOST_REQUIRE_GT(input.size(), 3 * 64 * 1024);

    memory_data_sink_buffers bufs;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
    auto written = pq::write_file_metadata(md, fw);
    fw.close();
    BOOST_REQUIRE_EQUAL(written.size, input.size());
    BOOST_REQUIRE_EQUAL(written.crc32, ::crc32(0, reinterpret_cast<const Bytef*>(input.data()), input.size()));
    std::string out;
    for (const auto& b : bufs.buffers()) {
        out.append(b.get(), b.size());
    }
    BOOST_REQUIRE_EQUAL(out, input);
    BOOST_REQUIRE(parse_footer(input, 128 * 1024) == md);
}

SEASTAR_THREAD_TEST_CASE(test_file_metadata_malformed) {
    const auto input = to_string(pq::serialize_file_metadata(make_file_metadata(3)));

    // Every truncation.
    for (size_t n = 0; n < input.size(); ++n) {
        BOOST_REQUIRE_THROW(parse_footer(input.substr(0, n), 5), malformed_sstable_exception);
    }
    // Trailing bytes.
    BOOST_REQUIRE_THROW(parse_footer(input + '\0', 5), malformed_sstable_exception);

    // Every single bit flip either parses or raises the malformed sstable
    // error. Nothing else may escape, and the sanitizers of the debug mode
    // check that nothing reads out of bounds.
    size_t parsed = 0;
    for (size_t bit = 0; bit < input.size() * 8; ++bit) {
        auto damaged = input;
        damaged[bit / 8] ^= char(1 << (bit % 8));
        try {
            parse_footer(damaged, 13);
            ++parsed;
        } catch (const malformed_sstable_exception&) {
        }
    }
    // Flips in values parse, flips in the structure do not.
    BOOST_REQUIRE_GT(parsed, 0);
    BOOST_REQUIRE_LT(parsed, input.size() * 8);

    // A required field is missing.
    auto no_num_rows = encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.write_i32_field(1, 2);
        e.write_list_field(2, thrift::type::struct_, 0);
        e.write_list_field(4, thrift::type::struct_, 0);
        e.end_struct();
    });
    BOOST_REQUIRE_THROW(parse_footer(no_num_rows, 5), malformed_sstable_exception);
    // The schema list holds the wrong element type.
    auto bad_list = encode([] (thrift::encoder& e) {
        e.begin_struct();
        e.write_i32_field(1, 2);
        e.write_list_field(2, thrift::type::i32, 1);
        e.write_i32(1);
        e.write_i64_field(3, 0);
        e.write_list_field(4, thrift::type::struct_, 0);
        e.end_struct();
    });
    BOOST_REQUIRE_THROW(parse_footer(bad_list, 5), malformed_sstable_exception);
}

// Arrow reads our metadata

namespace {

// A memory handle whose target the test sets.
struct test_memory_handle final : pq::writer_memory_handle {
    uint64_t& target;
    explicit test_memory_handle(uint64_t& t) : target(t) {}
    uint64_t row_group_target() override { return target; }
};

struct arrow_column {
    std::vector<int16_t> def_levels;
    std::vector<int32_t> values;
};

arrow_column read_with_arrow(::parquet::ParquetFileReader& reader, int rg, int col, int64_t rows) {
    auto row_group = reader.RowGroup(rg);
    auto column = std::static_pointer_cast<::parquet::Int32Reader>(row_group->Column(col));
    arrow_column r;
    r.def_levels.resize(rows);
    r.values.resize(rows);
    int64_t values_read = 0;
    auto levels_read = column->ReadBatch(rows, r.def_levels.data(), nullptr, r.values.data(), &values_read);
    BOOST_REQUIRE_EQUAL(levels_read, rows);
    BOOST_REQUIRE(!column->HasNext());
    r.values.resize(values_read);
    return r;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_arrow_reads_our_metadata) {
    // Two row groups of columns a and b. Column b is null in the whole
    // first row group, so the writer places its chunk for that row group at
    // the end of the file. The row group is then not contiguous.
    // test/boost/parquet_writer_test.cc tests the writer itself.
    pq::schema s;
    const auto a = s.add_column(pq::schema::root, "a", pq::repetition_type::optional, pq::physical_type::int32);
    const auto b = s.add_column(pq::schema::root, "b", pq::repetition_type::optional, pq::physical_type::int32);
    // Page sets of 3 rows. The first ends the first row group, and later
    // ones do not end a row group or a page.
    uint64_t target = 0;
    const pq::writer_config cfg{
        .min_page_size = 1 << 20,
        .max_page_set_rows = 3,
        .created_by = "scylla-parquet-codec-test version 0.0.1",
    };
    memory_data_sink_buffers bufs;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
    pq::writer w(fw, std::move(s), cfg, std::make_unique<test_memory_handle>(target));
    const std::vector<std::optional<int32_t>> a_values = {1, std::nullopt, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    const std::vector<std::optional<int32_t>> b_values = {std::nullopt, std::nullopt, std::nullopt,
            std::nullopt, -1, std::nullopt, -2, -3, -4, -5, -6, -7};
    for (size_t row = 0; row < a_values.size(); ++row) {
        if (a_values[row]) {
            w.append(a, *a_values[row]);
        }
        if (b_values[row]) {
            w.append(b, *b_values[row]);
        }
        const bool cut = w.end_row();
        BOOST_REQUIRE_EQUAL(cut, row % 3 == 2);
        if (cut) {
            target = 1 << 20;
        }
    }
    auto result = w.finish({{.key = "scylla.layout", .value = "1"}});
    fw.close();
    std::string file;
    for (const auto& buf : bufs.buffers()) {
        file.append(buf.get(), buf.size());
    }
    BOOST_REQUIRE_EQUAL(result.file_size, file.size());
    BOOST_REQUIRE_EQUAL(result.row_groups, 2);

    ::parquet::ReaderProperties props;
    props.set_page_checksum_verification(true);
    auto reader = ::parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::FromString(file)), props);
    auto meta = reader->metadata();
    BOOST_REQUIRE_EQUAL(meta->num_rows(), 12);
    BOOST_REQUIRE_EQUAL(meta->num_row_groups(), 2);
    BOOST_REQUIRE_EQUAL(meta->num_columns(), 2);
    BOOST_REQUIRE_EQUAL(meta->schema()->Column(0)->name(), "a");
    BOOST_REQUIRE_EQUAL(meta->schema()->Column(1)->name(), "b");
    BOOST_REQUIRE_EQUAL(meta->schema()->Column(1)->max_definition_level(), 1);
    BOOST_REQUIRE_EQUAL(meta->created_by(), "scylla-parquet-codec-test version 0.0.1");
    BOOST_REQUIRE_EQUAL(meta->key_value_metadata()->Get("scylla.layout").ValueOrDie(), "1");

    // The chunk of b in the first row group lies after the second row group.
    const auto b0 = meta->RowGroup(0)->ColumnChunk(1);
    const auto a1 = meta->RowGroup(1)->ColumnChunk(0);
    const auto b1 = meta->RowGroup(1)->ColumnChunk(1);
    BOOST_REQUIRE_GT(b0->data_page_offset(), b1->data_page_offset() + b1->total_compressed_size());
    BOOST_REQUIRE_GT(b0->data_page_offset(), a1->data_page_offset() + a1->total_compressed_size());

    const std::vector<std::vector<int64_t>> null_counts = {{1, 3}, {0, 2}};
    for (int rg = 0; rg < 2; ++rg) {
        for (int col = 0; col < 2; ++col) {
            auto chunk = meta->RowGroup(rg)->ColumnChunk(col);
            BOOST_REQUIRE(chunk->is_stats_set());
            BOOST_REQUIRE(chunk->statistics()->HasNullCount());
            BOOST_REQUIRE_EQUAL(chunk->statistics()->null_count(), null_counts[rg][col]);
        }
    }

    auto ac = read_with_arrow(*reader, 0, 0, 3);
    BOOST_REQUIRE((ac.def_levels == std::vector<int16_t>{1, 0, 1}));
    BOOST_REQUIRE((ac.values == std::vector<int32_t>{1, 3}));
    auto bc = read_with_arrow(*reader, 0, 1, 3);
    BOOST_REQUIRE((bc.def_levels == std::vector<int16_t>{0, 0, 0}));
    BOOST_REQUIRE(bc.values.empty());
    ac = read_with_arrow(*reader, 1, 0, 9);
    BOOST_REQUIRE((ac.values == std::vector<int32_t>{4, 5, 6, 7, 8, 9, 10, 11, 12}));
    bc = read_with_arrow(*reader, 1, 1, 9);
    BOOST_REQUIRE((bc.def_levels == std::vector<int16_t>{0, 1, 0, 1, 1, 1, 1, 1, 1}));
    BOOST_REQUIRE((bc.values == std::vector<int32_t>{-1, -2, -3, -4, -5, -6, -7}));

    // Each in-place chunk has one page, which its OffsetIndex locates. The
    // late chunk has no OffsetIndex.
    auto page_index = reader->GetPageIndexReader();
    BOOST_REQUIRE(page_index);
    BOOST_REQUIRE(!page_index->RowGroup(0)->GetOffsetIndex(1));
    for (auto [rg, col] : {std::pair(0, 0), std::pair(1, 0), std::pair(1, 1)}) {
        auto oi = page_index->RowGroup(rg)->GetOffsetIndex(col);
        BOOST_REQUIRE(oi);
        auto chunk = meta->RowGroup(rg)->ColumnChunk(col);
        const auto& locations = oi->page_locations();
        BOOST_REQUIRE_EQUAL(locations.size(), 1);
        BOOST_REQUIRE_EQUAL(locations[0].offset, chunk->data_page_offset());
        BOOST_REQUIRE_EQUAL(locations[0].compressed_page_size, chunk->total_compressed_size());
        BOOST_REQUIRE_EQUAL(locations[0].first_row_index, 0);
    }

    // A page with a wrong checksum is rejected, which shows that Arrow checks it.
    auto a0 = meta->RowGroup(0)->ColumnChunk(0);
    auto damaged = file;
    damaged[a0->data_page_offset() + a0->total_compressed_size() - 1] ^= 1;
    auto damaged_reader = ::parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::FromString(damaged)), props);
    BOOST_REQUIRE_THROW(read_with_arrow(*damaged_reader, 0, 0, 3), ::parquet::ParquetException);

    // Our parser reads the footer back, locating it from the end of the file as a reader does.
    uint32_t footer_length = 0;
    for (int i = 0; i < 4; ++i) {
        footer_length |= uint32_t(uint8_t(file[file.size() - 8 + i])) << (8 * i);
    }
    BOOST_REQUIRE_EQUAL(footer_length, result.footer.size);
    const auto footer = file.substr(file.size() - 8 - footer_length, footer_length);
    BOOST_REQUIRE_EQUAL(result.footer.crc32, ::crc32(0, reinterpret_cast<const Bytef*>(footer.data()), footer.size()));
    const auto md = parse_footer(footer, 64);
    BOOST_REQUIRE_EQUAL(md.num_rows, 12);
    BOOST_REQUIRE_EQUAL(md.row_groups.size(), 2);
    BOOST_REQUIRE_EQUAL(md.row_groups[0].num_rows, 3);
    BOOST_REQUIRE_EQUAL(md.row_groups[1].num_rows, 9);
    BOOST_REQUIRE_EQUAL(md.schema.size(), 3);
}
