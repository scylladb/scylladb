/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the Parquet file writer in sstables/parquet/writer.hh.
//
// The oracle is Arrow C++: it must read back the levels and values that the
// test appended, through its low-level reader on an in-memory buffer, which
// starts no threads. The tests also check the layout invariants that our
// reader will rely on, by walking the file with our own metadata parser,
// and they check the OffsetIndexes through Arrow's page index reader.
//
// Random inputs come from fixed seeds, so the tests are repeatable.

#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <zlib.h>

#include <seastar/core/memory.hh>
#include <seastar/testing/thread_test_case.hh>

// Arrow's headers use std::is_trivial, which C++26 deprecates.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/schema.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/page_index.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#pragma GCC diagnostic pop

#include "sstables/file_writer.hh"
#include "sstables/parquet/bit_util.hh"
#include "sstables/parquet/metadata.hh"
#include "sstables/parquet/schema.hh"
#include "sstables/parquet/thrift.hh"
#include "sstables/parquet/writer.hh"
#include "utils/managed_bytes.hh"
#include "utils/memory_data_sink.hh"

namespace pq = sstables::parquet;
namespace thrift = sstables::parquet::thrift;
using pq::column_id;
using pq::physical_type;
using pq::repetition_type;

namespace {

// An entry of a leaf column. Values are kept as their bytes in PLAIN
// order (one byte for a boolean), so that every type compares the same way,
// NaN payloads included.
struct entry {
    uint16_t rep = 0;
    uint16_t def = 0;
    std::optional<std::string> value;
    bool operator==(const entry&) const = default;
};

std::string to_string(const entry& e) {
    return fmt::format("{{rep {}, def {}, {}}}", e.rep, e.def, e.value ? fmt::format("{} bytes", e.value->size()) : "null");
}

template <typename T>
std::string bytes_of_value(T v) {
    return std::string(reinterpret_cast<const char*>(&v), sizeof(v));
}

template <typename T>
T value_of_bytes(const std::string& s) {
    T v;
    BOOST_REQUIRE_EQUAL(s.size(), sizeof(T));
    std::memcpy(&v, s.data(), sizeof(T));
    return v;
}

uint64_t raw_size_of(physical_type t, const std::string& v) {
    return t == physical_type::boolean ? 1 : v.size();
}

// The entries that a test appends, per column and row. A row without
// entries is null at level 0.
struct model {
    pq::schema schema;
    std::vector<std::vector<std::vector<entry>>> columns;
    size_t rows = 0;

    explicit model(pq::schema s) : schema(std::move(s)), columns(schema.columns()) {}

    void add_row() {
        ++rows;
        for (auto& c : columns) {
            c.resize(rows);
        }
    }
    // Adds an entry to the last row.
    void add(column_id c, entry e) {
        columns[c][rows - 1].push_back(std::move(e));
    }
    // Whether the column belongs in the file's schema: it is always
    // present, or it has a value or a null above level 0.
    bool included(column_id c) const {
        if (schema.get_column(c).always_present) {
            return true;
        }
        for (const auto& row : columns[c]) {
            for (const auto& e : row) {
                if (e.def > 0 || e.value) {
                    return true;
                }
            }
        }
        return false;
    }
    std::vector<column_id> file_columns() const {
        std::vector<column_id> r;
        for (column_id c : schema.columns_in_schema_order()) {
            if (included(c)) {
                r.push_back(c);
            }
        }
        return r;
    }
    // The entries of rows [from, to) as a reader sees them.
    std::vector<entry> flat(column_id c, size_t from, size_t to) const {
        std::vector<entry> r;
        for (size_t row = from; row < to; ++row) {
            if (columns[c][row].empty()) {
                r.push_back(entry{});
            } else {
                r.insert(r.end(), columns[c][row].begin(), columns[c][row].end());
            }
        }
        return r;
    }
    uint64_t row_raw_size(size_t row) const {
        uint64_t r = 0;
        for (column_id c = 0; c < columns.size(); ++c) {
            for (const auto& e : columns[c][row]) {
                if (e.value) {
                    r += raw_size_of(schema.get_column(c).type, *e.value);
                }
            }
        }
        return r;
    }
    size_t max_entries_per_row(column_id c) const {
        size_t r = 1;
        for (const auto& row : columns[c]) {
            r = std::max(r, row.size());
        }
        return r;
    }
};

// Appends an entry. Byte arrays rotate through the three view types that
// the writer accepts.
void append_entry(pq::writer& w, column_id c, const entry& e) {
    const pq::levels l{.rep = e.rep, .def = e.def};
    if (!e.value) {
        w.append_null(c, l);
        return;
    }
    const std::string& v = *e.value;
    switch (w.get_schema().get_column(c).type) {
    case physical_type::boolean: w.append(c, l, v[0] != 0); break;
    case physical_type::int32: w.append(c, l, value_of_bytes<int32_t>(v)); break;
    case physical_type::int64: w.append(c, l, value_of_bytes<int64_t>(v)); break;
    case physical_type::float_: w.append(c, l, value_of_bytes<float>(v)); break;
    case physical_type::double_: w.append(c, l, value_of_bytes<double>(v)); break;
    case physical_type::byte_array:
    case physical_type::fixed_len_byte_array: {
        const bytes_view bv(reinterpret_cast<const int8_t*>(v.data()), v.size());
        switch (v.size() % 3) {
        case 0: w.append(c, l, bv); break;
        case 1: {
            managed_bytes mb(bv);
            w.append(c, l, managed_bytes_view(mb));
            break;
        }
        default: {
            bytes_ostream os;
            os.write(bv);
            auto buf = pq::to_fragmented_buffer(os, 1000);
            w.append(c, l, pq::byte_array_view(buf));
        }
        }
        break;
    }
    default: BOOST_FAIL("unexpected type");
    }
}

// A file in memory, with what the writer reported.
struct written_file {
    std::string data;
    pq::writer_result result;
    pq::writer_stats stats;
    // The ordinals of the rows that start a page set, as end_row() reported.
    std::set<uint64_t> page_set_starts{0};
};

// Writes the rows of `m` with `cfg`, checking the size measures on the way.
// `extra(w, row)` may append more before each row ends.
template <typename Extra>
written_file write_model(const model& m, pq::writer_config cfg, std::vector<pq::key_value> kv, Extra&& extra,
        std::unique_ptr<pq::writer_memory_handle> handle = nullptr) {
    memory_data_sink_buffers bufs;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
    written_file f;
    {
        pq::writer w(fw, m.schema, cfg, std::move(handle));
        uint64_t raw = 0;
        for (size_t row = 0; row < m.rows; ++row) {
            for (column_id c = 0; c < m.columns.size(); ++c) {
                for (const auto& e : m.columns[c][row]) {
                    append_entry(w, c, e);
                }
            }
            extra(w, row);
            BOOST_REQUIRE_EQUAL(w.current_row_raw_size(), m.row_raw_size(row));
            raw += w.current_row_raw_size();
            BOOST_REQUIRE_LE(w.flushed_size(), w.estimated_size());
            if (w.end_row()) {
                f.page_set_starts.insert(w.rows());
            }
            BOOST_REQUIRE_EQUAL(w.rows(), row + 1);
            BOOST_REQUIRE_EQUAL(w.raw_size(), raw);
        }
        f.result = w.finish(std::move(kv));
        f.stats = w.stats();
        BOOST_REQUIRE_EQUAL(f.result.rows, m.rows);
        BOOST_REQUIRE_EQUAL(f.result.raw_size, raw);
    }
    fw.close();
    for (const auto& b : bufs.buffers()) {
        f.data.append(b.get(), b.size());
    }
    BOOST_REQUIRE_EQUAL(f.result.file_size, f.data.size());
    return f;
}

written_file write_model(const model& m, pq::writer_config cfg, std::vector<pq::key_value> kv = {}) {
    return write_model(m, std::move(cfg), std::move(kv), [] (pq::writer&, size_t) {});
}

std::unique_ptr<::parquet::ParquetFileReader> open_with_arrow(const std::string& file) {
    ::parquet::ReaderProperties props;
    props.set_page_checksum_verification(true);
    return ::parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::FromString(file)), props);
}

template <typename DType>
void read_typed(::parquet::ColumnReader& base, const ::parquet::ColumnDescriptor& d, std::vector<entry>& out) {
    auto& r = static_cast<::parquet::TypedColumnReader<DType>&>(base);
    using value_type = typename DType::c_type;
    constexpr int64_t batch = 1000;
    std::vector<int16_t> defs(batch);
    std::vector<int16_t> reps(batch);
    // Not a vector, which is special for bool.
    auto values = std::make_unique<value_type[]>(batch);
    while (r.HasNext()) {
        int64_t values_read = 0;
        const int64_t n = r.ReadBatch(batch, defs.data(), reps.data(), values.get(), &values_read);
        size_t vi = 0;
        for (int64_t i = 0; i < n; ++i) {
            entry e{
                .rep = uint16_t(d.max_repetition_level() ? reps[i] : 0),
                .def = uint16_t(d.max_definition_level() ? defs[i] : 0),
            };
            if (e.def == d.max_definition_level()) {
                const auto& v = values[vi++];
                if constexpr (std::same_as<DType, ::parquet::BooleanType>) {
                    e.value = std::string(1, char(v));
                } else if constexpr (std::same_as<DType, ::parquet::ByteArrayType>) {
                    e.value = std::string(reinterpret_cast<const char*>(v.ptr), v.len);
                } else if constexpr (std::same_as<DType, ::parquet::FLBAType>) {
                    e.value = std::string(reinterpret_cast<const char*>(v.ptr), d.type_length());
                } else {
                    e.value = bytes_of_value(v);
                }
            }
            out.push_back(std::move(e));
        }
        BOOST_REQUIRE_EQUAL(vi, size_t(values_read));
    }
}

// Reads column `col` of row group `rg` with Arrow.
std::vector<entry> read_with_arrow(::parquet::ParquetFileReader& reader, int rg, int col) {
    auto column = reader.RowGroup(rg)->Column(col);
    const auto& d = *reader.metadata()->schema()->Column(col);
    std::vector<entry> r;
    switch (d.physical_type()) {
    case ::parquet::Type::BOOLEAN: read_typed<::parquet::BooleanType>(*column, d, r); break;
    case ::parquet::Type::INT32: read_typed<::parquet::Int32Type>(*column, d, r); break;
    case ::parquet::Type::INT64: read_typed<::parquet::Int64Type>(*column, d, r); break;
    case ::parquet::Type::FLOAT: read_typed<::parquet::FloatType>(*column, d, r); break;
    case ::parquet::Type::DOUBLE: read_typed<::parquet::DoubleType>(*column, d, r); break;
    case ::parquet::Type::BYTE_ARRAY: read_typed<::parquet::ByteArrayType>(*column, d, r); break;
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: read_typed<::parquet::FLBAType>(*column, d, r); break;
    default: BOOST_FAIL("unexpected type");
    }
    return r;
}

void require_same_entries(const std::vector<entry>& got, const std::vector<entry>& expected, std::string_view what) {
    if (got == expected) {
        return;
    }
    size_t i = 0;
    while (i < got.size() && i < expected.size() && got[i] == expected[i]) {
        ++i;
    }
    BOOST_FAIL(fmt::format("{}: {} entries, expected {}; first difference at {}: {} instead of {}", what, got.size(), expected.size(), i,
            i < got.size() ? to_string(got[i]) : "none", i < expected.size() ? to_string(expected[i]) : "none"));
}

std::string dotted(const std::vector<sstring>& path) {
    return fmt::format("{}", fmt::join(path, "."));
}

// Arrow reads every column of the file, and the values match the model.
void check_with_arrow(const written_file& f, const model& m, const std::vector<pq::key_value>& kv = {}) {
    auto reader = open_with_arrow(f.data);
    auto meta = reader->metadata();
    const auto cols = m.file_columns();
    BOOST_REQUIRE_EQUAL(meta->num_rows(), int64_t(m.rows));
    BOOST_REQUIRE_EQUAL(meta->num_columns(), int(cols.size()));
    BOOST_REQUIRE_EQUAL(meta->num_row_groups(), int(f.result.row_groups));
    for (const auto& e : kv) {
        BOOST_REQUIRE_EQUAL(meta->key_value_metadata()->Get(e.key).ValueOrDie(), *e.value);
    }
    // Arrow accepts the LIST and MAP shapes.
    std::shared_ptr<::arrow::Schema> arrow_schema;
    BOOST_REQUIRE(::parquet::arrow::FromParquetSchema(meta->schema(), &arrow_schema).ok());

    for (size_t i = 0; i < cols.size(); ++i) {
        const auto& d = *meta->schema()->Column(i);
        const auto& col = m.schema.get_column(cols[i]);
        BOOST_REQUIRE_EQUAL(d.path()->ToDotString(), dotted(m.schema.path(cols[i])));
        BOOST_REQUIRE_EQUAL(d.max_definition_level(), col.max_def);
        BOOST_REQUIRE_EQUAL(d.max_repetition_level(), col.max_rep);
        size_t row = 0;
        for (int rg = 0; rg < meta->num_row_groups(); ++rg) {
            const size_t rows = meta->RowGroup(rg)->num_rows();
            const auto expected = m.flat(cols[i], row, row + rows);
            require_same_entries(read_with_arrow(*reader, rg, i), expected,
                    fmt::format("column {} row group {}", d.path()->ToDotString(), rg));
            // Null counts in the footer.
            auto chunk = meta->RowGroup(rg)->ColumnChunk(i);
            BOOST_REQUIRE(chunk->is_stats_set());
            BOOST_REQUIRE(chunk->statistics()->HasNullCount());
            const auto nulls = std::ranges::count_if(expected, [&] (const entry& e) { return e.def < col.max_def; });
            BOOST_REQUIRE_EQUAL(chunk->statistics()->null_count(), nulls);
            BOOST_REQUIRE_EQUAL(chunk->num_values(), int64_t(expected.size()));
            BOOST_REQUIRE(!chunk->statistics()->HasMinMax());
            row += rows;
        }
        BOOST_REQUIRE_EQUAL(row, m.rows);
    }
}

uint32_t le32(const std::string& s, size_t pos) {
    uint32_t v;
    std::memcpy(&v, s.data() + pos, 4);
    return v;
}

uint32_t crc32_of(std::string_view s) {
    return ::crc32(0, reinterpret_cast<const Bytef*>(s.data()), s.size());
}

pq::file_metadata parse_footer(const written_file& f) {
    const auto& file = f.data;
    BOOST_REQUIRE_GE(file.size(), 12);
    BOOST_REQUIRE_EQUAL(file.substr(0, 4), pq::magic);
    BOOST_REQUIRE_EQUAL(file.substr(file.size() - 4), pq::magic);
    const uint32_t length = le32(file, file.size() - 8);
    BOOST_REQUIRE_EQUAL(length, f.result.footer.size);
    const auto footer = std::string_view(file).substr(file.size() - 8 - length, length);
    BOOST_REQUIRE_EQUAL(crc32_of(footer), f.result.footer.crc32);
    struct source final : thrift::byte_source {
        std::string_view data;
        std::span<const char> next() override {
            auto r = std::span<const char>(data.data(), data.size());
            data = {};
            return r;
        }
    } src;
    src.data = footer;
    pq::file_metadata_builder b;
    pq::parse_file_metadata(src, footer.size(), b);
    return std::move(b).get();
}

struct page_info {
    pq::page_header header;
    uint64_t offset;
    uint64_t size;
    int64_t first_row;
};

pq::page_header parse_page_header_at(const std::string& file, uint64_t pos, uint64_t& header_size) {
    thrift::decoder d(std::span<const char>(file.data() + pos, file.size() - pos));
    auto h = pq::parse_page_header(d);
    header_size = d.position();
    return h;
}

// Walks the pages of a chunk and checks them against the chunk's footer
// entry. `rows` is the row count of the row group.
std::vector<page_info> walk_chunk(const std::string& file, const pq::column_chunk& c, int64_t rows) {
    const auto& md = c.meta_data;
    const uint64_t start = md.dictionary_page_offset.value_or(md.data_page_offset);
    const uint64_t end = start + md.total_compressed_size;
    BOOST_REQUIRE_LE(end, file.size());
    uint64_t pos = start;
    uint64_t uncompressed = 0;
    auto encodings = std::set<pq::encoding>(md.encodings.begin(), md.encodings.end());
    if (md.dictionary_page_offset) {
        uint64_t hs;
        auto h = parse_page_header_at(file, pos, hs);
        BOOST_REQUIRE(h.type == pq::page_type::dictionary_page);
        BOOST_REQUIRE(h.dictionary_page);
        BOOST_REQUIRE(h.dictionary_page->enc == pq::encoding::plain);
        BOOST_REQUIRE(h.crc);
        BOOST_REQUIRE_EQUAL(uint32_t(*h.crc), crc32_of(std::string_view(file).substr(pos + hs, h.compressed_page_size)));
        pos += hs + h.compressed_page_size;
        uncompressed += hs + h.uncompressed_page_size;
        BOOST_REQUIRE_EQUAL(pos, uint64_t(md.data_page_offset));
        BOOST_REQUIRE(encodings.contains(pq::encoding::plain));
    }
    std::vector<page_info> pages;
    int64_t row = 0;
    int64_t values = 0;
    int64_t nulls = 0;
    bool uses_dictionary = false;
    while (pos < end) {
        uint64_t hs;
        auto h = parse_page_header_at(file, pos, hs);
        BOOST_REQUIRE(h.type == pq::page_type::data_page_v2);
        BOOST_REQUIRE(h.data_page_v2);
        BOOST_REQUIRE(h.crc);
        BOOST_REQUIRE_EQUAL(uint32_t(*h.crc), crc32_of(std::string_view(file).substr(pos + hs, h.compressed_page_size)));
        const auto& v2 = *h.data_page_v2;
        BOOST_REQUIRE(encodings.contains(v2.enc));
        uses_dictionary |= v2.enc == pq::encoding::rle_dictionary;
        if (!v2.is_compressed) {
            BOOST_REQUIRE_EQUAL(h.compressed_page_size, h.uncompressed_page_size);
        } else {
            BOOST_REQUIRE_LT(h.compressed_page_size, h.uncompressed_page_size);
        }
        BOOST_REQUIRE_GT(v2.num_rows, 0);
        pages.push_back(page_info{.header = h, .offset = pos, .size = hs + h.compressed_page_size, .first_row = row});
        row += v2.num_rows;
        values += v2.num_values;
        nulls += v2.num_nulls;
        pos += hs + h.compressed_page_size;
        uncompressed += hs + h.uncompressed_page_size;
    }
    BOOST_REQUIRE_EQUAL(pos, end);
    BOOST_REQUIRE(!pages.empty());
    BOOST_REQUIRE_EQUAL(row, rows);
    BOOST_REQUIRE_EQUAL(values, md.num_values);
    BOOST_REQUIRE(md.stats && md.stats->null_count);
    BOOST_REQUIRE_EQUAL(nulls, *md.stats->null_count);
    BOOST_REQUIRE_EQUAL(uncompressed, uint64_t(md.total_uncompressed_size));
    BOOST_REQUIRE_EQUAL(uses_dictionary, md.dictionary_page_offset.has_value());
    return pages;
}

// What check_layout() learned about a file.
struct layout {
    // Per file column: the first row group in which its chunk is in place.
    std::vector<size_t> first_in_place;
    // Per row group and file column: the data pages.
    std::vector<std::vector<std::vector<page_info>>> pages;
    size_t late_chunks = 0;
};

// Checks the layout invariants of writer.hh from the footer and the pages.
layout check_layout(const written_file& f, const model& m, const pq::writer_config& cfg) {
    const auto& file = f.data;
    const auto md = parse_footer(f);
    const auto cols = m.file_columns();
    std::vector<bool> included(m.schema.columns());
    for (auto c : cols) {
        included[c] = true;
    }
    BOOST_REQUIRE(md.schema == m.schema.to_elements(included));
    BOOST_REQUIRE_EQUAL(md.num_rows, int64_t(m.rows));
    BOOST_REQUIRE_EQUAL(md.row_groups.size(), f.result.row_groups);
    BOOST_REQUIRE_EQUAL(md.created_by.value_or(""), cfg.created_by);

    layout l;
    l.first_in_place.assign(cols.size(), md.row_groups.size());
    l.pages.resize(md.row_groups.size());
    uint64_t pos = pq::magic.size();
    int64_t row_base = 0;
    for (size_t rg = 0; rg < md.row_groups.size(); ++rg) {
        const auto& g = md.row_groups[rg];
        BOOST_REQUIRE_EQUAL(g.columns.size(), cols.size());
        BOOST_REQUIRE_GT(g.num_rows, 0);
        l.pages[rg].resize(cols.size());
        int64_t total_uncompressed = 0;
        int64_t total_compressed = 0;
        std::vector<size_t> in_place;
        for (size_t k = 0; k < cols.size(); ++k) {
            const auto& c = g.columns[k];
            total_uncompressed += c.meta_data.total_uncompressed_size;
            total_compressed += c.meta_data.total_compressed_size;
            BOOST_REQUIRE(c.meta_data.codec == cfg.codec);
            BOOST_REQUIRE(c.meta_data.type == m.schema.get_column(cols[k]).type);
            if (c.offset_index_offset) {
                in_place.push_back(k);
                l.first_in_place[k] = std::min(l.first_in_place[k], rg);
            } else {
                // Once in place, a column stays in place.
                BOOST_REQUIRE_EQUAL(l.first_in_place[k], md.row_groups.size());
            }
        }
        BOOST_REQUIRE_EQUAL(g.total_byte_size, total_uncompressed);
        BOOST_REQUIRE_EQUAL(g.total_compressed_size.value_or(-1), total_compressed);
        if (!in_place.empty()) {
            BOOST_REQUIRE_EQUAL(g.file_offset.value_or(-1), int64_t(pos));
        }
        // In-place chunks are adjacent and in schema order.
        for (size_t k : in_place) {
            const auto& c = g.columns[k];
            BOOST_REQUIRE_EQUAL(c.meta_data.dictionary_page_offset.value_or(c.meta_data.data_page_offset), int64_t(pos));
            auto pages = walk_chunk(file, c, g.num_rows);
            const auto& col = m.schema.get_column(cols[k]);
            for (const auto& p : pages) {
                // Pages nest in page sets.
                BOOST_REQUIRE(f.page_set_starts.contains(row_base + p.first_row));
                // The bound on entries per page.
                BOOST_REQUIRE_LE(uint64_t(p.header.data_page_v2->num_values),
                        uint64_t(cfg.max_page_values) + cfg.max_page_set_rows + m.max_entries_per_row(cols[k]));
                if (!col.max_rep) {
                    BOOST_REQUIRE_EQUAL(p.header.data_page_v2->num_values, p.header.data_page_v2->num_rows);
                }
            }
            pos += c.meta_data.total_compressed_size;
            l.pages[rg][k] = std::move(pages);
        }
        // The OffsetIndex block follows, in the same order.
        for (size_t k : in_place) {
            const auto& c = g.columns[k];
            BOOST_REQUIRE_EQUAL(*c.offset_index_offset, int64_t(pos));
            thrift::decoder d(std::span<const char>(file.data() + pos, *c.offset_index_length));
            auto idx = pq::parse_offset_index(d);
            BOOST_REQUIRE_EQUAL(d.position(), uint64_t(*c.offset_index_length));
            const auto& pages = l.pages[rg][k];
            BOOST_REQUIRE_EQUAL(idx.page_locations.size(), pages.size());
            for (size_t i = 0; i < pages.size(); ++i) {
                BOOST_REQUIRE_EQUAL(idx.page_locations[i].offset, int64_t(pages[i].offset));
                BOOST_REQUIRE_EQUAL(idx.page_locations[i].compressed_page_size, int64_t(pages[i].size));
                BOOST_REQUIRE_EQUAL(idx.page_locations[i].first_row_index, pages[i].first_row);
            }
            pos += *c.offset_index_length;
        }
        row_base += g.num_rows;
    }
    BOOST_REQUIRE_EQUAL(row_base, int64_t(m.rows));
    // Late chunks follow the last row group, by column and then by row group.
    for (size_t k = 0; k < cols.size(); ++k) {
        for (size_t rg = 0; rg < l.first_in_place[k]; ++rg) {
            const auto& g = md.row_groups[rg];
            const auto& c = g.columns[k];
            BOOST_REQUIRE_EQUAL(c.meta_data.data_page_offset, int64_t(pos));
            BOOST_REQUIRE(!c.meta_data.dictionary_page_offset);
            BOOST_REQUIRE_EQUAL(c.meta_data.num_values, g.num_rows);
            BOOST_REQUIRE_EQUAL(c.meta_data.stats->null_count.value_or(-1), g.num_rows);
            auto pages = walk_chunk(file, c, g.num_rows);
            for (const auto& p : pages) {
                BOOST_REQUIRE_LE(uint32_t(p.header.data_page_v2->num_values), cfg.max_page_values);
                BOOST_REQUIRE_EQUAL(p.header.data_page_v2->num_nulls, p.header.data_page_v2->num_values);
            }
            pos += c.meta_data.total_compressed_size;
            ++l.late_chunks;
        }
    }
    BOOST_REQUIRE_EQUAL(l.late_chunks, f.stats.late_chunks);
    BOOST_REQUIRE_EQUAL(pos, file.size() - 8 - f.result.footer.size);
    return l;
}

// Arrow's page index reader returns the same OffsetIndexes, and none for late chunks.
void check_page_index_with_arrow(const written_file& f, const layout& l) {
    auto reader = open_with_arrow(f.data);
    auto page_index = reader->GetPageIndexReader();
    BOOST_REQUIRE(page_index);
    for (size_t rg = 0; rg < l.pages.size(); ++rg) {
        auto rg_index = page_index->RowGroup(rg);
        for (size_t k = 0; k < l.first_in_place.size(); ++k) {
            if (!rg_index) {
                BOOST_REQUIRE_GT(l.first_in_place[k], rg);
                continue;
            }
            auto oi = rg_index->GetOffsetIndex(k);
            if (rg < l.first_in_place[k]) {
                BOOST_REQUIRE(!oi);
                continue;
            }
            BOOST_REQUIRE(oi);
            const auto& locations = oi->page_locations();
            const auto& pages = l.pages[rg][k];
            BOOST_REQUIRE_EQUAL(locations.size(), pages.size());
            for (size_t i = 0; i < pages.size(); ++i) {
                BOOST_REQUIRE_EQUAL(locations[i].offset, int64_t(pages[i].offset));
                BOOST_REQUIRE_EQUAL(locations[i].compressed_page_size, int32_t(pages[i].size));
                BOOST_REQUIRE_EQUAL(locations[i].first_row_index, pages[i].first_row);
            }
        }
    }
}

// Random values.

struct value_gen {
    // The probability that a row has an entry.
    double density = 0.5;
    // The number of distinct values, or 0 for any value.
    uint32_t cardinality = 0;
    size_t max_length = 20;
};

std::string random_value(std::mt19937_64& rng, const pq::schema::column& col, const value_gen& g) {
    // With a cardinality, values derive from a small seed.
    const uint64_t seed = g.cardinality ? std::uniform_int_distribution<uint64_t>(0, g.cardinality - 1)(rng) : rng();
    std::mt19937_64 vr(seed);
    switch (col.type) {
    case physical_type::boolean: return std::string(1, char(seed & 1));
    case physical_type::int32: return bytes_of_value(int32_t(g.cardinality ? seed * 7919 : vr()));
    case physical_type::int64: return bytes_of_value(int64_t(g.cardinality ? seed * 1000003 + 1700000000000000 : vr()));
    case physical_type::float_: {
        static const float specials[] = {0.0f, -0.0f, std::numeric_limits<float>::quiet_NaN(), std::numeric_limits<float>::infinity()};
        if (seed % 17 == 0) {
            return bytes_of_value(specials[(seed / 17) % 4]);
        }
        return bytes_of_value(float(int64_t(vr() % 1000000)) / 7.0f);
    }
    case physical_type::double_: {
        if (seed % 13 == 0) {
            return bytes_of_value(std::bit_cast<double>(vr() | 0x7ff0000000000001ull));
        }
        return bytes_of_value(double(int64_t(vr() % 1000000000)) / 3.0);
    }
    case physical_type::byte_array:
    case physical_type::fixed_len_byte_array: {
        const size_t len = col.type_length ? size_t(*col.type_length) : vr() % (g.max_length + 1);
        std::string s(len, '\0');
        for (auto& ch : s) {
            ch = char('a' + vr() % 26);
        }
        return s;
    }
    default: BOOST_FAIL("unexpected type");
    }
    return {};
}

// A field of the random schema: a scalar column, a list, or a map.
struct field {
    enum class kind { scalar, list, map } k;
    column_id col;
    column_id value_col = 0;
    value_gen gen;
    value_gen value_gen_;
    // The field has no entries before this row.
    size_t first_row = 0;
    // A null element of a list, or a dead map entry.
    double null_element = 0.2;
};

struct random_schema {
    std::vector<field> fields;
    pq::schema schema;
    column_id never;
    column_id key_all_null;
    column_id late;
};

random_schema make_random_schema(size_t late_start) {
    random_schema r;
    auto& s = r.schema;
    const auto opt = repetition_type::optional;
    auto scalar = [&] (pq::node_id parent, sstring name, physical_type t, value_gen g, pq::column_options o = {}) {
        auto c = s.add_column(parent, std::move(name), opt, t, o);
        r.fields.push_back(field{.k = field::kind::scalar, .col = c, .gen = g});
        return c;
    };
    scalar(pq::schema::root, "pk", physical_type::byte_array, {.density = 0.3},
            {.logical = pq::logical_type{.k = pq::logical_type::kind::string}, .always_present = true});
    r.key_all_null = s.add_column(pq::schema::root, "ck", opt, physical_type::int64, {.always_present = true});
    scalar(pq::schema::root, "b", physical_type::boolean, {.density = 0.7});
    scalar(pq::schema::root, "i32", physical_type::int32, {.density = 0.9, .cardinality = 5});
    scalar(pq::schema::root, "i64", physical_type::int64, {.density = 0.6});
    scalar(pq::schema::root, "f", physical_type::float_, {.density = 0.5, .cardinality = 40});
    scalar(pq::schema::root, "d", physical_type::double_, {.density = 0.4});
    scalar(pq::schema::root, "s", physical_type::byte_array, {.density = 0.8, .cardinality = 10, .max_length = 30});
    scalar(pq::schema::root, "blob", physical_type::byte_array, {.density = 0.2, .max_length = 300});
    scalar(pq::schema::root, "u", physical_type::fixed_len_byte_array, {.density = 0.5, .cardinality = 50},
            {.type_length = 16, .logical = pq::logical_type{.k = pq::logical_type::kind::uuid}});
    scalar(pq::schema::root, "sparse", physical_type::int64, {.density = 0.002});
    {
        auto c = s.add_column(pq::schema::root, "req", repetition_type::required, physical_type::int32);
        r.fields.push_back(field{.k = field::kind::scalar, .col = c, .gen = {.density = 1, .cardinality = 3}});
    }
    r.never = s.add_column(pq::schema::root, "never", opt, physical_type::int32);
    r.late = scalar(pq::schema::root, "late", physical_type::byte_array, {.density = 0.5, .cardinality = 4});
    r.fields.back().first_row = late_start;
    {
        auto c = s.add_list(pq::schema::root, "lst", physical_type::int32);
        r.fields.push_back(field{.k = field::kind::list, .col = c, .gen = {.density = 0.5, .cardinality = 100}});
    }
    {
        auto c = s.add_list(pq::schema::root, "lst_s", physical_type::byte_array);
        r.fields.push_back(field{.k = field::kind::list, .col = c, .gen = {.density = 0.3, .max_length = 10}});
    }
    {
        auto m = s.add_map(pq::schema::root, "m", physical_type::byte_array, {}, physical_type::int64);
        r.fields.push_back(field{.k = field::kind::map, .col = m.key, .value_col = m.value,
                .gen = {.density = 0.4, .cardinality = 20, .max_length = 8}, .value_gen_ = {.cardinality = 1000}});
    }
    // The reserved group: always present, so it adds no definition level.
    auto scylla = s.add_group(pq::schema::root, "_scylla", repetition_type::required);
    scalar(scylla, "kind", physical_type::int32, {.density = 1, .cardinality = 8});
    auto cells = s.add_group(scylla, "cells", repetition_type::required);
    auto x = s.add_group(cells, "x", repetition_type::required);
    scalar(x, "ts", physical_type::int64, {.density = 0.1, .cardinality = 3});
    scalar(x, "ttl", physical_type::int32, {.density = 0.05});
    {
        auto c = s.add_list(x, "meta", physical_type::int64);
        r.fields.push_back(field{.k = field::kind::list, .col = c, .gen = {.density = 0.2, .cardinality = 2}, .null_element = 0.5});
    }
    // Declared last but placed in the middle of the schema order.
    scalar(x, "flag", physical_type::boolean, {.density = 0.3});
    return r;
}

model make_random_model(const random_schema& rs, size_t rows, uint64_t seed) {
    std::mt19937_64 rng(seed);
    model m(rs.schema);
    auto chance = [&] (double p) { return std::uniform_real_distribution<double>(0, 1)(rng) < p; };
    for (size_t row = 0; row < rows; ++row) {
        m.add_row();
        for (const auto& f : rs.fields) {
            if (row < f.first_row || !chance(f.gen.density)) {
                continue;
            }
            const auto& col = m.schema.get_column(f.col);
            switch (f.k) {
            case field::kind::scalar:
                m.add(f.col, entry{.rep = 0, .def = col.max_def, .value = random_value(rng, col, f.gen)});
                break;
            case field::kind::list: {
                // The writer never writes an empty list, so a list has at least one element.
                const size_t n = 1 + rng() % 5;
                for (size_t i = 0; i < n; ++i) {
                    const uint16_t rep = i ? col.max_rep : 0;
                    if (chance(f.null_element)) {
                        m.add(f.col, entry{.rep = rep, .def = uint16_t(col.max_def - 1)});
                    } else {
                        m.add(f.col, entry{.rep = rep, .def = col.max_def, .value = random_value(rng, col, f.gen)});
                    }
                }
                break;
            }
            case field::kind::map: {
                const auto& vcol = m.schema.get_column(f.value_col);
                const size_t n = 1 + rng() % 4;
                for (size_t i = 0; i < n; ++i) {
                    const uint16_t rep = i ? col.max_rep : 0;
                    m.add(f.col, entry{.rep = rep, .def = col.max_def, .value = random_value(rng, col, f.gen)});
                    if (chance(f.null_element)) {
                        m.add(f.value_col, entry{.rep = rep, .def = uint16_t(vcol.max_def - 1)});
                    } else {
                        m.add(f.value_col, entry{.rep = rep, .def = vcol.max_def, .value = random_value(rng, vcol, f.value_gen_)});
                    }
                }
                break;
            }
            }
        }
    }
    return m;
}

// Writes a random file with `cfg` and runs all checks. Returns the file.
written_file run_random(pq::writer_config cfg, size_t rows, uint64_t seed, size_t late_start) {
    const auto rs = make_random_schema(late_start);
    const auto m = make_random_model(rs, rows, seed);
    std::vector<pq::key_value> kv{{.key = "scylla.layout", .value = "1"}, {.key = "other", .value = "x"}};
    // Explicit nulls at def 0 are the same as no entry.
    auto f = write_model(m, cfg, kv, [&] (pq::writer& w, size_t row) {
        if (row % 3 == 0) {
            w.append_null(rs.never, pq::levels{});
            w.append_null(rs.key_all_null, pq::levels{});
        }
    });
    BOOST_REQUIRE(!m.included(rs.never));
    BOOST_REQUIRE(m.included(rs.key_all_null));
    check_with_arrow(f, m, kv);
    auto l = check_layout(f, m, cfg);
    check_page_index_with_arrow(f, l);
    // The always-present column that is null in the whole file has only late chunks.
    const auto cols = m.file_columns();
    const auto key_pos = std::ranges::find(cols, rs.key_all_null) - cols.begin();
    BOOST_REQUIRE_EQUAL(l.first_in_place[key_pos], f.result.row_groups);
    // The late column is in place from a later row group.
    const auto late_pos = std::ranges::find(cols, rs.late) - cols.begin();
    if (f.result.row_groups > 1 && late_start > 0) {
        BOOST_REQUIRE_GT(l.first_in_place[late_pos], 0);
    }
    return f;
}

pq::writer_config small_config(pq::compression_codec codec = pq::compression_codec::uncompressed) {
    return pq::writer_config{
        .row_group_size = 32 << 10,
        .page_set_size = 2 << 10,
        .min_page_size = 256,
        .dictionary_page_limit = 1 << 10,
        .codec = codec,
        .created_by = "parquet_writer_test",
    };
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_random_files_all_codecs) {
    using codec = pq::compression_codec;
    uint64_t seed = 1;
    for (auto c : {codec::uncompressed, codec::snappy, codec::gzip, codec::lz4_raw, codec::zstd}) {
        auto f = run_random(small_config(c), 4000, seed++, 2500);
        BOOST_REQUIRE_GT(f.result.row_groups, 3);
        BOOST_REQUIRE_GT(f.stats.late_chunks, 0);
        BOOST_REQUIRE_GT(f.stats.dictionary_pages, 0);
    }
    // With compression levels.
    auto cfg = small_config(codec::zstd);
    cfg.compression_level = 9;
    run_random(cfg, 1000, seed++, 0);
    cfg = small_config(codec::gzip);
    cfg.compression_level = 1;
    run_random(cfg, 1000, seed++, 0);
}

SEASTAR_THREAD_TEST_CASE(test_default_config) {
    auto f = run_random(pq::writer_config{}, 3000, 11, 1000);
    BOOST_REQUIRE_EQUAL(f.result.row_groups, 1);
}

SEASTAR_THREAD_TEST_CASE(test_page_sets_of_one_row) {
    auto cfg = small_config(pq::compression_codec::zstd);
    cfg.max_page_set_rows = 1;
    cfg.min_page_size = 0;
    auto f = run_random(cfg, 400, 21, 250);
    BOOST_REQUIRE_EQUAL(f.stats.page_set_cuts, 400);
    BOOST_REQUIRE_EQUAL(f.page_set_starts.size(), 401);
}

SEASTAR_THREAD_TEST_CASE(test_small_page_value_cap) {
    // A small cap on entries per page, so that it, rather than size, cuts
    // pages, including the null runs of sparse columns.
    auto cfg = small_config(pq::compression_codec::lz4_raw);
    cfg.max_page_values = 16;
    cfg.page_set_size = 1 << 20;
    cfg.row_group_size = 1 << 20;
    run_random(cfg, 2000, 31, 1500);
}

SEASTAR_THREAD_TEST_CASE(test_single_row_group_per_page_set) {
    // Every page set ends a row group.
    auto cfg = small_config(pq::compression_codec::snappy);
    cfg.page_set_size = 64;
    cfg.row_group_size = 0;
    auto f = run_random(cfg, 300, 41, 150);
    BOOST_REQUIRE_EQUAL(f.result.row_groups, f.stats.page_set_cuts);
}

SEASTAR_THREAD_TEST_CASE(test_memory_handle) {
    // The handle's target is asked at each page set cut, and caps the row group.
    struct handle final : pq::writer_memory_handle {
        uint64_t& calls;
        explicit handle(uint64_t& c) : calls(c) {}
        uint64_t row_group_target() override {
            ++calls;
            return 4 << 10;
        }
    };
    const auto rs = make_random_schema(0);
    const auto m = make_random_model(rs, 2000, 51);
    uint64_t calls = 0;
    auto f = write_model(m, small_config(), {}, [] (pq::writer&, size_t) {}, std::make_unique<handle>(calls));
    BOOST_REQUIRE_EQUAL(calls, f.stats.page_set_cuts);
    auto small = write_model(m, small_config());
    BOOST_REQUIRE_GT(f.result.row_groups, small.result.row_groups);
    check_with_arrow(f, m);
    check_layout(f, m, small_config());
}

SEASTAR_THREAD_TEST_CASE(test_size_tunables_are_clamped) {
    // A page set size and a minimum page size above the maximum act as the
    // maximum, so pages of 4 MiB of values are cut into 1 MiB pages.
    pq::schema s;
    auto c = s.add_column(pq::schema::root, "v", repetition_type::required, physical_type::int64);
    memory_data_sink_buffers bufs;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
    pq::writer_config cfg;
    cfg.row_group_size = uint64_t(1) << 40;
    cfg.page_set_size = 64 << 20;
    cfg.min_page_size = 64 << 20;
    cfg.dictionary_page_limit = 64 << 20;
    cfg.max_page_values = 1 << 30;
    cfg.max_page_set_rows = 1 << 29;
    const uint64_t rows = 4 * pq::writer_config::max_page_size / sizeof(int64_t);
    {
        pq::writer w(fw, s, cfg, std::make_unique<pq::fixed_writer_memory_handle>(uint64_t(1) << 40));
        for (uint64_t row = 0; row < rows; ++row) {
            w.append(c, int64_t(row));
            w.end_row();
        }
        w.finish();
        BOOST_REQUIRE_EQUAL(w.stats().page_set_cuts, 4);
        BOOST_REQUIRE_EQUAL(w.stats().data_pages, 4);
    }
    fw.close();
}

SEASTAR_THREAD_TEST_CASE(test_memory_usage_report) {
    // The writer reports what it holds: the row group buffer, the
    // dictionaries, and the footer records, which grow with each row group.
    struct handle final : pq::writer_memory_handle {
        std::vector<uint64_t>& reports;
        explicit handle(std::vector<uint64_t>& r) : reports(r) {}
        // A row group ends at each page set cut.
        uint64_t row_group_target() override { return 0; }
        void report_memory_usage(uint64_t bytes) noexcept override { reports.push_back(bytes); }
    };
    pq::schema s;
    auto c = s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::byte_array);
    memory_data_sink_buffers bufs;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
    std::vector<uint64_t> reports;
    std::vector<uint64_t> after_row_groups;
    {
        pq::writer w(fw, s, small_config(), std::make_unique<handle>(reports));
        BOOST_REQUIRE_EQUAL(w.memory_usage(), 0);
        for (int row = 0; row < 2000; ++row) {
            const auto v = fmt::format("value-{}", row % 10);
            w.append(c, bytes_view(reinterpret_cast<const int8_t*>(v.data()), v.size()));
            // The dictionary has an entry, which the buffered size does not count.
            BOOST_REQUIRE_GT(w.memory_usage(), w.buffered_size());
            if (w.end_row()) {
                BOOST_REQUIRE_EQUAL(w.buffered_size(), 0);
                BOOST_REQUIRE_EQUAL(reports.back(), w.memory_usage());
                after_row_groups.push_back(reports.back());
            }
        }
        w.finish();
        BOOST_REQUIRE_EQUAL(reports.back(), w.memory_usage());
    }
    fw.close();
    BOOST_REQUIRE_GT(after_row_groups.size(), 3);
    for (size_t i = 1; i < after_row_groups.size(); ++i) {
        BOOST_REQUIRE_GT(after_row_groups[i], after_row_groups[i - 1]);
    }
}

SEASTAR_THREAD_TEST_CASE(test_long_names) {
    // A name that a reader would reject in the footer is rejected.
    pq::schema s;
    s.add_column(pq::schema::root, sstring(pq::max_name_size, 'a'), repetition_type::optional, physical_type::int32);
    BOOST_REQUIRE_THROW(s.add_column(pq::schema::root, sstring(pq::max_name_size + 1, 'b'), repetition_type::optional, physical_type::int32),
            std::invalid_argument);
    BOOST_REQUIRE_THROW(s.add_group(pq::schema::root, sstring(pq::max_name_size + 1, 'c'), repetition_type::required), std::invalid_argument);
}

SEASTAR_THREAD_TEST_CASE(test_dictionary_fallback) {
    // A column whose values repeat at first and then do not. Its first
    // pages use the dictionary, and the dictionary then exceeds the limit
    // in the middle of the chunk.
    pq::schema s;
    auto c = s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::byte_array);
    auto n = s.add_column(pq::schema::root, "n", repetition_type::optional, physical_type::int64);
    model m(s);
    std::mt19937_64 rng(61);
    // Few distinct values that are far apart, so that delta encoding does not beat the dictionary.
    std::vector<int64_t> ints(8);
    for (auto& i : ints) {
        i = rng();
    }
    for (size_t row = 0; row < 3000; ++row) {
        m.add_row();
        const uint64_t k = row < 1000 ? rng() % 8 : rng();
        m.add(c, entry{.def = 1, .value = fmt::format("value-{}", k)});
        m.add(n, entry{.def = 1, .value = bytes_of_value(row < 1500 ? ints[k % 8] : int64_t(rng()))});
    }
    auto cfg = small_config();
    cfg.row_group_size = 1 << 20;
    cfg.dictionary_page_limit = 512;
    auto f = write_model(m, cfg);
    BOOST_REQUIRE_EQUAL(f.result.row_groups, 1);
    BOOST_REQUIRE_GE(f.stats.dictionary_fallbacks, 2);
    check_with_arrow(f, m);
    auto l = check_layout(f, m, cfg);
    for (size_t k = 0; k < 2; ++k) {
        std::set<pq::encoding> encodings;
        for (const auto& p : l.pages[0][k]) {
            encodings.insert(p.header.data_page_v2->enc);
        }
        BOOST_REQUIRE(encodings.contains(pq::encoding::rle_dictionary));
        BOOST_REQUIRE_GT(encodings.size(), 1);
    }
    check_page_index_with_arrow(f, l);
}

SEASTAR_THREAD_TEST_CASE(test_large_values) {
    // Values larger than 128 KiB, in all compression codecs, among small ones.
    using codec = pq::compression_codec;
    pq::schema s;
    auto c = s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::byte_array);
    auto l = s.add_list(pq::schema::root, "l", physical_type::byte_array);
    model m(s);
    std::mt19937_64 rng(71);
    for (size_t row = 0; row < 40; ++row) {
        m.add_row();
        const size_t len = row % 4 == 0 ? (128 << 10) + rng() % (300 << 10) : rng() % 100;
        std::string v(len, '\0');
        for (size_t i = 0; i < len; ++i) {
            v[i] = char(i % 7 == 0 ? rng() : 'x');
        }
        m.add(c, entry{.def = 1, .value = v});
        if (row % 5 == 0) {
            m.add(l, entry{.rep = 0, .def = 3, .value = v});
            m.add(l, entry{.rep = 1, .def = 2});
            m.add(l, entry{.rep = 1, .def = 3, .value = v.substr(0, len / 2)});
        }
    }
    for (auto cd : {codec::uncompressed, codec::snappy, codec::gzip, codec::lz4_raw, codec::zstd}) {
        auto cfg = small_config(cd);
        cfg.row_group_size = 1 << 20;
        auto f = write_model(m, cfg);
        check_with_arrow(f, m);
        check_page_index_with_arrow(f, check_layout(f, m, cfg));
    }
}

SEASTAR_THREAD_TEST_CASE(test_empty_and_null_files) {
    // No rows at all: only the always-present column is in the schema.
    {
        pq::schema s;
        s.add_column(pq::schema::root, "k", repetition_type::optional, physical_type::int32, {.always_present = true});
        s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::int32);
        model m(s);
        auto f = write_model(m, small_config());
        BOOST_REQUIRE_EQUAL(f.result.row_groups, 0);
        check_with_arrow(f, m);
        check_layout(f, m, small_config());
    }
    // Rows that are all null: row groups without in-place chunks, and late chunks for all of them.
    {
        pq::schema s;
        s.add_column(pq::schema::root, "k", repetition_type::optional, physical_type::int32, {.always_present = true});
        s.add_list(pq::schema::root, "l", physical_type::int32, {.always_present = true});
        s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::int32);
        model m(s);
        for (int i = 0; i < 1000; ++i) {
            m.add_row();
        }
        auto cfg = small_config();
        cfg.max_page_set_rows = 100;
        cfg.max_page_values = 300;
        cfg.row_group_size = 0;
        auto f = write_model(m, cfg);
        BOOST_REQUIRE_EQUAL(f.result.row_groups, 10);
        BOOST_REQUIRE_EQUAL(f.stats.late_chunks, 20);
        check_with_arrow(f, m);
        check_page_index_with_arrow(f, check_layout(f, m, cfg));
    }
}

SEASTAR_THREAD_TEST_CASE(test_cost_per_row) {
    // A wide, sparse schema: 2000 columns, and 2 values per row. Page set
    // cuts visit only the columns that received a value, so the column
    // visits stay proportional to the values, plus one visit per column
    // with state at the end of each row group.
    constexpr size_t n_columns = 2000;
    pq::schema s;
    auto kind = s.add_column(pq::schema::root, "kind", repetition_type::optional, physical_type::int32);
    std::vector<column_id> cols;
    for (size_t i = 0; i < n_columns; ++i) {
        cols.push_back(s.add_column(pq::schema::root, fmt::format("c{}", i), repetition_type::optional, physical_type::int64));
    }
    memory_data_sink_buffers bufs;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
    auto cfg = small_config(pq::compression_codec::zstd);
    cfg.page_set_size = 512;
    cfg.row_group_size = 4 << 20;
    pq::writer w(fw, s, cfg);
    std::mt19937_64 rng(81);
    constexpr size_t rows = 30000;
    // The writer's own allocations stay below 128 KiB each. The compression
    // codec contexts are allocated once and do not count.
    const auto before = seastar::memory::stats();
    {
        const seastar::memory::scoped_large_allocation_warning_threshold guard(128 * 1024 + 1);
        for (size_t row = 0; row < rows; ++row) {
            w.append(kind, int32_t(row % 3));
            // Only the first 500 columns get values, so the others stay without state.
            w.append(cols[rng() % 500], int64_t(rng()));
            w.end_row();
        }
        w.finish();
    }
    const auto after = seastar::memory::stats();
    BOOST_REQUIRE_EQUAL(after.large_allocations(), before.large_allocations());
    const auto& st = w.stats();
    BOOST_REQUIRE_EQUAL(st.entries, 2 * rows);
    BOOST_REQUIRE_GT(st.page_set_cuts, 500);
    // Visits at cuts are at most the entries. Visits at row group ends are at most the columns with state.
    BOOST_REQUIRE_LE(st.column_visits, st.entries + st.row_groups * 501);
    fw.close();
}

SEASTAR_THREAD_TEST_CASE(test_large_values_allocations) {
    // Values above 128 KiB go into chains of contiguous pieces. The
    // compression codecs compress a chain without a contiguous copy, so no
    // single allocation of the writer exceeds 128 KiB. The codec contexts
    // are allocated once and do not count.
    for (auto codec : {pq::compression_codec::uncompressed, pq::compression_codec::snappy, pq::compression_codec::gzip,
            pq::compression_codec::lz4_raw, pq::compression_codec::zstd}) {
        pq::schema s;
        auto c = s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::byte_array);
        memory_data_sink_buffers bufs;
        sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
        pq::writer w(fw, s, small_config(codec));
        managed_bytes value(bytes(bytes::initialized_later(), 1 << 20));
        for (auto frag : fragment_range(managed_bytes_mutable_view(value))) {
            std::ranges::fill(frag, int8_t('v'));
        }
        const auto before = seastar::memory::stats();
        {
            const seastar::memory::scoped_large_allocation_warning_threshold guard(128 * 1024 + 1);
            for (int row = 0; row < 8; ++row) {
                w.append(c, managed_bytes_view(value));
                w.end_row();
            }
            w.finish();
        }
        const auto after = seastar::memory::stats();
        BOOST_REQUIRE_EQUAL(after.large_allocations(), before.large_allocations());
        fw.close();
    }
}
