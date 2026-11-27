/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the read side of the Parquet codec: the footer summary, the
// page decoders, the fetcher, and the column cursors.
//
// The oracle is an in-memory model of the columns: the entries that the test
// appended to our writer. The files are read from memory, through a
// seastar::file that records every read.
//
// Random inputs come from fixed seeds, so the tests are repeatable.

#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <seastar/core/memory.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/closeable.hh>
#include <seastar/testing/thread_test_case.hh>

// Arrow's headers use std::is_trivial, which C++26 deprecates.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>
#pragma GCC diagnostic pop

#include "reader_concurrency_semaphore.hh"
#include "readers/empty.hh"
#include "readers/mutation_reader.hh"
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "tracing/trace_state.hh"
#include "sstables/checksum_utils.hh"
#include "sstables/exceptions.hh"
#include "sstables/file_writer.hh"
#include "sstables/parquet/file_summary.hh"
#include "sstables/parquet/metadata.hh"
#include "sstables/parquet/page_decoder.hh"
#include "sstables/parquet/reader.hh"
#include "sstables/parquet/schema.hh"
#include "sstables/parquet/writer.hh"
#include "test/lib/memory_file.hh"
#include "utils/memory_data_sink.hh"

namespace pq = sstables::parquet;
using pq::column_id;
using pq::physical_type;
using pq::repetition_type;
using sstables::malformed_sstable_exception;

namespace {

// An entry of a leaf column. Values are kept as their bytes in PLAIN
// order (one byte for a boolean), so that every type compares the same way.
struct entry {
    uint16_t rep = 0;
    uint16_t def = 0;
    std::optional<std::string> value;
    bool operator==(const entry&) const = default;
};

std::string to_string(const std::vector<entry>& es) {
    std::string r;
    for (const auto& e : es) {
        r += fmt::format("{{rep {}, def {}, {}}} ", e.rep, e.def, e.value ? fmt::format("{} bytes", e.value->size()) : "null");
    }
    return r;
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

// The entries that a test appends, per column and row. A row without
// entries is null at level 0.
struct model {
    pq::schema schema;
    std::vector<std::vector<std::vector<entry>>> columns;
    size_t rows = 0;

    explicit model(pq::schema s) : schema(std::move(s)), columns(schema.columns()) {}

    void add_rows(size_t n) {
        rows += n;
        for (auto& c : columns) {
            c.resize(rows);
        }
    }
    std::vector<entry> row(column_id c, size_t r) const {
        const auto& es = columns[c][r];
        return es.empty() ? std::vector<entry>{entry{}} : es;
    }
    // Whether the column is in the file's schema.
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
};

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
    case physical_type::fixed_len_byte_array:
        w.append(c, l, bytes_view(reinterpret_cast<const int8_t*>(v.data()), v.size()));
        break;
    default: BOOST_FAIL("unexpected type");
    }
}

struct written_file {
    std::string data;
    pq::writer_result result;
};

written_file write_model(const model& m, pq::writer_config cfg) {
    memory_data_sink_buffers bufs;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
    written_file f;
    {
        pq::writer w(fw, m.schema, cfg);
        for (size_t row = 0; row < m.rows; ++row) {
            for (column_id c = 0; c < m.columns.size(); ++c) {
                for (const auto& e : m.columns[c][row]) {
                    append_entry(w, c, e);
                }
            }
            w.end_row();
        }
        f.result = w.finish({pq::key_value{.key = "k", .value = "v"}});
    }
    fw.close();
    for (const auto& b : bufs.buffers()) {
        f.data.append(b.get(), b.size());
    }
    return f;
}

// A file opened for reading.
struct opened_file {
    file f;
    shared_ptr<tests::memory_file_impl> impl;
    pq::file_summary summary;
};

std::unique_ptr<opened_file> open_file(const std::string& data, std::optional<uint32_t> crc) {
    auto [f, impl] = tests::make_memory_file(data);
    auto summary = pq::read_file_summary(f, data.size(), crc).get();
    return std::make_unique<opened_file>(opened_file{f, impl, std::move(summary)});
}

// The decoded OffsetIndex block of row group `g`. It reads the bytes
// without the file, so that the file records no read.
lw_shared_ptr<const pq::row_group_index> read_index(const opened_file& o, uint32_t g) {
    const auto& rg = o.summary.row_group(g);
    const auto& data = o.impl->data();
    return make_lw_shared<const pq::row_group_index>(pq::decode_row_group_index(o.summary, g,
            fragmented_temporary_buffer(data.data() + rg.offset_index_offset, rg.offset_index_length)).get());
}

// A metadata cache with an LSA region and an LRU of its own, which evict
// nothing unless a test does.
struct local_metadata_cache {
    logalloc::region region;
    ::lru lru_list;
    pq::metadata_cache_stats stats;
    pq::metadata_cache cache{region, lru_list, stats};

    operator pq::metadata_cache&() noexcept { return cache; }
};

// The column of the model that a cursor reads, with what the check needs.
struct read_column {
    column_id id;
    physical_type type;
    uint16_t max_def;
    uint16_t max_rep;
    uint16_t null_threshold;
};

// The projection of `cols` of the model, and the columns' properties.
std::pair<std::vector<std::optional<pq::leaf_index>>, std::vector<read_column>>
projection_of(const model& m, const pq::file_summary& s, const std::vector<column_id>& cols) {
    std::vector<std::optional<pq::leaf_index>> projection;
    std::vector<read_column> info;
    for (column_id c : cols) {
        const auto path = m.schema.path(c);
        std::vector<std::string_view> p(path.begin(), path.end());
        auto leaf = s.find_column(std::span<const std::string_view>(p));
        BOOST_REQUIRE_EQUAL(leaf.has_value(), m.included(c));
        const auto& col = m.schema.get_column(c);
        read_column rc{.id = c, .type = col.type, .max_def = col.max_def, .max_rep = col.max_rep, .null_threshold = col.max_def};
        if (leaf) {
            const auto& l = s.column(*leaf);
            BOOST_REQUIRE_EQUAL(l.max_def, col.max_def);
            BOOST_REQUIRE_EQUAL(l.max_rep, col.max_rep);
            BOOST_REQUIRE(l.type == col.type);
            rc.null_threshold = l.null_threshold();
        } else if (col.max_rep) {
            // Every row is null anyway.
            rc.null_threshold = 1;
        }
        projection.push_back(leaf);
        info.push_back(rc);
    }
    return {std::move(projection), std::move(info)};
}

std::vector<column_id> all_columns(const model& m) {
    std::vector<column_id> r;
    for (column_id c = 0; c < m.columns.size(); ++c) {
        r.push_back(c);
    }
    return r;
}

std::string value_bytes(const pq::column_cursor& cur, physical_type t, uint32_t i) {
    switch (t) {
    case physical_type::boolean: return std::string(1, char(cur.value_at<bool>(i)));
    case physical_type::int32: return bytes_of_value(cur.value_at<int32_t>(i));
    case physical_type::int64: return bytes_of_value(cur.value_at<int64_t>(i));
    case physical_type::float_: return bytes_of_value(cur.value_at<float>(i));
    case physical_type::double_: return bytes_of_value(cur.value_at<double>(i));
    default: {
        std::string r;
        for (bytes_view frag : cur.value_at<pq::byte_array_view>(i)) {
            r.append(reinterpret_cast<const char*>(frag.data()), frag.size());
        }
        return r;
    }
    }
}

bool is_null_row(const std::vector<entry>& es, const read_column& rc) {
    return es.size() == 1 && es[0].rep == 0 && es[0].def < rc.null_threshold;
}

// Checks the cursor's current row against the model.
void check_row(const pq::column_cursor& cur, const model& m, const read_column& rc) {
    BOOST_REQUIRE(cur.positioned());
    const uint64_t row = cur.row();
    const auto expected = m.row(rc.id, row);
    std::vector<entry> got;
    auto it = cur.entries();
    while (auto e = it.next()) {
        entry x{.rep = e->rep, .def = e->def};
        if (e->def == rc.max_def && cur.column()) {
            x.value = value_bytes(cur, rc.type, e->value_index);
        }
        got.push_back(std::move(x));
    }
    if (got != expected) {
        BOOST_FAIL(fmt::format("column {} row {}: got {}, expected {}", rc.id, row, to_string(got), to_string(expected)));
    }
    BOOST_REQUIRE_EQUAL(cur.def(), expected[0].def);
    BOOST_REQUIRE_EQUAL(cur.has_value(), expected[0].def == rc.max_def && cur.column());
    const bool null = is_null_row(expected, rc);
    BOOST_REQUIRE_EQUAL(cur.is_null(), null);
    const uint64_t forward = cur.null_rows_forward();
    const uint64_t backward = cur.null_rows_backward();
    if (!null) {
        BOOST_REQUIRE_EQUAL(forward, 0);
        BOOST_REQUIRE_EQUAL(backward, 0);
        return;
    }
    BOOST_REQUIRE_GE(forward, 1);
    BOOST_REQUIRE_GE(backward, 1);
    BOOST_REQUIRE_LE(row + forward, m.rows);
    BOOST_REQUIRE_LE(backward, row + 1);
    // The rows in between are checked when the cursor reaches them.
    BOOST_REQUIRE(is_null_row(m.row(rc.id, row + forward - 1), rc));
    BOOST_REQUIRE(is_null_row(m.row(rc.id, row + 1 - backward), rc));
}

void check_value_of_first_entry(const pq::column_cursor& cur, const read_column& rc, const model& m) {
    if (rc.max_rep || !cur.has_value()) {
        return;
    }
    std::string v;
    switch (rc.type) {
    case physical_type::boolean: v = std::string(1, char(cur.value<bool>())); break;
    case physical_type::int32: v = bytes_of_value(cur.value<int32_t>()); break;
    case physical_type::int64: v = bytes_of_value(cur.value<int64_t>()); break;
    case physical_type::float_: v = bytes_of_value(cur.value<float>()); break;
    case physical_type::double_: v = bytes_of_value(cur.value<double>()); break;
    default:
        for (bytes_view frag : cur.value<pq::byte_array_view>()) {
            v.append(reinterpret_cast<const char*>(frag.data()), frag.size());
        }
    }
    BOOST_REQUIRE(v == *m.row(rc.id, cur.row())[0].value);
}

void step(pq::column_cursor& cur, bool forward) {
    if (forward ? !cur.try_next() : !cur.try_previous()) {
        (forward ? cur.next() : cur.previous()).get();
    }
}

// Reads every row of every projected column in one direction, as a scan does.
void scan(pq::reader& r, const model& m, const std::vector<read_column>& info, bool forward) {
    if (m.rows == 0) {
        return;
    }
    r.seek_all(forward ? 0 : m.rows - 1).get();
    for (size_t i = 0; i < m.rows; ++i) {
        seastar::thread::maybe_yield();
        for (size_t k = 0; k < r.columns(); ++k) {
            auto& cur = r.cursor(k);
            if (i) {
                step(cur, forward);
            }
            BOOST_REQUIRE_EQUAL(cur.row(), forward ? i : m.rows - 1 - i);
            check_row(cur, m, info[k]);
            check_value_of_first_entry(cur, info[k], m);
        }
    }
}

// Reads each column by jumping over its null runs, as a reader that parks
// its cursors does.
void scan_parked(pq::reader& r, const model& m, const std::vector<read_column>& info, bool forward) {
    for (size_t k = 0; k < r.columns(); ++k) {
        auto& cur = r.cursor(k);
        uint64_t row = forward ? 0 : m.rows - 1;
        while (true) {
            if (!cur.try_seek(row)) {
                cur.seek(row).get();
            }
            check_row(cur, m, info[k]);
            const uint64_t n = std::max<uint64_t>(1, forward ? cur.null_rows_forward() : cur.null_rows_backward());
            if (forward ? row + n >= m.rows : row < n) {
                break;
            }
            row = forward ? row + n : row - n;
        }
    }
}

void read_and_check(const written_file& f, const model& m) {
    auto o = open_file(f.data, f.result.footer.crc32);
    BOOST_REQUIRE_EQUAL(o->summary.rows(), m.rows);
    BOOST_REQUIRE_EQUAL(o->summary.row_groups(), f.result.row_groups);
    BOOST_REQUIRE_EQUAL(o->summary.key_value_metadata().size(), 1);
    auto [projection, info] = projection_of(m, o->summary, all_columns(m));
    for (bool forward : {true, false}) {
        for (auto hint : {pq::read_hint::scan, pq::read_hint::single_partition}) {
            local_metadata_cache cache;
            pq::reader r(o->f, o->summary, cache, projection, pq::reader_options{
                .direction = forward ? pq::read_direction::forward : pq::read_direction::backward,
                .hint = hint,
            });
            scan(r, m, info, forward);
            if (hint == pq::read_hint::scan) {
                scan_parked(r, m, info, forward);
            }
            r.close().get();
        }
    }
}

// Random models.

std::string random_bytes(std::mt19937_64& rng, size_t len, uint64_t seed) {
    std::string s(len, '\0');
    std::mt19937_64 g(seed);
    for (auto& ch : s) {
        ch = char(g());
    }
    return s;
}

struct value_generator {
    physical_type type;
    int32_t type_length = 0;
    // 0 for unlimited cardinality.
    uint64_t cardinality = 0;
    size_t max_length = 20;

    std::string operator()(std::mt19937_64& rng) const {
        const uint64_t v = cardinality ? rng() % cardinality : rng();
        switch (type) {
        case physical_type::boolean: return std::string(1, char(v & 1));
        case physical_type::int32: return bytes_of_value(int32_t(v));
        case physical_type::int64: return bytes_of_value(int64_t(v));
        case physical_type::float_: return bytes_of_value(uint32_t(v));
        case physical_type::double_: return bytes_of_value(uint64_t(v));
        case physical_type::fixed_len_byte_array: return random_bytes(rng, type_length, v);
        default: return random_bytes(rng, cardinality ? v % (max_length + 1) : rng() % (max_length + 1), v);
        }
    }
};

// Decides, row by row, whether a column has content. Either independent
// coin flips, or alternating runs, which the writer encodes as RLE runs.
struct presence {
    double probability = 0.5;
    bool runs = false;
    uint64_t left = 0;
    bool current = false;

    bool operator()(std::mt19937_64& rng) {
        if (!runs) {
            return std::uniform_real_distribution<>(0, 1)(rng) < probability;
        }
        if (left == 0) {
            current = std::uniform_real_distribution<>(0, 1)(rng) < probability;
            left = 1 + rng() % 200;
        }
        --left;
        return current;
    }
};

physical_type random_type(std::mt19937_64& rng) {
    static constexpr physical_type types[] = {physical_type::boolean, physical_type::int32, physical_type::int64,
            physical_type::float_, physical_type::double_, physical_type::byte_array, physical_type::fixed_len_byte_array};
    return types[rng() % std::size(types)];
}

value_generator random_values(std::mt19937_64& rng, physical_type t) {
    static constexpr uint64_t cardinalities[] = {0, 2, 10, 1000};
    return value_generator{.type = t, .type_length = t == physical_type::fixed_len_byte_array ? 16 : 0,
            .cardinality = cardinalities[rng() % std::size(cardinalities)], .max_length = size_t(rng() % 3 ? 20 : 300)};
}

presence random_presence(std::mt19937_64& rng) {
    static constexpr double probabilities[] = {0.0, 0.03, 0.5, 0.97, 1.0};
    return presence{.probability = probabilities[rng() % std::size(probabilities)], .runs = bool(rng() % 2)};
}

pq::column_options options_of(const value_generator& g) {
    pq::column_options o;
    if (g.type == physical_type::fixed_len_byte_array) {
        o.type_length = g.type_length;
    }
    return o;
}

// A model with random columns of every shape that our mapping uses.
model random_model(std::mt19937_64& rng, size_t rows, size_t columns) {
    pq::schema s;
    const auto scylla = s.add_group(pq::schema::root, "_scylla", repetition_type::required);
    using filler = std::function<void(model&, std::mt19937_64&)>;
    std::vector<filler> fillers;
    for (size_t i = 0; i < columns; ++i) {
        const auto t = random_type(rng);
        const auto values = random_values(rng, t);
        auto has = random_presence(rng);
        const auto name = fmt::format("c{}", i);
        switch (rng() % 9) {
        case 0: { // An optional value.
            const auto c = s.add_column(pq::schema::root, name, repetition_type::optional, t, options_of(values));
            fillers.push_back([=] (model& m, std::mt19937_64& rng) mutable {
                for (size_t r = 0; r < m.rows; ++r) {
                    if (has(rng)) {
                        m.columns[c][r].push_back(entry{.def = 1, .value = values(rng)});
                    }
                }
            });
            break;
        }
        case 1: { // A required value.
            const auto c = s.add_column(scylla, name, repetition_type::required, t, options_of(values));
            fillers.push_back([=] (model& m, std::mt19937_64& rng) mutable {
                for (size_t r = 0; r < m.rows; ++r) {
                    m.columns[c][r].push_back(entry{.def = 0, .value = values(rng)});
                }
            });
            break;
        }
        case 2: { // An optional value in an optional group.
            const auto g = s.add_group(pq::schema::root, name, repetition_type::optional);
            const auto c = s.add_column(g, "v", repetition_type::optional, t, options_of(values));
            fillers.push_back([=] (model& m, std::mt19937_64& rng) mutable {
                for (size_t r = 0; r < m.rows; ++r) {
                    if (has(rng)) {
                        if (rng() % 3) {
                            m.columns[c][r].push_back(entry{.def = 2, .value = values(rng)});
                        } else {
                            m.columns[c][r].push_back(entry{.def = 1});
                        }
                    }
                }
            });
            break;
        }
        case 3:
        case 4: { // A list, with optional or required elements.
            const bool required = rng() % 2;
            const auto c = s.add_list(scylla, name, t, options_of(values), required ? repetition_type::required : repetition_type::optional);
            const uint16_t value_def = required ? 2 : 3;
            const size_t max_elements = rng() % 4 ? 5 : 100;
            fillers.push_back([=] (model& m, std::mt19937_64& rng) mutable {
                for (size_t r = 0; r < m.rows; ++r) {
                    if (!has(rng)) {
                        continue;
                    }
                    const size_t n = rng() % (max_elements + 1);
                    if (n == 0) {
                        m.columns[c][r].push_back(entry{.def = 1});
                    }
                    for (size_t k = 0; k < n; ++k) {
                        const uint16_t rep = k ? 1 : 0;
                        if (!required && rng() % 4 == 0) {
                            m.columns[c][r].push_back(entry{.rep = rep, .def = 2});
                        } else {
                            m.columns[c][r].push_back(entry{.rep = rep, .def = value_def, .value = values(rng)});
                        }
                    }
                }
            });
            break;
        }
        case 5: { // A map.
            const auto key_values = random_values(rng, physical_type::byte_array);
            const auto mc = s.add_map(pq::schema::root, name, physical_type::byte_array, {}, t, options_of(values));
            fillers.push_back([=] (model& m, std::mt19937_64& rng) mutable {
                for (size_t r = 0; r < m.rows; ++r) {
                    if (!has(rng)) {
                        continue;
                    }
                    const size_t n = rng() % 6;
                    if (n == 0) {
                        m.columns[mc.key][r].push_back(entry{.def = 1});
                        m.columns[mc.value][r].push_back(entry{.def = 1});
                    }
                    for (size_t k = 0; k < n; ++k) {
                        const uint16_t rep = k ? 1 : 0;
                        m.columns[mc.key][r].push_back(entry{.rep = rep, .def = 2, .value = key_values(rng)});
                        if (rng() % 4 == 0) {
                            m.columns[mc.value][r].push_back(entry{.rep = rep, .def = 2});
                        } else {
                            m.columns[mc.value][r].push_back(entry{.rep = rep, .def = 3, .value = values(rng)});
                        }
                    }
                }
            });
            break;
        }
        case 6: { // A late column: its first value comes late in the file.
            const auto c = s.add_column(pq::schema::root, name, repetition_type::optional, t, options_of(values));
            fillers.push_back([=] (model& m, std::mt19937_64& rng) mutable {
                const size_t start = rng() % (m.rows + 1);
                for (size_t r = start; r < m.rows; ++r) {
                    if (has(rng)) {
                        m.columns[c][r].push_back(entry{.def = 1, .value = values(rng)});
                    }
                }
            });
            break;
        }
        case 7: { // Always present, and null in the whole file.
            pq::column_options o = options_of(values);
            o.always_present = true;
            s.add_column(pq::schema::root, name, repetition_type::optional, t, o);
            break;
        }
        default: { // Null in the whole file, so left out of the file.
            s.add_column(scylla, name, repetition_type::optional, t, options_of(values));
            break;
        }
        }
    }
    model m(std::move(s));
    m.add_rows(rows);
    for (auto& f : fillers) {
        f(m, rng);
    }
    return m;
}

pq::writer_config random_config(std::mt19937_64& rng) {
    static constexpr pq::compression_codec codecs[] = {pq::compression_codec::uncompressed, pq::compression_codec::snappy,
            pq::compression_codec::gzip, pq::compression_codec::lz4_raw, pq::compression_codec::zstd};
    static constexpr uint64_t page_set_sizes[] = {256, 2048, 128 << 10};
    static constexpr uint32_t page_set_rows[] = {1, 3, 64, 1 << 14};
    static constexpr uint64_t min_page_sizes[] = {0, 1024, 8 << 10};
    static constexpr uint64_t row_group_sizes[] = {2048, 64 << 10, 8 << 20};
    static constexpr uint64_t dictionary_limits[] = {0, 64, 4 << 10};
    return pq::writer_config{
        .row_group_size = row_group_sizes[rng() % std::size(row_group_sizes)],
        .page_set_size = page_set_sizes[rng() % std::size(page_set_sizes)],
        .min_page_size = min_page_sizes[rng() % std::size(min_page_sizes)],
        .dictionary_page_limit = dictionary_limits[rng() % std::size(dictionary_limits)],
        .max_page_values = rng() % 4 ? 1u << 16 : 100,
        .max_page_set_rows = page_set_rows[rng() % std::size(page_set_rows)],
        .codec = codecs[rng() % std::size(codecs)],
    };
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_read_random_files) {
    std::mt19937_64 rng(1);
    for (int i = 0; i < 40; ++i) {
        const auto m = random_model(rng, 1 + rng() % 1500, 1 + rng() % 12);
        const auto cfg = random_config(rng);
        BOOST_TEST_MESSAGE(fmt::format("file {}: {} rows, codec {}, row groups of {} bytes, page sets of {} bytes and {} rows",
                i, m.rows, int(cfg.codec), cfg.row_group_size, cfg.page_set_size, cfg.max_page_set_rows));
        const auto f = write_model(m, cfg);
        read_and_check(f, m);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_every_codec) {
    for (auto codec : {pq::compression_codec::uncompressed, pq::compression_codec::snappy, pq::compression_codec::gzip,
            pq::compression_codec::lz4_raw, pq::compression_codec::zstd}) {
        std::mt19937_64 rng(2);
        const auto m = random_model(rng, 1500, 12);
        auto cfg = random_config(rng);
        cfg.codec = codec;
        read_and_check(write_model(m, cfg), m);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_empty_file) {
    pq::schema s;
    s.add_column(pq::schema::root, "a", repetition_type::optional, physical_type::int64);
    model m(std::move(s));
    const auto f = write_model(m, {});
    auto o = open_file(f.data, f.result.footer.crc32);
    BOOST_REQUIRE_EQUAL(o->summary.rows(), 0);
    BOOST_REQUIRE_EQUAL(o->summary.row_groups(), 0);
    BOOST_REQUIRE_EQUAL(o->summary.columns().size(), 0);
}

// Values larger than a contiguous piece of 128 KiB, in every compression
// codec.
SEASTAR_THREAD_TEST_CASE(test_read_large_values) {
    for (auto codec : {pq::compression_codec::uncompressed, pq::compression_codec::snappy, pq::compression_codec::gzip,
            pq::compression_codec::lz4_raw, pq::compression_codec::zstd}) {
        pq::schema s;
        const auto big = s.add_column(pq::schema::root, "big", repetition_type::optional, physical_type::byte_array);
        const auto list = s.add_list(pq::schema::root, "list", physical_type::byte_array);
        model m(std::move(s));
        m.add_rows(40);
        std::mt19937_64 rng(3);
        for (size_t r = 0; r < m.rows; ++r) {
            if (r % 3 == 0) {
                const size_t len = r % 2 ? 300 << 10 : 1 + rng() % 1000;
                m.columns[big][r].push_back(entry{.def = 1, .value = random_bytes(rng, len, rng())});
            }
            if (r % 5 == 1) {
                for (int k = 0; k < 3; ++k) {
                    m.columns[list][r].push_back(entry{.rep = uint16_t(k ? 1 : 0), .def = 3, .value = random_bytes(rng, 200 << 10, rng())});
                }
            }
        }
        auto cfg = pq::writer_config{.codec = codec};
        cfg.page_set_size = 256 << 10;
        read_and_check(write_model(m, cfg), m);
    }
}

// A reader with a permit charges each decoded page to it. It asks for the
// memory before it decompresses the page, so that under memory pressure the
// semaphore makes it wait there instead of killing it later.
SEASTAR_THREAD_TEST_CASE(test_page_memory_is_charged_to_the_permit) {
    pq::schema s;
    const auto big = s.add_column(pq::schema::root, "big", repetition_type::optional, physical_type::byte_array);
    model m(std::move(s));
    m.add_rows(4);
    // A value that compresses well, so that its page is small on disk. The
    // other rows have values too, so that each row gets a page of its own.
    const size_t big_size = 1 << 20;
    for (size_t r = 0; r < m.rows; ++r) {
        m.columns[big][r].push_back(entry{.def = 1, .value = r == 2 ? std::string(big_size, 'z') : std::string(10, 'a')});
    }
    auto cfg = pq::writer_config{.codec = pq::compression_codec::lz4_raw};
    cfg.min_page_size = 0;
    cfg.max_page_set_rows = 1;
    const auto f = write_model(m, cfg);
    auto o = open_file(f.data, f.result.footer.crc32);

    const ssize_t memory = 1 << 20;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, memory, 100,
            utils::updateable_value<uint32_t>(2));
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    auto sponge = semaphore.obtain_permit(nullptr, "sponge", 0, db::no_timeout, {}).get();

    local_metadata_cache cache;
    const auto [projection, info] = projection_of(m, o->summary, {big});
    pq::reader r(o->f, o->summary, cache, projection, pq::reader_options{.permit = permit});
    auto& c = r.cursor(0);
    // The scan's read-ahead fetches the whole file with the first page.
    c.seek(0).get();
    const auto reads = r.stats().reads;
    BOOST_REQUIRE_LT(semaphore.consumed_resources().memory, memory);

    // The sponge takes all memory beyond the serialize limit, and becomes
    // the only permit that gets more.
    auto sponge_units = sponge.request_memory(3 * memory).get();
    sponge_units.add(sponge.request_memory(1).get());
    auto load = c.seek(2);
    // In debug mode, the load may yield before it asks for memory.
    for (int i = 0; i < 100 && !load.available() && !semaphore.get_stats().reads_enqueued_for_memory; ++i) {
        seastar::thread::yield();
    }
    BOOST_REQUIRE(!load.available());
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_memory, 1);
    sponge_units.reset_to_zero();
    load.get();
    BOOST_REQUIRE_EQUAL(r.stats().reads, reads);
    check_row(c, m, info[0]);
    // The decompressed page is charged while the cursor holds it.
    BOOST_REQUIRE_GE(semaphore.consumed_resources().memory, ssize_t(big_size));

    r.release_pages();
    BOOST_REQUIRE_LT(semaphore.consumed_resources().memory, 64 << 10);
    // A released cursor reads its page again.
    c.seek(2).get();
    check_row(c, m, info[0]);
    BOOST_REQUIRE_GT(r.stats().reads, reads);
    r.close().get();
}

namespace {

std::string string_of(const fragmented_temporary_buffer& b) {
    std::string r;
    for (const auto& frag : b) {
        BOOST_REQUIRE_LE(frag.size(), 128 << 10);
        r.append(frag.get(), frag.size());
    }
    return r;
}

std::string pattern(size_t n, char seed) {
    std::string r(n, 0);
    for (size_t i = 0; i < n; ++i) {
        r[i] = char(seed + i * 7 + i / 251);
    }
    return r;
}

} // anonymous namespace

// The metadata cache keeps copies of bytes in its LSA region, as index
// entries of its LRU. A copy that a reader gets is charged to the reader's
// permit while it lives. An entry that is evicted while a reader waits for
// memory is a miss. A second insertion of a key keeps the first entry, and
// an insertion that fails to allocate leaves no entry.
SEASTAR_THREAD_TEST_CASE(test_metadata_cache) {
    local_metadata_cache tc;
    auto& cache = tc.cache;
    const auto index_key = pq::metadata_cache::index_key(3);
    const auto dictionary_key = pq::metadata_cache::dictionary_key(3, 5);
    BOOST_REQUIRE(index_key != pq::metadata_cache::dictionary_key(3, 0));
    const auto small = pattern(1000, 1);
    // Larger than a contiguous piece of 128 KiB.
    const auto large = pattern(300 << 10, 2);
    const fragmented_temporary_buffer small_bytes(small.data(), small.size());
    const fragmented_temporary_buffer large_bytes(large.data(), large.size());

    BOOST_REQUIRE(!cache.get(index_key, std::nullopt).get());
    cache.insert(index_key, pq::byte_array_view(small_bytes));
    cache.insert(dictionary_key, pq::byte_array_view(large_bytes));
    BOOST_REQUIRE(cache.contains(index_key));
    BOOST_REQUIRE(cache.contains(dictionary_key));
    BOOST_REQUIRE_EQUAL(cache.size(), 2);
    BOOST_REQUIRE_GE(cache.memory_usage(), small.size() + large.size());
    BOOST_REQUIRE_EQUAL(tc.stats.used_bytes, cache.memory_usage());
    BOOST_REQUIRE_GE(tc.region.occupancy().used_space(), cache.memory_usage());

    const auto other = pattern(1000, 9);
    cache.insert(index_key, pq::byte_array_view(fragmented_temporary_buffer(other.data(), other.size())));
    BOOST_REQUIRE_EQUAL(cache.size(), 2);
    BOOST_REQUIRE(string_of(cache.get(index_key, std::nullopt).get()->bytes) == small);
    BOOST_REQUIRE(string_of(cache.get(dictionary_key, std::nullopt).get()->bytes) == large);

    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, 1 << 20, 100,
            utils::updateable_value<uint32_t>(2));
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    {
        auto copy = cache.get(dictionary_key, permit).get();
        BOOST_REQUIRE(copy);
        BOOST_REQUIRE_EQUAL(semaphore.consumed_resources().memory, ssize_t(large.size()));
        BOOST_REQUIRE(string_of(copy->bytes) == large);
    }
    BOOST_REQUIRE_EQUAL(semaphore.consumed_resources().memory, 0);

    // The sponge takes all memory beyond the serialize limit, so the next
    // copy waits for memory.
    auto sponge = semaphore.obtain_permit(nullptr, "sponge", 0, db::no_timeout, {}).get();
    auto sponge_units = sponge.request_memory(3 << 20).get();
    sponge_units.add(sponge.request_memory(1).get());
    auto waiting = cache.get(dictionary_key, permit);
    BOOST_REQUIRE(!waiting.available());
    // The LRU evicts the least recently used entry first.
    tc.lru_list.evict();
    BOOST_REQUIRE(!cache.contains(index_key));
    BOOST_REQUIRE(cache.contains(dictionary_key));
    tc.lru_list.evict();
    BOOST_REQUIRE_EQUAL(cache.size(), 0);
    BOOST_REQUIRE_EQUAL(cache.memory_usage(), 0);
    BOOST_REQUIRE_EQUAL(tc.stats.used_bytes, 0);
    sponge_units.reset_to_zero();
    BOOST_REQUIRE(!waiting.get());
    BOOST_REQUIRE_EQUAL(semaphore.consumed_resources().memory, 0);

    auto& injector = memory::local_failure_injector();
    for (uint64_t i = 0;; ++i) {
        injector.fail_after(i);
        cache.insert(dictionary_key, pq::byte_array_view(large_bytes));
        injector.cancel();
        if (cache.contains(dictionary_key)) {
            BOOST_REQUIRE_EQUAL(tc.stats.used_bytes, cache.memory_usage());
            BOOST_REQUIRE(string_of(cache.get(dictionary_key, std::nullopt).get()->bytes) == large);
            cache.evict_gently().get();
        }
        BOOST_REQUIRE_EQUAL(cache.size(), 0);
        BOOST_REQUIRE_EQUAL(tc.stats.used_bytes, 0);
        if (!injector.failed()) {
            break;
        }
    }
}

// The cache keeps the OffsetIndexes of a row group in compact form. A hit
// copies the records of the projected columns only, charges their memory
// to the permit before it allocates them, and matches a decoding of the
// block. In the first file, column a has more pages than one step of the
// copy takes. In the second, column b has a dictionary. An entry that is
// evicted while a copy waits for memory is a miss.
SEASTAR_THREAD_TEST_CASE(test_metadata_cache_index) {
    pq::schema s;
    const auto a = s.add_column(pq::schema::root, "a", repetition_type::optional, physical_type::int64);
    const auto b = s.add_column(pq::schema::root, "b", repetition_type::optional, physical_type::byte_array);
    const auto c = s.add_column(pq::schema::root, "c", repetition_type::optional, physical_type::int64);
    model m(std::move(s));
    m.add_rows(20000);
    for (size_t r = 0; r < m.rows; ++r) {
        m.columns[a][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(r))});
        m.columns[b][r].push_back(entry{.def = 1, .value = fmt::format("value {}", r % 3)});
        if (r % 2) {
            m.columns[c][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(r))});
        }
    }
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, 1 << 20, 100,
            utils::updateable_value<uint32_t>(2));
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    const std::vector<pq::leaf_index> all{0, 1, 2};
    for (const bool many_pages : {true, false}) {
        BOOST_TEST_CONTEXT("many pages " << many_pages) {
            const auto f = write_model(m, many_pages
                    ? pq::writer_config{.row_group_size = 64 << 20, .page_set_size = 1, .min_page_size = 0}
                    : pq::writer_config{.row_group_size = 64 << 20});
            auto o = open_file(f.data, f.result.footer.crc32);
            BOOST_REQUIRE_EQUAL(o->summary.row_groups(), 1);
            const auto full = read_index(*o, 0);
            if (many_pages) {
                BOOST_REQUIRE_GT(full->pages(0), 8192 * 2);
            } else {
                BOOST_REQUIRE(o->summary.has_dictionary(0, 1));
            }
            const auto& rg = o->summary.row_group(0);
            const auto compact = pq::decode_offset_index_block(o->summary, 0,
                    fragmented_temporary_buffer(f.data.data() + rg.offset_index_offset, rg.offset_index_length)).get();
            BOOST_REQUIRE_EQUAL(compact.size_bytes(), (3 + full->pages(0) + full->pages(1) + full->pages(2)) * 12);

            local_metadata_cache tc;
            auto& cache = tc.cache;
            BOOST_REQUIRE(!cache.get_index(o->summary, 0, all, permit).get());
            cache.insert(pq::metadata_cache::index_key(0), pq::byte_array_view(compact));
            for (const std::vector<pq::leaf_index>& columns : {std::vector<pq::leaf_index>{0}, {1, 2}, {2}, all}) {
                BOOST_TEST_CONTEXT("columns " << fmt::to_string(fmt::join(columns, ","))) {
                    auto hit = cache.get_index(o->summary, 0, columns, permit).get();
                    BOOST_REQUIRE(hit);
                    const auto& idx = hit->index;
                    size_t pages = 0;
                    for (auto col : columns) {
                        pages += full->pages(col);
                        BOOST_REQUIRE_EQUAL(idx.pages(col), full->pages(col));
                        BOOST_REQUIRE_EQUAL(idx.chunk_start(col), full->chunk_start(col));
                        if (o->summary.has_dictionary(0, col)) {
                            BOOST_REQUIRE_EQUAL(idx.dictionary_size(col), full->dictionary_size(col));
                        }
                        for (uint32_t j = 0; j < idx.pages(col); ++j) {
                            const auto p = idx.page_at(col, j);
                            const auto q = full->page_at(col, j);
                            BOOST_REQUIRE_EQUAL(p.offset, q.offset);
                            BOOST_REQUIRE_EQUAL(p.size, q.size);
                            BOOST_REQUIRE_EQUAL(p.first_row, q.first_row);
                            BOOST_REQUIRE_EQUAL(idx.page_end_row(col, j), full->page_end_row(col, j));
                        }
                        BOOST_REQUIRE_EQUAL(idx.page_of(col, 12345), full->page_of(col, 12345));
                    }
                    BOOST_REQUIRE_EQUAL(idx.memory_usage(), pq::row_group_index::memory_usage(columns.size(), pages));
                    BOOST_REQUIRE_EQUAL(semaphore.consumed_resources().memory, ssize_t(idx.memory_usage()));
                }
                BOOST_REQUIRE_EQUAL(semaphore.consumed_resources().memory, 0);
            }
            if (!many_pages) {
                continue;
            }

            // The sponge takes all memory beyond the serialize limit, so
            // the next copy waits for memory.
            auto sponge = semaphore.obtain_permit(nullptr, "sponge", 0, db::no_timeout, {}).get();
            auto sponge_units = sponge.request_memory(3 << 20).get();
            sponge_units.add(sponge.request_memory(1).get());
            auto waiting = cache.get_index(o->summary, 0, all, permit);
            BOOST_REQUIRE(!waiting.available());
            tc.lru_list.evict();
            BOOST_REQUIRE_EQUAL(cache.size(), 0);
            sponge_units.reset_to_zero();
            BOOST_REQUIRE(!waiting.get());
            BOOST_REQUIRE_EQUAL(semaphore.consumed_resources().memory, 0);
        }
    }
}

// A reader charges its copies of OffsetIndexes and dictionaries to its
// permit, and releases them with the reader, both when it populates the
// cache and when it reads from it. In an uncompressed file, a dictionary
// page as fetched shares a read buffer that the permit pays for. Neither
// the cache nor the dictionaries keep such a buffer.
SEASTAR_THREAD_TEST_CASE(test_cached_metadata_holds_no_reader_memory) {
    pq::schema s;
    const auto a = s.add_column(pq::schema::root, "a", repetition_type::optional, physical_type::byte_array);
    const auto b = s.add_column(pq::schema::root, "b", repetition_type::optional, physical_type::int64);
    model m(std::move(s));
    m.add_rows(20000);
    for (size_t r = 0; r < m.rows; ++r) {
        m.columns[a][r].push_back(entry{.def = 1, .value = fmt::format("value {}", r % 7)});
        m.columns[b][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(r % 5))});
    }
    const auto f = write_model(m, pq::writer_config{.row_group_size = 2048, .page_set_size = 256, .codec = pq::compression_codec::uncompressed});
    auto o = open_file(f.data, f.result.footer.crc32);
    BOOST_REQUIRE_GT(o->summary.row_groups(), 2);
    BOOST_REQUIRE(o->summary.has_dictionary(0, 0));

    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, 100 << 20, 100,
            utils::updateable_value<uint32_t>(2));
    auto stop = deferred_stop(semaphore);
    local_metadata_cache cache;
    const auto [projection, info] = projection_of(m, o->summary, all_columns(m));
    // The first read populates the cache, and the second reads from it.
    for (int i = 0; i < 2; ++i) {
        auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
        uint64_t index_reads = 0;
        {
            pq::reader r(o->f, o->summary, cache, projection, pq::reader_options{.permit = permit});
            scan(r, m, info, true);
            BOOST_REQUIRE_GT(semaphore.consumed_resources().memory, 0);
            r.close().get();
            index_reads = r.stats().index_reads;
        }
        BOOST_REQUIRE_EQUAL(semaphore.consumed_resources().memory, 0);
        BOOST_REQUIRE_GT(cache.cache.size(), o->summary.row_groups());
        // Only the first read reads OffsetIndex blocks. A block that lies
        // in the read of a page needs no read of its own.
        if (i) {
            BOOST_REQUIRE_EQUAL(index_reads, 0);
        } else {
            BOOST_REQUIRE_LE(index_reads, o->summary.row_groups());
        }
    }
}

// Readers that miss the same entries at the same time populate the cache
// once.
SEASTAR_THREAD_TEST_CASE(test_metadata_cache_concurrent_population) {
    std::mt19937_64 rng(11);
    const auto m = random_model(rng, 1500, 8);
    auto cfg = random_config(rng);
    cfg.row_group_size = 16 << 10;
    cfg.dictionary_page_limit = 4 << 10;
    const auto f = write_model(m, cfg);
    auto o = open_file(f.data, f.result.footer.crc32);
    const auto [projection, info] = projection_of(m, o->summary, all_columns(m));
    auto read = [&] (pq::metadata_cache& cache) {
        pq::reader r(o->f, o->summary, cache, projection);
        scan(r, m, info, true);
        r.close().get();
        return r.stats().index_reads;
    };
    local_metadata_cache single;
    const auto index_reads = read(single);
    BOOST_REQUIRE_GE(single.cache.size(), o->summary.row_groups());

    // Each read yields, so the readers miss the same entries.
    o->impl->set_yield(true);
    local_metadata_cache shared;
    uint64_t shared_index_reads = 0;
    auto read_shared = [&] { shared_index_reads += read(shared); };
    when_all_succeed(seastar::async(read_shared), seastar::async(read_shared), seastar::async(read_shared)).get();
    BOOST_REQUIRE_GT(shared_index_reads, index_reads);
    BOOST_REQUIRE_EQUAL(shared.cache.size(), single.cache.size());
    BOOST_REQUIRE_EQUAL(shared.cache.memory_usage(), single.cache.memory_usage());
    BOOST_REQUIRE_EQUAL(shared.stats.used_bytes, single.stats.used_bytes);
}

// Pages above 128 KiB decode without an allocation above 128 KiB. The
// compression codec contexts are allocated once, and do not count. The
// values compress, so that the pages are stored compressed.
SEASTAR_THREAD_TEST_CASE(test_read_large_values_allocations) {
    for (auto codec : {pq::compression_codec::uncompressed, pq::compression_codec::snappy, pq::compression_codec::gzip,
            pq::compression_codec::lz4_raw, pq::compression_codec::zstd}) {
        BOOST_TEST_CONTEXT("codec " << int(codec)) {
            pq::schema s;
            const auto big = s.add_column(pq::schema::root, "big", repetition_type::optional, physical_type::byte_array);
            model m(std::move(s));
            m.add_rows(6);
            std::mt19937_64 rng(5);
            for (size_t r = 0; r < m.rows; ++r) {
                auto value = random_bytes(rng, 300 << 10, rng());
                for (auto& ch : value) {
                    ch = "abcd"[ch & 3];
                }
                m.columns[big][r].push_back(entry{.def = 1, .value = std::move(value)});
            }
            const auto f = write_model(m, pq::writer_config{.codec = codec});
            auto o = open_file(f.data, f.result.footer.crc32);
            auto [projection, info] = projection_of(m, o->summary, {big});
            // Compares the values in place, so that the check allocates nothing.
            auto read = [&] {
                local_metadata_cache cache;
                pq::reader r(o->f, o->summary, cache, projection);
                auto& c = r.cursor(0);
                for (size_t row = 0; row < m.rows; ++row) {
                    c.seek(row).get();
                    BOOST_REQUIRE(c.has_value());
                    const auto& expected = *m.columns[big][row][0].value;
                    const auto& v = c.value<pq::byte_array_view>();
                    BOOST_REQUIRE_EQUAL(v.size_bytes(), expected.size());
                    size_t pos = 0;
                    for (bytes_view frag : v) {
                        BOOST_REQUIRE(std::memcmp(frag.data(), expected.data() + pos, frag.size()) == 0);
                        pos += frag.size();
                    }
                }
                r.close().get();
            };
            read();
            const auto before = seastar::memory::stats();
            {
                const seastar::memory::scoped_large_allocation_warning_threshold guard((128 << 10) + 1);
                read();
            }
            BOOST_REQUIRE_EQUAL(seastar::memory::stats().large_allocations(), before.large_allocations());
        }
    }
}

// A scan of a row group of many tiny pages fills its window with
// thousands of pages at once. The fetcher keeps them without an allocation
// above 128 KiB.
SEASTAR_THREAD_TEST_CASE(test_read_many_small_pages_allocations) {
    pq::schema s;
    const auto v = s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::int64);
    model m(std::move(s));
    m.add_rows(20000);
    for (size_t r = 0; r < m.rows; ++r) {
        m.columns[v][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(r))});
    }
    // A page per row.
    const auto f = write_model(m, pq::writer_config{.page_set_size = 1, .min_page_size = 0, .dictionary_page_limit = 0});
    auto o = open_file(f.data, f.result.footer.crc32);
    BOOST_REQUIRE_EQUAL(o->summary.row_groups(), 1);
    BOOST_REQUIRE_EQUAL(read_index(*o, 0)->pages(0), m.rows);
    auto [projection, info] = projection_of(m, o->summary, {v});
    auto read = [&] {
        local_metadata_cache cache;
        pq::reader r(o->f, o->summary, cache, projection);
        auto& c = r.cursor(0);
        c.seek(0).get();
        for (size_t row = 0; row < m.rows; ++row) {
            if (row) {
                c.next().get();
            }
            BOOST_REQUIRE_EQUAL(c.value<int64_t>(), int64_t(row));
        }
        BOOST_REQUIRE_GT(r.stats().max_pending_bytes, 128 << 10);
        r.close().get();
    };
    read();
    const auto before = seastar::memory::stats();
    {
        const seastar::memory::scoped_large_allocation_warning_threshold guard((128 << 10) + 1);
        read();
    }
    BOOST_REQUIRE_EQUAL(seastar::memory::stats().large_allocations(), before.large_allocations());
}

// A page that declares more uncompressed bytes than the largest chunk of its
// row group is malformed, before anything is allocated for it.
SEASTAR_THREAD_TEST_CASE(test_page_size_bound) {
    for (auto codec : {pq::compression_codec::lz4_raw, pq::compression_codec::zstd}) {
        pq::schema s;
        const auto v = s.add_column(pq::schema::root, "v", repetition_type::optional, physical_type::byte_array);
        model m(std::move(s));
        m.add_rows(3);
        // Distinct values, too large for a dictionary, so that the data page
        // holds 6000 bytes of values.
        std::mt19937_64 rng(6);
        for (size_t r = 0; r < m.rows; ++r) {
            m.columns[v][r].push_back(entry{.def = 1, .value = random_bytes(rng, 2000, rng())});
        }
        const auto f = write_model(m, pq::writer_config{.codec = codec});
        auto o = open_file(f.data, f.result.footer.crc32);
        local_metadata_cache cache;
        pq::reader r(o->f, o->summary, cache, projection_of(m, o->summary, {v}).first);
        r.cursor(0).seek(0).get();
        r.close().get();
        const auto idx = read_index(*o, 0);
        const auto p = idx->page_at(0, 0);
        const std::string page = f.data.substr(p.offset, p.size);
        const auto& col = o->summary.column(0);
        const uint64_t max = o->summary.row_group(0).max_chunk_uncompressed_size;
        BOOST_REQUIRE_GE(max, 6000);
        pq::decoded_page::check(col, 0, fragmented_temporary_buffer(page.data(), page.size()), m.rows, max).get();
        BOOST_REQUIRE_THROW(pq::decoded_page::check(col, 0, fragmented_temporary_buffer(page.data(), page.size()), m.rows, 5999).get(),
                malformed_sstable_exception);
    }
}

// A page header has no checksum, and the memory that decoding a page asks
// for follows from its entries. A repeated column's page that declares more
// entries than the largest chunk of its row group is malformed, before
// anything is asked for. Without the bound, the damaged header passes the
// other checks and asks for gigabytes.
SEASTAR_THREAD_TEST_CASE(test_page_values_bound) {
    pq::schema s;
    const auto l = s.add_list(pq::schema::root, "l", physical_type::int32);
    model m(std::move(s));
    m.add_rows(10);
    for (size_t r = 0; r < m.rows; ++r) {
        for (uint16_t k = 0; k < 3; ++k) {
            m.columns[l][r].push_back(entry{.rep = uint16_t(k ? 1 : 0), .def = 3, .value = bytes_of_value(int32_t(r * 3 + k))});
        }
    }
    const auto f = write_model(m, pq::writer_config{});
    auto o = open_file(f.data, f.result.footer.crc32);
    const auto idx = read_index(*o, 0);
    const auto p = idx->page_at(0, 0);
    const std::string page = f.data.substr(p.offset, p.size);
    pq::thrift::decoder d(std::span<const char>(page.data(), page.size()));
    auto h = pq::parse_page_header(d);
    const std::string body = page.substr(d.position());
    const auto& col = o->summary.column(0);
    const uint64_t max_values = o->summary.row_group(0).max_chunk_values;
    BOOST_REQUIRE_EQUAL(max_values, 30);
    BOOST_REQUIRE_EQUAL(h.data_page_v2->num_values, 30);
    pq::decoded_page::check(col, 0, fragmented_temporary_buffer(page.data(), page.size()), m.rows,
            std::numeric_limits<uint64_t>::max(), max_values).get();

    h.data_page_v2->num_values = 1 << 30;
    bytes_ostream out;
    pq::write_page_header(h, out);
    std::string damaged;
    for (bytes_view frag : out.fragments()) {
        damaged.append(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    damaged += body;
    const auto unbounded = pq::decoded_page::check(col, 0, fragmented_temporary_buffer(damaged.data(), damaged.size()), m.rows).get();
    BOOST_REQUIRE_GT(unbounded.decode_memory(), size_t(1) << 32);
    BOOST_REQUIRE_THROW(pq::decoded_page::check(col, 0, fragmented_temporary_buffer(damaged.data(), damaged.size()), m.rows,
            std::numeric_limits<uint64_t>::max(), max_values).get(), malformed_sstable_exception);
}

// A small file with several row groups, pages, a late column, a list, and
// long null runs.
static model small_model() {
    pq::schema s;
    const auto a = s.add_column(pq::schema::root, "a", repetition_type::optional, physical_type::int64);
    const auto b = s.add_list(pq::schema::root, "b", physical_type::int32);
    const auto c = s.add_column(pq::schema::root, "c", repetition_type::optional, physical_type::byte_array);
    const auto d = s.add_column(pq::schema::root, "d", repetition_type::optional, physical_type::int32);
    s.add_column(pq::schema::root, "absent", repetition_type::optional, physical_type::int32);
    model m(std::move(s));
    m.add_rows(30);
    for (size_t r = 0; r < m.rows; ++r) {
        if (r % 4 != 1) {
            m.columns[a][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(r * 7))});
        }
        if (r % 3 == 0) {
            for (size_t k = 0; k < r % 5; ++k) {
                m.columns[b][r].push_back(k % 2 ? entry{.rep = uint16_t(k ? 1 : 0), .def = 2}
                        : entry{.rep = uint16_t(k ? 1 : 0), .def = 3, .value = bytes_of_value(int32_t(r + k))});
            }
        }
        if (r == 2 || r == 17 || r == 18) {
            m.columns[c][r].push_back(entry{.def = 1, .value = std::string(r, 'x')});
        }
        if (r >= 20 && r % 2) {
            m.columns[d][r].push_back(entry{.def = 1, .value = bytes_of_value(int32_t(r))});
        }
    }
    return m;
}

static pq::writer_config small_config() {
    return pq::writer_config{
        .row_group_size = 1,
        .page_set_size = 1 << 20,
        .min_page_size = 0,
        .max_page_set_rows = 3,
    };
}

// Every short sequence of seek, next, and previous, against the model.
SEASTAR_THREAD_TEST_CASE(test_cursor_operation_sequences) {
    const auto m = small_model();
    auto cfg = small_config();
    // Row groups of a few page sets.
    cfg.row_group_size = 150;
    const auto f = write_model(m, cfg);
    auto o = open_file(f.data, f.result.footer.crc32);
    BOOST_REQUIRE_GE(o->summary.row_groups(), 3);
    auto [projection, info] = projection_of(m, o->summary, all_columns(m));

    const std::vector<uint64_t> targets{0, 1, 2, 3, 5, 8, 9, 14, 15, 17, 20, 28, 29};
    constexpr unsigned next_op = 1000;
    constexpr unsigned previous_op = 1001;
    std::vector<unsigned> ops(targets.begin(), targets.end());
    ops.push_back(next_op);
    ops.push_back(previous_op);

    uint64_t sequences = 0;
    for (uint64_t start : targets) {
        for (unsigned x : ops) {
            for (unsigned y : ops) {
                for (unsigned z : ops) {
                    local_metadata_cache cache;
                    pq::reader r(o->f, o->summary, cache, projection);
                    const bool sync_first = (x + y + z) % 2;
                    for (size_t k = 0; k < r.columns(); ++k) {
                        auto& cur = r.cursor(k);
                        cur.seek(start).get();
                        check_row(cur, m, info[k]);
                        bool sync = sync_first;
                        for (unsigned op : {x, y, z}) {
                            const uint64_t row = cur.row();
                            if (op == next_op) {
                                if (row + 1 == m.rows) {
                                    continue;
                                }
                                if (!sync || !cur.try_next()) {
                                    cur.next().get();
                                }
                                BOOST_REQUIRE_EQUAL(cur.row(), row + 1);
                            } else if (op == previous_op) {
                                if (row == 0) {
                                    continue;
                                }
                                if (!sync || !cur.try_previous()) {
                                    cur.previous().get();
                                }
                                BOOST_REQUIRE_EQUAL(cur.row(), row - 1);
                            } else {
                                if (!sync || !cur.try_seek(op)) {
                                    cur.seek(op).get();
                                }
                                BOOST_REQUIRE_EQUAL(cur.row(), op);
                            }
                            check_row(cur, m, info[k]);
                            sync = !sync;
                        }
                    }
                    r.close().get();
                    ++sequences;
                    seastar::thread::maybe_yield();
                }
            }
        }
    }
    BOOST_TEST_MESSAGE(fmt::format("{} sequences", sequences));
}

// A reader repositioned to several ranges, as the sstable reader does when
// it skips between partitions and clustering ranges.
SEASTAR_THREAD_TEST_CASE(test_fast_forward) {
    std::mt19937_64 rng(4);
    const auto m = random_model(rng, 2000, 10);
    const auto f = write_model(m, random_config(rng));
    auto o = open_file(f.data, f.result.footer.crc32);
    auto [projection, info] = projection_of(m, o->summary, all_columns(m));
    for (bool forward : {true, false}) {
        for (auto hint : {pq::read_hint::scan, pq::read_hint::single_partition}) {
            local_metadata_cache cache;
            pq::reader r(o->f, o->summary, cache, projection, pq::reader_options{
                .direction = forward ? pq::read_direction::forward : pq::read_direction::backward,
                .hint = hint,
            });
            for (int i = 0; i < 30; ++i) {
                uint64_t begin = rng() % m.rows;
                uint64_t end = std::min<uint64_t>(m.rows, begin + 1 + rng() % 100);
                r.fast_forward_to(begin, end);
                r.seek_all(forward ? begin : end - 1).get();
                for (uint64_t row = begin; row < end; ++row) {
                    for (size_t k = 0; k < r.columns(); ++k) {
                        auto& cur = r.cursor(k);
                        if (row != begin) {
                            step(cur, forward);
                        }
                        check_row(cur, m, info[k]);
                    }
                }
            }
            r.close().get();
        }
    }
}

// Fetcher

namespace {

struct byte_range {
    uint64_t start;
    uint64_t end;
};

bool overlaps(const tests::memory_file_impl::read_record& r, const byte_range& b) {
    return r.offset < b.end && b.start < r.offset + r.size;
}

pq::file_metadata parse_footer(const std::string& data) {
    uint32_t length;
    std::memcpy(&length, data.data() + data.size() - 8, 4);
    const auto footer = std::string_view(data).substr(data.size() - 8 - length, length);
    struct source final : pq::thrift::byte_source {
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

byte_range chunk_range(const pq::column_chunk& c) {
    const uint64_t start = c.meta_data.dictionary_page_offset.value_or(c.meta_data.data_page_offset);
    return {start, start + c.meta_data.total_compressed_size};
}

// A file with columns a, b, c, of which b is null in its second row group.
struct fetch_file {
    model m;
    written_file f;
    std::unique_ptr<opened_file> o;
};

fetch_file make_fetch_file(size_t rows, pq::writer_config cfg) {
    pq::schema s;
    const auto a = s.add_column(pq::schema::root, "a", repetition_type::optional, physical_type::int64);
    const auto b = s.add_column(pq::schema::root, "b", repetition_type::optional, physical_type::int64);
    const auto c = s.add_column(pq::schema::root, "c", repetition_type::optional, physical_type::byte_array);
    model m(std::move(s));
    m.add_rows(rows);
    std::mt19937_64 rng(5);
    for (size_t r = 0; r < rows; ++r) {
        m.columns[a][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(rng()))});
        if (r < rows / 3 || r >= 2 * rows / 3) {
            m.columns[b][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(rng()))});
        }
        m.columns[c][r].push_back(entry{.def = 1, .value = random_bytes(rng, 50, rng())});
    }
    auto f = write_model(m, cfg);
    auto o = open_file(f.data, f.result.footer.crc32);
    return fetch_file{std::move(m), std::move(f), std::move(o)};
}

// The ranges that a read of `columns` needs: their chunks that are not
// entirely null, and the OffsetIndex blocks.
std::vector<byte_range> needed_ranges(const pq::file_metadata& md, const pq::file_summary& s, const std::vector<pq::leaf_index>& columns) {
    std::vector<byte_range> r;
    for (size_t g = 0; g < md.row_groups.size(); ++g) {
        for (auto c : columns) {
            if (!s.all_null(g, c)) {
                r.push_back(chunk_range(md.row_groups[g].columns[c]));
            }
        }
        const auto& rg = s.row_group(g);
        r.push_back({rg.offset_index_offset, rg.offset_index_offset + rg.offset_index_length});
    }
    return r;
}

// Checks the reads of `impl` from the `first` one on: each is aligned to
// `alignment`, is at most 128 KiB, allocates its size, and reads only
// blocks of `alignment` bytes that hold bytes of `needed`. So no read
// reads bytes that the read does not need, beyond the blocks that it reads
// for bytes that it needs. Returns the number of reads that cover bytes of
// several needed ranges.
size_t check_reads(const tests::memory_file_impl& impl, size_t first, uint64_t alignment, std::vector<byte_range> needed) {
    std::ranges::sort(needed, {}, &byte_range::start);
    size_t merged = 0;
    for (size_t i = first; i < impl.reads().size(); ++i) {
        const auto& rd = impl.reads()[i];
        BOOST_REQUIRE_EQUAL(rd.offset % alignment, 0);
        BOOST_REQUIRE_EQUAL(rd.size % alignment, 0);
        BOOST_REQUIRE_LE(rd.size, 128 << 10);
        BOOST_REQUIRE_EQUAL(rd.allocated, rd.size);
        for (uint64_t b = rd.offset; b < rd.offset + rd.size; b += alignment) {
            const byte_range block{b, b + alignment};
            // The first range that ends after the block starts.
            auto it = std::ranges::upper_bound(needed, block.start, {}, &byte_range::start);
            if (it != needed.begin() && std::prev(it)->end > block.start) {
                --it;
            }
            if (it == needed.end() || it->start >= block.end) {
                BOOST_FAIL(fmt::format("a read of {} bytes at {} reads the block at {}, which holds no byte that the read needs",
                        rd.size, rd.offset, block.start));
            }
        }
        size_t touched = 0;
        for (const auto& n : needed) {
            touched += overlaps(rd, n);
        }
        merged += touched > 1;
    }
    return merged;
}

// Returns the number of blocks of `alignment` bytes that the reads of
// `impl` read more than once.
size_t blocks_read_again(const tests::memory_file_impl& impl, uint64_t alignment) {
    std::unordered_map<uint64_t, unsigned> times;
    size_t again = 0;
    for (const auto& rd : impl.reads()) {
        for (uint64_t b = rd.offset; b < rd.offset + rd.size; b += alignment) {
            again += times[b]++ > 0;
        }
    }
    return again;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_fetcher_reads_only_what_is_needed) {
    const size_t rows = 30000;
    // Many row groups, of which those in the middle third have b entirely null.
    pq::writer_config cfg;
    cfg.row_group_size = 100 << 10;
    cfg.page_set_size = 8 << 10;
    cfg.min_page_size = 0;
    cfg.max_page_set_rows = 512;
    auto ff = make_fetch_file(rows, cfg);
    auto& o = *ff.o;
    const auto md = parse_footer(ff.f.data);
    BOOST_REQUIRE_GE(md.row_groups.size(), 3);
    size_t null_chunks = 0;
    for (size_t g = 0; g < md.row_groups.size(); ++g) {
        const auto& bm = md.row_groups[g].columns[1].meta_data;
        if (bm.stats->null_count == bm.num_values) {
            ++null_chunks;
            BOOST_REQUIRE(o.summary.all_null(g, 1));
        }
    }
    BOOST_REQUIRE_GE(null_chunks, 1);

    // A read of a and b must not read c's chunks, or b's chunks that are
    // entirely null.
    const auto needed = needed_ranges(md, o.summary, {0, 1});
    auto [projection, info] = projection_of(ff.m, o.summary, {0, 1});
    for (unsigned alignment : {512, 4096}) {
        o.impl->set_read_alignment(alignment);
        for (bool forward : {true, false}) {
            for (auto hint : {pq::read_hint::scan, pq::read_hint::single_partition}) {
                o.impl->clear_reads();
                local_metadata_cache cache;
                pq::reader r(o.f, o.summary, cache, projection, pq::reader_options{
                    .direction = forward ? pq::read_direction::forward : pq::read_direction::backward,
                    .hint = hint,
                });
                scan(r, ff.m, info, forward);
                r.close().get();
                BOOST_REQUIRE(!o.impl->reads().empty());
                check_reads(*o.impl, 0, alignment, needed);
                const auto& st = r.stats();
                const size_t again = blocks_read_again(*o.impl, alignment);
                BOOST_TEST_MESSAGE(fmt::format("alignment {} forward {} hint {}: {} reads, {} bytes, {} pages, {} misses, at most {} bytes pending, "
                        "{} blocks read again",
                        alignment, forward, int(hint), st.reads, st.bytes_read, st.pages_taken, st.page_misses, st.max_pending_bytes, again));
                BOOST_REQUIRE_EQUAL(st.reads, o.impl->reads().size());
                // An OffsetIndex block that lies in the read of a page needs no read of its own.
                BOOST_REQUIRE_LE(st.index_reads, md.row_groups.size());
                if (hint == pq::read_hint::scan) {
                    // Read-ahead merges the pages of a column into larger requests.
                    BOOST_REQUIRE_LT(st.reads - st.index_reads, st.pages_taken / 4);
                    BOOST_REQUIRE_LE(st.page_misses, md.row_groups.size());
                }
            }
        }
    }
    o.impl->set_read_alignment(4096);
}

// A scan refills its window many times. The pages of a refill start in
// the block where the pages of the previous refill end, and the fetcher
// reuses the request that read the block instead of reading it again.
SEASTAR_THREAD_TEST_CASE(test_fetcher_reads_no_block_twice_in_a_scan) {
    pq::writer_config cfg;
    cfg.row_group_size = 16 << 20;
    cfg.page_set_size = 8 << 10;
    cfg.min_page_size = 0;
    cfg.max_page_set_rows = 512;
    cfg.dictionary_page_limit = 0;
    auto ff = make_fetch_file(30000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_EQUAL(o.summary.row_groups(), 1);
    auto [projection, info] = projection_of(ff.m, o.summary, {0, 1, 2});
    for (unsigned alignment : {512, 4096}) {
        o.impl->set_read_alignment(alignment);
        for (bool forward : {true, false}) {
            o.impl->clear_reads();
            local_metadata_cache cache;
            pq::reader r(o.f, o.summary, cache, projection, pq::reader_options{
                .direction = forward ? pq::read_direction::forward : pq::read_direction::backward,
                .policy = pq::fetch_policy{.budget_per_column = 16 << 10, .min_budget = 32 << 10},
            });
            scan(r, ff.m, info, forward);
            r.close().get();
            const size_t again = blocks_read_again(*o.impl, alignment);
            BOOST_TEST_MESSAGE(fmt::format("alignment {} forward {}: {} reads, {} blocks read again", alignment, forward, r.stats().reads, again));
            // Many refills, each of which reads every column.
            BOOST_REQUIRE_GT(r.stats().reads, 30);
            // What remains are the blocks that two neighbours share when the
            // scan reads them at different times: the two boundaries between
            // the chunks, whose sides it reads at its start and at its end,
            // and the start of the OffsetIndex block. Without the reuse,
            // almost every request reads a block again.
            BOOST_REQUIRE_LE(again, 3);
        }
    }
    o.impl->set_read_alignment(4096);
}

// Ranges are merged into one request only when they share a block or
// their blocks are adjacent, so a merged request reads no block of a chunk
// between them that the read does not need. Here b's chunks are a few
// KiB, and lie between a's chunks and c's chunks.
SEASTAR_THREAD_TEST_CASE(test_fetcher_merges_reads_within_blocks) {
    pq::writer_config cfg;
    cfg.row_group_size = 16 << 10;
    cfg.page_set_size = 4 << 10;
    cfg.min_page_size = 0;
    cfg.max_page_set_rows = 512;
    auto ff = make_fetch_file(6000, cfg);
    auto& o = *ff.o;
    const auto md = parse_footer(ff.f.data);
    BOOST_REQUIRE_GE(md.row_groups.size(), 3);
    const auto b = chunk_range(md.row_groups[0].columns[1]);
    BOOST_TEST_MESSAGE(fmt::format("b's first chunk has {} bytes", b.end - b.start));
    BOOST_REQUIRE_GE(b.end - b.start, 1024);
    BOOST_REQUIRE_LE(b.end - b.start, 4096);

    const auto needed = needed_ranges(md, o.summary, {0, 2});
    auto [projection, info] = projection_of(ff.m, o.summary, {0, 2});
    for (unsigned alignment : {512, 4096}) {
        o.impl->set_read_alignment(alignment);
        for (auto hint : {pq::read_hint::scan, pq::read_hint::single_partition}) {
            o.impl->clear_reads();
            local_metadata_cache cache;
            pq::reader r(o.f, o.summary, cache, projection, pq::reader_options{.hint = hint});
            scan(r, ff.m, info, true);
            r.close().get();
            const auto merged = check_reads(*o.impl, 0, alignment, needed);
            BOOST_TEST_MESSAGE(fmt::format("alignment {} hint {}: {} reads, {} of them merged", alignment, int(hint), o.impl->reads().size(), merged));
            if (alignment == 4096) {
                BOOST_REQUIRE_GT(merged, 0);
            }
        }
    }
    o.impl->set_read_alignment(4096);
}

SEASTAR_THREAD_TEST_CASE(test_fetcher_reads_an_offset_index_block_at_once) {
    auto cfg = pq::writer_config{.page_set_size = 1 << 10, .max_page_set_rows = 64};
    cfg.row_group_size = 64 << 10;
    auto ff = make_fetch_file(20000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_GE(o.summary.row_groups(), 3);
    auto [projection, info] = projection_of(ff.m, o.summary, {0, 1, 2});
    for (uint32_t g = 0; g < o.summary.row_groups(); ++g) {
        const auto& rg = o.summary.row_group(g);
        BOOST_REQUIRE_GT(rg.offset_index_length, 0);
        BOOST_REQUIRE_LE(rg.offset_index_length, 120 << 10);
        o.impl->clear_reads();
        local_metadata_cache cache;
        pq::reader r(o.f, o.summary, cache, projection, pq::reader_options{.hint = pq::read_hint::single_partition});
        r.seek_all(o.summary.row_group_first_row(g) + 3).get();
        r.close().get();
        const byte_range block{rg.offset_index_offset, rg.offset_index_offset + rg.offset_index_length};
        BOOST_REQUIRE_EQUAL(r.stats().index_reads, 1);
        BOOST_REQUIRE(std::ranges::any_of(o.impl->reads(), [&] (const auto& rd) {
            return rd.offset <= block.start && rd.offset + rd.size >= block.end;
        }));

        // A single-partition read starts with one page per column, and
        // reads nothing else.
        const auto idx = read_index(o, g);
        BOOST_REQUIRE(idx);
        std::vector<byte_range> needed{block};
        for (pq::leaf_index c = 0; c < 3; ++c) {
            if (!o.summary.all_null(g, c)) {
                const auto p = idx->page_at(c, idx->page_of(c, 3));
                needed.push_back({p.offset, p.offset + p.size});
                if (o.summary.has_dictionary(g, c)) {
                    needed.push_back({idx->chunk_start(c), idx->chunk_start(c) + idx->dictionary_size(c)});
                }
            }
        }
        check_reads(*o.impl, 0, 4096, needed);
        uint64_t bytes = 0;
        for (const auto& rd : o.impl->reads()) {
            bytes += rd.size;
        }
        BOOST_REQUIRE_EQUAL(r.stats().bytes_read, bytes);
    }
}

// When the window of a scan without a permit reaches a row group whose
// OffsetIndex block is loading, it goes on once the block is loaded,
// without waiting for the next page request.
SEASTAR_THREAD_TEST_CASE(test_fetcher_window_resumes_after_an_index_load) {
    pq::writer_config cfg;
    cfg.row_group_size = 16 << 10;
    cfg.page_set_size = 2 << 10;
    cfg.min_page_size = 0;
    auto ff = make_fetch_file(3000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_GE(o.summary.row_groups(), 3);
    // A page of c in row group 1 that shares no block with an OffsetIndex
    // block, so that only a read of c's pages reads it.
    const auto idx1 = read_index(o, 1);
    BOOST_REQUIRE_GE(idx1->pages(2), 4);
    const auto p = idx1->page_at(2, idx1->pages(2) / 2);
    const byte_range c1{p.offset, p.offset + p.size};
    BOOST_REQUIRE_GE(c1.start, o.summary.region_start(1) + 4096);
    BOOST_REQUIRE_LE(c1.end + 4096, o.summary.row_group(1).offset_index_offset);
    auto reads_c1 = [&] {
        return std::ranges::any_of(o.impl->reads(), [&] (const auto& rd) { return overlaps(rd, c1); });
    };
    // Reads yield, so the OffsetIndex block of row group 1 loads after the
    // window reaches it.
    o.impl->set_yield(true);
    o.impl->clear_reads();
    local_metadata_cache cache;
    pq::fetcher f(o.f, o.summary, cache, {2}, pq::read_direction::forward, pq::read_hint::scan, pq::fetch_policy{});
    auto idx = f.get_index(0).get();
    auto page = f.get_page(0, 2, idx, 0, 0).get();
    for (int i = 0; i < 100 && !reads_c1(); ++i) {
        seastar::yield().get();
    }
    BOOST_REQUIRE(reads_c1());
    f.close().get();
    o.impl->set_yield(false);
}

// With a permit, the fetcher charges each read to it before the read, at
// the size of the buffer that the file allocates, and the charge lives as
// long as the buffer. A fetcher that closed holds nothing.
SEASTAR_THREAD_TEST_CASE(test_fetcher_charges_reads_to_the_permit) {
    pq::writer_config cfg;
    cfg.row_group_size = 1 << 20;
    cfg.page_set_size = 16 << 10;
    cfg.min_page_size = 0;
    cfg.dictionary_page_limit = 0;
    auto ff = make_fetch_file(20000, cfg);
    auto& o = *ff.o;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, 100 << 20, 100);
    auto stop = deferred_stop(semaphore);
    for (unsigned alignment : {512, 4096}) {
        o.impl->set_read_alignment(alignment);
        for (const uint64_t row : {0, 5000}) {
            auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
            const auto base = permit.consumed_resources().memory;
            local_metadata_cache cache;
            // The first extension of a single-partition read is one page.
            pq::fetcher f(o.f, o.summary, cache, {2}, pq::read_direction::forward, pq::read_hint::single_partition, pq::fetch_policy{}, permit);
            auto idx = f.get_index(0).get();
            // The index is charged, and the buffers of the block are gone.
            BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory - base, ssize_t(idx->memory_usage()));
            const auto with_index = permit.consumed_resources().memory;
            const size_t first = o.impl->reads().size();
            const uint32_t j = idx->page_of(2, row);
            auto page = f.get_page(0, 2, idx, j, row).get();
            BOOST_REQUIRE_EQUAL(page.size_bytes(), idx->page_at(2, j).size);
            BOOST_REQUIRE_GT(o.impl->reads().size(), first);
            uint64_t allocated = 0;
            for (size_t i = first; i < o.impl->reads().size(); ++i) {
                allocated += o.impl->reads()[i].allocated;
            }
            check_reads(*o.impl, first, alignment, {{idx->page_at(2, j).offset, idx->page_at(2, j).offset + idx->page_at(2, j).size}});
            BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory - with_index, ssize_t(allocated));
            page = {};
            BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, with_index);
            idx = nullptr;
            f.close().get();
            BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, base);
        }
    }
    o.impl->set_read_alignment(4096);
}

// Closing the fetcher waits for the reads in flight, and issues no read
// after it starts: here an OffsetIndex load that read-ahead started, and
// whose completion would extend the window.
SEASTAR_THREAD_TEST_CASE(test_fetcher_close_with_reads_in_flight) {
    pq::writer_config cfg;
    cfg.row_group_size = 64 << 10;
    cfg.page_set_size = 8 << 10;
    auto ff = make_fetch_file(20000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_GE(o.summary.row_groups(), 3);
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, 100 << 20, 100);
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    const auto base = permit.consumed_resources().memory;
    local_metadata_cache cache;
    pq::fetcher f(o.f, o.summary, cache, {0, 1, 2}, pq::read_direction::forward, pq::read_hint::scan, pq::fetch_policy{}, permit);
    // The budget of a scan is the first row group, which is smaller than
    // the default budget. Once half of it was taken, the window goes on to
    // the next row group.
    o.impl->hold_reads(o.summary.region_start(1));
    auto idx = f.get_index(0).get();
    fragmented_temporary_buffer page;
    // Column c holds most of the bytes.
    for (uint32_t j = 0; j < idx->pages(2) && !o.impl->held_reads(); ++j) {
        page = f.get_page(0, 2, idx, j, idx->page_at(2, j).first_row).get();
        // In debug mode, a read may reach the file only after a few yields.
        for (int i = 0; i < 100 && !o.impl->held_reads(); ++i) {
            seastar::thread::yield();
        }
    }
    BOOST_REQUIRE_GE(o.impl->held_reads(), 1);
    const auto reads = o.impl->reads().size();
    auto closed = f.close();
    for (int i = 0; i < 10; ++i) {
        seastar::yield().get();
    }
    BOOST_REQUIRE(!closed.available());
    o.impl->release_reads();
    closed.get();
    BOOST_REQUIRE_EQUAL(o.impl->reads().size(), reads);
    page = {};
    idx = nullptr;
    BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, base);
}

// A read that waits for memory when the fetcher closes is not issued once
// it gets the memory. The semaphore cannot withdraw a memory request, so
// close() waits for it.
SEASTAR_THREAD_TEST_CASE(test_fetcher_close_with_reads_waiting_for_memory) {
    pq::writer_config cfg;
    cfg.row_group_size = 16 << 20;
    cfg.page_set_size = 8 << 10;
    cfg.min_page_size = 0;
    cfg.max_page_set_rows = 512;
    cfg.dictionary_page_limit = 0;
    auto ff = make_fetch_file(30000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_EQUAL(o.summary.row_groups(), 1);
    const ssize_t memory = 1 << 20;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, memory, 100,
            utils::updateable_value<uint32_t>(2));
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    auto sponge = semaphore.obtain_permit(nullptr, "sponge", 0, db::no_timeout, {}).get();
    const auto base = permit.consumed_resources().memory;
    local_metadata_cache cache;
    const pq::fetch_policy policy{.budget_per_column = 16 << 10, .min_budget = 16 << 10};
    pq::fetcher f(o.f, o.summary, cache, {0}, pq::read_direction::forward, pq::read_hint::scan, policy, permit);
    auto idx = f.get_index(0).get();
    BOOST_REQUIRE_GT(idx->pages(0), 20);
    f.get_page(0, 0, idx, 0, 0).get();

    // The sponge takes all memory beyond the serialize limit, and becomes
    // the only permit that gets more. The scan takes pages until its
    // window refills, which waits for memory.
    auto sponge_units = sponge.request_memory(3 * memory).get();
    sponge_units.add(sponge.request_memory(1).get());
    uint32_t j = 1;
    for (; semaphore.get_stats().reads_enqueued_for_memory == 0; ++j) {
        BOOST_REQUIRE_LT(j, 20);
        f.get_page(0, 0, idx, j, idx->page_at(0, j).first_row).get();
    }
    const auto reads = o.impl->reads().size();
    auto closed = f.close();
    for (int i = 0; i < 10; ++i) {
        seastar::yield().get();
    }
    BOOST_REQUIRE(!closed.available());
    sponge_units.reset_to_zero();
    closed.get();
    BOOST_REQUIRE_EQUAL(o.impl->reads().size(), reads);
    idx = nullptr;
    BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, base);
}

namespace {

// Registers a reader of `permit` as an inactive read, as a querier does
// between two pages of a query.
reader_concurrency_semaphore::inactive_read_handle pause_read(reader_concurrency_semaphore& semaphore, reader_permit permit) {
    auto s = schema_builder(1, "ks", "cf").with_column("pk", int32_type, column_kind::partition_key).build();
    return semaphore.register_inactive_read(make_empty_mutation_reader(std::move(s), std::move(permit)));
}

void resume_read(reader_concurrency_semaphore& semaphore, reader_concurrency_semaphore::inactive_read_handle handle) {
    auto reader = semaphore.unregister_inactive_read(std::move(handle));
    BOOST_REQUIRE(reader);
    reader->close().get();
}

// Reads every page of `columns` and compares it with the file.
void check_all_pages(pq::fetcher& f, const opened_file& o, const std::vector<pq::leaf_index>& columns) {
    const auto& data = o.impl->data();
    for (uint32_t g = 0; g < o.summary.row_groups(); ++g) {
        auto idx = f.get_index(g).get();
        for (auto c : columns) {
            if (o.summary.all_null(g, c)) {
                continue;
            }
            for (uint32_t j = 0; j < idx->pages(c); ++j) {
                const auto p = idx->page_at(c, j);
                auto page = f.get_page(g, c, idx, j, o.summary.row_group_first_row(g) + p.first_row).get();
                BOOST_REQUIRE(string_of(page) == data.substr(p.offset, p.size));
            }
        }
    }
}

} // anonymous namespace

// A read may be registered as inactive between two calls of its consumer,
// while an OffsetIndex load that read-ahead started is in flight. The load
// then completes without asking for memory: an inactive permit that asks
// for memory above the serialize limit is queued as a waiter, and resuming
// the read fails an assertion in the semaphore. The window goes on at the
// next call.
SEASTAR_THREAD_TEST_CASE(test_fetcher_index_load_completes_while_the_read_is_inactive) {
    pq::writer_config cfg;
    cfg.row_group_size = 64 << 10;
    cfg.page_set_size = 8 << 10;
    auto ff = make_fetch_file(20000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_GE(o.summary.row_groups(), 3);
    const ssize_t memory = 4 << 20;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, memory, 100,
            utils::updateable_value<uint32_t>(1));
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    auto sponge = semaphore.obtain_permit(nullptr, "sponge", 0, db::no_timeout, {}).get();
    const auto base = permit.consumed_resources().memory;
    local_metadata_cache cache;
    pq::fetcher f(o.f, o.summary, cache, {0, 1, 2}, pq::read_direction::forward, pq::read_hint::scan, pq::fetch_policy{}, permit);
    // As in test_fetcher_close_with_reads_in_flight, the window reaches
    // row group 1 and starts loading its OffsetIndex block, whose read is
    // held.
    o.impl->hold_reads(o.summary.region_start(1));
    auto idx = f.get_index(0).get();
    fragmented_temporary_buffer page;
    for (uint32_t j = 0; j < idx->pages(2) && !o.impl->held_reads(); ++j) {
        page = f.get_page(0, 2, idx, j, idx->page_at(2, j).first_row).get();
        for (int i = 0; i < 100 && !o.impl->held_reads(); ++i) {
            seastar::thread::yield();
        }
    }
    BOOST_REQUIRE_GE(o.impl->held_reads(), 1);
    BOOST_REQUIRE_EQUAL(f.stats().index_reads, 2);

    auto handle = pause_read(semaphore, permit);
    BOOST_REQUIRE(handle);
    // The sponge takes the rest of the memory, and becomes the only permit
    // that gets more.
    auto sponge_units = sponge.request_memory(semaphore.available_resources().memory).get();
    sponge_units.add(sponge.request_memory(0).get());
    o.impl->release_reads();
    for (int i = 0; i < 100; ++i) {
        seastar::yield().get();
    }
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_memory, 0);
    BOOST_REQUIRE(permit.get_state() == reader_permit::state::inactive);
    const auto reads = o.impl->reads().size();
    resume_read(semaphore, std::move(handle));
    sponge_units.reset_to_zero();

    // The window goes on at the next call, and reads the pages of row
    // group 1 ahead.
    auto idx1 = f.get_index(1).get();
    // In debug mode, a read may reach the file only after a few yields.
    for (int i = 0; i < 100 && o.impl->reads().size() == reads; ++i) {
        seastar::thread::yield();
    }
    BOOST_REQUIRE_GT(o.impl->reads().size(), reads);
    page = {};
    idx = nullptr;
    idx1 = nullptr;
    check_all_pages(f, o, {0, 1, 2});
    f.close().get();
    BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, base);
}

// Registering a read as inactive fails the memory requests of its permit
// that wait, with std::bad_alloc. A read-ahead request that waits then is
// read again when a caller asks for its page, and the read goes on.
SEASTAR_THREAD_TEST_CASE(test_fetcher_read_ahead_fails_for_memory_while_the_read_is_inactive) {
    pq::writer_config cfg;
    cfg.row_group_size = 16 << 20;
    cfg.page_set_size = 8 << 10;
    cfg.min_page_size = 0;
    cfg.max_page_set_rows = 512;
    cfg.dictionary_page_limit = 0;
    auto ff = make_fetch_file(30000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_EQUAL(o.summary.row_groups(), 1);
    const ssize_t memory = 1 << 20;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, memory, 100,
            utils::updateable_value<uint32_t>(1));
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    auto sponge = semaphore.obtain_permit(nullptr, "sponge", 0, db::no_timeout, {}).get();
    const auto base = permit.consumed_resources().memory;
    local_metadata_cache cache;
    const pq::fetch_policy policy{.budget_per_column = 16 << 10, .min_budget = 16 << 10};
    pq::fetcher f(o.f, o.summary, cache, {0}, pq::read_direction::forward, pq::read_hint::scan, policy, permit);
    auto idx = f.get_index(0).get();
    BOOST_REQUIRE_GT(idx->pages(0), 20);
    // The pages are kept, so that the read releases no memory.
    std::vector<fragmented_temporary_buffer> pages;
    pages.push_back(f.get_page(0, 0, idx, 0, 0).get());

    // The sponge takes the rest of the memory, and becomes the only permit
    // that gets more. The scan takes pages until its window refills, which
    // waits for memory. No memory is left, so registering the read as
    // inactive does not evict it.
    auto sponge_units = sponge.request_memory(semaphore.available_resources().memory).get();
    sponge_units.add(sponge.request_memory(0).get());
    uint32_t j = 1;
    for (; semaphore.get_stats().reads_enqueued_for_memory == 0; ++j) {
        BOOST_REQUIRE_LT(j, 20);
        pages.push_back(f.get_page(0, 0, idx, j, idx->page_at(0, j).first_row).get());
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 0);
    auto handle = pause_read(semaphore, permit);
    BOOST_REQUIRE(handle);
    for (int i = 0; i < 10; ++i) {
        seastar::yield().get();
    }
    resume_read(semaphore, std::move(handle));
    sponge_units.reset_to_zero();

    const auto& data = o.impl->data();
    for (; j < idx->pages(0); ++j) {
        const auto p = idx->page_at(0, j);
        pages.push_back(f.get_page(0, 0, idx, j, p.first_row).get());
        BOOST_REQUIRE(string_of(pages.back()) == data.substr(p.offset, p.size));
        pages.clear();
    }
    f.close().get();
    idx = nullptr;
    BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, base);
}

// An OffsetIndex load that read-ahead starts from the cache waits for the
// memory of its copy. If the entry is evicted meanwhile, and no call is in
// progress when the memory is granted, the load reads nothing: the read
// may be inactive. The next call that needs the block loads it again.
SEASTAR_THREAD_TEST_CASE(test_fetcher_drops_an_index_load_whose_cache_entry_is_evicted) {
    pq::writer_config cfg;
    cfg.row_group_size = 64 << 10;
    cfg.page_set_size = 8 << 10;
    auto ff = make_fetch_file(20000, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_GE(o.summary.row_groups(), 3);
    local_metadata_cache cache;
    {
        pq::fetcher f(o.f, o.summary, cache, {0, 1, 2}, pq::read_direction::forward, pq::read_hint::single_partition, pq::fetch_policy{});
        f.get_index(1).get();
        f.close().get();
    }
    BOOST_REQUIRE(cache.cache.contains(pq::metadata_cache::index_key(1)));
    const ssize_t memory = 4 << 20;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, memory, 100,
            utils::updateable_value<uint32_t>(1));
    auto stop = deferred_stop(semaphore);
    auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
    auto sponge = semaphore.obtain_permit(nullptr, "sponge", 0, db::no_timeout, {}).get();
    const auto base = permit.consumed_resources().memory;
    pq::fetcher f(o.f, o.summary, cache, {0, 1, 2}, pq::read_direction::forward, pq::read_hint::scan, pq::fetch_policy{}, permit);
    auto idx = f.get_index(0).get();
    // The window holds row group 0, and refills once half of it was taken.
    auto page = f.get_page(0, 2, idx, 0, 0).get();
    auto sponge_units = sponge.request_memory(semaphore.available_resources().memory).get();
    sponge_units.add(sponge.request_memory(0).get());
    for (uint32_t j = 1; semaphore.get_stats().reads_enqueued_for_memory == 0; ++j) {
        BOOST_REQUIRE_LT(j, idx->pages(2));
        page = f.get_page(0, 2, idx, j, idx->page_at(2, j).first_row).get();
    }
    const auto index_reads = f.stats().index_reads;
    cache.cache.evict_gently().get();
    sponge_units.reset_to_zero();
    for (int i = 0; i < 100; ++i) {
        seastar::yield().get();
    }
    BOOST_REQUIRE_EQUAL(f.stats().index_reads, index_reads);
    auto idx1 = f.get_index(1).get();
    BOOST_REQUIRE_EQUAL(f.stats().index_reads, index_reads + 1);
    page = {};
    idx = nullptr;
    idx1 = nullptr;
    check_all_pages(f, o, {0, 1, 2});
    f.close().get();
    BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, base);
}

// An allocation failure in the fetcher, in the foreground or in
// read-ahead, reaches the caller that asks for the data, or does not
// matter. The fetcher returns correct pages, does not hang, and releases
// all memory when it closes.
SEASTAR_THREAD_TEST_CASE(test_fetcher_allocation_failures) {
    pq::writer_config cfg;
    cfg.row_group_size = 8 << 10;
    cfg.page_set_size = 2 << 10;
    cfg.min_page_size = 0;
    auto ff = make_fetch_file(600, cfg);
    auto& o = *ff.o;
    BOOST_REQUIRE_GE(o.summary.row_groups(), 3);
    const auto& data = o.impl->data();
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, 100 << 20, 100);
    auto stop = deferred_stop(semaphore);
    auto& injector = memory::local_failure_injector();
    uint64_t failures = 0;
    for (uint64_t i = 0;; ++i) {
        auto permit = semaphore.obtain_permit(nullptr, "read", 0, db::no_timeout, {}).get();
        const auto base = permit.consumed_resources().memory;
        {
            local_metadata_cache cache;
            pq::fetcher f(o.f, o.summary, cache, {0, 1, 2}, pq::read_direction::forward, pq::read_hint::scan, pq::fetch_policy{}, permit);
            injector.fail_after(i);
            try {
                for (uint32_t g = 0; g < o.summary.row_groups(); ++g) {
                    auto idx = f.get_index(g).get();
                    for (pq::leaf_index c = 0; c < 3; ++c) {
                        for (uint32_t j = 0; j < idx->pages(c); ++j) {
                            const auto p = idx->page_at(c, j);
                            auto page = f.get_page(g, c, idx, j, o.summary.row_group_first_row(g) + p.first_row).get();
                            BOOST_REQUIRE(string_of(page) == data.substr(p.offset, p.size));
                        }
                    }
                }
            } catch (const std::bad_alloc&) {
                ++failures;
            }
            injector.cancel();
            f.close().get();
        }
        BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, base);
        if (!injector.failed()) {
            BOOST_TEST_MESSAGE(fmt::format("{} allocation points, {} failures reached the caller", i, failures));
            break;
        }
    }
}

// The bytes that a scan fetches ahead stay within the budget, plus at most
// one page per column, which the last step of the window adds.
SEASTAR_THREAD_TEST_CASE(test_fetcher_read_ahead_budget) {
    pq::schema s;
    std::vector<column_id> cols;
    for (int i = 0; i < 8; ++i) {
        cols.push_back(s.add_column(pq::schema::root, fmt::format("c{}", i), repetition_type::optional, physical_type::byte_array));
    }
    model m(std::move(s));
    m.add_rows(30000);
    std::mt19937_64 rng(6);
    for (size_t r = 0; r < m.rows; ++r) {
        for (auto c : cols) {
            m.columns[c][r].push_back(entry{.def = 1, .value = random_bytes(rng, 40, rng())});
        }
    }
    auto cfg = pq::writer_config{.row_group_size = 16 << 20, .page_set_size = 32 << 10};
    cfg.dictionary_page_limit = 0;
    const auto f = write_model(m, cfg);
    auto o = open_file(f.data, f.result.footer.crc32);
    BOOST_REQUIRE_EQUAL(o->summary.row_groups(), 1);
    auto [projection, info] = projection_of(m, o->summary, cols);
    for (bool forward : {true, false}) {
        for (uint64_t per_column : {16 << 10, 128 << 10}) {
            const pq::fetch_policy policy{.budget_per_column = per_column, .min_budget = 64 << 10};
            local_metadata_cache cache;
            pq::reader r(o->f, o->summary, cache, projection, pq::reader_options{
                .direction = forward ? pq::read_direction::forward : pq::read_direction::backward,
                .policy = policy,
            });
            scan(r, m, info, forward);
            r.close().get();
            const auto idx = read_index(*o, 0);
            uint64_t largest_page = 0;
            for (auto c = 0u; c < cols.size(); ++c) {
                for (uint32_t j = 0; j < idx->pages(c); ++j) {
                    largest_page = std::max<uint64_t>(largest_page, idx->page_at(c, j).size);
                }
            }
            const uint64_t budget = std::max(per_column * cols.size(), policy.min_budget);
            BOOST_REQUIRE_LT(budget, o->summary.region_size(0));
            const auto& st = r.stats();
            BOOST_TEST_MESSAGE(fmt::format("forward {} budget {}: {} reads, {} pages, at most {} bytes pending, pages of at most {} bytes",
                    forward, budget, st.reads, st.pages_taken, st.max_pending_bytes, largest_page));
            BOOST_REQUIRE_LE(st.max_pending_bytes, budget + cols.size() * largest_page);
            BOOST_REQUIRE_GE(st.max_pending_bytes, budget / 2);
            BOOST_REQUIRE_LE(st.page_misses, 1);
            BOOST_REQUIRE_LE(st.largest_request, 128 << 10);
        }
    }
}

// Corruption

namespace {

// Reads all columns of the file. Returns true if the data matches the
// model, false if the reader raised the malformed sstable error. Any other
// outcome fails the test.
bool read_all(const std::string& data, std::optional<uint32_t> crc, const model& m, bool forward = true) {
    try {
        auto o = open_file(data, crc);
        auto [projection, info] = projection_of(m, o->summary, all_columns(m));
        if (o->summary.rows() != m.rows) {
            BOOST_FAIL("a damaged file opened with a different row count");
        }
        local_metadata_cache cache;
        pq::reader r(o->f, o->summary, cache, projection, pq::reader_options{
            .direction = forward ? pq::read_direction::forward : pq::read_direction::backward,
        });
        try {
            scan(r, m, info, forward);
        } catch (...) {
            r.close().get();
            throw;
        }
        r.close().get();
        return true;
    } catch (const malformed_sstable_exception&) {
        return false;
    }
}

std::string with_footer(const std::string& data, const pq::file_metadata& md, uint32_t& crc) {
    uint32_t length;
    std::memcpy(&length, data.data() + data.size() - 8, 4);
    std::string r = data.substr(0, data.size() - 8 - length);
    const auto footer = pq::serialize_file_metadata(md);
    crc = crc32_utils::init_checksum();
    for (bytes_view frag : footer.fragments()) {
        r.append(reinterpret_cast<const char*>(frag.data()), frag.size());
        crc = crc32_utils::checksum(crc, reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    const uint32_t new_length = footer.size();
    r.append(reinterpret_cast<const char*>(&new_length), 4);
    r.append(pq::magic);
    return r;
}

uint32_t footer_crc(const std::string& data) {
    uint32_t length;
    std::memcpy(&length, data.data() + data.size() - 8, 4);
    return crc32_utils::checksum(data.data() + data.size() - 8 - length, length);
}

model corruption_model() {
    pq::schema s;
    const auto a = s.add_column(pq::schema::root, "a", repetition_type::optional, physical_type::int64);
    const auto b = s.add_column(pq::schema::root, "b", repetition_type::optional, physical_type::byte_array);
    const auto l = s.add_list(pq::schema::root, "l", physical_type::int32);
    const auto d = s.add_column(pq::schema::root, "d", repetition_type::optional, physical_type::double_);
    model m(std::move(s));
    m.add_rows(600);
    std::mt19937_64 rng(7);
    for (size_t r = 0; r < m.rows; ++r) {
        if (rng() % 5) {
            m.columns[a][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(rng() % 50))});
        }
        if (r % 7 == 0) {
            m.columns[b][r].push_back(entry{.def = 1, .value = random_bytes(rng, rng() % 30, rng() % 20)});
        }
        if (r > 100 && rng() % 3 == 0) {
            for (size_t k = 0; k < 1 + rng() % 4; ++k) {
                m.columns[l][r].push_back(entry{.rep = uint16_t(k ? 1 : 0), .def = 3, .value = bytes_of_value(int32_t(rng()))});
            }
        }
        if (r >= 200 && r < 260) {
            m.columns[d][r].push_back(entry{.def = 1, .value = bytes_of_value(double(r))});
        }
    }
    return m;
}

pq::writer_config corruption_config(pq::compression_codec codec) {
    auto cfg = pq::writer_config{.row_group_size = 2 << 10, .page_set_size = 512, .min_page_size = 0, .max_page_set_rows = 40};
    cfg.codec = codec;
    return cfg;
}

} // anonymous namespace

// Bit flips in pages, page headers, and OffsetIndexes give the malformed
// sstable error or correct data. The footer is intact.
SEASTAR_THREAD_TEST_CASE(test_corrupt_data) {
    const auto m = corruption_model();
    for (auto codec : {pq::compression_codec::uncompressed, pq::compression_codec::snappy, pq::compression_codec::gzip,
            pq::compression_codec::lz4_raw, pq::compression_codec::zstd}) {
        const auto f = write_model(m, corruption_config(codec));
        BOOST_REQUIRE(read_all(f.data, f.result.footer.crc32, m));
        auto o = open_file(f.data, f.result.footer.crc32);
        BOOST_REQUIRE_GE(o->summary.row_groups(), 3);
        // Interesting places: OffsetIndex blocks and page starts.
        std::vector<byte_range> targets;
        {
            local_metadata_cache cache;
            pq::reader r(o->f, o->summary, cache, projection_of(m, o->summary, all_columns(m)).first);
            for (uint32_t g = 0; g < o->summary.row_groups(); ++g) {
                r.cursor(0).seek(o->summary.row_group_first_row(g)).get();
                const auto& rg = o->summary.row_group(g);
                targets.push_back({rg.offset_index_offset, rg.offset_index_offset + rg.offset_index_length});
                const auto idx = read_index(*o, g);
                for (pq::leaf_index c = 0; c < o->summary.columns().size(); ++c) {
                    for (uint32_t j = 0; j < idx->pages(c); ++j) {
                        const auto p = idx->page_at(c, j);
                        targets.push_back({p.offset, p.offset + std::min<uint64_t>(p.size, 40)});
                    }
                }
            }
            r.close().get();
        }
        const byte_range data_region{4, o->summary.footer_offset()};
        std::mt19937_64 rng(8);
        size_t failures = 0;
        const int iterations = 300;
        // LZ4 linearizes a page larger than 128 KiB, which a damaged header can claim.
        const size_t threshold = codec == pq::compression_codec::lz4_raw ? 4 << 20 : (128 << 10) + 1;
        const auto before = seastar::memory::stats();
        {
            const seastar::memory::scoped_large_allocation_warning_threshold guard(threshold);
            for (int i = 0; i < iterations; ++i) {
                const auto& t = rng() % 3 ? targets[rng() % targets.size()] : data_region;
                const uint64_t pos = t.start + rng() % (t.end - t.start);
                auto damaged = f.data;
                damaged[pos] ^= char(1 << (rng() % 8));
                failures += !read_all(damaged, f.result.footer.crc32, m, rng() % 2);
            }
        }
        const auto after = seastar::memory::stats();
        BOOST_REQUIRE_EQUAL(after.large_allocations(), before.large_allocations());
        BOOST_TEST_MESSAGE(fmt::format("codec {}: {} of {} damaged files raised the error", int(codec), failures, iterations));
        // Almost every bit is covered by a checksum or checked against the footer.
        BOOST_REQUIRE_GT(failures, iterations * 9 / 10);
    }
}

// Truncated and shortened files.
SEASTAR_THREAD_TEST_CASE(test_truncated_files) {
    const auto m = corruption_model();
    const auto f = write_model(m, corruption_config(pq::compression_codec::zstd));
    std::mt19937_64 rng(9);
    for (int i = 0; i < 100; ++i) {
        // Cut the end of the file.
        const size_t len = rng() % f.data.size();
        BOOST_REQUIRE_THROW(open_file(f.data.substr(0, len), f.result.footer.crc32), malformed_sstable_exception);
        // Remove bytes inside the data, so the footer points to the wrong places.
        const size_t pos = 4 + rng() % (f.data.size() / 2);
        const size_t n = 1 + rng() % 100;
        auto shortened = f.data.substr(0, pos) + f.data.substr(pos + n);
        BOOST_REQUIRE(!read_all(shortened, f.result.footer.crc32, m));
    }
    // A footer length beyond the file.
    auto bad = f.data;
    const uint32_t huge = std::numeric_limits<uint32_t>::max();
    std::memcpy(bad.data() + bad.size() - 8, &huge, 4);
    BOOST_REQUIRE_THROW(open_file(bad, std::nullopt), malformed_sstable_exception);
}

// Truncated pages, given to the decoder directly.
SEASTAR_THREAD_TEST_CASE(test_truncated_pages) {
    const auto m = corruption_model();
    const auto f = write_model(m, corruption_config(pq::compression_codec::snappy));
    auto o = open_file(f.data, f.result.footer.crc32);
    local_metadata_cache cache;
    pq::reader r(o->f, o->summary, cache, projection_of(m, o->summary, all_columns(m)).first);
    r.cursor(0).seek(0).get();
    r.close().get();
    const auto idx = read_index(*o, 0);
    auto compressor = pq::make_page_compressor(pq::compression_codec::snappy);
    for (pq::leaf_index c = 0; c < o->summary.columns().size(); ++c) {
        if (!o->summary.in_place(0, c)) {
            continue;
        }
        const auto& col = o->summary.column(c);
        for (uint32_t j = 0; j < idx->pages(c); ++j) {
            const auto p = idx->page_at(c, j);
            const std::string page = f.data.substr(p.offset, p.size);
            const uint64_t rows = idx->page_end_row(c, j) - p.first_row;
            // The whole page decodes, unless it needs the dictionary.
            if (!o->summary.has_dictionary(0, c)) {
                pq::decoded_page::decode(col, c, fragmented_temporary_buffer(page.data(), page.size()), rows, nullptr, *compressor).get();
            }
            for (size_t len : {size_t(0), size_t(1), size_t(5), page.size() / 2, page.size() - 1}) {
                BOOST_REQUIRE_THROW(pq::decoded_page::decode(col, c, fragmented_temporary_buffer(page.data(), len), rows, nullptr, *compressor).get(),
                        malformed_sstable_exception);
            }
            // The wrong row count.
            BOOST_REQUIRE_THROW(pq::decoded_page::decode(col, c, fragmented_temporary_buffer(page.data(), page.size()), rows + 1, nullptr, *compressor).get(),
                    malformed_sstable_exception);
        }
    }
    // A truncated OffsetIndex block.
    const auto& rg = o->summary.row_group(0);
    for (size_t len : {size_t(0), size_t(rg.offset_index_length / 2), size_t(rg.offset_index_length - 1)}) {
        const auto block = f.data.substr(rg.offset_index_offset, len);
        BOOST_REQUIRE_THROW(pq::decode_row_group_index(o->summary, 0, fragmented_temporary_buffer(block.data(), block.size())).get(),
                malformed_sstable_exception);
    }
}

// Footer damage: a checksum mismatch, and structural damage with a
// matching checksum.
SEASTAR_THREAD_TEST_CASE(test_corrupt_footer) {
    const auto m = corruption_model();
    const auto f = write_model(m, corruption_config(pq::compression_codec::zstd));
    uint32_t length;
    std::memcpy(&length, f.data.data() + f.data.size() - 8, 4);
    const size_t footer_start = f.data.size() - 8 - length;
    std::mt19937_64 rng(10);
    size_t opened = 0;
    // The first decompression allocates the shard's zstd context, which
    // the check below does not count.
    read_and_check(f, m);
    const auto before = seastar::memory::stats();
    {
        const seastar::memory::scoped_large_allocation_warning_threshold guard((128 << 10) + 1);
        for (int i = 0; i < 300; ++i) {
            auto damaged = f.data;
            damaged[footer_start + rng() % length] ^= char(1 << (rng() % 8));
            BOOST_REQUIRE_THROW(open_file(damaged, f.result.footer.crc32), malformed_sstable_exception);
            // With a matching checksum, the reader either rejects the file or
            // reads data. A change of a value that no structure depends on, such
            // as a null count, is invisible to it.
            try {
                auto o = open_file(damaged, footer_crc(damaged));
                ++opened;
                local_metadata_cache cache;
                std::vector<std::optional<pq::leaf_index>> projection;
                for (pq::leaf_index c = 0; c < o->summary.columns().size(); ++c) {
                    projection.push_back(c);
                }
                pq::reader r(o->f, o->summary, cache, projection);
                try {
                    for (size_t k = 0; k < r.columns(); ++k) {
                        auto& cur = r.cursor(k);
                        for (uint64_t row = 0; row < o->summary.rows(); ++row) {
                            cur.seek(row).get();
                        }
                    }
                } catch (const malformed_sstable_exception&) {
                }
                r.close().get();
            } catch (const malformed_sstable_exception&) {
            }
        }
    }
    const auto after = seastar::memory::stats();
    BOOST_REQUIRE_EQUAL(after.large_allocations(), before.large_allocations());
    BOOST_TEST_MESSAGE(fmt::format("{} damaged footers with a matching checksum opened", opened));
}

// Hand-made footer damage that the summary must reject: each change breaks
// a layout invariant.
SEASTAR_THREAD_TEST_CASE(test_footer_layout_violations) {
    const auto m = corruption_model();
    const auto f = write_model(m, corruption_config(pq::compression_codec::zstd));
    const auto md = parse_footer(f.data);
    BOOST_REQUIRE_GE(md.row_groups.size(), 3);
    {
        uint32_t crc;
        const auto same = with_footer(f.data, md, crc);
        BOOST_REQUIRE(same == f.data);
        BOOST_REQUIRE(read_all(same, crc, m));
    }
    using change = std::function<void(pq::file_metadata&)>;
    const std::vector<std::pair<std::string, change>> changes{
        {"data page offset", [] (pq::file_metadata& md) { md.row_groups[1].columns[0].meta_data.data_page_offset += 1; }},
        {"chunk size", [] (pq::file_metadata& md) { md.row_groups[1].columns[0].meta_data.total_compressed_size += 1; }},
        {"OffsetIndex offset", [] (pq::file_metadata& md) { *md.row_groups[0].columns[1].offset_index_offset += 1; }},
        {"OffsetIndex length", [] (pq::file_metadata& md) { *md.row_groups[0].columns[0].offset_index_length += 1; }},
        {"row group rows", [] (pq::file_metadata& md) { md.row_groups[0].num_rows += 1; md.num_rows += 1; }},
        {"file rows", [] (pq::file_metadata& md) { md.num_rows += 1; }},
        {"chunk type", [] (pq::file_metadata& md) { md.row_groups[2].columns[0].meta_data.type = physical_type::int32; }},
        {"codec", [] (pq::file_metadata& md) { md.row_groups[2].columns[0].meta_data.codec = pq::compression_codec::snappy; }},
        {"no null count", [] (pq::file_metadata& md) { md.row_groups[0].columns[0].meta_data.stats.reset(); }},
        {"too many nulls", [] (pq::file_metadata& md) { auto& cm = md.row_groups[0].columns[0].meta_data; cm.stats->null_count = cm.num_values + 1; }},
        {"leaf type", [] (pq::file_metadata& md) { md.schema[1].type = physical_type::int32; }},
        {"unsupported codec", [] (pq::file_metadata& md) { for (auto& rg : md.row_groups) { rg.columns[0].meta_data.codec = pq::compression_codec::lzo; } }},
        {"no OffsetIndex", [] (pq::file_metadata& md) { md.row_groups[1].columns[0].offset_index_offset.reset(); md.row_groups[1].columns[0].offset_index_length.reset(); }},
        {"version", [] (pq::file_metadata& md) { md.version = 3; }},
    };
    for (const auto& [name, fn] : changes) {
        BOOST_TEST_MESSAGE(name);
        auto damaged_md = md;
        fn(damaged_md);
        uint32_t crc;
        const auto damaged = with_footer(f.data, damaged_md, crc);
        bool rejected = false;
        try {
            open_file(damaged, crc);
        } catch (const malformed_sstable_exception& e) {
            BOOST_TEST_MESSAGE(e.what());
            rejected = true;
        }
        if (!rejected) {
            // The OffsetIndex length is checked when the block is read.
            BOOST_REQUIRE_EQUAL(name, "OffsetIndex length");
            BOOST_REQUIRE(!read_all(damaged, crc, m));
        }
    }
}

// Arrow does not follow our layout: by default it writes Data Page V1, and
// it places the OffsetIndexes of all row groups before the footer. The
// reader rejects such files instead of misreading them.
SEASTAR_THREAD_TEST_CASE(test_reject_arrow_files) {
    using namespace ::parquet;
    for (int variant = 0; variant < 3; ++variant) {
        auto node = ::parquet::schema::GroupNode::Make("schema", Repetition::REQUIRED, {
            ::parquet::schema::PrimitiveNode::Make("a", Repetition::OPTIONAL, Type::INT64),
            ::parquet::schema::PrimitiveNode::Make("b", Repetition::OPTIONAL, Type::INT32),
        });
        auto sink = ::arrow::io::BufferOutputStream::Create().ValueOrDie();
        WriterProperties::Builder props;
        if (variant > 0) {
            props.data_page_version(ParquetDataPageVersion::V2);
            props.enable_page_checksum();
            props.enable_write_page_index();
        }
        auto writer = ParquetFileWriter::Open(sink, std::static_pointer_cast<::parquet::schema::GroupNode>(node), props.build());
        const int row_groups = variant == 2 ? 1 : 3;
        for (int g = 0; g < row_groups; ++g) {
            auto rg = writer->AppendRowGroup();
            for (int c = 0; c < 2; ++c) {
                std::vector<int16_t> defs;
                std::vector<int64_t> values64;
                std::vector<int32_t> values32;
                for (int r = 0; r < 100; ++r) {
                    defs.push_back(r % 3 ? 1 : 0);
                    if (r % 3) {
                        values64.push_back(r);
                        values32.push_back(r);
                    }
                }
                if (c == 0) {
                    static_cast<Int64Writer*>(rg->NextColumn())->WriteBatch(100, defs.data(), nullptr, values64.data());
                } else {
                    static_cast<Int32Writer*>(rg->NextColumn())->WriteBatch(100, defs.data(), nullptr, values32.data());
                }
            }
            rg->Close();
        }
        writer->Close();
        auto buf = sink->Finish().ValueOrDie();
        const std::string data(reinterpret_cast<const char*>(buf->data()), buf->size());
        BOOST_TEST_MESSAGE(fmt::format("variant {}: {} bytes", variant, data.size()));
        try {
            open_file(data, std::nullopt);
            BOOST_FAIL("an Arrow file was accepted");
        } catch (const malformed_sstable_exception& e) {
            BOOST_TEST_MESSAGE(e.what());
        }
    }
}

// The memory that a summary keeps per row group and per chunk.
SEASTAR_THREAD_TEST_CASE(test_summary_memory) {
    pq::schema s;
    for (int i = 0; i < 100; ++i) {
        s.add_column(pq::schema::root, fmt::format("column_{}", i), repetition_type::optional, physical_type::int64);
    }
    model m(std::move(s));
    m.add_rows(2000);
    for (size_t r = 0; r < m.rows; ++r) {
        for (column_id c = 0; c < 100; ++c) {
            if ((r + c) % 7 == 0) {
                m.columns[c][r].push_back(entry{.def = 1, .value = bytes_of_value(int64_t(r))});
            }
        }
    }
    for (uint64_t row_group_size : {64 << 10, 4 << 10}) {
        auto cfg = pq::writer_config{.row_group_size = row_group_size, .page_set_size = 1 << 10};
        const auto f = write_model(m, cfg);
        auto o = open_file(f.data, f.result.footer.crc32);
        const auto& sm = o->summary;
        BOOST_TEST_MESSAGE(fmt::format("{} row groups, {} columns, footer of {} bytes: the summary takes {} bytes",
                sm.row_groups(), sm.columns().size(), f.result.footer.size, sm.memory_usage()));
        // 24 bytes per row group and 2 bits per chunk, plus the schema.
        BOOST_REQUIRE_LE(sm.memory_usage(), 64 << 10);
    }
}
