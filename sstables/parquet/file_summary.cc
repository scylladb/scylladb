/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/file_summary.hh"

#include <algorithm>
#include <cstring>
#include <limits>

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/thread.hh>

#include <fmt/ranges.h>

#include "sstables/checksum_utils.hh"
#include "sstables/exceptions.hh"
#include "sstables/parquet/compression.hh"

namespace sstables::parquet {

[[noreturn]] static void malformed(std::string_view what) {
    throw malformed_sstable_exception(seastar::format("Parquet footer does not describe a supported file: {}", what));
}

// Offsets and sizes from the footer are checked against this bound before
// any arithmetic, so that sums cannot overflow.
static constexpr int64_t max_offset = int64_t(1) << 60;

// Deeper schemas are valid Parquet, but our mapping never nests that deep,
// and levels then fit in a byte.
static constexpr unsigned max_schema_depth = 64;
static constexpr unsigned max_level = 255;

std::optional<leaf_index> file_summary::find_column(std::span<const std::string_view> path) const noexcept {
    for (leaf_index c = 0; c < _columns.size(); ++c) {
        if (std::ranges::equal(_columns[c].path, path, [] (const sstring& a, std::string_view b) { return std::string_view(a) == b; })) {
            return c;
        }
    }
    return std::nullopt;
}

uint32_t file_summary::row_group_of(uint64_t row) const noexcept {
    auto it = std::upper_bound(_row_groups.begin(), _row_groups.end(), row,
            [] (uint64_t r, const row_group_entry& e) { return r < e.first_row; });
    return uint32_t(std::distance(_row_groups.begin(), it)) - 1;
}

size_t file_summary::memory_usage() const noexcept {
    size_t r = sizeof(*this);
    r += _row_groups.size() * sizeof(row_group_entry);
    r += _chunk_bits.size() * sizeof(uint64_t);
    for (const auto& c : _columns) {
        r += sizeof(c);
        for (const auto& p : c.path) {
            r += p.size();
        }
    }
    for (const auto& e : _elements) {
        r += sizeof(e) + e.name.size();
    }
    for (const auto& kv : _key_value_metadata) {
        r += sizeof(kv) + kv.key.size() + (kv.value ? kv.value->size() : 0);
    }
    return r;
}

// Builds a file_summary from the footer, and checks it against the layout
// invariants of writer.hh.
class summary_builder final : public file_metadata_visitor {
    file_summary _s;

    // The schema tree, while its elements arrive in depth-first order.
    struct frame {
        uint32_t remaining;
        uint8_t def;
        uint8_t rep;
        utils::small_vector<uint8_t, 2> repeated_def;
        bool named;
    };
    std::vector<frame> _stack;
    std::vector<sstring> _path;
    bool _root_seen = false;
    bool _schema_done = false;

    // The chunks of the current row group, until on_row_group() gives its row count.
    struct chunk_info {
        uint64_t start;
        uint64_t end;
        bool has_dictionary;
        std::optional<uint64_t> offset_index_offset;
        uint32_t offset_index_length;
        uint64_t num_values;
        uint64_t null_count;
        uint64_t uncompressed_size;
        compression_codec codec;
    };
    utils::chunked_vector<chunk_info> _chunks;

    std::optional<int64_t> _num_rows;
    bool _version_seen = false;
    // The span of the late chunks.
    uint64_t _late_start = std::numeric_limits<uint64_t>::max();
    uint64_t _late_end = 0;
    size_t _bits = 0;

    static constexpr uint32_t not_in_place = std::numeric_limits<uint32_t>::max();

    void push_bit(bool v) {
        if (_bits % 64 == 0) {
            _s._chunk_bits.push_back(0);
        }
        if (v) {
            _s._chunk_bits.back() |= uint64_t(1) << (_bits % 64);
        }
        ++_bits;
    }

    static void check_offset(int64_t v, std::string_view what) {
        if (v < 0 || v > max_offset) {
            malformed(seastar::format("{} {} is out of range", what, v));
        }
    }

    void add_leaf(const schema_element& e, uint8_t def, uint8_t rep, const utils::small_vector<uint8_t, 2>& repeated_def) {
        const auto t = *e.type;
        switch (t) {
        case physical_type::boolean:
        case physical_type::int32:
        case physical_type::int64:
        case physical_type::float_:
        case physical_type::double_:
        case physical_type::byte_array:
            break;
        case physical_type::fixed_len_byte_array:
            if (!e.type_length || *e.type_length <= 0) {
                malformed(seastar::format("column {} is a fixed-length byte array without a valid length", e.name));
            }
            break;
        default:
            malformed(seastar::format("column {} has unsupported physical type {}", e.name, int(t)));
        }
        leaf_column c{
            .path = _path,
            .type = t,
            .type_length = t == physical_type::fixed_len_byte_array ? *e.type_length : 0,
            .max_def = def,
            .max_rep = rep,
            .repeated_def = repeated_def,
            .element = uint32_t(_s._elements.size()),
            .first_in_place = not_in_place,
        };
        c.path.push_back(e.name);
        _s._columns.push_back(std::move(c));
    }

public:
    void on_version(int32_t version) override {
        if (version != 1 && version != 2) {
            malformed(seastar::format("unknown version {}", version));
        }
        _version_seen = true;
    }

    void on_schema_element(schema_element&& e) override {
        if (_schema_done) {
            malformed("a schema element after the end of the schema tree");
        }
        if (!_root_seen) {
            if (!e.num_children || *e.num_children < 0 || e.type) {
                malformed("the schema root is not a group");
            }
            _root_seen = true;
            _stack.push_back(frame{.remaining = uint32_t(*e.num_children), .def = 0, .rep = 0, .named = false});
        } else {
            auto& parent = _stack.back();
            if (!e.repetition || *e.repetition < repetition_type::required || *e.repetition > repetition_type::repeated) {
                malformed(seastar::format("schema element {} has no valid repetition", e.name));
            }
            const unsigned def = parent.def + (*e.repetition != repetition_type::required);
            const unsigned rep = parent.rep + (*e.repetition == repetition_type::repeated);
            if (def > max_level || rep > max_level) {
                malformed("the schema nests too deep");
            }
            auto repeated_def = parent.repeated_def;
            if (*e.repetition == repetition_type::repeated) {
                repeated_def.push_back(def);
            }
            --parent.remaining;
            if (e.num_children) {
                if (e.type || *e.num_children <= 0) {
                    malformed(seastar::format("schema group {} is invalid", e.name));
                }
                if (_stack.size() >= max_schema_depth) {
                    malformed("the schema nests too deep");
                }
                _path.push_back(e.name);
                _stack.push_back(frame{.remaining = uint32_t(*e.num_children), .def = uint8_t(def), .rep = uint8_t(rep),
                        .repeated_def = std::move(repeated_def), .named = true});
            } else {
                if (!e.type) {
                    malformed(seastar::format("schema element {} is neither a group nor a leaf", e.name));
                }
                add_leaf(e, def, rep, repeated_def);
            }
        }
        _s._elements.push_back(std::move(e));
        while (!_stack.empty() && _stack.back().remaining == 0) {
            if (_stack.back().named) {
                _path.pop_back();
            }
            _stack.pop_back();
        }
        _schema_done = _stack.empty();
    }

    void on_num_rows(int64_t num_rows) override {
        _num_rows = num_rows;
    }

    void on_column_chunk(size_t row_group_index, size_t column_index, std::span<const sstring> path, column_chunk&& c) override {
        if (!_schema_done) {
            malformed("a column chunk before the end of the schema");
        }
        if (row_group_index != _s._row_groups.size() || column_index != _chunks.size() || column_index >= _s._columns.size()) {
            malformed(seastar::format("row group {} has too many column chunks", row_group_index));
        }
        const auto& leaf = _s._columns[column_index];
        if (!std::ranges::equal(path, leaf.path)) {
            malformed(seastar::format("chunk {} of row group {} does not belong to column {}", column_index, row_group_index, fmt::join(leaf.path, ".")));
        }
        const auto& m = c.meta_data;
        if (m.type != leaf.type) {
            malformed(seastar::format("a chunk of column {} has type {}", fmt::join(leaf.path, "."), int(m.type)));
        }
        if (!is_supported(m.codec)) {
            malformed(seastar::format("a chunk of column {} has unsupported codec {}", fmt::join(leaf.path, "."), int(m.codec)));
        }
        if (!m.stats || !m.stats->null_count) {
            malformed(seastar::format("a chunk of column {} has no null count", fmt::join(leaf.path, ".")));
        }
        const int64_t null_count = *m.stats->null_count;
        if (m.num_values < 0 || m.num_values > max_offset || null_count < 0 || null_count > m.num_values) {
            malformed(seastar::format("a chunk of column {} has {} values and {} nulls", fmt::join(leaf.path, "."), m.num_values, null_count));
        }
        check_offset(m.data_page_offset, "data page offset");
        check_offset(m.total_compressed_size, "chunk size");
        uint64_t start = m.data_page_offset;
        if (m.dictionary_page_offset) {
            check_offset(*m.dictionary_page_offset, "dictionary page offset");
            if (*m.dictionary_page_offset >= m.data_page_offset) {
                malformed("a dictionary page does not precede the data pages");
            }
            start = *m.dictionary_page_offset;
        }
        const uint64_t end = start + m.total_compressed_size;
        if (uint64_t(m.data_page_offset) >= end) {
            malformed(seastar::format("a chunk of column {} has no data pages", fmt::join(leaf.path, ".")));
        }
        if (c.offset_index_offset.has_value() != c.offset_index_length.has_value()) {
            malformed("an OffsetIndex location is incomplete");
        }
        chunk_info info{
            .start = start,
            .end = end,
            .has_dictionary = m.dictionary_page_offset.has_value(),
            .offset_index_length = 0,
            .num_values = uint64_t(m.num_values),
            .null_count = uint64_t(null_count),
            .uncompressed_size = uint64_t(std::max<int64_t>(m.total_uncompressed_size, 0)),
            .codec = m.codec,
        };
        if (c.offset_index_offset) {
            check_offset(*c.offset_index_offset, "OffsetIndex offset");
            if (*c.offset_index_length <= 0) {
                malformed(seastar::format("an OffsetIndex of {} bytes", *c.offset_index_length));
            }
            info.offset_index_offset = *c.offset_index_offset;
            info.offset_index_length = *c.offset_index_length;
        }
        _chunks.push_back(info);
    }

    void on_row_group(size_t g, row_group&& rg) override {
        if (!_schema_done) {
            malformed("a row group before the end of the schema");
        }
        if (_chunks.size() != _s._columns.size()) {
            malformed(seastar::format("row group {} has {} column chunks, the schema has {} columns", g, _chunks.size(), _s._columns.size()));
        }
        if (g > std::numeric_limits<uint32_t>::max() - 1) {
            malformed("too many row groups");
        }
        if (rg.num_rows <= 0 || rg.num_rows > max_offset) {
            malformed(seastar::format("row group {} has {} rows", g, rg.num_rows));
        }
        const uint64_t rows = rg.num_rows;
        const uint64_t first_row = _s._rows;
        _s._rows += rows;
        if (_s._rows > uint64_t(max_offset)) {
            malformed("too many rows");
        }
        uint64_t pos = _s.region_start(g);
        uint64_t max_chunk_uncompressed_size = 0;
        uint64_t max_chunk_values = 0;
        for (leaf_index c = 0; c < _chunks.size(); ++c) {
            const auto& ch = _chunks[c];
            max_chunk_uncompressed_size = std::max(max_chunk_uncompressed_size, ch.uncompressed_size);
            max_chunk_values = std::max(max_chunk_values, ch.num_values);
            auto& leaf = _s._columns[c];
            const auto name = [&] { return fmt::join(leaf.path, "."); };
            if (g == 0) {
                leaf.codec = ch.codec;
            } else if (ch.codec != leaf.codec) {
                malformed(seastar::format("column {} has chunks with different codecs", name()));
            }
            if (ch.num_values < rows || (leaf.max_rep == 0 && ch.num_values != rows)) {
                malformed(seastar::format("the chunk of column {} in row group {} has {} values for {} rows", name(), g, ch.num_values, rows));
            }
            if (leaf.max_def == 0 && ch.null_count) {
                malformed(seastar::format("required column {} has nulls", name()));
            }
            bool all_null;
            if (!ch.offset_index_offset) {
                // A late chunk: entirely null at level 0, after the last row group.
                if (leaf.first_in_place != not_in_place) {
                    malformed(seastar::format("the chunk of column {} in row group {} has no OffsetIndex but an earlier chunk has one", name(), g));
                }
                if (leaf.max_def == 0 || ch.null_count != rows || ch.num_values != rows || ch.has_dictionary) {
                    malformed(seastar::format("the chunk of column {} in row group {} has no OffsetIndex but is not entirely null", name(), g));
                }
                _late_start = std::min(_late_start, ch.start);
                _late_end = std::max(_late_end, ch.end);
                all_null = true;
            } else {
                if (leaf.first_in_place == not_in_place) {
                    leaf.first_in_place = g;
                }
                if (ch.start != pos) {
                    malformed(seastar::format("the chunk of column {} in row group {} starts at {}, expected {}", name(), g, ch.start, pos));
                }
                pos = ch.end;
                // Only with a single definition level is a chunk whose
                // entries are all null known to be entirely at level 0.
                all_null = leaf.max_def == 1 && ch.null_count == ch.num_values;
                if (all_null && ch.num_values != rows) {
                    malformed(seastar::format("the chunk of column {} in row group {} is null but has {} entries for {} rows", name(), g, ch.num_values, rows));
                }
            }
            push_bit(all_null);
            push_bit(ch.has_dictionary);
        }
        const uint64_t block_offset = pos;
        for (const auto& ch : _chunks) {
            if (ch.offset_index_offset) {
                if (*ch.offset_index_offset != pos) {
                    malformed(seastar::format("an OffsetIndex of row group {} is at {}, expected {}", g, *ch.offset_index_offset, pos));
                }
                pos += ch.offset_index_length;
            }
        }
        if (pos - block_offset > std::numeric_limits<uint32_t>::max()) {
            malformed(seastar::format("the OffsetIndex block of row group {} is too large", g));
        }
        _s._row_groups.push_back(file_summary::row_group_entry{
            .first_row = first_row,
            .offset_index_offset = block_offset,
            .offset_index_length = uint32_t(pos - block_offset),
            .max_chunk_uncompressed_size = max_chunk_uncompressed_size,
            .max_chunk_values = max_chunk_values,
        });
        _chunks.clear();
        seastar::thread::maybe_yield();
    }

    void on_key_value(key_value&& kv) override {
        _s._key_value_metadata.push_back(std::move(kv));
    }

    file_summary finish(uint64_t footer_offset) && {
        if (!_version_seen || !_root_seen || !_schema_done) {
            malformed("the schema is incomplete");
        }
        if (!_chunks.empty()) {
            malformed("column chunks without a row group");
        }
        if (!_num_rows || uint64_t(*_num_rows) != _s._rows) {
            malformed(seastar::format("the file declares {} rows, its row groups hold {}", _num_rows.value_or(-1), _s._rows));
        }
        const uint64_t regions_end = _s.region_start(_s.row_groups());
        if (_late_start < regions_end || _late_end > footer_offset || regions_end > footer_offset) {
            malformed("column chunks overlap the row groups or the footer");
        }
        for (auto& c : _s._columns) {
            if (c.first_in_place == not_in_place) {
                c.first_in_place = _s.row_groups();
            }
        }
        _s._footer_offset = footer_offset;
        return std::move(_s);
    }
};

namespace {

// Reads the footer from an input stream and computes its checksum on the way.
class checksummed_source final : public thrift::byte_source {
    input_stream<char>& _in;
    temporary_buffer<char> _buf;
public:
    uint32_t crc = crc32_utils::init_checksum();
    uint64_t bytes = 0;

    explicit checksummed_source(input_stream<char>& in) : _in(in) {}

    std::span<const char> next() override {
        _buf = _in.read().get();
        crc = crc32_utils::checksum(crc, _buf.get(), _buf.size());
        bytes += _buf.size();
        return {_buf.get(), _buf.size()};
    }
};

} // anonymous namespace

future<file_summary> read_file_summary(file f, uint64_t file_size, std::optional<uint32_t> expected_crc32) {
    constexpr uint64_t trailer_size = 8;
    if (file_size < magic.size() + trailer_size) {
        malformed(seastar::format("a file of {} bytes is too small", file_size));
    }
    auto trailer = co_await f.dma_read_bulk<char>(file_size - trailer_size, trailer_size);
    if (trailer.size() < trailer_size) {
        malformed("the file is shorter than its declared size");
    }
    if (std::string_view(trailer.get() + 4, 4) != magic) {
        malformed("the file does not end with the Parquet magic bytes");
    }
    uint32_t length;
    std::memcpy(&length, trailer.get(), sizeof(length));
    if (length > file_size - magic.size() - trailer_size) {
        malformed(seastar::format("a footer of {} bytes does not fit in a file of {} bytes", length, file_size));
    }
    const uint64_t footer_offset = file_size - trailer_size - length;
    auto in = make_file_input_stream(f, footer_offset, length, file_input_stream_options{.buffer_size = 128 * 1024, .read_ahead = 1});
    summary_builder builder;
    std::exception_ptr ex;
    try {
        co_await seastar::async([&] {
            checksummed_source source(in);
            parse_file_metadata(source, length, builder);
            if (source.bytes != length) {
                malformed("the footer is truncated");
            }
            if (expected_crc32 && source.crc != *expected_crc32) {
                malformed(seastar::format("the footer checksum is {:#x}, expected {:#x}", source.crc, *expected_crc32));
            }
        });
    } catch (...) {
        ex = std::current_exception();
    }
    co_await in.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return std::move(builder).finish(footer_offset);
}

} // namespace sstables::parquet
