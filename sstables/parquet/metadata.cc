/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/metadata.hh"

#include <limits>

#include <seastar/core/on_internal_error.hh>
#include <seastar/core/thread.hh>

#include "sstables/file_writer.hh"
#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger parquet_metadata_logger("parquet_metadata");

using thrift::field_header;
using thrift::field_set;
using thrift::type;

// Limits on lists that a parser holds in memory as a whole. Our files stay
// far below them. They prevent a corrupt footer from making the parser
// build a large contiguous array.
static constexpr uint32_t max_path_length = 128;
static constexpr uint32_t max_encodings = 32;

// Buffered footer bytes are written out when they exceed this size.
static constexpr size_t footer_flush_threshold = 64 * 1024;

static void maybe_yield() {
    if (seastar::thread::running_in_thread()) {
        seastar::thread::maybe_yield();
    }
}

static uint32_t list_size(size_t n) {
    if (n > size_t(std::numeric_limits<int32_t>::max())) {
        on_internal_error(parquet_metadata_logger, "Parquet metadata: list too long");
    }
    return n;
}

// Serialization

namespace {

using thrift::encoder;

void write_logical_type(encoder& e, const logical_type& lt) {
    e.begin_struct();
    switch (lt.k) {
    case logical_type::kind::string:
    case logical_type::kind::map:
    case logical_type::kind::list:
    case logical_type::kind::uuid:
        e.begin_struct_field(static_cast<int16_t>(lt.k));
        e.end_struct();
        break;
    case logical_type::kind::integer:
        e.begin_struct_field(static_cast<int16_t>(lt.k));
        e.write_i8_field(1, lt.bit_width);
        e.write_bool_field(2, lt.is_signed);
        e.end_struct();
        break;
    default:
        on_internal_error(parquet_metadata_logger, seastar::format("Parquet metadata: cannot write logical type {}", static_cast<int16_t>(lt.k)));
    }
    e.end_struct();
}

void write_schema_element(encoder& e, const schema_element& s) {
    e.begin_struct();
    if (s.type) {
        e.write_i32_field(1, static_cast<int32_t>(*s.type));
    }
    if (s.type_length) {
        e.write_i32_field(2, *s.type_length);
    }
    if (s.repetition) {
        e.write_i32_field(3, static_cast<int32_t>(*s.repetition));
    }
    e.write_binary_field(4, s.name);
    if (s.num_children) {
        e.write_i32_field(5, *s.num_children);
    }
    if (s.converted) {
        e.write_i32_field(6, static_cast<int32_t>(*s.converted));
    }
    if (s.logical) {
        e.field_header(10, type::struct_);
        write_logical_type(e, *s.logical);
    }
    e.end_struct();
}

void write_key_value(encoder& e, const key_value& kv) {
    e.begin_struct();
    e.write_binary_field(1, kv.key);
    if (kv.value) {
        e.write_binary_field(2, *kv.value);
    }
    e.end_struct();
}

void write_column_metadata(encoder& e, const column_metadata& m, std::span<const std::string_view> path) {
    e.begin_struct();
    e.write_i32_field(1, static_cast<int32_t>(m.type));
    e.write_list_field(2, type::i32, list_size(m.encodings.size()));
    for (auto enc : m.encodings) {
        e.write_i32(static_cast<int32_t>(enc));
    }
    e.write_list_field(3, type::binary, list_size(path.size()));
    for (auto name : path) {
        e.write_binary(name);
    }
    e.write_i32_field(4, static_cast<int32_t>(m.codec));
    e.write_i64_field(5, m.num_values);
    e.write_i64_field(6, m.total_uncompressed_size);
    e.write_i64_field(7, m.total_compressed_size);
    e.write_i64_field(9, m.data_page_offset);
    if (m.dictionary_page_offset) {
        e.write_i64_field(11, *m.dictionary_page_offset);
    }
    if (m.stats) {
        e.begin_struct_field(12);
        if (m.stats->null_count) {
            e.write_i64_field(3, *m.stats->null_count);
        }
        e.end_struct();
    }
    e.end_struct();
}

void write_column_chunk(encoder& e, const column_chunk& c, std::span<const std::string_view> path) {
    e.begin_struct();
    e.write_i64_field(2, c.file_offset);
    e.field_header(3, type::struct_);
    write_column_metadata(e, c.meta_data, path);
    if (c.offset_index_offset) {
        e.write_i64_field(4, *c.offset_index_offset);
    }
    if (c.offset_index_length) {
        e.write_i32_field(5, *c.offset_index_length);
    }
    e.end_struct();
}

using leaf_path = std::vector<std::string_view>;

// Returns the path of each leaf of the schema, in schema order.
// The schema is the depth-first list of the schema tree.
utils::chunked_vector<leaf_path> leaf_paths(const utils::chunked_vector<schema_element>& schema) {
    auto bad_schema = [] (std::string_view what) {
        on_internal_error(parquet_metadata_logger, seastar::format("Parquet metadata: invalid schema: {}", what));
    };
    if (schema.empty() || !schema.front().num_children) {
        bad_schema("no root group");
    }
    utils::chunked_vector<leaf_path> paths;
    leaf_path prefix;
    // The number of children not yet seen, for each open group.
    // The root is the first entry and has no name in `prefix`.
    std::vector<int32_t> pending{*schema.front().num_children};
    for (size_t i = 1; i < schema.size(); ++i) {
        while (!pending.empty() && pending.back() == 0) {
            pending.pop_back();
            if (!prefix.empty()) {
                prefix.pop_back();
            }
        }
        if (pending.empty()) {
            bad_schema("more elements than the tree has");
        }
        --pending.back();
        const auto& el = schema[i];
        if (el.num_children) {
            if (*el.num_children <= 0) {
                bad_schema("empty group");
            }
            prefix.push_back(el.name);
            pending.push_back(*el.num_children);
        } else {
            auto& path = paths.emplace_back(prefix);
            path.push_back(el.name);
        }
        maybe_yield();
    }
    if (std::ranges::any_of(pending, [] (int32_t n) { return n != 0; })) {
        bad_schema("fewer elements than the tree has");
    }
    return paths;
}

void write_row_group(encoder& e, const row_group& rg, const utils::chunked_vector<leaf_path>& paths) {
    if (rg.columns.size() != paths.size()) {
        on_internal_error(parquet_metadata_logger, seastar::format("Parquet metadata: row group has {} columns, but the schema has {} leaves",
                rg.columns.size(), paths.size()));
    }
    e.begin_struct();
    e.write_list_field(1, type::struct_, list_size(rg.columns.size()));
    for (size_t i = 0; i < rg.columns.size(); ++i) {
        write_column_chunk(e, rg.columns[i], paths[i]);
        maybe_yield();
    }
    e.write_i64_field(2, rg.total_byte_size);
    e.write_i64_field(3, rg.num_rows);
    if (rg.file_offset) {
        e.write_i64_field(5, *rg.file_offset);
    }
    if (rg.total_compressed_size) {
        e.write_i64_field(6, *rg.total_compressed_size);
    }
    if (rg.ordinal) {
        e.field_header(7, type::i16);
        e.write_i16(*rg.ordinal);
    }
    e.end_struct();
}

// Serializes the footer into `buf`. Calls `flush(buf)` at points where the
// caller may move the buffered bytes elsewhere and clear `buf`.
template <typename Flush>
void serialize_file_metadata(const file_metadata& md, bytes_ostream& buf, Flush&& flush) {
    const auto paths = leaf_paths(md.schema);
    encoder e(buf);
    e.begin_struct();
    e.write_i32_field(1, md.version);
    e.write_list_field(2, type::struct_, list_size(md.schema.size()));
    for (const auto& el : md.schema) {
        write_schema_element(e, el);
        maybe_yield();
    }
    e.write_i64_field(3, md.num_rows);
    e.write_list_field(4, type::struct_, list_size(md.row_groups.size()));
    for (const auto& rg : md.row_groups) {
        write_row_group(e, rg, paths);
        flush(buf);
        maybe_yield();
    }
    if (!md.key_value_metadata.empty()) {
        e.write_list_field(5, type::struct_, list_size(md.key_value_metadata.size()));
        for (const auto& kv : md.key_value_metadata) {
            write_key_value(e, kv);
        }
    }
    if (md.created_by) {
        e.write_binary_field(6, *md.created_by);
    }
    e.end_struct();
}

} // anonymous namespace

void write_page_header(const page_header& h, bytes_ostream& out) {
    encoder e(out);
    e.begin_struct();
    e.write_i32_field(1, static_cast<int32_t>(h.type));
    e.write_i32_field(2, h.uncompressed_page_size);
    e.write_i32_field(3, h.compressed_page_size);
    if (h.crc) {
        e.write_i32_field(4, *h.crc);
    }
    if (h.dictionary_page) {
        const auto& d = *h.dictionary_page;
        e.begin_struct_field(7);
        e.write_i32_field(1, d.num_values);
        e.write_i32_field(2, static_cast<int32_t>(d.enc));
        if (d.is_sorted) {
            e.write_bool_field(3, *d.is_sorted);
        }
        e.end_struct();
    }
    if (h.data_page_v2) {
        const auto& d = *h.data_page_v2;
        e.begin_struct_field(8);
        e.write_i32_field(1, d.num_values);
        e.write_i32_field(2, d.num_nulls);
        e.write_i32_field(3, d.num_rows);
        e.write_i32_field(4, static_cast<int32_t>(d.enc));
        e.write_i32_field(5, d.definition_levels_byte_length);
        e.write_i32_field(6, d.repetition_levels_byte_length);
        e.write_bool_field(7, d.is_compressed);
        e.end_struct();
    }
    e.end_struct();
}

void write_offset_index(const offset_index& idx, bytes_ostream& out) {
    encoder e(out);
    e.begin_struct();
    e.write_list_field(1, type::struct_, list_size(idx.page_locations.size()));
    for (const auto& loc : idx.page_locations) {
        e.begin_struct();
        e.write_i64_field(1, loc.offset);
        e.write_i32_field(2, loc.compressed_page_size);
        e.write_i64_field(3, loc.first_row_index);
        e.end_struct();
    }
    e.end_struct();
}

bytes_ostream serialize_file_metadata(const file_metadata& md) {
    bytes_ostream out;
    serialize_file_metadata(md, out, [] (bytes_ostream&) {});
    return out;
}

uint64_t write_file_metadata(const file_metadata& md, sstables::file_writer& out) {
    uint64_t written = 0;
    auto write_out = [&] (bytes_ostream& buf) {
        for (bytes_view frag : buf.fragments()) {
            out.write(frag);
        }
        written += buf.size();
        buf.clear();
    };
    bytes_ostream buf;
    serialize_file_metadata(md, buf, [&] (bytes_ostream& pending) {
        if (pending.size() >= footer_flush_threshold) {
            write_out(pending);
        }
    });
    write_out(buf);
    return written;
}

// Parsing

namespace {

using thrift::decoder;

template <typename Enum>
Enum read_enum(decoder& d, const field_header& f) {
    return static_cast<Enum>(d.read_i32(f));
}

logical_type parse_logical_type(decoder& d, const field_header& f) {
    std::optional<logical_type> lt;
    d.read_struct(f, [&] (const field_header& member) {
        if (lt) {
            d.fail("LogicalType has more than one member");
        }
        lt.emplace(static_cast<logical_type::kind>(member.id));
        if (member.id == static_cast<int16_t>(logical_type::kind::integer)) {
            field_set seen;
            d.read_struct(member, [&] (const field_header& g) {
                switch (g.id) {
                case 1: seen.add(d, g); lt->bit_width = d.read_i8(g); break;
                case 2: seen.add(d, g); lt->is_signed = d.read_bool(g); break;
                default: d.skip(g);
                }
            });
            seen.require(d, {1, 2}, "IntType");
        } else {
            d.skip(member);
        }
    });
    if (!lt) {
        d.fail("LogicalType has no member");
    }
    return *lt;
}

schema_element parse_schema_element(decoder& d) {
    schema_element s;
    field_set seen;
    d.read_struct([&] (const field_header& f) {
        switch (f.id) {
        case 1: seen.add(d, f); s.type = read_enum<physical_type>(d, f); break;
        case 2: seen.add(d, f); s.type_length = d.read_i32(f); break;
        case 3: seen.add(d, f); s.repetition = read_enum<repetition_type>(d, f); break;
        case 4: seen.add(d, f); s.name = d.read_string(f); break;
        case 5: seen.add(d, f); s.num_children = d.read_i32(f); break;
        case 6: seen.add(d, f); s.converted = read_enum<converted_type>(d, f); break;
        case 10: seen.add(d, f); s.logical = parse_logical_type(d, f); break;
        default: d.skip(f);
        }
    });
    seen.require(d, {4}, "SchemaElement");
    return s;
}

key_value parse_key_value(decoder& d) {
    key_value kv;
    field_set seen;
    d.read_struct([&] (const field_header& f) {
        switch (f.id) {
        case 1: {
            seen.add(d, f);
            d.expect(f, type::binary);
            const uint32_t n = d.read_binary_size();
            if (n > max_key_value_size) {
                d.fail(seastar::format("a key-value metadata key of {} bytes", n));
            }
            kv.key = sstring(sstring::initialized_later(), n);
            d.read_bytes(kv.key.data(), n);
            break;
        }
        case 2: {
            seen.add(d, f);
            d.expect(f, type::binary);
            const uint32_t n = d.read_binary_size();
            if (n > max_key_value_size) {
                d.skip_bytes(n);
                kv.value.reset();
            } else {
                kv.value = sstring(sstring::initialized_later(), n);
                d.read_bytes(kv.value->data(), n);
            }
            break;
        }
        default: d.skip(f);
        }
    });
    seen.require(d, {1}, "KeyValue");
    return kv;
}

statistics parse_statistics(decoder& d, const field_header& f) {
    statistics s;
    field_set seen;
    d.read_struct(f, [&] (const field_header& g) {
        switch (g.id) {
        case 3: seen.add(d, g); s.null_count = d.read_i64(g); break;
        default: d.skip(g);
        }
    });
    return s;
}

using path_buffer = utils::small_vector<sstring, 8>;

column_metadata parse_column_metadata(decoder& d, const field_header& f, path_buffer& path) {
    column_metadata m;
    field_set seen;
    d.read_struct(f, [&] (const field_header& g) {
        switch (g.id) {
        case 1: seen.add(d, g); m.type = read_enum<physical_type>(d, g); break;
        case 2: {
            seen.add(d, g);
            auto h = d.read_list_header(g, type::i32);
            if (h.size > max_encodings) {
                d.fail("too many encodings");
            }
            for (uint32_t i = 0; i < h.size; ++i) {
                m.encodings.push_back(static_cast<encoding>(d.read_i32()));
            }
            break;
        }
        case 3: {
            seen.add(d, g);
            auto h = d.read_list_header(g, type::binary);
            if (h.size > max_path_length) {
                d.fail("path_in_schema is too long");
            }
            path.clear();
            for (uint32_t i = 0; i < h.size; ++i) {
                path.push_back(d.read_string());
            }
            break;
        }
        case 4: seen.add(d, g); m.codec = read_enum<compression_codec>(d, g); break;
        case 5: seen.add(d, g); m.num_values = d.read_i64(g); break;
        case 6: seen.add(d, g); m.total_uncompressed_size = d.read_i64(g); break;
        case 7: seen.add(d, g); m.total_compressed_size = d.read_i64(g); break;
        case 9: seen.add(d, g); m.data_page_offset = d.read_i64(g); break;
        case 11: seen.add(d, g); m.dictionary_page_offset = d.read_i64(g); break;
        case 12: seen.add(d, g); m.stats = parse_statistics(d, g); break;
        default: d.skip(g);
        }
    });
    seen.require(d, {1, 2, 3, 4, 5, 6, 7, 9}, "ColumnMetaData");
    return m;
}

column_chunk parse_column_chunk(decoder& d, path_buffer& path) {
    column_chunk c;
    field_set seen;
    d.read_struct([&] (const field_header& f) {
        switch (f.id) {
        case 1:
            // A chunk stored in another file.
            d.fail("ColumnChunk.file_path is not supported");
        case 2: seen.add(d, f); c.file_offset = d.read_i64(f); break;
        case 3: seen.add(d, f); c.meta_data = parse_column_metadata(d, f, path); break;
        case 4: seen.add(d, f); c.offset_index_offset = d.read_i64(f); break;
        case 5: seen.add(d, f); c.offset_index_length = d.read_i32(f); break;
        case 8:
        case 9:
            d.fail("Parquet modular encryption is not supported");
        default: d.skip(f);
        }
    });
    // file_offset is required in the Thrift, but it is deprecated, so its absence is tolerated.
    seen.require(d, {3}, "ColumnChunk");
    return c;
}

void parse_row_group(decoder& d, size_t index, file_metadata_visitor& v, path_buffer& path) {
    row_group rg;
    field_set seen;
    d.read_struct([&] (const field_header& f) {
        switch (f.id) {
        case 1: {
            seen.add(d, f);
            auto h = d.read_list_header(f, type::struct_);
            for (uint32_t i = 0; i < h.size; ++i) {
                auto c = parse_column_chunk(d, path);
                v.on_column_chunk(index, i, std::span<const sstring>(path.data(), path.size()), std::move(c));
                maybe_yield();
            }
            break;
        }
        case 2: seen.add(d, f); rg.total_byte_size = d.read_i64(f); break;
        case 3: seen.add(d, f); rg.num_rows = d.read_i64(f); break;
        case 5: seen.add(d, f); rg.file_offset = d.read_i64(f); break;
        case 6: seen.add(d, f); rg.total_compressed_size = d.read_i64(f); break;
        case 7: seen.add(d, f); d.expect(f, type::i16); rg.ordinal = d.read_i16(); break;
        default: d.skip(f);
        }
    });
    seen.require(d, {1, 2, 3}, "RowGroup");
    v.on_row_group(index, std::move(rg));
}

void parse_file_metadata(decoder& d, file_metadata_visitor& v) {
    field_set seen;
    path_buffer path;
    d.read_struct([&] (const field_header& f) {
        switch (f.id) {
        case 1: seen.add(d, f); v.on_version(d.read_i32(f)); break;
        case 2: {
            seen.add(d, f);
            auto h = d.read_list_header(f, type::struct_);
            for (uint32_t i = 0; i < h.size; ++i) {
                v.on_schema_element(parse_schema_element(d));
                maybe_yield();
            }
            break;
        }
        case 3: seen.add(d, f); v.on_num_rows(d.read_i64(f)); break;
        case 4: {
            seen.add(d, f);
            auto h = d.read_list_header(f, type::struct_);
            for (uint32_t i = 0; i < h.size; ++i) {
                parse_row_group(d, i, v, path);
            }
            break;
        }
        case 5: {
            seen.add(d, f);
            auto h = d.read_list_header(f, type::struct_);
            for (uint32_t i = 0; i < h.size; ++i) {
                v.on_key_value(parse_key_value(d));
                maybe_yield();
            }
            break;
        }
        case 6: seen.add(d, f); v.on_created_by(d.read_string(f)); break;
        case 8:
            d.fail("Parquet modular encryption is not supported");
        default: d.skip(f);
        }
    });
    seen.require(d, {1, 2, 3, 4}, "FileMetaData");
    if (d.remaining()) {
        d.fail("trailing bytes after FileMetaData");
    }
}

class input_stream_source final : public thrift::byte_source {
    input_stream<char>& _in;
    temporary_buffer<char> _buf;
public:
    explicit input_stream_source(input_stream<char>& in) : _in(in) {}
    std::span<const char> next() override {
        _buf = _in.read().get();
        return {_buf.get(), _buf.size()};
    }
};

} // anonymous namespace

page_header parse_page_header(decoder& d) {
    page_header h;
    field_set seen;
    d.read_struct([&] (const field_header& f) {
        switch (f.id) {
        case 1: seen.add(d, f); h.type = read_enum<page_type>(d, f); break;
        case 2: seen.add(d, f); h.uncompressed_page_size = d.read_i32(f); break;
        case 3: seen.add(d, f); h.compressed_page_size = d.read_i32(f); break;
        case 4: seen.add(d, f); h.crc = d.read_i32(f); break;
        case 7: {
            seen.add(d, f);
            dictionary_page_header dict;
            field_set dict_seen;
            d.read_struct(f, [&] (const field_header& g) {
                switch (g.id) {
                case 1: dict_seen.add(d, g); dict.num_values = d.read_i32(g); break;
                case 2: dict_seen.add(d, g); dict.enc = read_enum<encoding>(d, g); break;
                case 3: dict_seen.add(d, g); dict.is_sorted = d.read_bool(g); break;
                default: d.skip(g);
                }
            });
            dict_seen.require(d, {1, 2}, "DictionaryPageHeader");
            h.dictionary_page = dict;
            break;
        }
        case 8: {
            seen.add(d, f);
            data_page_header_v2 dp;
            field_set dp_seen;
            d.read_struct(f, [&] (const field_header& g) {
                switch (g.id) {
                case 1: dp_seen.add(d, g); dp.num_values = d.read_i32(g); break;
                case 2: dp_seen.add(d, g); dp.num_nulls = d.read_i32(g); break;
                case 3: dp_seen.add(d, g); dp.num_rows = d.read_i32(g); break;
                case 4: dp_seen.add(d, g); dp.enc = read_enum<encoding>(d, g); break;
                case 5: dp_seen.add(d, g); dp.definition_levels_byte_length = d.read_i32(g); break;
                case 6: dp_seen.add(d, g); dp.repetition_levels_byte_length = d.read_i32(g); break;
                case 7: dp_seen.add(d, g); dp.is_compressed = d.read_bool(g); break;
                default: d.skip(g);
                }
            });
            dp_seen.require(d, {1, 2, 3, 4, 5, 6}, "DataPageHeaderV2");
            h.data_page_v2 = dp;
            break;
        }
        default: d.skip(f);
        }
    });
    seen.require(d, {1, 2, 3}, "PageHeader");
    return h;
}

offset_index parse_offset_index(decoder& d) {
    offset_index idx;
    field_set seen;
    d.read_struct([&] (const field_header& f) {
        switch (f.id) {
        case 1: {
            seen.add(d, f);
            auto h = d.read_list_header(f, type::struct_);
            for (uint32_t i = 0; i < h.size; ++i) {
                page_location loc;
                field_set loc_seen;
                d.read_struct([&] (const field_header& g) {
                    switch (g.id) {
                    case 1: loc_seen.add(d, g); loc.offset = d.read_i64(g); break;
                    case 2: loc_seen.add(d, g); loc.compressed_page_size = d.read_i32(g); break;
                    case 3: loc_seen.add(d, g); loc.first_row_index = d.read_i64(g); break;
                    default: d.skip(g);
                    }
                });
                loc_seen.require(d, {1, 2, 3}, "PageLocation");
                idx.page_locations.push_back(loc);
            }
            break;
        }
        default: d.skip(f);
        }
    });
    seen.require(d, {1}, "OffsetIndex");
    return idx;
}

void parse_file_metadata(thrift::byte_source& source, uint64_t length, file_metadata_visitor& v) {
    decoder d(source, length);
    parse_file_metadata(d, v);
}

future<> parse_file_metadata(input_stream<char>& in, uint64_t length, file_metadata_visitor& v) {
    return seastar::async([&in, length, &v] {
        input_stream_source source(in);
        parse_file_metadata(source, length, v);
    });
}

void file_metadata_builder::on_version(int32_t version) {
    _md.version = version;
}

void file_metadata_builder::on_schema_element(schema_element&& e) {
    _md.schema.push_back(std::move(e));
}

void file_metadata_builder::on_num_rows(int64_t num_rows) {
    _md.num_rows = num_rows;
}

void file_metadata_builder::on_column_chunk(size_t, size_t, std::span<const sstring>, column_chunk&& c) {
    _current.columns.push_back(std::move(c));
}

void file_metadata_builder::on_row_group(size_t, row_group&& rg) {
    rg.columns = std::exchange(_current.columns, {});
    _md.row_groups.push_back(std::move(rg));
}

void file_metadata_builder::on_key_value(key_value&& kv) {
    _md.key_value_metadata.push_back(std::move(kv));
}

void file_metadata_builder::on_created_by(sstring&& created_by) {
    _md.created_by = std::move(created_by);
}

file_metadata file_metadata_builder::get() && {
    return std::move(_md);
}

} // namespace sstables::parquet
