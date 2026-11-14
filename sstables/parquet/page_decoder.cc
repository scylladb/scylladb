/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/page_decoder.hh"

#include <algorithm>
#include <array>

#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <fmt/ranges.h>

#include "sstables/checksum_utils.hh"
#include "sstables/exceptions.hh"
#include "sstables/parquet/rle.hh"
#include "sstables/parquet/thrift.hh"
#include "utils/log.hh"
#include "utils/managed_bytes.hh"

namespace sstables::parquet {

static logging::logger page_decoder_logger("parquet_page_decoder");

template <typename... Args>
[[noreturn]] static void malformed(fmt::format_string<Args...> fmt, Args&&... args) {
    throw malformed_sstable_exception(seastar::format("Parquet file is malformed: {}", fmt::format(fmt, std::forward<Args>(args)...)));
}

// row_group_index

const compact_chunk& row_group_index::chunk(leaf_index c) const {
    // An index of all columns holds column c at position c.
    if (c < _columns.size() && _columns[c].leaf == c) {
        return _columns[c].chunk;
    }
    size_t lo = 0;
    size_t hi = _columns.size();
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (_columns[mid].leaf < c) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo == _columns.size() || _columns[lo].leaf != c) {
        on_internal_error(page_decoder_logger, seastar::format("Parquet row group index: column {} is not in the index", c));
    }
    return _columns[lo].chunk;
}

uint32_t row_group_index::page_of(leaf_index c, uint64_t row) const {
    const auto& ch = chunk(c);
    uint32_t lo = 0;
    uint32_t hi = ch.pages;
    // The last page whose first row is at most `row`. Page 0 starts at row 0.
    while (hi - lo > 1) {
        const uint32_t mid = lo + (hi - lo) / 2;
        if (_pages[ch.first_page + mid].first_row <= row) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    return lo;
}

namespace {

// Reads records from a view, at positions that do not decrease.
template <FragmentedView View>
class record_reader {
    View _v;
    size_t _pos = 0;
public:
    explicit record_reader(View v) noexcept : _v(v) {}
    void seek(size_t pos) noexcept {
        _v.remove_prefix(pos - _pos);
        _pos = pos;
    }
    template <typename T>
    T read() noexcept {
        T r;
        read_fragmented(_v, sizeof(T), reinterpret_cast<bytes::value_type*>(&r));
        _pos += sizeof(T);
        return r;
    }
    // Reads `n` records into `out`.
    template <typename T>
    void read(T* out, size_t n) noexcept {
        read_fragmented(_v, n * sizeof(T), reinterpret_cast<bytes::value_type*>(out));
        _pos += n * sizeof(T);
    }
};

} // anonymous namespace

template <FragmentedView View>
row_group_index_copier::row_group_index_copier(const file_summary& s, uint32_t g, std::span<const leaf_index> columns, View compact)
    : _columns(columns)
    , _all(false)
    , _leaves(s.columns().size())
{
    _index._region_start = s.region_start(g);
    _index._rows = s.row_group_rows(g);
    check(compact);
}

template <FragmentedView View>
row_group_index_copier::row_group_index_copier(const file_summary& s, uint32_t g, all_columns_tag, View compact)
    : _all(true)
    , _leaves(s.columns().size())
{
    _index._region_start = s.region_start(g);
    _index._rows = s.row_group_rows(g);
    check(compact);
}

// The compact form comes from decode_offset_index_block(), so a failed
// check is a bug. The checks keep copy() within the form.
template <FragmentedView View>
void row_group_index_copier::check(View compact) {
    const size_t table = _leaves * sizeof(compact_chunk);
    if (compact.size_bytes() < table || (compact.size_bytes() - table) % sizeof(compact_page)) {
        on_internal_error(page_decoder_logger, seastar::format("Parquet row group index: a compact form of {} bytes for {} columns",
                compact.size_bytes(), _leaves));
    }
    _compact_pages = (compact.size_bytes() - table) / sizeof(compact_page);
    record_reader<View> in(compact);
    // The end of the pages of the previous column.
    uint64_t end = 0;
    for (size_t i = 0; i < columns(); ++i) {
        const leaf_index c = column_at(i);
        if (c >= _leaves || (i && c <= column_at(i - 1))) {
            on_internal_error(page_decoder_logger, seastar::format("Parquet row group index: column {} out of order or not in the file", c));
        }
        in.seek(c * sizeof(compact_chunk));
        const auto ch = in.template read<compact_chunk>();
        if (ch.first_page < end || uint64_t(ch.first_page) + ch.pages > _compact_pages) {
            on_internal_error(page_decoder_logger, seastar::format("Parquet row group index: column {} has pages [{}, {}) of {}, after {}",
                    c, ch.first_page, uint64_t(ch.first_page) + ch.pages, _compact_pages, end));
        }
        end = ch.first_page + ch.pages;
        _pages += ch.pages;
    }
}

void row_group_index_copier::allocate() {
    _index._columns.reserve(columns());
    _index._pages.reserve(_pages);
}

template <FragmentedView View>
bool row_group_index_copier::copy(View compact, size_t max_records) noexcept {
    const size_t table = _leaves * sizeof(compact_chunk);
    record_reader<View> in(compact);
    // The records of the column table come first, in one step. The pages
    // follow them in the compact form, so the reader only moves forward.
    if (_index._columns.size() < columns()) {
        for (size_t i = 0; i < columns(); ++i) {
            const leaf_index c = column_at(i);
            in.seek(c * sizeof(compact_chunk));
            _index._columns.push_back(row_group_index::column{.leaf = c, .chunk = in.template read<compact_chunk>()});
        }
    }
    // A column's first_page keeps its position in the compact form until
    // its pages are copied, and then takes their position in the index.
    while (_next_column < columns() && max_records) {
        auto& ch = _index._columns[_next_column].chunk;
        if (_next_page == 0) {
            _column_base = _index._pages.size();
        }
        const uint32_t n = std::min<size_t>(ch.pages - _next_page, max_records);
        if (n) {
            in.seek(table + (size_t(ch.first_page) + _next_page) * sizeof(compact_page));
            // The records are copied a chunk of the vector at a time,
            // which is contiguous. allocate() reserved the room.
            const size_t chunk_capacity = utils::chunked_vector<compact_page>::max_chunk_capacity();
            size_t pos = _index._pages.size();
            _index._pages.resize(pos + n);
            for (size_t left = n; left;) {
                const size_t now = std::min(left, chunk_capacity - pos % chunk_capacity);
                in.read(&_index._pages[pos], now);
                pos += now;
                left -= now;
            }
        }
        _next_page += n;
        max_records -= n;
        if (_next_page == ch.pages) {
            ch.first_page = _column_base;
            ++_next_column;
            _next_page = 0;
        }
    }
    return _next_column == columns();
}

template row_group_index_copier::row_group_index_copier(const file_summary&, uint32_t, std::span<const leaf_index>, managed_bytes_view);
template row_group_index_copier::row_group_index_copier(const file_summary&, uint32_t, std::span<const leaf_index>, byte_array_view);
template row_group_index_copier::row_group_index_copier(const file_summary&, uint32_t, all_columns_tag, byte_array_view);
template bool row_group_index_copier::copy(managed_bytes_view, size_t) noexcept;
template bool row_group_index_copier::copy(byte_array_view, size_t) noexcept;

// The page records that one step of a copy copies: about 96 KiB.
static constexpr size_t copy_step = 8192;

static future<row_group_index> copy_row_group_index(row_group_index_copier copier, const fragmented_temporary_buffer& compact) {
    copier.allocate();
    while (!copier.copy(byte_array_view(compact), copy_step)) {
        co_await coroutine::maybe_yield();
    }
    co_return std::move(copier).release();
}

future<row_group_index> make_row_group_index(const file_summary& s, uint32_t g, std::span<const leaf_index> columns,
        const fragmented_temporary_buffer& compact) {
    return copy_row_group_index(row_group_index_copier(s, g, columns, byte_array_view(compact)), compact);
}

future<row_group_index> decode_row_group_index(const file_summary& s, uint32_t g, fragmented_temporary_buffer block) {
    const auto compact = co_await decode_offset_index_block(s, g, std::move(block));
    co_return co_await copy_row_group_index(row_group_index_copier(s, g, row_group_index_copier::all_columns_tag{}, byte_array_view(compact)), compact);
}

size_t row_group_index_decode_memory(const file_summary& s, uint32_t g) noexcept {
    const uint64_t leaves = s.columns().size();
    // A PageLocation takes at least 7 bytes: three field headers, three
    // varints, and the stop byte.
    const uint64_t max_pages = s.row_group(g).offset_index_length / 7;
    const uint64_t compact = leaves * sizeof(compact_chunk) + max_pages * sizeof(compact_page);
    // The decoder parses the PageLocations of one chunk into a vector, and
    // appends the pages to a chunked_vector. While such a vector grows, it
    // holds at most three times its size, and at least 512 bytes. Then it
    // writes the compact form, and the index is copied out of it.
    return leaves * sizeof(compact_chunk) + 3 * max_pages * (sizeof(compact_page) + sizeof(page_location)) + 2 * 512
            + compact + (compact / fragmented_temporary_buffer::default_fragment_size + 1) * sizeof(temporary_buffer<char>)
            + row_group_index::memory_usage(leaves, max_pages);
}

future<fragmented_temporary_buffer> decode_offset_index_block(const file_summary& s, uint32_t g, fragmented_temporary_buffer block) {
    const auto& rg = s.row_group(g);
    if (block.size_bytes() != rg.offset_index_length) {
        malformed("the OffsetIndex block of row group {} has {} bytes, expected {}", g, block.size_bytes(), rg.offset_index_length);
    }
    const uint64_t rows = s.row_group_rows(g);
    const uint64_t region_start = s.region_start(g);
    const uint64_t region_end = rg.offset_index_offset;
    // The compact form keeps offsets and rows in 32 bits. The writer's
    // limits on the size of a row group keep both well below.
    if (rows > std::numeric_limits<uint32_t>::max() || region_end - region_start > std::numeric_limits<uint32_t>::max()) {
        malformed("row group {} has {} rows in {} bytes, more than 32-bit offsets cover", g, rows, region_end - region_start);
    }
    utils::chunked_vector<compact_chunk> chunks;
    utils::chunked_vector<compact_page> pages;
    chunks.reserve(s.columns().size());
    const byte_array_view input(block);
    thrift::fragment_range_source<byte_array_view> source(input);
    thrift::decoder d(source, input.size_bytes());
    // Where the next in-place chunk starts.
    uint64_t pos = region_start;
    for (leaf_index c = 0; c < s.columns().size(); ++c) {
        if (!s.in_place(g, c)) {
            chunks.push_back(compact_chunk{.start = uint32_t(pos - region_start), .first_page = uint32_t(pages.size()), .pages = 0});
            continue;
        }
        const auto oi = parse_offset_index(d);
        const auto& locations = oi.page_locations;
        if (locations.empty() || locations.size() > std::numeric_limits<uint32_t>::max()) {
            malformed("the OffsetIndex of column {} in row group {} has {} pages", c, g, locations.size());
        }
        const compact_chunk ch{.start = uint32_t(pos - region_start), .first_page = uint32_t(pages.size()), .pages = uint32_t(locations.size())};
        uint64_t expected = pos;
        int64_t previous_row = -1;
        for (const auto& loc : locations) {
            if (loc.offset < 0 || loc.compressed_page_size <= 0 || loc.first_row_index <= previous_row
                    || uint64_t(loc.first_row_index) >= rows || (previous_row < 0 && loc.first_row_index != 0)) {
                malformed("the OffsetIndex of column {} in row group {} has an invalid page location: offset {}, size {}, first row {}",
                        c, g, loc.offset, loc.compressed_page_size, loc.first_row_index);
            }
            if (previous_row < 0 && s.has_dictionary(g, c)) {
                // The dictionary page spans from the chunk's start to the first data page.
                if (uint64_t(loc.offset) <= pos || uint64_t(loc.offset) >= region_end) {
                    malformed("the first data page of column {} in row group {} is at {}, its chunk starts at {}", c, g, loc.offset, pos);
                }
                expected = loc.offset;
            }
            if (uint64_t(loc.offset) != expected || region_end - expected < uint64_t(loc.compressed_page_size)) {
                malformed("a page of column {} in row group {} is at {} with {} bytes, expected at {} and before {}",
                        c, g, loc.offset, loc.compressed_page_size, expected, region_end);
            }
            pages.push_back(compact_page{
                .offset = uint32_t(expected - region_start),
                .size = uint32_t(loc.compressed_page_size),
                .first_row = uint32_t(loc.first_row_index),
            });
            expected += loc.compressed_page_size;
            previous_row = loc.first_row_index;
        }
        chunks.push_back(ch);
        pos = expected;
        co_await coroutine::maybe_yield();
    }
    if (pos != region_end) {
        malformed("the chunks of row group {} end at {}, its OffsetIndex block starts at {}", g, pos, region_end);
    }
    if (d.remaining()) {
        malformed("the OffsetIndex block of row group {} has {} trailing bytes", g, d.remaining());
    }
    auto compact = fragmented_temporary_buffer::allocate_to_fit(chunks.size() * sizeof(compact_chunk) + pages.size() * sizeof(compact_page));
    auto out = compact.get_ostream();
    for (const auto& ch : chunks) {
        out.write(reinterpret_cast<const char*>(&ch), sizeof(ch));
    }
    for (size_t i = 0; i < pages.size(); ++i) {
        out.write(reinterpret_cast<const char*>(&pages[i]), sizeof(compact_page));
        if (i % copy_step == copy_step - 1) {
            co_await coroutine::maybe_yield();
        }
    }
    co_return compact;
}

// Page headers and bodies

void check_page_sizes(const page_header& h, uint64_t body_size) {
    if (h.compressed_page_size < 0 || uint64_t(h.compressed_page_size) != body_size || h.uncompressed_page_size < 0) {
        malformed("a page has {} bytes after its header, the header declares {} compressed and {} uncompressed",
                body_size, h.compressed_page_size, h.uncompressed_page_size);
    }
    if (!h.crc) {
        malformed("a page has no checksum");
    }
}

void check_page_checksum(const page_header& h, uint32_t crc) {
    if (crc != uint32_t(*h.crc)) {
        malformed("page checksum mismatch: computed {:#x}, the header says {:#x}", crc, uint32_t(*h.crc));
    }
}

void check_dictionary_page_header(const leaf_column& col, leaf_index c, const page_header& h) {
    if (h.type != page_type::dictionary_page || !h.dictionary_page) {
        malformed("column {} has a page of type {} where its dictionary page should be", c, int(h.type));
    }
    const auto& dh = *h.dictionary_page;
    if (dh.enc != encoding::plain && dh.enc != encoding::plain_dictionary) {
        malformed("a dictionary page of column {} has encoding {}", c, int(dh.enc));
    }
    if (dh.num_values < 0) {
        malformed("a dictionary page of column {} has {} values", c, dh.num_values);
    }
    if (col.codec == compression_codec::uncompressed && h.uncompressed_page_size != h.compressed_page_size) {
        malformed("an uncompressed dictionary page of column {} declares {} bytes, it has {}", c, h.uncompressed_page_size, h.compressed_page_size);
    }
}

void check_data_page_header(const leaf_column& col, leaf_index c, const page_header& h, uint64_t rows) {
    if (h.type != page_type::data_page_v2 || !h.data_page_v2) {
        malformed("column {} has a page of type {}, expected Data Page V2", c, int(h.type));
    }
    const auto& dp = *h.data_page_v2;
    if (dp.num_rows < 0 || uint64_t(dp.num_rows) != rows) {
        malformed("a page of column {} declares {} rows, the OffsetIndex says {}", c, dp.num_rows, rows);
    }
    // The bound on the entries: one per row if the column is not repeated.
    // A repeated column's entries are bounded by the levels, which the
    // checksum covers, because the levels must decode to exactly num_values entries.
    if (dp.num_values < dp.num_rows || (!col.max_rep && dp.num_values != dp.num_rows) || dp.num_nulls < 0 || dp.num_nulls > dp.num_values) {
        malformed("a page of column {} has {} entries, {} nulls, and {} rows", c, dp.num_values, dp.num_nulls, dp.num_rows);
    }
    const int64_t rep_length = dp.repetition_levels_byte_length;
    const int64_t def_length = dp.definition_levels_byte_length;
    if (rep_length < 0 || def_length < 0 || (!col.max_rep && rep_length) || (!col.max_def && def_length)
            || rep_length + def_length > h.compressed_page_size || rep_length + def_length > h.uncompressed_page_size) {
        malformed("a page of column {} has invalid level lengths {} and {}", c, rep_length, def_length);
    }
}

static void check_uncompressed_size(leaf_index c, const page_header& h, uint64_t max_uncompressed_size) {
    if (uint64_t(h.uncompressed_page_size) > max_uncompressed_size) {
        malformed("a page of column {} declares {} uncompressed bytes, more than the {} bytes of the largest chunk of its row group",
                c, h.uncompressed_page_size, max_uncompressed_size);
    }
}

namespace {

struct parsed_header {
    page_header header;
    // The page body, after the header.
    byte_array_view body;
};

// The largest page header that parse_header_only() parses, as in the
// verifier. The headers of our writer take less than 100 bytes. The bound
// keeps the decoder, which runs without preemption points, from scanning a
// whole damaged page.
constexpr uint64_t max_page_header_size = 64 << 10;

// Parses and checks the header, without the checksum of the body.
parsed_header parse_header_only(byte_array_view page) {
    thrift::fragment_range_source<byte_array_view> source(page);
    thrift::decoder d(source, std::min<uint64_t>(page.size_bytes(), max_page_header_size));
    parsed_header r{.header = parse_page_header(d), .body = page};
    r.body.remove_prefix(d.position());
    check_page_sizes(r.header, r.body.size_bytes());
    return r;
}

// The checksum of a page body, with a preemption point between fragments.
// A fetched page is a chain of contiguous pieces of at most one read
// request (fetch_policy::max_request_size, 128 KiB by default).
future<uint32_t> body_checksum(byte_array_view body) {
    uint32_t crc = crc32_utils::init_checksum();
    for (bytes_view frag : body) {
        crc = crc32_utils::checksum(crc, reinterpret_cast<const char*>(frag.data()), frag.size());
        co_await coroutine::maybe_yield();
    }
    co_return crc;
}

// Decodes the values with a preemption point between batches.
template <decoded_value T>
future<utils::chunked_vector<T>> decode_values(const leaf_column& col, encoding enc, byte_array_view data, uint32_t count, const any_dictionary* dict) {
    const dictionary<T>* typed = nullptr;
    if constexpr (!std::same_as<T, bool>) {
        if (dict) {
            typed = std::get_if<dictionary<T>>(dict);
        }
    }
    auto decoder = make_value_decoder<T>(col.type, enc, data, count, typed, col.type_length);
    utils::chunked_vector<T> out;
    out.resize(count);
    // The decoder writes into the vector, batch by batch. A batch lies in
    // one chunk of the vector, so it is contiguous.
    static constexpr size_t batch_size = 256;
    const size_t chunk_capacity = utils::chunked_vector<T>::max_chunk_capacity();
    for (size_t i = 0; i < count;) {
        const size_t n = std::min({batch_size, count - i, chunk_capacity - i % chunk_capacity});
        decoder->decode(std::span<T>(&out[i], n));
        i += n;
        co_await coroutine::maybe_yield();
    }
    co_return out;
}

} // anonymous namespace

// The size of one decoded value of a column of physical type `t`.
static size_t decoded_value_size(physical_type t) noexcept {
    switch (t) {
    case physical_type::boolean: return sizeof(bool);
    case physical_type::int32: return sizeof(int32_t);
    case physical_type::int64: return sizeof(int64_t);
    case physical_type::float_: return sizeof(float);
    case physical_type::double_: return sizeof(double);
    default: return sizeof(byte_array_view);
    }
}

future<checked_dictionary_page> check_dictionary_page(const leaf_column& col, leaf_index c, fragmented_temporary_buffer page,
        uint64_t max_uncompressed_size) {
    const auto p = parse_header_only(byte_array_view(page));
    const auto& h = p.header;
    check_page_checksum(h, co_await body_checksum(p.body));
    check_uncompressed_size(c, h, max_uncompressed_size);
    check_dictionary_page_header(col, c, h);
    checked_dictionary_page r;
    r._header_size = page.size_bytes() - p.body.size_bytes();
    r._header = std::move(p.header);
    r._bytes = std::move(page);
    const uint64_t entries = r._header.dictionary_page->num_values;
    // The body, uncompressed, which the dictionary copies, and the dictionary.
    const uint64_t body = r._header.uncompressed_page_size;
    r._decode_memory = sizeof(any_dictionary) + (col.codec == compression_codec::uncompressed ? 0 : body)
            + dictionary_compact_size(col.type, body, entries);
    co_return r;
}

future<any_dictionary> decode_dictionary_page(const leaf_column& col, leaf_index c, const checked_dictionary_page& page, page_compressor& compressor) {
    const auto& h = page._header;
    const auto& dh = *h.dictionary_page;
    auto body = byte_array_view(page._bytes);
    body.remove_prefix(page._header_size);
    // The dictionary copies the body, so it keeps neither the buffers that
    // the page was read into nor the decompressed body.
    fragmented_temporary_buffer decompressed;
    if (col.codec != compression_codec::uncompressed) {
        decompressed = co_await compressor.decompress(c, body, h.uncompressed_page_size);
        body = byte_array_view(decompressed);
    }
    switch (col.type) {
    case physical_type::int32: co_return co_await dictionary<int32_t>::decode(body, dh.num_values, col.type, col.type_length);
    case physical_type::int64: co_return co_await dictionary<int64_t>::decode(body, dh.num_values, col.type, col.type_length);
    case physical_type::float_: co_return co_await dictionary<float>::decode(body, dh.num_values, col.type, col.type_length);
    case physical_type::double_: co_return co_await dictionary<double>::decode(body, dh.num_values, col.type, col.type_length);
    case physical_type::byte_array:
    case physical_type::fixed_len_byte_array:
        co_return co_await dictionary<byte_array_view>::decode(body, dh.num_values, col.type, col.type_length);
    default:
        malformed("column {} of physical type {} has a dictionary", c, int(col.type));
    }
}

any_dictionary make_dictionary(const leaf_column& col, fragmented_temporary_buffer compact) {
    switch (col.type) {
    case physical_type::int32: return dictionary<int32_t>::from_compact(std::move(compact), col.type);
    case physical_type::int64: return dictionary<int64_t>::from_compact(std::move(compact), col.type);
    case physical_type::float_: return dictionary<float>::from_compact(std::move(compact), col.type);
    case physical_type::double_: return dictionary<double>::from_compact(std::move(compact), col.type);
    case physical_type::byte_array:
    case physical_type::fixed_len_byte_array:
        return dictionary<byte_array_view>::from_compact(std::move(compact), col.type, col.type_length);
    default:
        on_internal_error(page_decoder_logger, seastar::format("Parquet: a dictionary of physical type {}", int(col.type)));
    }
}

const fragmented_temporary_buffer& compact_dictionary(const any_dictionary& d) noexcept {
    return std::visit([] (const auto& typed) -> const fragmented_temporary_buffer& { return typed.compact(); }, d);
}

size_t dictionary_memory_usage(const any_dictionary& d) noexcept {
    return std::visit([] (const auto& typed) { return typed.memory_usage(); }, d);
}

// Turns runs of levels into segments, and checks that the levels are
// consistent with the schema and with each other.
class decoded_page::level_builder {
    decoded_page& _p;
    const leaf_column& _col;
    uint32_t _entry = 0;
    uint32_t _rows = 0;
    uint32_t _values = 0;
    uint8_t _previous_def = 0;

    // The highest repetition level that may follow an entry at definition
    // level `def`: the repeated nodes that the entry has elements in.
    unsigned allowed_rep(uint8_t def) const noexcept {
        unsigned r = 0;
        while (r < _col.max_rep && _col.repeated_def[r] <= def) {
            ++r;
        }
        return r;
    }
public:
    level_builder(decoded_page& p, const leaf_column& col) : _p(p), _col(col) {}

    // Adds `n` entries with the given levels.
    void add(uint32_t rep, uint32_t def, uint32_t n) {
        if (rep > _col.max_rep || def > _col.max_def) {
            malformed("an entry has levels rep {} def {}, the column's maximums are {} and {}", rep, def, _col.max_rep, _col.max_def);
        }
        if (rep > 0 && def < _col.repeated_def[rep - 1]) {
            malformed("an entry repeats at level {} with definition level {}", rep, def);
        }
        if (_entry == 0 ? rep != 0 : rep > allowed_rep(_previous_def)) {
            malformed("an entry repeats at level {} after an entry at definition level {}", rep, _previous_def);
        }
        if (n > 1 && rep > allowed_rep(def)) {
            malformed("an entry repeats at level {} after an entry at definition level {}", rep, def);
        }
        auto& segments = _p._segments;
        const bool same_as_last = !segments.empty() && segments.back().literal == uniform
                && segments.back().rep == rep && segments.back().def == def;
        if (n >= rle_bp_encoder::min_rle_run || same_as_last) {
            if (!same_as_last) {
                segments.push_back(segment{.entry = _entry, .row = _rows, .value = _values, .literal = uniform,
                        .rep = uint8_t(rep), .def = uint8_t(def)});
            }
        } else {
            if (segments.empty() || segments.back().literal == uniform) {
                segments.push_back(segment{.entry = _entry, .row = _rows, .value = _values,
                        .literal = uint32_t(_p._literal_def.size()), .rep = 0, .def = 0});
            }
            for (uint32_t i = 0; i < n; ++i) {
                _p._literal_def.push_back(def);
                if (_col.max_rep) {
                    _p._literal_rep.push_back(rep);
                }
            }
        }
        _entry += n;
        _rows += rep == 0 ? n : 0;
        _values += def == _col.max_def ? n : 0;
        _previous_def = def;
    }

    // Adds the entries of a column that is not repeated, one per
    // definition level, as literals.
    void add_literals(std::span<const uint8_t> defs) {
        uint8_t highest = 0;
        uint32_t values = 0;
        for (const uint8_t def : defs) {
            highest = std::max(highest, def);
            values += def == _col.max_def;
        }
        if (highest > _col.max_def) {
            malformed("an entry has definition level {}, the column's maximum is {}", highest, _col.max_def);
        }
        auto& segments = _p._segments;
        auto& literals = _p._literal_def;
        if (segments.empty() || segments.back().literal == uniform) {
            segments.push_back(segment{.entry = _entry, .row = _rows, .value = _values,
                    .literal = uint32_t(literals.size()), .rep = 0, .def = 0});
        }
        // A chunk of the vector is contiguous.
        const size_t chunk_capacity = utils::chunked_vector<uint8_t>::max_chunk_capacity();
        size_t pos = literals.size();
        literals.resize(pos + defs.size());
        for (auto left = defs; !left.empty();) {
            const size_t n = std::min(left.size(), chunk_capacity - pos % chunk_capacity);
            std::copy_n(left.data(), n, &literals[pos]);
            pos += n;
            left = left.subspan(n);
        }
        _entry += defs.size();
        _rows += defs.size();
        _values += values;
        _previous_def = defs.back();
    }

    void finish() {
        _p._segments.push_back(segment{.entry = _entry, .row = _rows, .value = _values, .literal = uniform, .rep = 0, .def = 0});
        _p._entries = _entry;
        _p._rows = _rows;
    }

    uint32_t values() const noexcept { return _values; }
};

future<decoded_page::checked_page> decoded_page::check(const leaf_column& col, leaf_index c, fragmented_temporary_buffer page, uint64_t rows,
        uint64_t max_uncompressed_size, uint64_t max_values) {
    checked_page r;
    auto parsed = parse_header_only(byte_array_view(page));
    check_uncompressed_size(c, parsed.header, max_uncompressed_size);
    check_page_checksum(parsed.header, co_await body_checksum(parsed.body));
    check_data_page_header(col, c, parsed.header, rows);
    const auto& dp = *parsed.header.data_page_v2;
    if (uint64_t(dp.num_values) > max_values) {
        malformed("a page of column {} declares {} entries, more than the {} entries of the largest chunk of its row group",
                c, dp.num_values, max_values);
    }
    r._header_size = page.size_bytes() - parsed.body.size_bytes();
    r._header = std::move(parsed.header);
    r._rows = rows;
    r._bytes = std::move(page);
    const uint64_t levels = dp.repetition_levels_byte_length + dp.definition_levels_byte_length;
    const uint64_t decompressed = dp.is_compressed && col.codec != compression_codec::uncompressed
            ? r._header.uncompressed_page_size - levels : 0;
    // A bound on the memory of the levels. A uniform segment comes from an
    // RLE run, which takes at least two bytes, and literal segments lie
    // between uniform ones. A literal entry comes from a bit-packed group,
    // which takes at least one byte per 8 entries, and takes one byte per
    // kind of level.
    const uint64_t entries = dp.num_values;
    const uint64_t level_memory = (levels + 2) * sizeof(segment) + std::min(entries, 8 * levels) * (col.max_rep ? 2 : 1);
    r._decode_memory = sizeof(decoded_page) + decompressed + uint64_t(dp.num_values - dp.num_nulls) * decoded_value_size(col.type) + level_memory;
    co_return r;
}

future<std::unique_ptr<decoded_page>> decoded_page::decode(const leaf_column& col, leaf_index c, checked_page page,
        dictionary_ptr dict, page_compressor& compressor) {
    auto p = std::make_unique<decoded_page>(private_tag{});
    const uint64_t rows = page._rows;
    p->_page = std::move(page._bytes);
    p->_max_def = col.max_def;
    p->_max_rep = col.max_rep;
    p->_null_threshold = col.null_threshold();
    parsed_header parsed{.header = std::move(page._header), .body = byte_array_view(p->_page)};
    parsed.body.remove_prefix(page._header_size);
    const auto& h = parsed.header;
    const auto& dp = *h.data_page_v2;
    const int64_t rep_length = dp.repetition_levels_byte_length;
    const int64_t def_length = dp.definition_levels_byte_length;

    level_builder levels(*p, col);
    {
        rle_bp_decoder rep_decoder;
        rle_bp_decoder def_decoder;
        if (col.max_rep) {
            rep_decoder = rle_bp_decoder(parsed.body.prefix(rep_length), level_bit_width(col.max_rep));
        }
        if (col.max_def) {
            auto v = parsed.body;
            v.remove_prefix(rep_length);
            def_decoder = rle_bp_decoder(v.prefix(def_length), level_bit_width(col.max_def));
        }
        const uint32_t total = dp.num_values;
        uint64_t rep_left = 0;
        uint64_t def_left = 0;
        uint32_t rep = 0;
        uint32_t def = 0;
        uint32_t runs = 0;
        uint32_t e = 0;
        if (!col.max_rep && col.max_def) {
            // The common case: one entry per row. Bit-packed levels are
            // taken a batch at a time, and become literals. A bit-packed
            // group takes at least one byte per 8 entries, which bounds
            // the literals.
            p->_literal_def.reserve(std::min<uint64_t>(total, 8 * uint64_t(def_length)));
            std::array<uint8_t, 256> batch;
            while (e < total) {
                const size_t n = def_decoder.next_packed(std::span(batch.data(), std::min<size_t>(batch.size(), total - e)));
                if (n) {
                    levels.add_literals(std::span<const uint8_t>(batch.data(), n));
                    e += n;
                } else {
                    const auto r = def_decoder.next_run(total - e);
                    levels.add(0, r.value, r.length);
                    e += r.length;
                }
                if ((++runs & 15) == 0) {
                    co_await coroutine::maybe_yield();
                }
            }
        }
        while (e < total) {
            uint64_t n = total - e;
            if (col.max_rep) {
                if (!rep_left) {
                    const auto r = rep_decoder.next_run(n);
                    rep = r.value;
                    rep_left = r.length;
                }
                n = std::min(n, rep_left);
            }
            if (col.max_def) {
                if (!def_left) {
                    const auto r = def_decoder.next_run(total - e);
                    def = r.value;
                    def_left = r.length;
                }
                n = std::min(n, def_left);
            }
            levels.add(rep, def, n);
            e += n;
            rep_left -= col.max_rep ? n : 0;
            def_left -= col.max_def ? n : 0;
            // A run costs constant time, a literal entry a little more.
            // max_page_values bounds the entries, so the check need not
            // run on every run.
            if ((++runs & 255) == 0) {
                co_await coroutine::maybe_yield();
            }
        }
        levels.finish();
    }
    if (p->_rows != rows) {
        malformed("a page of column {} has {} rows in its levels, the OffsetIndex says {}", c, p->_rows, rows);
    }
    const uint32_t values = levels.values();
    if (values != uint32_t(dp.num_values - dp.num_nulls)) {
        malformed("a page of column {} has {} values in its levels, its header says {}", c, values, dp.num_values - dp.num_nulls);
    }

    auto values_data = parsed.body;
    values_data.remove_prefix(rep_length + def_length);
    const uint64_t values_size = h.uncompressed_page_size - rep_length - def_length;
    if (dp.is_compressed && col.codec != compression_codec::uncompressed) {
        p->_decompressed = co_await compressor.decompress(c, values_data, values_size);
        values_data = byte_array_view(p->_decompressed);
        // The levels are decoded, and the values are views of the decompressed buffer.
        p->_page = {};
    } else if (values_data.size_bytes() != values_size) {
        malformed("an uncompressed page of column {} declares {} bytes of values, it has {}", c, values_size, values_data.size_bytes());
    }
    const bool uses_dictionary = dp.enc == encoding::rle_dictionary || dp.enc == encoding::plain_dictionary;
    const any_dictionary* d = uses_dictionary ? dict.get() : nullptr;
    switch (col.type) {
    case physical_type::boolean: p->_values = co_await decode_values<bool>(col, dp.enc, values_data, values, d); break;
    case physical_type::int32: p->_values = co_await decode_values<int32_t>(col, dp.enc, values_data, values, d); break;
    case physical_type::int64: p->_values = co_await decode_values<int64_t>(col, dp.enc, values_data, values, d); break;
    case physical_type::float_: p->_values = co_await decode_values<float>(col, dp.enc, values_data, values, d); break;
    case physical_type::double_: p->_values = co_await decode_values<double>(col, dp.enc, values_data, values, d); break;
    case physical_type::byte_array:
    case physical_type::fixed_len_byte_array:
        p->_values = co_await decode_values<byte_array_view>(col, dp.enc, values_data, values, d);
        break;
    default:
        malformed("column {} has unsupported physical type {}", c, int(col.type));
    }
    if (uses_dictionary) {
        p->_dictionary = std::move(dict);
    }
    co_return p;
}

// Navigation

decoded_page::position decoded_page::row_start(uint32_t row) const noexcept {
    // The segment that holds the start of `row`: the last one with at most
    // `row` rows before it. The sentinel has all rows before it.
    uint32_t lo = 0;
    uint32_t hi = _segments.size() - 1;
    while (hi - lo > 1) {
        const uint32_t mid = lo + (hi - lo) / 2;
        if (_segments[mid].row <= row) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    const auto& s = _segments[lo];
    position p;
    p.segment = lo;
    p.entry = s.entry;
    p.row = row;
    p.value = s.value;
    enter_segment(p);
    if (s.literal == uniform) {
        const uint32_t k = row - s.row;
        p.entry += k;
        p.value += s.def == _max_def ? k : 0;
        return p;
    }
    if (!_max_rep) {
        // One entry per row, so only the values before the row are counted.
        const uint32_t k = row - s.row;
        uint32_t values = 0;
        for (uint32_t i = 0; i < k; ++i) {
            values += _literal_def[s.literal + i] == _max_def;
        }
        p.entry += k;
        p.value += values;
        return p;
    }
    uint32_t r = s.row;
    while (true) {
        if (rep_at(p) == 0) {
            if (r == row) {
                return p;
            }
            ++r;
        }
        p.value += def_at(p) == _max_def;
        ++p.entry;
    }
}

void decoded_page::next_row_repeated(position& p) const noexcept {
    step(p);
    while (p.entry < _entries && rep_at(p) > 0) {
        if (in_uniform(p)) {
            const uint32_t k = p.segment_end - p.entry;
            p.value += p.def == _max_def ? k : 0;
            p.entry += k;
            ++p.segment;
            enter_segment(p);
        } else {
            step(p);
        }
    }
    ++p.row;
}

void decoded_page::previous_row(position& p) const noexcept {
    step_back(p);
    while (rep_at(p) > 0) {
        if (in_uniform(p)) {
            const uint32_t k = p.entry - p.segment_entry;
            p.value -= p.def == _max_def ? k : 0;
            p.entry = p.segment_entry;
        }
        step_back(p);
    }
    --p.row;
}

uint32_t decoded_page::null_rows_forward(position p) const noexcept {
    uint32_t n = 0;
    while (p.entry < _entries && is_null_row(p)) {
        if (in_uniform(p)) {
            const uint32_t k = p.segment_end - p.entry;
            n += k;
            p.entry += k;
            ++p.segment;
            enter_segment(p);
        } else {
            ++n;
            step(p);
        }
    }
    return n;
}

uint32_t decoded_page::null_rows_backward(position p) const noexcept {
    uint32_t n = 0;
    while (is_null_row(p)) {
        // A null row is a single entry, so entries and rows coincide here.
        const uint32_t k = in_uniform(p) ? p.entry - p.segment_entry + 1 : 1;
        n += k;
        if (p.entry < k) {
            break;
        }
        p.entry -= k;
        if (p.entry < p.segment_entry) {
            --p.segment;
            enter_segment(p);
        }
    }
    return n;
}

size_t decoded_page::memory_usage() const noexcept {
    size_t r = sizeof(*this) + _page.size_bytes() + _decompressed.size_bytes();
    r += _segments.size() * sizeof(segment) + _literal_def.size() + _literal_rep.size();
    std::visit([&] (const auto& v) { r += v.size() * sizeof(v[0]); }, _values);
    return r;
}

} // namespace sstables::parquet
