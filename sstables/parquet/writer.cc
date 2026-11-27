/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/writer.hh"

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>

#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/thread.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/file_writer.hh"
#include "sstables/parquet/encoding.hh"
#include "sstables/parquet/rle.hh"
#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger writer_logger("parquet_writer");

[[noreturn]] static void bad_use(std::string_view what) {
    on_internal_error(writer_logger, seastar::format("Parquet writer: {}", what));
}

static constexpr uint64_t max_int32 = std::numeric_limits<int32_t>::max();

// The checksum of `b`, with a preemption point between its contiguous
// pieces, which are at most 128 KiB each.
static uint32_t crc32_of(uint32_t crc, const bytes_ostream& b) {
    for (bytes_view frag : b.fragments()) {
        crc = crc32_utils::checksum(crc, reinterpret_cast<const char*>(frag.data()), frag.size());
        seastar::thread::maybe_yield();
    }
    return crc;
}

static uint16_t encoding_bit(encoding e) {
    return uint16_t(1) << static_cast<int32_t>(e);
}

namespace {

// The footer record of a column chunk. The writer keeps one per chunk until
// the end of the file, so it is kept small. The rest of the footer entry
// follows from the schema and the configuration.
struct chunk_record {
    uint64_t start = 0;
    uint64_t compressed_size = 0;
    uint64_t uncompressed_size = 0;
    uint64_t num_values = 0;
    uint64_t null_count = 0;
    uint64_t offset_index_offset = 0;
    // 0 if the chunk has no OffsetIndex.
    uint32_t offset_index_length = 0;
    // The size of the dictionary page, header included. 0 if there is none.
    uint32_t dictionary_size = 0;
    // A bit per encoding, see encoding_bit().
    uint16_t encodings = 0;
};

column_chunk to_column_chunk(const chunk_record& r, physical_type type, compression_codec codec) {
    column_chunk c{
        .meta_data = column_metadata{
            .type = type,
            .codec = codec,
            .num_values = int64_t(r.num_values),
            .total_uncompressed_size = int64_t(r.uncompressed_size),
            .total_compressed_size = int64_t(r.compressed_size),
            .data_page_offset = int64_t(r.start + r.dictionary_size),
            .stats = statistics{.null_count = int64_t(r.null_count)},
        },
    };
    for (int32_t e = 0; e < 16; ++e) {
        if (r.encodings & (uint16_t(1) << e)) {
            c.meta_data.encodings.push_back(static_cast<encoding>(e));
        }
    }
    if (r.dictionary_size) {
        c.meta_data.dictionary_page_offset = int64_t(r.start);
    }
    if (r.offset_index_length) {
        c.offset_index_offset = int64_t(r.offset_index_offset);
        c.offset_index_length = int32_t(r.offset_index_length);
    }
    return c;
}

// Assembles a Data Page V2: header, repetition levels, definition levels,
// and the values, compressed if that makes them smaller.
struct data_page {
    bytes_ostream rep;
    bytes_ostream def;
    bytes_ostream values;
    encoding enc = encoding::plain;
    uint64_t num_values = 0;
    uint64_t num_nulls = 0;
    uint64_t num_rows = 0;
};

struct written_page {
    uint64_t size;
    uint64_t uncompressed_size;
};

// A data page as pieces to be written in order: the header, the repetition
// levels, the definition levels, and the values. Some may be empty.
struct page_pieces {
    std::array<bytes_ostream, 4> pieces;
    written_page sizes;
};

// Consumes the levels and the values of `p`, which the pieces take over
// without a copy. Only one of the values and their compressed form is
// left when it returns.
page_pieces write_data_page(data_page& p, page_compressor& compressor, column_id column) {
    bytes_ostream compressed;
    bool is_compressed = false;
    if (compressor.codec() != compression_codec::uncompressed && !p.values.empty()) {
        compressor.compress(column, p.values, compressed).get();
        is_compressed = compressed.size() < p.values.size();
    }
    bytes_ostream& stored = is_compressed ? compressed : p.values;
    const uint64_t levels_size = p.rep.size() + p.def.size();
    const uint64_t uncompressed = levels_size + p.values.size();
    const uint64_t size = levels_size + stored.size();
    if (uncompressed > max_int32 || p.num_values > max_int32) {
        bad_use(seastar::format("a page of {} bytes and {} values", uncompressed, p.num_values));
    }
    uint32_t crc = crc32_utils::init_checksum();
    crc = crc32_of(crc, p.rep);
    crc = crc32_of(crc, p.def);
    crc = crc32_of(crc, stored);
    page_header h{
        .type = page_type::data_page_v2,
        .uncompressed_page_size = int32_t(uncompressed),
        .compressed_page_size = int32_t(size),
        .crc = int32_t(crc),
        .data_page_v2 = data_page_header_v2{
            .num_values = int32_t(p.num_values),
            .num_nulls = int32_t(p.num_nulls),
            .num_rows = int32_t(p.num_rows),
            .enc = p.enc,
            .definition_levels_byte_length = int32_t(p.def.size()),
            .repetition_levels_byte_length = int32_t(p.rep.size()),
            .is_compressed = is_compressed,
        },
    };
    page_pieces r;
    write_page_header(h, r.pieces[0]);
    const uint64_t header_size = r.pieces[0].size();
    r.sizes = {header_size + size, header_size + uncompressed};
    r.pieces[1] = std::move(p.rep);
    r.pieces[2] = std::move(p.def);
    r.pieces[3] = std::move(stored);
    return r;
}

// A dictionary page has no flag for an uncompressed body, so it is always
// compressed with the file's compression codec.
written_page write_dictionary_page(const bytes_ostream& body, uint32_t num_values, page_compressor& compressor, column_id column, bytes_ostream& out) {
    bytes_ostream compressed;
    const bool is_compressed = compressor.codec() != compression_codec::uncompressed;
    if (is_compressed) {
        compressor.compress(column, body, compressed).get();
    }
    const bytes_ostream& stored = is_compressed ? compressed : body;
    if (body.size() > max_int32 || stored.size() > max_int32) {
        bad_use(seastar::format("a dictionary page of {} bytes", body.size()));
    }
    page_header h{
        .type = page_type::dictionary_page,
        .uncompressed_page_size = int32_t(body.size()),
        .compressed_page_size = int32_t(stored.size()),
        .crc = int32_t(crc32_of(crc32_utils::init_checksum(), stored)),
        .dictionary_page = dictionary_page_header{.num_values = int32_t(num_values), .enc = encoding::plain},
    };
    const uint64_t start = out.size();
    write_page_header(h, out);
    const uint64_t header_size = out.size() - start;
    out.append(stored);
    return {header_size + stored.size(), header_size + body.size()};
}

} // anonymous namespace

// The state of one leaf column: the levels and values of its pending page,
// the encoded pages of its chunk in the current row group, and the footer
// records of its chunks.
class column_writer {
protected:
    writer& _w;
    const column_id _id;
    const schema::column& _col;
    rle_bp_encoder _rep;
    rle_bp_encoder _def;
    // Rows recorded in the current row group. A row is recorded once it has
    // an entry, or once a later entry or the end of the row group fills it
    // with a null.
    uint64_t _rows = 0;
    // The pending page.
    uint64_t _page_first_row = 0;
    uint64_t _page_levels = 0;
    uint64_t _page_nulls = 0;
    uint64_t _page_raw = 0;
    // The data pages of the current chunk, as pieces to be written in
    // order, and their locations relative to the first data page.
    utils::chunked_vector<bytes_ostream> _pages;
    uint64_t _pages_size = 0;
    struct page_entry {
        uint64_t offset;
        int32_t size;
        int64_t first_row;
    };
    utils::chunked_vector<page_entry> _page_index;
    chunk_record _chunk;
    uint64_t _data_start = 0;

    void add_nulls(uint64_t n) {
        if (n == 0) {
            return;
        }
        if (_col.max_rep) {
            _rep.append_run(0, n);
        }
        _def.append_run(0, n);
        _page_levels += n;
        _page_nulls += n;
        _rows += n;
    }

    // Encodes the pending values into `out` and returns the encoding.
    // Clears the values.
    virtual encoding encode_values(bytes_ostream& out) = 0;
    virtual uint64_t values_plain_size() const noexcept = 0;
    // Writes the dictionary page of the chunk, if its pages use one, and
    // resets the dictionary for the next chunk. Returns its sizes.
    virtual std::optional<written_page> write_dictionary(bytes_ostream& out) = 0;

    // Appends a piece of a page to the chunk. A large piece is moved, which
    // saves a copy of a page that large values make large. A smaller one is
    // copied to the end of the last piece, so that small pages share buffers.
    // Either way a page exists in at most two copies at a time.
    void add_piece(bytes_ostream&& b) {
        if (b.empty()) {
            return;
        }
        if (!_pages.empty() && b.size() < bytes_ostream::max_chunk_size()) {
            _pages.back().append(b);
            return;
        }
        // The last buffer of a large piece may be mostly empty, so its bytes
        // move to a buffer of their size. Otherwise a page just above 128 KiB
        // would take up to twice its size.
        bytes_view last;
        for (bytes_view frag : b.fragments()) {
            last = frag;
        }
        if (last.size() == b.size() || last.size() >= bytes_ostream::max_chunk_size()) {
            _pages.push_back(std::move(b));
            return;
        }
        bytes_ostream tail;
        tail.reserve(last.size());
        tail.write(last);
        _pages.push_back(std::move(b));
        _pages.push_back(std::move(tail));
        // Once both are in place, so that an exception leaves the whole piece.
        _pages[_pages.size() - 2].remove_suffix(last.size());
    }

    void add_raw(uint64_t raw) noexcept {
        _page_raw += raw;
        _w._pending_raw += raw;
        _w._row_raw += raw;
        _w._page_set_raw += raw;
    }
public:
    const uint32_t first_row_group;
    bool touched = false;
    // Records of the chunks in place, from first_row_group on.
    utils::chunked_vector<chunk_record> chunks;
    // Records of the late chunks, for the row groups before first_row_group.
    utils::chunked_vector<chunk_record> late;

    column_writer(writer& w, column_id id, uint32_t first_rg)
        : _w(w)
        , _id(id)
        , _col(w._schema.get_column(id))
        , _rep(level_bit_width(_col.max_rep))
        , _def(level_bit_width(_col.max_def))
        , first_row_group(first_rg)
    {}
    virtual ~column_writer() = default;

    column_id id() const noexcept { return _id; }
    const schema::column& col() const noexcept { return _col; }
    uint64_t page_levels() const noexcept { return _page_levels; }

    // Records rows up to `row` (exclusive) as nulls. A long gap is split at
    // page set boundaries, so that a page does not exceed max_page_values
    // entries by more than the rows of one page set.
    void catch_up(uint64_t row) {
        if (_rows >= row) {
            return;
        }
        if (_col.max_def == 0) {
            bad_use(seastar::format("required column {} has no entry in row {}", fmt::join(_w._schema.path(_id), "."), _w.rows()));
        }
        const auto& starts = _w._page_set_starts;
        const uint64_t cap = _w._cfg.max_page_values;
        while (_page_levels + (row - _rows) > cap) {
            // A cut must leave the page non-empty and lie in the gap. Take
            // the last boundary that keeps the page within the cap, or else
            // the first boundary in the gap.
            const uint64_t lo = std::max(_rows, _page_first_row + 1);
            const uint64_t hi = std::min(row, _rows + (cap > _page_levels ? cap - _page_levels : 0));
            std::optional<uint64_t> cut;
            if (lo <= hi) {
                auto it = std::upper_bound(starts.begin(), starts.end(), hi);
                if (it != starts.begin() && *std::prev(it) >= lo) {
                    cut = *std::prev(it);
                }
            }
            if (!cut) {
                auto it = std::lower_bound(starts.begin(), starts.end(), lo);
                if (it != starts.end() && *it <= row) {
                    cut = *it;
                }
            }
            if (!cut) {
                break;
            }
            add_nulls(*cut - _rows);
            flush_page();
        }
        add_nulls(row - _rows);
    }

    // Records the levels of an entry in row `row` of the row group.
    void add_levels(levels l, uint64_t row) {
        if (l.rep == 0) {
            if (_rows > row) {
                bad_use(seastar::format("column {} has two entries with rep 0 in row {}", fmt::join(_w._schema.path(_id), "."), _w.rows()));
            }
            catch_up(row);
            _rows = row + 1;
        } else if (_rows != row + 1) {
            bad_use(seastar::format("column {} has an entry with rep {} that starts row {}", fmt::join(_w._schema.path(_id), "."), l.rep, _w.rows()));
        }
        if (_col.max_rep) {
            _rep.append(l.rep);
        }
        if (_col.max_def) {
            _def.append(l.def);
        }
        ++_page_levels;
        _page_nulls += l.def < _col.max_def;
    }

    // Called when a page set ends at row `row`, if the column received an
    // entry since the previous cut.
    void on_page_set_cut(uint64_t row) {
        catch_up(row);
        const uint64_t pending = values_plain_size() + _rep.estimated_size() + _def.estimated_size();
        if (pending >= _w._cfg.min_page_size || _page_levels >= _w._cfg.max_page_values / 2) {
            flush_page();
        }
    }

    // Encodes the pending page and appends it to the chunk.
    void flush_page() {
        if (_page_levels == 0) {
            return;
        }
        data_page p{
            .rep = _col.max_rep ? _rep.finish() : bytes_ostream(),
            .def = _col.max_def ? _def.finish() : bytes_ostream(),
            .num_values = _page_levels,
            .num_nulls = _page_nulls,
            .num_rows = _rows - _page_first_row,
        };
        p.enc = encode_values(p.values);
        seastar::thread::maybe_yield();
        const uint64_t offset = _pages_size;
        auto [pieces, written] = write_data_page(p, *_w._compressor, _id);
        for (auto& piece : pieces) {
            add_piece(std::move(piece));
        }
        _pages_size += written.size;
        _page_index.push_back(page_entry{.offset = offset, .size = int32_t(written.size), .first_row = int64_t(_page_first_row)});
        _chunk.num_values += _page_levels;
        _chunk.null_count += _page_nulls;
        _chunk.uncompressed_size += written.uncompressed_size;
        _chunk.encodings |= encoding_bit(p.enc);
        if (_col.max_def || _col.max_rep) {
            _chunk.encodings |= encoding_bit(encoding::rle);
        }
        _w._rg_encoded += written.size;
        _w._pending_raw -= _page_raw;
        ++_w._stats.data_pages;
        _page_levels = 0;
        _page_nulls = 0;
        _page_raw = 0;
        _page_first_row = _rows;
    }

    // Writes the chunk of the current row group to the file. The last page
    // must have been flushed.
    void write_chunk() {
        _chunk.start = _w._out.offset();
        bytes_ostream dict;
        if (auto written = write_dictionary(dict)) {
            if (written->size > max_int32) {
                bad_use("a dictionary page above 2 GiB");
            }
            _chunk.dictionary_size = written->size;
            _chunk.uncompressed_size += written->uncompressed_size;
            _chunk.encodings |= encoding_bit(encoding::plain);
            _w.write(dict);
            ++_w._stats.dictionary_pages;
        }
        _data_start = _w._out.offset();
        for (const auto& piece : _pages) {
            _w.write(piece);
        }
        _chunk.compressed_size = _chunk.dictionary_size + _pages_size;
    }

    // Writes the OffsetIndex of the chunk that write_chunk() wrote, and
    // starts the next chunk.
    void write_offset_index() {
        offset_index idx;
        idx.page_locations.reserve(_page_index.size());
        for (const auto& e : _page_index) {
            idx.page_locations.push_back(page_location{
                .offset = int64_t(_data_start + e.offset),
                .compressed_page_size = e.size,
                .first_row_index = e.first_row,
            });
        }
        bytes_ostream out;
        parquet::write_offset_index(idx, out);
        _chunk.offset_index_offset = _w._out.offset();
        _chunk.offset_index_length = out.size();
        _w.write(out);
        chunks.push_back(_chunk);
        _w._footer_memory += sizeof(chunk_record);
        _chunk = {};
        _pages.clear();
        _pages_size = 0;
        _page_index.clear();
        _rows = 0;
        _page_first_row = 0;
    }
};

namespace {

struct no_dictionary {};

} // anonymous namespace

// A column writer for values of one physical type. `Store` holds the
// pending page's values. `Dictionary` builds the chunk's dictionary, or is
// no_dictionary for BOOLEAN.
//
// Encoding choice: when a page is cut, the column computes the size of each
// candidate encoding of the page (exact for PLAIN and the delta encodings,
// an estimate for the RLE ones) and picks the smallest. The size of a
// dictionary-encoded page is the size of its indices plus the growth of the
// dictionary page since the previous page. So each dictionary entry is
// charged once, to the first page that uses it, and over a chunk the
// charges add up to the dictionary page. Sizes are compared before
// compression.
//
// A chunk starts with a dictionary. It falls back to the best other
// encoding for the rest of the chunk at the first page where the dictionary
// does not win, or when the dictionary would exceed dictionary_page_limit.
// The pending page then uses the other encoding as a whole, because the
// column keeps its raw values too. If no page used the dictionary, the
// chunk has no dictionary page. Otherwise the dictionary page holds all
// entries added before the fallback, including any that the pending page
// added and then did not use.
template <typename Store, typename Dictionary>
class value_column final : public column_writer {
    static constexpr bool has_dictionary = !std::same_as<Dictionary, no_dictionary>;
    Store _values;
    [[no_unique_address]] std::conditional_t<has_dictionary, std::optional<Dictionary>, no_dictionary> _dict;
    dictionary_indices _indices;
    bool _dict_active = false;
    bool _dict_used = false;
    uint64_t _dict_size_at_last_page = 0;
    // The part of writer::_dictionary_memory that this column holds.
    uint64_t _dict_memory = 0;

    // Updates writer::_dictionary_memory after a change of the dictionary
    // or of the indices.
    void update_dictionary_memory() noexcept {
        if constexpr (has_dictionary) {
            const uint64_t m = (_dict ? _dict->memory_usage() : 0) + _indices.size() * sizeof(uint32_t);
            _w._dictionary_memory = _w._dictionary_memory - _dict_memory + m;
            _dict_memory = m;
        }
    }

    void start_dictionary() {
        if constexpr (has_dictionary) {
            _dict_active = _w._cfg.dictionary_page_limit > 0;
            _dict_used = false;
            _dict_size_at_last_page = 0;
            _indices.clear();
            if (_dict) {
                _dict->clear();
            }
            update_dictionary_memory();
        }
    }

    void fall_back() {
        if constexpr (has_dictionary) {
            _dict_active = false;
            _indices.clear();
            if (!_dict_used) {
                _dict->clear();
            }
            update_dictionary_memory();
            ++_w._stats.dictionary_fallbacks;
        }
    }

    static Store make_store(const schema::column& col) {
        if constexpr (std::same_as<Store, byte_array_values>) {
            return byte_array_values(col.type_length ? std::optional<uint32_t>(*col.type_length) : std::nullopt);
        } else {
            return Store();
        }
    }

    Dictionary make_dictionary() {
        if constexpr (std::same_as<Dictionary, byte_array_dictionary_builder>) {
            return byte_array_dictionary_builder(_w._cfg.dictionary_page_limit,
                    _col.type_length ? std::optional<uint32_t>(*_col.type_length) : std::nullopt);
        } else {
            return Dictionary(_w._cfg.dictionary_page_limit);
        }
    }

protected:
    encoding encode_values(bytes_ostream& out) override {
        if (_values.empty()) {
            // An all-null page. Its encoding does not matter, and it must not
            // make the chunk give up its dictionary.
            _indices.clear();
            return encoding::plain;
        }
        encoding best = encoding::plain;
        uint64_t best_size = std::numeric_limits<uint64_t>::max();
        for (encoding e : writer_encodings(_col.type)) {
            const uint64_t size = _values.encoded_size(e);
            if (size < best_size) {
                best = e;
                best_size = size;
            }
        }
        if constexpr (has_dictionary) {
            if (_dict_active) {
                const uint64_t growth = _dict->page_size() - _dict_size_at_last_page;
                const uint64_t cost = _indices.encoded_size(_dict->size()) + growth;
                if (cost < best_size) {
                    _indices.encode(_dict->size(), out);
                    _indices.clear();
                    update_dictionary_memory();
                    _values.clear();
                    _dict_used = true;
                    _dict_size_at_last_page = _dict->page_size();
                    return encoding::rle_dictionary;
                }
                fall_back();
            }
        }
        _values.encode(best, out);
        _values.clear();
        return best;
    }

    uint64_t values_plain_size() const noexcept override {
        return _values.plain_size();
    }

    std::optional<written_page> write_dictionary(bytes_ostream& out) override {
        std::optional<written_page> r;
        if constexpr (has_dictionary) {
            if (_dict_used) {
                bytes_ostream body;
                _dict->write_page(body);
                r = write_dictionary_page(body, _dict->size(), *_w._compressor, _id, out);
            }
            start_dictionary();
        }
        return r;
    }

public:
    value_column(writer& w, column_id id, uint32_t first_rg)
        : column_writer(w, id, first_rg)
        , _values(make_store(_col))
    {
        if constexpr (has_dictionary) {
            _dict.emplace(make_dictionary());
        }
        start_dictionary();
    }

    template <typename V>
    void add(V v, uint64_t raw) {
        _values.append(v);
        add_raw(raw);
        if constexpr (has_dictionary) {
            if (_dict_active) {
                if (auto i = _dict->insert(v)) {
                    _indices.append(*i);
                    update_dictionary_memory();
                } else {
                    fall_back();
                }
            }
        }
    }
};

using boolean_column = value_column<fixed_width_values<bool>, no_dictionary>;
template <typename T>
using fixed_column = value_column<fixed_width_values<T>, dictionary_builder<T>>;
using byte_array_column = value_column<byte_array_values, byte_array_dictionary_builder>;

writer::writer(sstables::file_writer& out, schema s, writer_config cfg, std::unique_ptr<writer_memory_handle> memory)
    : _out(out)
    , _schema(std::move(s))
    , _cfg(std::move(cfg))
    , _memory(std::move(memory))
    , _compressor(make_page_compressor(_cfg.codec, _cfg.compression_level))
{
    _schema_position.resize(_schema.columns());
    _columns.resize(_schema.columns());
    // Leave room for the rows of a page set and of one row in a page's
    // entry count, which is a 32-bit signed number.
    _cfg.max_page_values = std::clamp<uint32_t>(_cfg.max_page_values, 2, 1 << 30);
    _cfg.max_page_set_rows = std::clamp<uint32_t>(_cfg.max_page_set_rows, 1, _cfg.max_page_values / 2);
    _cfg.row_group_size = std::min(_cfg.row_group_size, writer_config::max_row_group_size);
    _cfg.page_set_size = std::min(_cfg.page_set_size, writer_config::max_page_size);
    _cfg.min_page_size = std::min(_cfg.min_page_size, writer_config::max_page_size);
    _cfg.dictionary_page_limit = std::min(_cfg.dictionary_page_limit, writer_config::max_dictionary_page_limit);
    const auto order = _schema.columns_in_schema_order();
    for (uint32_t i = 0; i < order.size(); ++i) {
        _schema_position[order[i]] = i;
    }
    for (column_id c = 0; c < _schema.columns(); ++c) {
        const auto t = _schema.get_column(c).type;
        if (t == physical_type::int96 || t > physical_type::fixed_len_byte_array) {
            bad_use(seastar::format("column {} has unsupported type {}", fmt::join(_schema.path(c), "."), int(t)));
        }
    }
}

writer::~writer() = default;

column_writer& writer::state(column_id c) {
    auto& p = _columns.at(c);
    if (!p) {
        const uint32_t rg = _row_groups.size();
        switch (_schema.get_column(c).type) {
        case physical_type::boolean: p = std::make_unique<boolean_column>(*this, c, rg); break;
        case physical_type::int32: p = std::make_unique<fixed_column<int32_t>>(*this, c, rg); break;
        case physical_type::int64: p = std::make_unique<fixed_column<int64_t>>(*this, c, rg); break;
        case physical_type::float_: p = std::make_unique<fixed_column<float>>(*this, c, rg); break;
        case physical_type::double_: p = std::make_unique<fixed_column<double>>(*this, c, rg); break;
        case physical_type::byte_array:
        case physical_type::fixed_len_byte_array: p = std::make_unique<byte_array_column>(*this, c, rg); break;
        default: bad_use("unsupported type");
        }
        if (!_finished) {
            _new.push_back(c);
        }
    }
    return *p;
}

template <typename T>
T& writer::typed(column_id c, physical_type t) {
    const auto actual = _schema.get_column(c).type;
    const bool ok = actual == t || (t == physical_type::byte_array && actual == physical_type::fixed_len_byte_array);
    if (!ok) {
        bad_use(seastar::format("a value of type {} for column {} of type {}", int(t), fmt::join(_schema.path(c), "."), int(actual)));
    }
    return static_cast<T&>(state(c));
}

void writer::before_entry(column_id c, levels l, bool has_value) {
    if (_finished) {
        bad_use("an entry after finish()");
    }
    const auto& col = _schema.get_column(c);
    if (l.def > col.max_def || l.rep > col.max_rep || has_value != (l.def == col.max_def)) {
        bad_use(seastar::format("levels rep {} def {} with{} a value for column {}, which has max rep {} and max def {}",
                l.rep, l.def, has_value ? "" : "out", fmt::join(_schema.path(c), "."), col.max_rep, col.max_def));
    }
}

void writer::after_entry(column_id c, column_writer& cw) {
    ++_stats.entries;
    _row_open = true;
    if (!cw.touched) {
        cw.touched = true;
        _touched.push_back(c);
    }
    if (cw.page_levels() >= _cfg.max_page_values) {
        _force_cut = true;
    }
}

template <typename Column, typename V>
void writer::append_value(column_id c, levels l, physical_type t, V v, uint64_t raw) {
    before_entry(c, l, true);
    auto& cw = typed<Column>(c, t);
    cw.add_levels(l, _rg_rows);
    cw.add(v, raw);
    after_entry(c, cw);
}

void writer::append(column_id c, levels l, bool v) {
    append_value<boolean_column>(c, l, physical_type::boolean, v, 1);
}

void writer::append(column_id c, levels l, int32_t v) {
    append_value<fixed_column<int32_t>>(c, l, physical_type::int32, v, sizeof(v));
}

void writer::append(column_id c, levels l, int64_t v) {
    append_value<fixed_column<int64_t>>(c, l, physical_type::int64, v, sizeof(v));
}

void writer::append(column_id c, levels l, float v) {
    append_value<fixed_column<float>>(c, l, physical_type::float_, v, sizeof(v));
}

void writer::append(column_id c, levels l, double v) {
    append_value<fixed_column<double>>(c, l, physical_type::double_, v, sizeof(v));
}

void writer::append(column_id c, levels l, bytes_view v) {
    append_value<byte_array_column>(c, l, physical_type::byte_array, single_fragmented_view(v), v.size());
}

void writer::append(column_id c, levels l, managed_bytes_view v) {
    append_value<byte_array_column>(c, l, physical_type::byte_array, v, v.size_bytes());
}

void writer::append(column_id c, levels l, byte_array_view v) {
    append_value<byte_array_column>(c, l, physical_type::byte_array, v, v.size_bytes());
}

void writer::append_null(column_id c, levels l) {
    before_entry(c, l, false);
    if (l.def == 0) {
        if (l.rep != 0) {
            bad_use(seastar::format("a null at def 0 with rep {} for column {}", l.rep, fmt::join(_schema.path(c), ".")));
        }
        // The same as no entry. A column whose entries are all at def 0 has no state.
        return;
    }
    auto& cw = state(c);
    cw.add_levels(l, _rg_rows);
    after_entry(c, cw);
}

void writer::write(const bytes_ostream& b) {
    for (bytes_view frag : b.fragments()) {
        _out.write(frag);
        seastar::thread::maybe_yield();
    }
}

void writer::write_magic() {
    if (!_magic_written) {
        if (_out.offset() != 0) {
            bad_use("the file_writer is not at the start of the file");
        }
        _out.write(magic.data(), magic.size());
        _magic_written = true;
    }
}

uint64_t writer::flushed_size() const noexcept {
    return _out.offset();
}

uint64_t writer::estimated_size() const noexcept {
    // The row group target applies to the encoded size, so with values
    // that compress well, a row group holds many times its size in raw
    // size. The encoded pages of the open row group therefore count with
    // their own size, and they take part in the ratio.
    const uint64_t encoded_raw = _total_raw + _row_raw - _pending_raw;
    const uint64_t encoded = _flushed_data + _rg_encoded;
    const double ratio = encoded_raw ? double(encoded) / double(encoded_raw) : 1.0;
    return _out.offset() + _rg_encoded + uint64_t(double(_pending_raw) * ratio);
}

void writer::cut_page_set() {
    _page_set_starts.push_back(_rg_rows);
    for (column_id c : _touched) {
        auto& cw = *_columns[c];
        cw.on_page_set_cut(_rg_rows);
        cw.touched = false;
        ++_stats.column_visits;
        // A column may end its page here, and encode and compress it.
        seastar::thread::maybe_yield();
    }
    _touched.clear();
    _page_set_rows = 0;
    _page_set_raw = 0;
    _force_cut = false;
    ++_stats.page_set_cuts;
}

bool writer::end_row() {
    if (_finished) {
        bad_use("end_row() after finish()");
    }
    _total_raw += _row_raw;
    _row_raw = 0;
    _row_open = false;
    ++_rg_rows;
    ++_page_set_rows;
    if (_page_set_raw < _cfg.page_set_size && _page_set_rows < _cfg.max_page_set_rows && !_force_cut) {
        return false;
    }
    cut_page_set();
    report_memory_usage();
    // A row group ends only at a page set cut, so it holds at least one page set.
    const uint64_t target = _memory ? std::min(_memory->row_group_target(), _cfg.row_group_size) : _cfg.row_group_size;
    if (buffered_size() >= target) {
        end_row_group();
    }
    return true;
}

void writer::end_row_group() {
    if (_rg_rows == 0) {
        return;
    }
    // Every column with state gets a chunk, in schema order.
    auto by_position = [this] (column_id a, column_id b) { return _schema_position[a] < _schema_position[b]; };
    std::ranges::sort(_new, by_position);
    utils::chunked_vector<column_id> merged;
    merged.reserve(_active.size() + _new.size());
    std::ranges::merge(_active, _new, std::back_inserter(merged), by_position);
    _active = std::move(merged);
    _new.clear();

    for (column_id c : _active) {
        auto& cw = *_columns[c];
        cw.catch_up(_rg_rows);
        cw.flush_page();
        cw.touched = false;
        ++_stats.column_visits;
        seastar::thread::maybe_yield();
    }
    write_magic();
    row_group_record rec{.num_rows = int64_t(_rg_rows)};
    for (column_id c : _active) {
        if (!rec.file_offset) {
            rec.file_offset = _out.offset();
        }
        _columns[c]->write_chunk();
        seastar::thread::maybe_yield();
    }
    for (column_id c : _active) {
        _columns[c]->write_offset_index();
        seastar::thread::maybe_yield();
    }
    _row_groups.push_back(rec);
    ++_stats.row_groups;
    if (_pending_raw != 0) {
        on_internal_error(writer_logger, seastar::format("Parquet writer: {} bytes of pending values after the end of a row group", _pending_raw));
    }
    _touched.clear();
    _page_set_starts.clear();
    _rows_before += _rg_rows;
    _rg_rows = 0;
    _page_set_rows = 0;
    _page_set_raw = 0;
    _force_cut = false;
    _rg_encoded = 0;
    _flushed_data = _out.offset() - magic.size();
    report_memory_usage();
}

void writer::report_memory_usage() noexcept {
    if (_memory) {
        _memory->report_memory_usage(memory_usage());
    }
}

// An all-null chunk, for a column that got its first entry after the row
// group. Pages hold at most max_page_values rows. Any row boundary is a
// valid page boundary, because each null row is a single entry.
void writer::write_late_chunk(column_writer& cw, uint64_t rows) {
    const auto& col = cw.col();
    if (col.max_def == 0) {
        bad_use("a required column has no entries");
    }
    chunk_record r{.start = _out.offset(), .num_values = rows, .null_count = rows,
            .encodings = uint16_t(encoding_bit(encoding::rle) | encoding_bit(encoding::plain))};
    for (uint64_t left = rows; left;) {
        const uint64_t n = std::min<uint64_t>(left, _cfg.max_page_values);
        data_page p{.num_values = n, .num_nulls = n, .num_rows = n};
        if (col.max_rep) {
            rle_bp_encoder rep(level_bit_width(col.max_rep));
            rep.append_run(0, n);
            p.rep = rep.finish();
        }
        rle_bp_encoder def(level_bit_width(col.max_def));
        def.append_run(0, n);
        p.def = def.finish();
        auto [pieces, written] = write_data_page(p, *_compressor, cw.id());
        r.uncompressed_size += written.uncompressed_size;
        for (const auto& piece : pieces) {
            write(piece);
        }
        ++_stats.data_pages;
        left -= n;
    }
    r.compressed_size = _out.offset() - r.start;
    cw.late.push_back(r);
    _footer_memory += sizeof(chunk_record);
    ++_stats.late_chunks;
}

writer_result writer::finish(std::vector<key_value> key_value_metadata) {
    if (_finished) {
        bad_use("finish() called twice");
    }
    if (_row_open) {
        bad_use("finish() in the middle of a row");
    }
    for (const auto& kv : key_value_metadata) {
        if (kv.key.size() > max_key_value_size || (kv.value && kv.value->size() > max_key_value_size)) {
            bad_use(seastar::format("key-value metadata {} of {} bytes", kv.key.substr(0, 64), kv.value ? kv.value->size() : kv.key.size()));
        }
    }
    end_row_group();
    _finished = true;
    write_magic();

    // The columns in the file, in schema order.
    std::vector<bool> included(_schema.columns());
    utils::chunked_vector<column_id> order;
    for (column_id c : _schema.columns_in_schema_order()) {
        if (_columns[c] || _schema.get_column(c).always_present) {
            included[c] = true;
            order.push_back(c);
        }
    }
    // Late chunks, in schema order and then in row group order.
    for (column_id c : order) {
        auto& cw = state(c);
        for (uint32_t rg = 0; rg < cw.first_row_group; ++rg) {
            write_late_chunk(cw, _row_groups[rg].num_rows);
            seastar::thread::maybe_yield();
        }
    }
    report_memory_usage();

    file_metadata md{
        .schema = _schema.to_elements(included),
        .num_rows = int64_t(rows()),
        .key_value_metadata = std::move(key_value_metadata),
        .created_by = _cfg.created_by,
    };
    const auto codec = _compressor->codec();
    auto source = [&] (size_t i) {
        row_group rg{.total_byte_size = 0, .num_rows = _row_groups[i].num_rows, .file_offset = _row_groups[i].file_offset};
        int64_t compressed = 0;
        rg.columns.reserve(order.size());
        for (column_id c : order) {
            const auto& cw = *_columns[c];
            const auto& r = i < cw.first_row_group ? cw.late[i] : cw.chunks[i - cw.first_row_group];
            rg.columns.push_back(to_column_chunk(r, cw.col().type, codec));
            rg.total_byte_size += r.uncompressed_size;
            compressed += r.compressed_size;
            seastar::thread::maybe_yield();
        }
        rg.total_compressed_size = compressed;
        if (i <= size_t(std::numeric_limits<int16_t>::max())) {
            rg.ordinal = int16_t(i);
        }
        return rg;
    };
    const auto footer = write_file_metadata(md, _row_groups.size(), source, _out);
    if (footer.size > std::numeric_limits<uint32_t>::max()) {
        bad_use(seastar::format("a footer of {} bytes", footer.size));
    }
    const uint32_t length = footer.size;
    _out.write(reinterpret_cast<const char*>(&length), sizeof(length));
    _out.write(magic.data(), magic.size());
    return writer_result{
        .file_size = _out.offset(),
        .rows = rows(),
        .raw_size = _total_raw,
        .row_groups = _row_groups.size(),
        .footer = footer,
    };
}

} // namespace sstables::parquet
