/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "parquet_writer.hh"
#include "page_header.hh"
#include "thrift_compact_writer.hh"
#include "encoders.hh"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <lz4.h>
#include <zstd.h>

namespace sstables::parquet::format {

namespace {

std::vector<uint8_t> compress(const std::vector<uint8_t>& in, codec c, int level) {
    if (c == codec::uncompressed || in.empty()) { return in; }
    if (c == codec::lz4_raw) {
        // LZ4_RAW, not LZ4: codec 7, a bare LZ4 block with no framing and no length prefix, which
        // is what every current Parquet implementation writes. Codec 5 ("LZ4") is the deprecated
        // Hadoop-framed variant and is deliberately not produced here.
        //
        // The uncompressed size is not stored in the block -- the reader takes it from the page
        // header, which Parquet requires to be exact -- so nothing is prefixed.
        const int bound = LZ4_compressBound(int(in.size()));
        if (bound <= 0) { throw std::runtime_error("lz4: page too large to compress"); }
        // Named, because `std::vector<uint8_t> out(size_t(bound))` is a function declaration and
        // the brace form would build a one-element vector holding `bound`.
        const size_t cap = size_t(bound);
        std::vector<uint8_t> out(cap);
        const int n = LZ4_compress_default(reinterpret_cast<const char*>(in.data()),
                                           reinterpret_cast<char*>(out.data()),
                                           int(in.size()), bound);
        if (n <= 0) { throw std::runtime_error("lz4: compression failed"); }
        out.resize(size_t(n));
        return out;
    }
    if (c != codec::zstd) {
        throw std::runtime_error(std::string("writer: unsupported codec ") + to_string(c));
    }
    size_t bound = ZSTD_compressBound(in.size());
    std::vector<uint8_t> out(bound);
    size_t n = ZSTD_compress(out.data(), bound, in.data(), in.size(), level);
    if (ZSTD_isError(n)) { throw std::runtime_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
    out.resize(n);
    return out;
}

// PageHeader for a V2 data page. Field ids per parquet.thrift.
void write_data_page_v2_header(std::vector<uint8_t>& out,
                               int32_t uncompressed, int32_t compressed,
                               int32_t num_values, int32_t num_nulls, int32_t num_rows,
                               encoding value_enc, int32_t def_len, int32_t rep_len,
                               bool is_compressed) {
    compact_writer w(out);
    compact_writer::struct_scope s(w);
    w.field_i32(1, int32_t(page_type::data_page_v2));
    w.field_i32(2, uncompressed);
    w.field_i32(3, compressed);
    w.field_struct(8);
    {
        compact_writer::elem_scope v2(w);
        w.field_i32(1, num_values);
        w.field_i32(2, num_nulls);
        w.field_i32(3, num_rows);
        w.field_i32(4, int32_t(value_enc));
        w.field_i32(5, def_len);
        w.field_i32(6, rep_len);
        w.field_bool(7, is_compressed);
    }
}

void write_dictionary_page_header(std::vector<uint8_t>& out,
                                  int32_t uncompressed, int32_t compressed,
                                  int32_t num_values) {
    compact_writer w(out);
    compact_writer::struct_scope s(w);
    w.field_i32(1, int32_t(page_type::dictionary_page));
    w.field_i32(2, uncompressed);
    w.field_i32(3, compressed);
    w.field_struct(7);
    {
        compact_writer::elem_scope d(w);
        w.field_i32(1, num_values);
        w.field_i32(2, int32_t(encoding::plain));
    }
}

} // namespace

namespace {

// The leading magic tells a reader which envelope to expect at the tail: "PARE" means the footer
// is encrypted and preceded by a FileCryptoMetaData, "PAR1" means it is not.
const char* leading_magic(const writer_options& o) {
    return o.encryption.enabled ? magic_encrypted : magic_plain;
}

} // namespace

parquet_file_writer::parquet_file_writer(std::vector<column_spec> schema, writer_options opt)
        : _schema(std::move(schema)), _opt(opt) {
    {
        const char* m = leading_magic(_opt);
        _buf.insert(_buf.end(), m, m + 4);
    }
    if (_opt.encryption.enabled) {
        if (!_opt.encryption.footer_key.valid()) {
            throw std::runtime_error("writer: encryption enabled without a valid footer key");
        }
        // 8 bytes, per file. It is what stops a module from one file being replayed into
        // another written under the same key.
        std::string u(8, '\0');
        random_bytes(std::span<uint8_t>(reinterpret_cast<uint8_t*>(u.data()), u.size()));
        _aad_file_unique = std::move(u);
    }
    // Synthesise the tree a flat schema implies, so footer emission has one path.
    schema_element root;
    root.name = "schema";
    root.num_children = int32_t(_schema.size());
    _tree.push_back(root);
    for (const auto& c : _schema) {
        schema_element e;
        e.type = c.type;
        e.repetition_type = c.rep;
        e.name = c.name;
        e.converted_type = c.converted_type;
        _tree.push_back(e);
    }
}

parquet_file_writer::parquet_file_writer(nested_schema ns, writer_options opt)
        : _tree(std::move(ns.tree)), _opt(opt) {
    {
        const char* m = leading_magic(_opt);
        _buf.insert(_buf.end(), m, m + 4);
    }
    if (_opt.encryption.enabled) {
        if (!_opt.encryption.footer_key.valid()) {
            throw std::runtime_error("writer: encryption enabled without a valid footer key");
        }
        // 8 bytes, per file. It is what stops a module from one file being replayed into
        // another written under the same key.
        std::string u(8, '\0');
        random_bytes(std::span<uint8_t>(reinterpret_cast<uint8_t*>(u.data()), u.size()));
        _aad_file_unique = std::move(u);
    }
    if (_tree.empty()) { throw std::runtime_error("writer: nested schema has no root"); }

    // Derive the leaves and their levels with the same walker the reader uses, so
    // the two cannot disagree about what a level means.
    file_metadata probe;
    probe.schema = _tree;
    const auto leaves = walk_leaves(probe);
    // A hint list, if given, must line up with the leaves one for one. Silently
    // taking the shorter of the two is how the hints came to be dropped in the
    // first place, so a mismatch is an error rather than a partial application.
    if (!ns.preferred.empty() && ns.preferred.size() != leaves.size()) {
        throw std::runtime_error("writer: " + std::to_string(ns.preferred.size()) +
                                 " encoding hints for " + std::to_string(leaves.size()) +
                                 " leaves");
    }
    for (size_t i = 0; i < leaves.size(); ++i) {
        const auto& li = leaves[i];
        const auto& el = _tree[li.index];
        if (!el.type) { throw std::runtime_error("writer: leaf without a physical type"); }
        column_spec c;
        c.name = el.name;
        c.type = *el.type;
        c.rep = el.repetition_type.value_or(repetition::required);
        c.converted_type = el.converted_type;
        c.max_def = li.max_def;
        c.max_rep = li.max_rep;
        c.path = li.path;
        if (!ns.preferred.empty()) { c.preferred = ns.preferred[i]; }
        _schema.push_back(std::move(c));
    }
}

void parquet_file_writer::drain() {
    if (!_sink || _buf.empty()) {
        return;
    }
    _sink(std::span<const uint8_t>(_buf.data(), _buf.size()));
    _flushed += _buf.size();
    // clear() and not shrink_to_fit(): the capacity is one row group's worth, which is
    // exactly what the next row group needs, so keeping it avoids reallocating per group.
    _buf.clear();
}

void parquet_file_writer::write_column_chunk(const column_spec& spec, const column_data& col,
                                     chunk_meta& out_meta, int column_ordinal) {
    // Slots, not values. Parquet's ColumnMetaData.num_values counts level
    // entries, and for a repeated column there are more of those than values --
    // reporting the value count makes readers stop early with no error.
    const size_t n = !col.def_levels.empty() ? col.def_levels.size()
                   : (!col.rep_levels.empty() ? col.rep_levels.size() : col.num_values());
    // A leaf inside a repeated group states its levels explicitly, because the
    // schema tree they come from is not visible here. A flat leaf derives them.
    const uint8_t max_rep = spec.max_rep;
    const uint8_t max_def = spec.max_def ? spec.max_def
                                        : uint8_t(spec.rep == repetition::optional ? 1 : 0);
    const bool has_def = max_def > 0;

    out_meta.cm.type = spec.type;
    out_meta.cm.path_in_schema = spec.path_or_name();
    out_meta.cm.compression = _opt.compression;
    out_meta.cm.num_values = int64_t(n);
    out_meta.first_page_offset = pos();

    // ---- decide encoding
    bool use_dict = false;
    dict_result dict;
    // An explicit encoding hint wins outright -- with one exception. Otherwise a monotonic key
    // column would qualify for a dictionary on cardinality alone and lose its delta encoding: on a
    // time-series table that is the difference between 37 kB and 528 kB for the clustering key.
    //
    // The exception is the front-coding hints on byte_array. Those are applied to text and blob
    // *clustering* keys on the strength of sortedness, and a sorted key can just as easily be low
    // cardinality -- a weekday, a category, a status. There a dictionary stores each distinct value
    // once where front coding stores every occurrence, and letting the hint win costs **3.5x**:
    // measured on 100 000 sorted weekday strings, zstd gives dictionary 99 bytes against delta 349
    // (and PLAIN 192). So for those hints the dictionary is evaluated first and the hint applies only
    // if the dictionary's own cardinality test rejects it -- which is precisely the case the hint is
    // for, a key with many distinct values that happen to be ordered.
    const bool hinted = spec.preferred.has_value();
    const bool hint_yields_to_dict = hinted
            && (*spec.preferred == encoding::delta_byte_array
                || *spec.preferred == encoding::delta_length_byte_array);
    // An explicit request for a dictionary -- only reachable through the per-column CQL override --
    // skips the repeat-ratio test. That test is a heuristic about when a dictionary pays, and someone
    // naming the encoding has overridden the heuristic on purpose. The *size* cap still applies,
    // because it is not a heuristic: a dictionary larger than the budget cannot be held.
    const bool hint_forces_dict = hinted && *spec.preferred == encoding::rle_dictionary;
    if (_opt.use_dictionary && (!hinted || hint_yields_to_dict || hint_forces_dict)
        && spec.type == phys_type::byte_array && !col.str.empty()) {
        // Only the present values go into the dictionary.
        // Values are dense per slot only when the column does not repeat; a
        // repeated one supplies present values only, so walk a value cursor
        // rather than indexing by slot.
        std::vector<std::string> present;
        present.reserve(n);
        size_t vi = 0;
        for (size_t i = 0; i < n; ++i) {
            const bool p = !has_def || col.def_levels[i] == max_def;
            if (p) { present.push_back(col.str[vi]); }
            if (p || max_rep == 0) { ++vi; }
        }
        dict = encode_dictionary_byte_array(present);
        // Dictionary only pays if it is small relative to the data.
        // Cardinality has to be well below the row count, not merely below it.
        // A dictionary is decompressed in full before a single value can be
        // decoded, so a near-unique dictionary is pure cost on the read path --
        // it dominated point-read latency at the old 2x threshold (design doc
        // 10.4) while saving almost nothing, because zstd already finds those
        // repeats. 8x keeps the low-cardinality columns a dictionary is for.
        if (dict.dictionary_page.size() <= _opt.dictionary_max_bytes &&
            (hint_forces_dict
             || dict.num_distinct * _opt.dictionary_min_repeat < present.size())) {
            use_dict = true;
        }
    } else if (_opt.use_dictionary && (!hinted || hint_forces_dict)
               && (_opt.numeric_dictionary || hint_forces_dict)
               && spec.type != phys_type::byte_array) {
        // Numerics get a dictionary too, on the same terms.
        //
        // Measured on a time-series table (design doc 10.1g): leaving these on PLAIN
        // costs about 10% of the file. It is not uniform -- PLAIN plus zstd beats a
        // dictionary on the higher-cardinality columns (`temp`, 992 distinct, 353 kB
        // against 359 kB) and loses badly on the low-cardinality ones (`precip_6h`,
        // 135 distinct, 26 kB against 11 kB). The distinct-count threshold below is
        // what separates them, and it is the same one the byte_array path uses.
        auto build = [&] (const auto& values) {
            using V = typename std::decay_t<decltype(values)>::value_type;
            dict = encode_dictionary_fixed(std::span<const V>(values));
            if (dict.dictionary_page.size() <= _opt.dictionary_max_bytes &&
                (hint_forces_dict
                 || dict.num_distinct * _opt.dictionary_min_repeat < values.size())) {
                use_dict = true;
            }
        };
        // Present values only, and dense per slot only when the column does not
        // repeat -- same cursor discipline as the byte_array path above.
        auto gather = [&] (const auto& src, auto& dst) {
            dst.reserve(n);
            size_t vi = 0;
            for (size_t i = 0; i < n; ++i) {
                const bool pr = !has_def || col.def_levels[i] == max_def;
                if (pr) { dst.push_back(src[vi]); }
                if (pr || max_rep == 0) { ++vi; }
            }
        };
        switch (spec.type) {
        case phys_type::int32: {
            if (col.i32.empty()) { break; }
            std::vector<int32_t> v; gather(col.i32, v); build(v);
            break;
        }
        case phys_type::int64: {
            if (col.i64.empty()) { break; }
            std::vector<int64_t> v; gather(col.i64, v); build(v);
            break;
        }
        case phys_type::dbl: {
            if (col.f64.empty()) { break; }
            std::vector<double> v; gather(col.f64, v); build(v);
            break;
        }
        default:
            break;
        }
    }

    int64_t total_uncompressed = 0, total_compressed = 0;
    int64_t null_count = 0;

    if (use_dict) {
        out_meta.cm.dictionary_page_offset = pos();
        auto comp = compress(dict.dictionary_page, _opt.compression, _opt.zstd_level);
        // A dictionary page is its own pair of modules and, unlike a data page, carries no page
        // ordinal -- there is exactly one per chunk, so the AAD would have nothing to
        // distinguish. Getting that wrong is invisible to our own reader and fatal to anyone
        // else's, which is why the conformance test checks the ordinals against parquet-cpp.
        const bool ctr_body = _opt.encryption.algo == cipher::aes_gcm_ctr_v1;
        std::vector<uint8_t> hdr;
        write_dictionary_page_header(hdr, int32_t(dict.dictionary_page.size()),
                                     int32_t(encrypting()
                                             ? envelope_size(comp.size(), ctr_body)
                                             : comp.size()),
                                     int32_t(dict.num_distinct));
        size_t on_disk_hdr = hdr.size(), on_disk_body = comp.size();
        if (encrypting()) {
            const auto& ckey = key_for_column(spec.name);
            on_disk_hdr = emit_module(hdr, module_type::dictionary_page_header,
                                      int(_rgs.size()), column_ordinal, -1, false, &ckey);
            on_disk_body = emit_module(comp, module_type::dictionary_page,
                                       int(_rgs.size()), column_ordinal, -1, ctr_body, &ckey);
        } else {
            _buf.insert(_buf.end(), hdr.begin(), hdr.end());
            _buf.insert(_buf.end(), comp.begin(), comp.end());
        }
        total_uncompressed += int64_t(dict.dictionary_page.size() + hdr.size());
        total_compressed   += int64_t(on_disk_body + on_disk_hdr);
        out_meta.cm.encodings.push_back(encoding::plain);
        out_meta.cm.encodings.push_back(encoding::rle_dictionary);
    }

    // ---- data pages
    // Dictionary-encoded chunks are paged like any other. The dictionary page itself stays
    // per chunk, which is what Parquet specifies; each data page carries its own RLE stream
    // over its slice of the retained indices. Before this the chunk was emitted whole
    // because the index stream had been encoded once for all of it, so a point read on a
    // dictionary column decoded every row in the row group to return one -- and dictionary
    // columns are exactly the low-cardinality ones this format is best at (10.4f).
    const size_t page_sz = _opt.page_values;
    bool first_data_page = true;
    size_t page_ordinal = 0;     // per chunk, and part of every data page's AAD
    size_t dict_cursor = 0;      // present values already emitted, for slicing dict.indices
    int64_t rows_written = 0;
    size_t val_cursor = 0;   // next present value, for repeated columns   // first_row_index of the next page

    // A page must never split a row, so with repetition the cut points are the
    // rep_level==0 positions rather than every `page_values`-th value.
    auto page_end = [&] (size_t from) {
        size_t want = std::min(from + page_sz, n);
        if (max_rep == 0 || want >= n) { return want; }
        // Walk back to the start of the row `want` lands inside; never emit an
        // empty page, so if that walk reaches `from`, walk forward instead.
        size_t e = want;
        while (e > from && col.rep_levels[e] != 0) { --e; }
        if (e == from) {
            e = want;
            while (e < n && col.rep_levels[e] != 0) { ++e; }
        }
        return e;
    };

    for (size_t off = 0; off < n || (n == 0 && off == 0); ) {
        if (n == 0) { break; }
        const size_t stop = page_end(off);
        const size_t cnt = stop - off;

        // definition and repetition levels (never compressed in V2)
        std::vector<uint8_t> def_bytes, rep_bytes;
        int32_t page_nulls = 0;
        if (has_def) {
            std::span<const uint64_t> lv(col.def_levels.data() + off, cnt);
            for (auto x : lv) { if (x < max_def) { ++page_nulls; } }
            def_bytes = encode_levels_v2(lv, max_def);
        }
        if (max_rep > 0) {
            std::span<const uint64_t> rv(col.rep_levels.data() + off, cnt);
            rep_bytes = encode_levels_v2(rv, max_rep);
        }
        null_count += page_nulls;

        // Where this page's values start. A flat column supplies one value per
        // slot, so the slot index doubles as the value index; a repeated one
        // supplies present values only, so they have to be counted.
        const size_t vbase = max_rep == 0 ? off : val_cursor;
        size_t page_present = 0;
        for (size_t i = 0; i < cnt; ++i) {
            if (!has_def || col.def_levels[off + i] == max_def) { ++page_present; }
        }

        std::vector<uint8_t> body;
        encoding used = encoding::plain;
        if (use_dict) {
            // Indices are per *present* value, so the slice is tracked by a running count
            // of present values rather than by slot -- nulls occupy a slot and no index.
            body = encode_dict_index_page(
                    std::span<const uint64_t>(dict.indices.data() + dict_cursor, page_present),
                    dict.bit_width);
            used = encoding::rle_dictionary;
        } else {
            switch (spec.type) {
            case phys_type::int32: {
                std::vector<int32_t> present;
                present.reserve(cnt);
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.i32[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
                    }
                encode_plain<int32_t>(body, present);
                break;
            }
            case phys_type::int64: {
                std::vector<int64_t> present;
                present.reserve(cnt);
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.i64[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
                    }
                if (spec.preferred && *spec.preferred == encoding::delta_binary_packed) {
                    encode_delta_binary_packed(body, present);
                    used = encoding::delta_binary_packed;
                } else {
                    encode_plain<int64_t>(body, present);
                }
                break;
            }
            case phys_type::dbl: {
                std::vector<double> present;
                present.reserve(cnt);
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.f64[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
                    }
                if (spec.preferred && *spec.preferred == encoding::byte_stream_split) {
                    encode_byte_stream_split<double>(body, present);
                    used = encoding::byte_stream_split;
                } else {
                    encode_plain<double>(body, present);
                }
                break;
            }
            case phys_type::byte_array: {
                std::vector<std::string> present;
                present.reserve(cnt);
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.str[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
                    }
                if (spec.preferred && *spec.preferred == encoding::delta_byte_array) {
                    encode_delta_byte_array(body, present);
                    used = encoding::delta_byte_array;
                } else if (spec.preferred
                           && *spec.preferred == encoding::delta_length_byte_array) {
                    encode_delta_length_byte_array(body, present);
                    used = encoding::delta_length_byte_array;
                } else {
                    encode_plain_byte_array(body, present);
                }
                break;
            }
            default:
                throw std::runtime_error("writer: unsupported physical type");
            }
        }

        auto comp = compress(body, _opt.compression, _opt.zstd_level);
        // In V2 the level bytes sit before the (compressed) values and are
        // counted in both page sizes but never compressed.
        const size_t lvl_bytes = def_bytes.size() + rep_bytes.size();
        const int32_t uncompressed = int32_t(lvl_bytes + body.size());
        const int32_t compressed   = int32_t(lvl_bytes + comp.size());

        // num_rows counts rows, not values; they differ once a column repeats.
        int32_t page_rows = int32_t(cnt);
        if (max_rep > 0) {
            page_rows = 0;
            for (size_t i = 0; i < cnt; ++i) {
                if (col.rep_levels[off + i] == 0) { ++page_rows; }
            }
        }

        // Under encryption the page body -- levels *and* values, as one buffer -- becomes a
        // single module, and `compressed_page_size` has to state the size of the resulting
        // envelope rather than of the plaintext. Verified against parquet-cpp's own V2 output:
        // uncompressed_page_size stays the plaintext length while compressed_page_size counts
        // the 4-byte length prefix, the nonce, the ciphertext and the tag.
        const bool ctr_body = _opt.encryption.algo == cipher::aes_gcm_ctr_v1;
        const size_t body_plain = size_t(lvl_bytes + comp.size());
        const int32_t on_disk = encrypting() ? int32_t(envelope_size(body_plain, ctr_body))
                                             : compressed;
        std::vector<uint8_t> hdr;
        write_data_page_v2_header(hdr, uncompressed, on_disk,
                                  int32_t(cnt), page_nulls, page_rows,
                                  used, int32_t(def_bytes.size()),
                                  int32_t(rep_bytes.size()),
                                  _opt.compression != codec::uncompressed);
        const int64_t page_start = pos();
        if (first_data_page) {
            out_meta.cm.data_page_offset = page_start;
            first_data_page = false;
        }
        // PageLocation.offset points at the page header, and the size covers
        // header + body -- that is what the spec means by "the page".
        out_meta.pages.push_back(page_location{
                page_start,
                int32_t(encrypting() ? int64_t(envelope_size(hdr.size(), false)) + on_disk
                                     : int64_t(hdr.size()) + compressed),
                int64_t(rows_written)});
        rows_written += int64_t(page_rows);
        size_t hdr_on_disk = hdr.size();
        if (encrypting()) {
            const auto& ckey = key_for_column(spec.name);
            hdr_on_disk = emit_module(hdr, module_type::data_page_header,
                                      int(_rgs.size()), column_ordinal, int(page_ordinal),
                                      false, &ckey);
            // Spec order inside the body: repetition levels, definition levels, then values.
            std::vector<uint8_t> body;
            body.reserve(body_plain);
            body.insert(body.end(), rep_bytes.begin(), rep_bytes.end());
            body.insert(body.end(), def_bytes.begin(), def_bytes.end());
            body.insert(body.end(), comp.begin(), comp.end());
            emit_module(body, module_type::data_page, int(_rgs.size()), column_ordinal,
                        int(page_ordinal), ctr_body, &ckey);
        } else {
            _buf.insert(_buf.end(), hdr.begin(), hdr.end());
            // Spec order: repetition levels first, then definition levels.
            _buf.insert(_buf.end(), rep_bytes.begin(), rep_bytes.end());
            _buf.insert(_buf.end(), def_bytes.begin(), def_bytes.end());
            _buf.insert(_buf.end(), comp.begin(), comp.end());
        }
        ++page_ordinal;

        total_uncompressed += uncompressed + int64_t(hdr.size());
        total_compressed   += on_disk      + int64_t(hdr_on_disk);

        if (std::find(out_meta.cm.encodings.begin(), out_meta.cm.encodings.end(), used)
            == out_meta.cm.encodings.end()) {
            out_meta.cm.encodings.push_back(used);
        }
        val_cursor += page_present;
        dict_cursor += page_present;
        off = stop;
    }

    if (first_data_page) {   // zero-row column: still needs a valid offset
        out_meta.cm.data_page_offset = pos();
    }
    out_meta.cm.total_uncompressed_size = total_uncompressed;
    out_meta.cm.total_compressed_size   = total_compressed;
    if (_opt.write_statistics) {
        statistics st;
        st.null_count = null_count;
        out_meta.cm.stats = st;
    }
}

void parquet_file_writer::add_row_group(std::span<const column_data> cols) {
    if (cols.size() != _schema.size()) {
        throw std::runtime_error("writer: column count does not match schema");
    }
    rg_meta rg;
    // Rows, not values: a repeated column holds more values than rows, so the
    // consistency check has to count rows or it rejects every nested schema.
    rg.num_rows = cols.empty() ? 0 : int64_t(cols[0].num_rows());
    for (size_t i = 0; i < cols.size(); ++i) {
        if (int64_t(cols[i].num_rows()) != rg.num_rows) {
            throw std::runtime_error("writer: ragged row group (column '" + _schema[i].name +
                                     "': " + std::to_string(cols[i].num_rows()) + " rows, expected " +
                                     std::to_string(rg.num_rows) + ")");
        }
        chunk_meta cmeta;
        write_column_chunk(_schema[i], cols[i], cmeta, int(i));
        rg.total_byte_size += cmeta.cm.total_uncompressed_size;
        rg.chunks.push_back(std::move(cmeta));
    }
    _num_rows += rg.num_rows;
    _rgs.push_back(std::move(rg));
    // A row group boundary is the only safe place to hand bytes out: every offset that
    // refers into it has already been recorded, and the page index and footer that will
    // reference those offsets are written later from _rgs, not from the buffer.
    drain();
}

void parquet_file_writer::write_page_indexes() {
    if (!_opt.write_page_index) { return; }
    for (auto& rg : _rgs) {
        for (auto& ch : rg.chunks) {
            if (ch.pages.empty()) { continue; }
            std::vector<uint8_t> blob;
            {
                compact_writer w(blob);
                compact_writer::struct_scope oi(w);          // OffsetIndex
                w.field_list(1, ctype::strct, ch.pages.size());
                for (const auto& pl : ch.pages) {
                    compact_writer::elem_scope e(w);          // PageLocation
                    w.field_i64(1, pl.offset);
                    w.field_i32(2, pl.compressed_page_size);
                    w.field_i64(3, pl.first_row_index);
                }
            }
            ch.offset_index_offset = pos();
            if (encrypting()) {
                // The OffsetIndex is a module too (type 7, no page ordinal). Leaving it in the
                // clear would leak the page layout -- row counts and sizes per page -- of an
                // otherwise encrypted column, and readers expect it encrypted when the file is.
                const auto& ckey = key_for_column(
                        ch.cm.path_in_schema.empty() ? std::string() : ch.cm.path_in_schema.back());
                const size_t n = emit_module(blob, module_type::offset_index,
                                             int(&rg - _rgs.data()), int(&ch - rg.chunks.data()),
                                             -1, false, &ckey);
                ch.offset_index_length = int32_t(n);
            } else {
                ch.offset_index_length = int32_t(blob.size());
                _buf.insert(_buf.end(), blob.begin(), blob.end());
            }
        }
    }
}

void parquet_file_writer::write_footer() {
    std::vector<uint8_t> meta;
    compact_writer w(meta);
    {
        compact_writer::struct_scope s(w);
        w.field_i32(1, 2);                          // version
        // --- schema: flat, root first
        w.field_list(2, ctype::strct, _tree.size());
        // Straight out of _tree, so a nested schema needs no special case: a group
        // has num_children and no physical type, a leaf the other way round.
        for (const auto& el : _tree) {
            compact_writer::elem_scope e(w);
            if (el.type)            { w.field_i32(1, int32_t(*el.type)); }
            if (el.repetition_type) { w.field_i32(3, int32_t(*el.repetition_type)); }
            w.field_binary(4, el.name);
            if (el.num_children)    { w.field_i32(5, *el.num_children); }
            if (el.converted_type)  { w.field_i32(6, *el.converted_type); }
        }
        w.field_i64(3, _num_rows);
        // --- row groups
        w.field_list(4, ctype::strct, _rgs.size());
        for (const auto& rg : _rgs) {
            compact_writer::elem_scope e(w);
            w.field_list(1, ctype::strct, rg.chunks.size());
            size_t chunk_ordinal = 0;
            for (const auto& ch : rg.chunks) {
                compact_writer::elem_scope ce(w);
                w.field_i64(2, ch.first_page_offset);       // file_offset
                // ColumnMetaData is serialised standalone so it can go either inline (field 3) or
                // encrypted under the column's own key (field 9). A separately-serialised struct
                // starts its own field-id delta sequence, which is exactly what a nested struct
                // needs, so the bytes splice in unchanged.
                std::vector<uint8_t> cmbuf;
                {
                    compact_writer cw(cmbuf);
                    compact_writer::struct_scope cm(cw);
                    const auto& m = ch.cm;
                    cw.field_i32(1, int32_t(m.type));
                    cw.field_list(2, ctype::i32, m.encodings.size());
                    for (auto en : m.encodings) { cw.zigzag(int32_t(en)); }
                    cw.field_list(3, ctype::binary, m.path_in_schema.size());
                    for (const auto& p : m.path_in_schema) {
                        cw.uvarint(p.size()); cw.raw(p.data(), p.size());
                    }
                    cw.field_i32(4, int32_t(m.compression));
                    cw.field_i64(5, m.num_values);
                    cw.field_i64(6, m.total_uncompressed_size);
                    cw.field_i64(7, m.total_compressed_size);
                    cw.field_i64(9, m.data_page_offset);
                    if (m.dictionary_page_offset) { cw.field_i64(11, *m.dictionary_page_offset); }
                    if (m.stats && m.stats->null_count) {
                        cw.field_struct(12);
                        compact_writer::elem_scope st(cw);
                        cw.field_i64(3, *m.stats->null_count);
                    }
                }
                const std::string leaf = ch.cm.path_in_schema.empty()
                                       ? std::string() : ch.cm.path_in_schema.back();
                const auto* ck = encrypting() ? column_key_for(leaf) : nullptr;
                if (!ck) {
                    w.field(3, ctype::strct);
                    w.raw(cmbuf.data(), cmbuf.size());
                }
                // ColumnChunk.offset_index_offset / _length come after meta_data
                // because Thrift field ids must be written in ascending order.
                if (ch.offset_index_offset) { w.field_i64(4, *ch.offset_index_offset); }
                if (ch.offset_index_length) { w.field_i32(5, *ch.offset_index_length); }
                if (encrypting()) {
                    // ColumnCryptoMetaData: field 1 is ENCRYPTION_WITH_FOOTER_KEY, an empty
                    // struct; field 2 is ENCRYPTION_WITH_COLUMN_KEY, which names the column and
                    // carries whatever the reader needs to find that column's key.
                    w.field_struct(8);
                    {
                        compact_writer::elem_scope cc(w);
                        if (!ck) {
                            w.field_struct(1);
                            compact_writer::elem_scope ek(w);
                        } else {
                            w.field_struct(2);
                            compact_writer::elem_scope ek(w);
                            w.field_list(1, ctype::binary, ch.cm.path_in_schema.size());
                            for (const auto& p : ch.cm.path_in_schema) {
                                w.uvarint(p.size()); w.raw(p.data(), p.size());
                            }
                            if (ck->key_metadata) { w.field_binary(2, *ck->key_metadata); }
                        }
                    }
                    if (ck) {
                        // The metadata itself, encrypted under the column key. A reader with only
                        // the footer key can see that this column exists and nothing else -- not
                        // its size, not its encodings, not where its pages are.
                        std::vector<uint8_t> enc;
                        const auto aad = build_aad(_opt.encryption.aad_prefix, _aad_file_unique,
                                                   module_type::column_metadata,
                                                   int(&rg - _rgs.data()), int(chunk_ordinal));
                        encrypt_module(enc, cmbuf, ck->key, aad, _opt.encryption.algo, false);
                        w.field_binary(9, std::string_view(
                                reinterpret_cast<const char*>(enc.data()), enc.size()));
                    }
                }
                ++chunk_ordinal;
            }
            w.field_i64(2, rg.total_byte_size);
            w.field_i64(3, rg.num_rows);
            // RowGroup.ordinal, and it is not optional in practice even though the schema says
            // it is. parquet-cpp derives the *encryption AAD's* row-group ordinal from this
            // field, and returns -1 when the field is absent -- so a file that omits it makes
            // every reader compute an AAD with 0xFFFF where the writer put 0x0000. That is
            // invisible in uniform mode (page and column ordinals for the modules parquet-cpp
            // reads there come from position, not from here) and fatal for a column-key column,
            // whose ColumnMetaData is the one module keyed off this value: "Failed decryption
            // finalization". Ours is always the positional index, which is what it means.
            const size_t ord = size_t(&rg - _rgs.data());
            if (ord > 0x7fff) {
                // The field is an i16, so past this there is no ordinal to write. Encryption
                // cannot proceed at all -- the AAD would repeat across row groups, which is
                // precisely what the ordinals exist to prevent.
                if (encrypting()) {
                    throw std::runtime_error("parquet: more than 32768 row groups cannot be "
                                             "encrypted -- RowGroup.ordinal is an i16");
                }
            } else {
                w.field_i16(7, int16_t(ord));
            }
        }
        // --- key/value metadata
        if (!_kv.empty()) {
            w.field_list(5, ctype::strct, _kv.size());
            for (const auto& kv : _kv) {
                compact_writer::elem_scope e(w);
                w.field_binary(1, kv.first);
                w.field_binary(2, kv.second);
            }
        }
        w.field_binary(6, "scylladb-parquet (sstables/parquet/format)");
    }
    if (!encrypting()) {
        _buf.insert(_buf.end(), meta.begin(), meta.end());
        uint32_t len = uint32_t(meta.size());
        const uint8_t* lp = reinterpret_cast<const uint8_t*>(&len);
        _buf.insert(_buf.end(), lp, lp + 4);
        _buf.insert(_buf.end(), magic_plain, magic_plain + 4);
        return;
    }
    // Encrypted-footer mode. The tail is
    //     [FileCryptoMetaData, in the clear][encrypted FileMetaData][length of both][PARE]
    // and the length covers *both* structures, not just the footer -- which is the one thing
    // easy to get wrong here, and the reason the conformance test measures it against
    // parquet-cpp's own output rather than trusting the arithmetic.
    file_crypto_metadata fcm;
    fcm.algo = _opt.encryption.algo;
    fcm.aad_file_unique = _aad_file_unique;
    if (_opt.encryption.store_aad_prefix) { fcm.aad_prefix = _opt.encryption.aad_prefix; }
    fcm.supply_aad_prefix = !_opt.encryption.aad_prefix.empty()
                            && !_opt.encryption.store_aad_prefix;
    fcm.key_metadata = _opt.encryption.key_metadata;
    const auto fcm_bytes = write_file_crypto_metadata(fcm);
    const size_t region_start = _buf.size();
    _buf.insert(_buf.end(), fcm_bytes.begin(), fcm_bytes.end());
    emit_module(meta, module_type::footer);
    uint32_t region = uint32_t(_buf.size() - region_start);
    const uint8_t* lp = reinterpret_cast<const uint8_t*>(&region);
    _buf.insert(_buf.end(), lp, lp + 4);
    _buf.insert(_buf.end(), magic_encrypted, magic_encrypted + 4);
}

size_t parquet_file_writer::emit_module(std::span<const uint8_t> plain, module_type mt,
                                        int row_group, int column, int page, bool ctr_body,
                                        const encryption_key* key) {
    const auto aad = build_aad(_opt.encryption.aad_prefix, _aad_file_unique, mt,
                               row_group, column, page);
    return encrypt_module(_buf, plain, key ? *key : _opt.encryption.footer_key, aad,
                          _opt.encryption.algo, ctr_body);
}

const writer_options::encryption_options::column_key*
parquet_file_writer::column_key_for(const std::string& leaf_name) const {
    auto it = _opt.encryption.column_keys.find(leaf_name);
    return it == _opt.encryption.column_keys.end() ? nullptr : &it->second;
}

const encryption_key& parquet_file_writer::key_for_column(const std::string& leaf_name) const {
    if (auto* ck = column_key_for(leaf_name)) { return ck->key; }
    return _opt.encryption.footer_key;
}

std::vector<uint8_t> parquet_file_writer::finish() {
    write_page_indexes();
    write_footer();
    if (_sink) {
        drain();
        return {};      // the file has already gone to the sink
    }
    return std::move(_buf);
}

} // namespace sstables::parquet::format
