/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "parquet_reader.hh"
#include "page_header.hh"
#include "decoders.hh"

#include <optional>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>

#include <cstring>
#include <lz4.h>
#include <snappy.h>
#include <zstd.h>

namespace sstables::parquet::format {

namespace {

struct dprof {
    static inline bool enabled = [] {
        const char* e = std::getenv("PQ_READER_PROFILE");
        return e && *e && *e != '0';
    }();
    static inline std::array<uint64_t, size_t(dphase::_count)> ns{};
    static inline std::array<uint64_t, size_t(dphase::_count)> hits{};
};

// Scoped, and every phase disjoint from the others -- same discipline as pq_reader's rtimer,
// for the same reason: a share column over overlapping phases means nothing.
class dtimer {
    dphase _p;
    std::chrono::steady_clock::time_point _t0;
public:
    explicit dtimer(dphase p) : _p(p) {
        if (dprof::enabled) { _t0 = std::chrono::steady_clock::now(); }
    }
    ~dtimer() {
        if (!dprof::enabled) { return; }
        const auto dt = std::chrono::steady_clock::now() - _t0;
        dprof::ns[size_t(_p)] += uint64_t(
                std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count());
        ++dprof::hits[size_t(_p)];
    }
};

} // namespace

void decode_profile_reset() {
    dprof::ns.fill(0);
    dprof::hits.fill(0);
}

std::string decode_profile_report() {
    if (!dprof::enabled) { return {}; }
    static constexpr const char* names[] = {
        "decompress", "decompress_dict", "levels", "values", "expand_nulls", "trim", "plan",
    };
    static_assert(std::size(names) == size_t(dphase::_count));
    uint64_t total = 0;
    for (auto v : dprof::ns) { total += v; }
    if (!total) { return {}; }
    // snprintf rather than fmt: run_tests.sh compiles this file standalone, without libfmt on the
    // link line, and the point of that suite is that it runs anywhere.
    std::string out = "  page decode (inside rg_decode + decode_cpu, not additive with them)\n";
    char line[160];
    for (size_t i = 0; i < size_t(dphase::_count); ++i) {
        const double ms = double(dprof::ns[i]) / 1e6;
        const double per = dprof::hits[i] ? double(dprof::ns[i]) / 1e3 / double(dprof::hits[i]) : 0.0;
        const double share = 100.0 * double(dprof::ns[i]) / double(total);
        std::snprintf(line, sizeof(line), "  %-18s %9.1f %8llu %11.2f %8.1f %%\n",
                      names[i], ms, (unsigned long long)dprof::hits[i], per, share);
        out += line;
    }
    std::snprintf(line, sizeof(line), "  %-18s %9.1f\n", "page decode total", double(total) / 1e6);
    out += line;
    return out;
}

namespace {

std::vector<uint8_t> decompress(std::span<const uint8_t> in, codec c, size_t expected,
                                dphase phase = dphase::decompress) {
    dtimer _dt{phase};
    switch (c) {
    case codec::uncompressed:
        return {in.begin(), in.end()};
    case codec::zstd: {
        std::vector<uint8_t> out(expected);
        const size_t n = ZSTD_decompress(out.data(), out.size(), in.data(), in.size());
        if (ZSTD_isError(n)) { throw decode_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
        out.resize(n);
        return out;
    }
    case codec::lz4_raw: {
        // A bare LZ4 block: the uncompressed length lives in the page header, not in the block, so
        // `expected` is the only thing that says how big the output is. LZ4_decompress_safe is
        // bounded by that and rejects anything that would overrun it, which is the property this
        // path needs -- page bodies come from files the node did not necessarily write.
        std::vector<uint8_t> out(expected);
        const int n = LZ4_decompress_safe(reinterpret_cast<const char*>(in.data()),
                                          reinterpret_cast<char*>(out.data()),
                                          int(in.size()), int(expected));
        if (n < 0) { throw decode_error("lz4: corrupt or truncated block"); }
        out.resize(size_t(n));
        return out;
    }
    case codec::snappy: {
        // Common in files written by other implementations; cheap to support
        // since Scylla already links snappy.
        size_t n = 0;
        if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(in.data()), in.size(), &n)) {
            throw decode_error("snappy: bad length header");
        }
        std::vector<uint8_t> out(n);
        if (!snappy::RawUncompress(reinterpret_cast<const char*>(in.data()), in.size(),
                                   reinterpret_cast<char*>(out.data()))) {
            throw decode_error("snappy: corrupt input");
        }
        return out;
    }
    default:
        throw decode_error(std::string("unsupported codec on the read path: ") + to_string(c));
    }
}

// Append values [skip, skip + take) of a page holding `total` values to `cd`.
//
// `total` is the page's whole present-value count and is not redundant: BYTE_STREAM_SPLIT strides
// by it, and the DELTA encodings need to know how far to decode before the wanted run begins. The
// scan path passes skip = 0, take = total, which is exactly what this did before it could be
// asked for a slice.
void append_values(column_data& cd, phys_type pt, encoding enc,
                   std::span<const uint8_t> body, size_t total, size_t skip, size_t take,
                   std::span<const std::string_view> dict_ba,
                   const std::vector<int32_t>& dict_i32,
                   const std::vector<int64_t>& dict_i64,
                   const std::vector<double>& dict_f64) {
    dtimer _dt{dphase::values};
    switch (pt) {
    case phys_type::int32: {
        auto v = (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary)
               ? decode_rle_dictionary<int32_t>(body, dict_i32, take, skip)
               : decode_plain<int32_t>(body, take, skip);
        cd.i32.insert(cd.i32.end(), v.begin(), v.end());
        break;
    }
    case phys_type::int64: {
        std::vector<int64_t> v;
        if (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary) {
            v = decode_rle_dictionary<int64_t>(body, dict_i64, take, skip);
        } else if (enc == encoding::delta_binary_packed) {
            v = decode_delta_binary_packed(body, take, nullptr, skip);
        } else {
            v = decode_plain<int64_t>(body, take, skip);
        }
        cd.i64.insert(cd.i64.end(), v.begin(), v.end());
        break;
    }
    case phys_type::dbl: {
        std::vector<double> v;
        if (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary) {
            v = decode_rle_dictionary<double>(body, dict_f64, take, skip);
        } else if (enc == encoding::byte_stream_split) {
            v = decode_byte_stream_split<double>(body, total, skip, take);
        } else {
            v = decode_plain<double>(body, take, skip);
        }
        cd.f64.insert(cd.f64.end(), v.begin(), v.end());
        break;
    }
    case phys_type::byte_array: {
        std::vector<std::string> v;
        if (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary) {
            auto views = decode_rle_dictionary_views(body, dict_ba, take, skip);
            v.assign(views.begin(), views.end());
        } else if (enc == encoding::delta_byte_array) {
            v = decode_delta_byte_array(body, total, skip, take);
        } else if (enc == encoding::delta_length_byte_array) {
            v = decode_delta_length_byte_array(body, total, skip, take);
        } else {
            v = decode_plain_byte_array(body, take, skip);
        }
        cd.str.insert(cd.str.end(), v.begin(), v.end());
        break;
    }
    default:
        throw decode_error("unsupported physical type on the read path");
    }
}

// A null in an optional column occupies a definition-level slot but no value, so
// the value vectors have to be re-expanded to one entry per row for the caller,
// which indexes them positionally alongside def_levels.
// Keep `keep` rows starting at `drop`. For a flat column values are dense after
// expand_nulls, so levels and values slice identically. For a repeated one a row
// spans several slots, so the row boundaries have to be found in rep_levels and
// the value vectors sliced by how many values those slots actually carried.
void trim(column_data& cd, size_t drop, size_t keep, bool repeated) {
    dtimer _dt{dphase::trim};
    auto cut = [&] (auto& v, size_t from, size_t count) {
        if (from >= v.size()) { v.clear(); return; }
        v.erase(v.begin(), v.begin() + long(from));
        if (v.size() > count) { v.resize(count); }
    };
    if (!repeated) {
        if (!cd.def_levels.empty()) { cut(cd.def_levels, drop, keep); }
        cut(cd.i32, drop, keep); cut(cd.i64, drop, keep);
        cut(cd.f64, drop, keep); cut(cd.str, drop, keep);
        return;
    }

    // Slot range covering rows [drop, drop + keep).
    const size_t n = cd.rep_levels.size();
    size_t slot_lo = n, slot_hi = n, row = 0;
    for (size_t i = 0; i < n; ++i) {
        if (cd.rep_levels[i] == 0) {
            if (row == drop) { slot_lo = i; }
            if (row == drop + keep) { slot_hi = i; break; }
            ++row;
        }
    }
    if (slot_lo == n) { slot_lo = n; }
    if (row < drop + keep) { slot_hi = n; }

    // Values are present-only, so count how many fall before and inside.
    const bool has_def = !cd.def_levels.empty();
    auto present_in = [&] (size_t from, size_t to) {
        if (!has_def) { return to - from; }
        size_t maxd = 0;
        for (auto d : cd.def_levels) { maxd = std::max<size_t>(maxd, d); }
        size_t k = 0;
        for (size_t i = from; i < to && i < cd.def_levels.size(); ++i) {
            if (cd.def_levels[i] == maxd) { ++k; }
        }
        return k;
    };
    const size_t vdrop = present_in(0, slot_lo);
    const size_t vkeep = present_in(slot_lo, slot_hi);

    cut(cd.rep_levels, slot_lo, slot_hi - slot_lo);
    if (has_def) { cut(cd.def_levels, slot_lo, slot_hi - slot_lo); }
    cut(cd.i32, vdrop, vkeep); cut(cd.i64, vdrop, vkeep);
    cut(cd.f64, vdrop, vkeep); cut(cd.str, vdrop, vkeep);
}

void expand_nulls(column_data& cd, phys_type pt, size_t first, size_t count,
                  std::span<const uint64_t> levels) {
    dtimer _dt{dphase::expand_nulls};
    const size_t present = size_t(std::count(levels.begin(), levels.end(), uint64_t(1)));
    if (present == count) { return; }   // dense: nothing to do
    auto expand = [&] (auto& vec, auto zero) {
        std::decay_t<decltype(vec)> out;
        out.reserve(count);
        size_t src = first;
        for (size_t i = 0; i < count; ++i) {
            if (levels[i]) { out.push_back(vec[src++]); }
            else           { out.push_back(zero); }
        }
        vec.resize(first);
        vec.insert(vec.end(), out.begin(), out.end());
    };
    switch (pt) {
    case phys_type::int32:      expand(cd.i32, int32_t(0)); break;
    case phys_type::int64:      expand(cd.i64, int64_t(0)); break;
    case phys_type::dbl:        expand(cd.f64, 0.0); break;
    case phys_type::byte_array: expand(cd.str, std::string()); break;
    default: throw decode_error("unsupported physical type when expanding nulls");
    }
}

} // namespace

// Decodes rows [row_lo, row_hi) of one row group, reading and decompressing only
// the pages that intersect that range. `base_offset` is the file offset that
// image[0] corresponds to, so a caller can pass a slice of the file rather than
// the whole thing -- which is the entire point: this is what lets a point read
// touch one page instead of one file.
std::vector<column_data> decode_columns(std::span<const column_input> in,
                                        const file_metadata& md, size_t rg_index,
                                        int64_t row_lo, int64_t row_hi,
                                        const read_crypto* crypto,
                                        page_cache* pcache) {
    // Never for an encrypted file. The cached form is plaintext, so caching it would keep decrypted
    // user data alive long after the read that was entitled to it -- a different bargain from the
    // one the footer cache makes, which deliberately keeps ids and not keys. Encrypted files pay
    // the decompression every read until that trade is decided deliberately rather than inherited.
    if (crypto) { pcache = nullptr; }
    if (rg_index >= md.row_groups.size()) { throw decode_error("row group index out of range"); }
    const auto& rg = md.row_groups[rg_index];
    if (row_lo < 0) { row_lo = 0; }
    if (row_hi > rg.num_rows) { row_hi = rg.num_rows; }
    if (row_hi < row_lo) { row_hi = row_lo; }

    // Levels come from the schema *tree*: a leaf's own repetition type does not
    // determine them once it sits inside a repeated group.
    std::optional<std::vector<leaf_info>> planned;
    { dtimer _dt{dphase::plan}; planned = walk_leaves(md); }
    auto leaves = std::move(*planned);
    if (leaves.size() != rg.columns.size()) {
        throw decode_error("row group chunk count does not match the schema");
    }

    std::vector<column_data> out(rg.columns.size());
    std::vector<int64_t> first_row_decoded(rg.columns.size(), -1);

    for (size_t c = 0; c < rg.columns.size(); ++c) {
        const auto& cc = rg.columns[c];
        // Deliberately not read: the caller has established that this leaf is null for every row
        // of the group, so the reassembler can be told "absent" instead of being handed levels
        // that say the same thing at the cost of a page walk per leaf per window.
        if (c < in.size() && in[c].absent) {
            out[c].skipped = true;
            continue;
        }
        if (!cc.meta) { throw decode_error("column chunk without metadata"); }
        const auto& cm = *cc.meta;
        const uint8_t max_def = leaves[c].max_def;
        const uint8_t max_rep = leaves[c].max_rep;
        const bool optional = max_def > 0;
        const bool repeated = max_rep > 0;

        // The dictionary page stays as raw bytes with a view table over it; see
        // index_plain_byte_array() for why materialising it was the single
        // largest cost in a point read.
        std::vector<uint8_t> dict_raw;
        std::vector<std::string_view> dict_ba;
        std::vector<int32_t> dict_i32;
        std::vector<int64_t> dict_i64;
        std::vector<double>  dict_f64;

        if (c >= in.size()) { throw decode_error("missing column input"); }
        const auto& ci = in[c];
        // Under per-column encryption each chunk may be under a different key, so this is
        // resolved per column rather than once for the file.
        const std::string leaf_name = cm.path_in_schema.empty() ? std::string()
                                                                : cm.path_in_schema.back();
        const encryption_key* ckey = crypto ? &crypto->key_for(leaf_name) : nullptr;

        // The dictionary page, when it is supplied separately. A caller that
        // hands over the whole chunk leaves `dict` empty and lets the page walk
        // below find it, which is the same bytes either way.
        if (!ci.dict.empty()) {
            size_t dconsumed = 0;
            // Under encryption the header is its own module and has to be decrypted before it
            // can be parsed; the dictionary page carries no page ordinal, there being one per
            // chunk. `hdr_plain`/`body_plain` own the decrypted bytes for as long as the spans
            // into them are used.
            std::vector<uint8_t> dhdr_plain, dbody_plain;
            page_header dph;
            std::span<const uint8_t> dbody;
            if (crypto) {
                dhdr_plain = decrypt_module(
                        ci.dict, *ckey,
                        build_aad(crypto->aad_prefix, crypto->aad_file_unique,
                                  module_type::dictionary_page_header, int(rg_index), int(c)),
                        &dconsumed, crypto->algo, false);
                size_t dummy = 0;
                dph = parse_page_header(dhdr_plain, dummy);
                dbody_plain = decrypt_module(
                        ci.dict.subspan(dconsumed), *ckey,
                        build_aad(crypto->aad_prefix, crypto->aad_file_unique,
                                  module_type::dictionary_page, int(rg_index), int(c)),
                        nullptr, crypto->algo, crypto->algo == cipher::aes_gcm_ctr_v1);
                dbody = dbody_plain;
            } else {
                dph = parse_page_header(ci.dict, dconsumed);
                dbody = ci.dict.subspan(dconsumed, size_t(dph.compressed_page_size));
            }
            if (dph.type != page_type::dictionary_page || !dph.dict) {
                throw decode_error("dictionary span does not start with a dictionary page");
            }
            auto raw = decompress(dbody, cm.compression, size_t(dph.uncompressed_page_size),
                                  dphase::decompress_dict);
            const size_t n = size_t(dph.dict->num_values);
            switch (cm.type) {
            case phys_type::int32:      dict_i32 = decode_plain<int32_t>(raw, n); break;
            case phys_type::int64:      dict_i64 = decode_plain<int64_t>(raw, n); break;
            case phys_type::dbl:        dict_f64 = decode_plain<double>(raw, n); break;
            case phys_type::byte_array:
                dict_raw = std::move(raw);
                dict_ba = index_plain_byte_array(dict_raw, n);
                break;
            default: throw decode_error("unsupported dictionary type");
            }
        }

        const std::span<const uint8_t> image = ci.pages;
        int64_t off = 0;
        const int64_t end = int64_t(image.size());

        // Row cursor within this row group, advanced by every data page whether
        // or not it is decoded. It starts wherever the supplied page run starts.
        int64_t row_at = ci.first_row;
        int64_t produced = 0;
        // Page ordinal within the chunk, counted over *data* pages only -- a dictionary page
        // has no ordinal, so it must not advance this or every AAD after it is wrong.
        int page_ordinal = 0;
        std::vector<uint8_t> hdr_plain, body_plain;
        while (off < end && row_at < row_hi) {
            size_t consumed = 0;
            page_header ph;
            std::span<const uint8_t> body;
            if (crypto) {
                // The header module must be decrypted to learn what kind of page this is, and
                // the module type in the AAD depends on that -- so try the data-page header
                // first and fall back to the dictionary one, which is the only ambiguity.
                bool as_data = true;
                try {
                    hdr_plain = decrypt_module(
                            image.subspan(size_t(off)), *ckey,
                            build_aad(crypto->aad_prefix, crypto->aad_file_unique,
                                      module_type::data_page_header, int(rg_index),
                                      int(c), page_ordinal),
                            &consumed, crypto->algo, false);
                } catch (const std::exception&) {
                    as_data = false;
                    hdr_plain = decrypt_module(
                            image.subspan(size_t(off)), *ckey,
                            build_aad(crypto->aad_prefix, crypto->aad_file_unique,
                                      module_type::dictionary_page_header, int(rg_index),
                                      int(c)),
                            &consumed, crypto->algo, false);
                }
                size_t dummy = 0;
                ph = parse_page_header(hdr_plain, dummy);
                const int64_t body_at = off + int64_t(consumed);
                if (body_at + ph.compressed_page_size > int64_t(image.size())) {
                    throw decode_error("page body extends past EOF");
                }
                body_plain = decrypt_module(
                        image.subspan(size_t(body_at)), *ckey,
                        build_aad(crypto->aad_prefix, crypto->aad_file_unique,
                                  as_data ? module_type::data_page : module_type::dictionary_page,
                                  int(rg_index), int(c),
                                  as_data ? page_ordinal : -1),
                        nullptr, crypto->algo, crypto->algo == cipher::aes_gcm_ctr_v1);
                body = body_plain;
                if (as_data) { ++page_ordinal; }
            } else {
                ph = parse_page_header(
                        image.subspan(size_t(off),
                                      size_t(std::min<int64_t>(end - off, 64 * 1024))),
                        consumed);
                const int64_t body_at = off + int64_t(consumed);
                if (body_at + ph.compressed_page_size > int64_t(image.size())) {
                    throw decode_error("page body extends past EOF");
                }
                body = image.subspan(size_t(body_at), size_t(ph.compressed_page_size));
            }
            const int64_t body_at = off + int64_t(consumed);

            if (ph.type == page_type::dictionary_page) {
                if (!ph.dict) { throw decode_error("dictionary page without header"); }
                auto raw = decompress(body, cm.compression, size_t(ph.uncompressed_page_size),
                                      dphase::decompress_dict);
                const size_t n = size_t(ph.dict->num_values);
                switch (cm.type) {
                case phys_type::int32:      dict_i32 = decode_plain<int32_t>(raw, n); break;
                case phys_type::int64:      dict_i64 = decode_plain<int64_t>(raw, n); break;
                case phys_type::dbl:        dict_f64 = decode_plain<double>(raw, n); break;
                case phys_type::byte_array:
                    dict_raw = std::move(raw);
                    dict_ba = index_plain_byte_array(dict_raw, n);
                    break;
                default: throw decode_error("unsupported dictionary type");
                }
            } else if (ph.type == page_type::data_page_v2 && ph.v2) {
                const auto& h = *ph.v2;
                const size_t n = size_t(h.num_values);
                // V2 headers carry num_rows, so a page outside the window can be
                // stepped over without decompressing it. This is the whole
                // mechanism behind bounded point reads.
                const int64_t page_rows = h.num_rows > 0 ? int64_t(h.num_rows) : int64_t(n);
                if (row_at + page_rows <= row_lo || row_at >= row_hi) {
                    row_at += page_rows;
                    produced += int64_t(n);
                    if (ph.compressed_page_size <= 0) { throw decode_error("non-positive page size"); }
                    off = body_at + ph.compressed_page_size;
                    continue;
                }
                // Which slots of this page the caller actually asked for. For a flat leaf a slot
                // is a row, so the page's wanted sub-range is known before any value is touched
                // and the decoders can be told to start there. This is what keeps a point read
                // from paying for the whole page: at the shipping defaults a page *is* the row
                // group, so decoding all of it to keep five rows was 29 % of the read and every
                // value of it was then thrown away by trim() (design doc 10.28).
                //
                // Under repetition a slot is not a row -- a row spans an unknown number of slots,
                // and finding the boundaries means walking the repetition levels -- so a repeated
                // leaf keeps decoding the page whole and is sliced afterwards by trim(), exactly
                // as before. The saving is not available there, and correctness is not negotiable.
                const size_t slot_lo = repeated ? 0
                        : size_t(std::max<int64_t>(0, row_lo - row_at));
                const size_t slot_hi = repeated ? n
                        : size_t(std::min<int64_t>(int64_t(n), row_hi - row_at));
                if (first_row_decoded[c] < 0) {
                    first_row_decoded[c] = row_at + int64_t(slot_lo);
                }
                row_at += page_rows;
                const size_t rl = size_t(h.repetition_levels_byte_length);
                const size_t dl = size_t(h.definition_levels_byte_length);
                if (rl + dl > body.size()) { throw decode_error("level lengths exceed page body"); }

                if (repeated) {
                    dtimer _dt{dphase::levels};
                    rle_decoder rd(body.subspan(0, rl), bit_width_for(max_rep));
                    auto reps = rd.decode_all(n);
                    if (reps.size() != n) { throw decode_error("short repetition level stream"); }
                    out[c].rep_levels.insert(out[c].rep_levels.end(), reps.begin(), reps.end());
                }
                // Levels are decoded for the whole page even when only part of it is wanted: the
                // definition levels before the window are what say how many *values* precede it,
                // and there is no cheaper way to learn that. They are an RLE stream over a
                // one-or-two-bit alphabet, so this is the cheap half of the page by a wide
                // margin -- 1.4 us against 21 us for the values it locates.
                std::vector<uint64_t> levels;
                if (optional) {
                    dtimer _dt{dphase::levels};
                    rle_decoder ld(body.subspan(rl, dl), bit_width_for(max_def));
                    levels = ld.decode_all(n);
                    if (levels.size() != n) { throw decode_error("short definition level stream"); }
                    out[c].def_levels.insert(out[c].def_levels.end(),
                                             levels.begin() + long(slot_lo),
                                             levels.begin() + long(slot_hi));
                }

                // V2 keeps levels outside the compressed region. The page's own
                // is_compressed flag wins over the chunk codec: parquet-cpp
                // leaves a page raw when compression does not pay, and honouring
                // only the chunk codec makes those pages fail to decode.
                auto vbody = body.subspan(rl + dl);
                const size_t uncompressed_values = size_t(ph.uncompressed_page_size) - rl - dl;
                // The value bytes of this page, by absolute file offset. `body_at` is relative to
                // the caller's span, so the key adds where that span starts in the file; the
                // + rl + dl keeps V2's uncompressed level block out of the key, since what is
                // cached is the part the codec produced.
                const int64_t vkey = ci.pages_file_offset + body_at + int64_t(rl + dl);
                std::vector<uint8_t> owned;
                std::span<const uint8_t> raw;
                if (const auto* hit = pcache ? pcache->get(vkey) : nullptr) {
                    raw = *hit;
                } else {
                    owned = decompress(vbody,
                                       h.is_compressed ? cm.compression : codec::uncompressed,
                                       uncompressed_values);
                    if (pcache && pcache->accepts(owned.size())) {
                        raw = *pcache->put(vkey, std::move(owned));
                    } else {
                        raw = owned;
                    }
                }

                const size_t present = optional
                        ? size_t(std::count(levels.begin(), levels.end(), uint64_t(max_def))) : n;
                // Values are stored present-only, so the value offset of a slot is the number of
                // present slots before it -- not the slot index.
                const size_t vskip = optional
                        ? size_t(std::count(levels.begin(), levels.begin() + long(slot_lo),
                                            uint64_t(max_def)))
                        : slot_lo;
                const size_t vtake = optional
                        ? size_t(std::count(levels.begin() + long(slot_lo),
                                            levels.begin() + long(slot_hi), uint64_t(max_def)))
                        : slot_hi - slot_lo;
                const size_t before = out[c].num_values();
                append_values(out[c], cm.type, h.value_encoding, raw, present, vskip, vtake,
                              dict_ba, dict_i32, dict_i64, dict_f64);
                // Densifying to one value per *slot* only makes sense when a slot
                // is a row. Under repetition the caller has to walk the levels, so
                // the values stay as the file has them: present only.
                if (optional && !repeated) {
                    expand_nulls(out[c], cm.type, before, slot_hi - slot_lo,
                                 std::span<const uint64_t>(levels).subspan(slot_lo,
                                                                           slot_hi - slot_lo));
                }
                produced += int64_t(n);
            } else {
                throw decode_error("only V2 data pages are supported on the read path");
            }

            if (ph.compressed_page_size <= 0) { throw decode_error("non-positive page size"); }
            off = body_at + ph.compressed_page_size;
        }
        const bool whole_chunk = row_lo == 0 && row_hi == rg.num_rows;
        if (whole_chunk && produced != cm.num_values) {
            throw decode_error("decoded " + std::to_string(produced) + " values but the chunk "
                               "declares " + std::to_string(cm.num_values));
        }
    }

    // Pages are decoded whole, so a column may start before row_lo, and columns
    // need not share page boundaries. Trim every column to exactly [row_lo,
    // row_hi) so the caller gets aligned rows. expand_nulls has already made the
    // value vectors dense, one entry per row, so this is a plain slice.
    for (size_t c = 0; c < out.size(); ++c) {
        if (out[c].skipped) { continue; }
        const int64_t start = first_row_decoded[c] < 0 ? row_lo : first_row_decoded[c];
        const size_t drop = size_t(row_lo - start);
        const size_t keep = size_t(row_hi - row_lo);
        trim(out[c], drop, keep, leaves[c].max_rep > 0);
    }
    return out;
}

std::vector<column_data> read_row_range(std::span<const uint8_t> image, int64_t base_offset,
                                        const file_metadata& md, size_t rg_index,
                                        int64_t row_lo, int64_t row_hi,
                                        const read_crypto* crypto,
                                        std::span<const uint8_t> skip) {
    if (rg_index >= md.row_groups.size()) { throw decode_error("row group index out of range"); }
    const auto& rg = md.row_groups[rg_index];
    std::vector<column_input> in(rg.columns.size());
    for (size_t c = 0; c < rg.columns.size(); ++c) {
        const auto& cc = rg.columns[c];
        if (c < skip.size() && skip[c]) {
            in[c].absent = true;
            continue;
        }
        if (!cc.meta) { throw decode_error("column chunk without metadata"); }
        const auto& cm = *cc.meta;
        const int64_t start = (cm.dictionary_page_offset ? *cm.dictionary_page_offset
                                                         : cm.data_page_offset) - base_offset;
        const int64_t end = start + cm.total_compressed_size;
        if (start < 0 || size_t(end) > image.size()) { throw decode_error("chunk extends past EOF"); }
        // Whole chunk in one span: the page walk finds the dictionary itself.
        in[c].pages = image.subspan(size_t(start), size_t(end - start));
        in[c].first_row = 0;
        in[c].pages_file_offset = base_offset + start;
    }
    return decode_columns(in, md, rg_index, row_lo, row_hi, crypto);
}

std::vector<column_data> read_row_group(std::span<const uint8_t> image,
                                        const file_metadata& md,
                                        size_t rg_index,
                                        const read_crypto* crypto) {
    if (rg_index >= md.row_groups.size()) { throw decode_error("row group index out of range"); }
    return read_row_range(image, 0, md, rg_index, 0, md.row_groups[rg_index].num_rows, crypto);
}

std::vector<column_data> read_file(std::span<const uint8_t> image) {
    auto md = parse_footer(image);
    if (md.row_groups.empty()) { return {}; }
    return read_row_group(image, md, 0);
}

encrypted_footer parse_encrypted_footer(std::span<const uint8_t> image, const encryption_key& key,
                                        std::string_view aad_prefix,
                                        const std::map<std::string, encryption_key>& column_keys,
                                        limits lim) {
    if (!has_encrypted_footer(image) || image.size() < 8) {
        throw decode_error("not an encrypted-footer Parquet file (no PARE magic)");
    }
    if (std::memcmp(image.data(), magic_encrypted, 4) != 0) {
        throw decode_error("encrypted file without a leading PARE magic");
    }
    const uint8_t* t = image.data() + image.size() - 8;
    const uint32_t region = uint32_t(t[0]) | (uint32_t(t[1]) << 8)
                          | (uint32_t(t[2]) << 16) | (uint32_t(t[3]) << 24);
    if (size_t(region) + 8 > image.size()) {
        throw decode_error("encrypted footer region length out of range");
    }
    auto tail = image.subspan(image.size() - 8 - size_t(region), size_t(region));
    size_t consumed = 0;
    auto fcm = parse_file_crypto_metadata(tail, &consumed);
    encrypted_footer out;
    out.crypto.key = key;
    out.crypto.algo = fcm.algo;
    out.crypto.aad_file_unique = fcm.aad_file_unique;
    // The writer either stored the prefix or told the reader to supply it. Preferring the stored
    // one keeps a caller that passes a redundant prefix from silently producing a file it cannot
    // read -- the failure would surface as an authentication error pages later.
    out.crypto.aad_prefix = !fcm.aad_prefix.empty() ? fcm.aad_prefix : std::string(aad_prefix);
    if (fcm.supply_aad_prefix && out.crypto.aad_prefix.empty()) {
        throw decode_error("file requires an AAD prefix that was not supplied");
    }
    auto aad = build_aad(out.crypto.aad_prefix, out.crypto.aad_file_unique, module_type::footer);
    auto plain = decrypt_module(tail.subspan(consumed), key, aad, nullptr, fcm.algo, false);
    out.md = parse_file_metadata(plain, lim);
    out.crypto.column_keys = column_keys;
    // Fill in the metadata of any column encrypted under a key we were given. Columns whose key
    // we lack keep `meta` empty, which is a legitimate state the parser now tolerates -- the file
    // is readable, that column is not.
    for (size_t g = 0; g < out.md.row_groups.size(); ++g) {
        auto& rg = out.md.row_groups[g];
        for (size_t c = 0; c < rg.columns.size(); ++c) {
            auto& ch = rg.columns[c];
            if (ch.meta || !ch.encrypted_column_metadata || !ch.crypto_metadata) { continue; }
            const auto& path = ch.crypto_metadata->path_in_schema;
            if (path.empty()) { continue; }
            auto it = column_keys.find(path.back());
            if (it == column_keys.end()) { continue; }
            auto caad = build_aad(out.crypto.aad_prefix, out.crypto.aad_file_unique,
                                  module_type::column_metadata, int(g), int(c));
            auto blob = std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(ch.encrypted_column_metadata->data()),
                    ch.encrypted_column_metadata->size());
            auto cm = decrypt_module(blob, it->second, caad, nullptr, out.crypto.algo, false);
            ch.meta = parse_column_metadata_blob(cm, lim);
        }
    }
    return out;
}

std::vector<column_data> read_encrypted_file(std::span<const uint8_t> image,
                                             const encryption_key& key,
                                             std::string_view aad_prefix) {
    auto ef = parse_encrypted_footer(image, key, aad_prefix);
    if (ef.md.row_groups.empty()) { return {}; }
    return read_row_group(image, ef.md, 0, &ef.crypto);
}

} // namespace sstables::parquet::format
