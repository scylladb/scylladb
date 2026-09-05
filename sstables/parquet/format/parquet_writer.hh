/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Parquet file writer: pages -> column chunks -> row groups -> Thrift footer.
//
// Layer 1, so it deals in columns of plain values, not mutation fragments. The
// Scylla-facing shredder that turns a mutation-fragment stream into these
// columns lives one level up, in schema_mapping.hh.
//
// Emits V2 data pages exclusively. V2 keeps definition levels outside the
// compressed body, which is what lets a reader skip nulls and locate rows
// without invoking a codec.

#include "parquet_metadata.hh"
#include "encryption.hh"

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace sstables::parquet::format {

// One entry per data page, as parquet.thrift PageLocation. `first_row_index` is
// what makes row-ordinal lookup work: given a row number, binary-searching these
// yields the exact page to decode, which is the mechanism Scylla's index relies
// on (design doc section 5.4, option A).
struct page_location {
    int64_t offset = 0;
    int32_t compressed_page_size = 0;
    int64_t first_row_index = 0;
};

struct writer_options {
    // zstd, and it stays zstd. LZ4_RAW is supported and reachable as `compression = 'lz4'`; it is
    // *measurably* the cheaper codec -- 260 us of a 370 us point read becomes 135 us -- and it is
    // still not the right default, because page geometry beats it on both axes at once: zstd at
    // page_rows=512 is both faster (137 us) and far smaller (0.414x of the row format) than lz4 at
    // the shipping geometry (245 us, 0.564x). lz4 exists because the row format's default is
    // LZ4WithDictsCompressor, so a codec-controlled comparison was impossible without it, and
    // because it is a defensible choice for a hot tier where disk is cheap (design doc 10.29).
    codec   compression = codec::zstd;
    int     zstd_level = 3;
    // Values per data page. The writer uses min(page_values, row group size), so at 8 192
    // against row groups cut at 5 000 rows (design doc 10.4c) this does not bind and a page
    // covers a whole row group.
    //
    // It was set to 2 048 on the strength of a +6.3 % size cost measured on the perf schema,
    // and reverted the same day when the corpus was measured: the real cost is **+16.7 %**,
    // consistently on a 10-column numeric table and a 197-column sparse one, which is 2.5x what
    // the one-schema measurement predicted (10.4m). At 2 048 Backblaze goes from 95.8 % of the
    // SSTable to 111.9 %, i.e. from a marginal win to a net loss.
    //
    // The latency it bought was genuine -- point-read p50 971 -> 587 us, 1.65x -- and it is
    // still not worth it. The point read remains 20-33x the row format either way, so the trade
    // spent a sixth of the format's entire reason for existing to move a metric that stays
    // uncompetitive. Disk is what Parquet is for; 4 096 is no better a compromise, buying 1.14x
    // for +7.5 %.
    //
    // **That reasoning was re-opened on 2026-08-23 and its premise no longer holds.** Two reader
    // fixes (10.29) took a point read from 1 036 us to 367 us with no format change, which leaves
    // 70 % of it in ZSTD_decompress on pages the size of a row group -- so page size is now the
    // whole of what is left, not a sixth of the format's value spent on a lost cause. Re-swept
    // against the fixed reader, seven columns and sixty-five, with write and scan throughput flat
    // across the whole range:
    //
    //     page_rows   7-col us / size    65-col us / size
    //     default        372 / 0.321x      1 289 / 0.321x
    //     2 048          206 / 0.345x        761 / 0.399x
    //     1 024          163 / 0.366x        627 / 0.463x
    //       512          137 / 0.414x        575 / 0.590x
    //
    // At 1 024 the seven-column point read is 5.6x the row format rather than 20-33x. The trade is
    // now worth making somewhere; it is still not clear it is worth making *by default*, because
    // the size cost is a function of leaf count -- +7.5 % on seven columns at 2 048, +24.3 % on
    // sixty-five -- and two schemas is the same kind of evidence that was wrong last time. What
    // settles it is the corpus (Backblaze at 197 columns, ClickBench at 105) at 1 024 and 2 048.
    //
    // Note also that latency bottoms out near 256 rows and rises below it: per-page overhead
    // starts costing more than the decode it saves. There is no point going smaller than that
    // whatever the size budget allows.
    size_t  page_values = 8192;
    bool    use_dictionary = true;
    // Dictionary-encode *numeric* columns too, not just byte_array ones.
    //
    // Off by default. Measured on a 20k-partition table over 10 000 random point reads,
    // three runs each and 0.3% spread: it costs +10.5% point-read latency (p50 1 977 ->
    // 2 185 us) and buys 3.9% of the file here, 10.9% on a numeric time-series table.
    // Point-read latency is this format's weakest metric and disk is its strongest, so
    // spending the former to improve the latter is the wrong direction by default.
    // Worth turning on for a bottom tier that is scanned and never point-read.
    bool    numeric_dictionary = false;
    size_t  dictionary_max_bytes = 1u << 20;
    // Minimum average repeats per distinct value before a dictionary is used.
    size_t  dictionary_min_repeat = 8;
    bool    write_statistics = true;
    // Emit the OffsetIndex. Required for row-ordinal lookup, and it also
    // lets scan-side readers skip pages.
    bool    write_page_index = true;

    // Parquet Modular Encryption. Off by default; when on, the file is written in
    // encrypted-footer mode ("PARE") with every column encrypted under the footer key.
    //
    // Two consequences the caller has to know about. The output stops being byte-reproducible,
    // because every module carries a fresh random nonce -- reusing one under a single key breaks
    // AES-GCM outright, so this is not a knob. And the file is unreadable without the key: there
    // is no "encrypted but degraded" read path, by design.
    struct encryption_options {
        bool                       enabled = false;
        cipher                     algo = cipher::aes_gcm_v1;
        encryption_key             footer_key;
        // Bound into every module's AAD. An aad_prefix ties a file to its identity (a table, a
        // generation) so a whole file cannot be swapped for another one written under the same
        // key. `store_aad_prefix` writes it into the footer for readers that cannot reconstruct
        // it; leaving it false means a reader must supply it out of band.
        std::string                aad_prefix;
        bool                       store_aad_prefix = false;
        // Opaque to us: whatever a reader's key-management needs in order to find the key again.
        std::optional<std::string> key_metadata;

        // Per-column keys, by leaf name. A column listed here is encrypted with its own key
        // instead of the footer key, and its ColumnMetaData moves out of the (footer-key
        // encrypted) footer into ColumnChunk.encrypted_column_metadata -- so a reader holding
        // only the footer key sees that the column exists and nothing else about it.
        //
        // This is what makes "encrypt the PII columns" expressible: give an analytics reader the
        // footer key and the keys for the columns it may see, and the rest of the file stays shut
        // to it while remaining a valid Parquet file.
        struct column_key {
            encryption_key             key;
            std::optional<std::string> key_metadata;
        };
        std::map<std::string, column_key> column_keys{};
    };
    encryption_options encryption{};
};

// One leaf column of a row group. Exactly one value vector is populated; which
// one must agree with the declared physical type.
struct column_data {
    // Set by the read path when this leaf was deliberately not read: it says "no value in this
    // leaf, for any row of this window", which is a different statement from "no values here"
    // and cannot be inferred from the empty vectors below, because a REQUIRED leaf legitimately
    // has no def_levels. reassemble() must consult it before indexing anything.
    //
    // It is only ever set when the file's own statistics prove the chunk all-null, so a skipped
    // leaf carries exactly the information a read leaf would have (see reader.cc,
    // elidable_leaves()).
    bool                     skipped = false;
    // Empty means the column is REQUIRED and every value is present.
    std::vector<uint64_t>    def_levels;
    // Dremel repetition levels. Empty for a column that is not inside a
    // repeated group. A zero starts a new row, so the number of zeroes is the
    // row count -- which is not the value count once a column repeats.
    std::vector<uint64_t>    rep_levels;
    std::vector<int32_t>     i32;
    std::vector<int64_t>     i64;
    std::vector<double>      f64;
    std::vector<std::string> str;

    size_t num_values() const {
        if (!i32.empty()) { return i32.size(); }
        if (!i64.empty()) { return i64.size(); }
        if (!f64.empty()) { return f64.size(); }
        if (!str.empty()) { return str.size(); }
        return 0;
    }

    // Rows, as opposed to values. They differ only for a repeated column.
    size_t num_rows() const {
        if (rep_levels.empty()) {
            return def_levels.empty() ? num_values() : def_levels.size();
        }
        size_t n = 0;
        for (auto r : rep_levels) { if (r == 0) { ++n; } }
        return n;
    }
};

struct column_spec {
    std::string            name;
    phys_type              type{};
    repetition             rep = repetition::optional;
    std::optional<int32_t> converted_type;     // parquet ConvertedType, if any
    // Encoding hint. The writer may fall back (e.g. dictionary -> plain when the
    // dictionary grows past dictionary_max_bytes).
    std::optional<encoding> preferred;

    // Dremel levels for this leaf. For a flat schema these follow from `rep`
    // alone; a leaf inside a repeated group needs them stated, because the
    // schema tree they come from is not visible here.
    uint8_t max_def = 0;
    uint8_t max_rep = 0;
    // Full path from the root, for the ColumnMetaData. Empty means just `name`.
    // Explicitly defaulted so that the existing positional initialisers in
    // schema_mapping.cc stay complete under -Wmissing-field-initializers.
    std::vector<std::string> path = {};

    std::vector<std::string> path_or_name() const {
        return path.empty() ? std::vector<std::string>{name} : path;
    }
};

class parquet_file_writer {
    struct chunk_meta {
        column_metadata cm;
        int64_t         first_page_offset = 0;
        std::vector<page_location> pages;      // for the OffsetIndex
        // ColumnChunk.offset_index_offset / _length -- these live on the chunk,
        // not on its meta_data.
        std::optional<int64_t> offset_index_offset;
        std::optional<int32_t> offset_index_length;
    };
    struct rg_meta {
        std::vector<chunk_meta> chunks;
        int64_t num_rows = 0;
        int64_t total_byte_size = 0;
    };

    // Bytes not yet handed to the sink. Without a sink this is the whole file image;
    // with one it holds at most the row group being written plus the footer.
    std::vector<uint8_t>     _buf;
    // Bytes already handed to the sink. Every offset this writer records is a position
    // in the *file*, so it must be _flushed + _buf.size() and never _buf.size() alone --
    // see pos(). An offset that forgets the base yields a file that parses and points at
    // the wrong bytes, which is why there is a single accessor for it.
    uint64_t                 _flushed = 0;
    std::function<void(std::span<const uint8_t>)> _sink;
    // The schema tree exactly as Parquet stores it: flat, depth-first, root at
    // index 0. A flat schema is just a root with one leaf per column; a nested
    // one is supplied by the caller.
    std::vector<schema_element> _tree;
    std::vector<column_spec> _schema;   // leaves, in column order
    writer_options           _opt;
    std::vector<rg_meta>     _rgs;
    int64_t                  _num_rows = 0;
    std::vector<std::pair<std::string, std::string>> _kv;

    // The one way to ask "where are we in the file". Never use _buf.size() for an offset.
    int64_t pos() const { return int64_t(_flushed + _buf.size()); }
    // Hand everything buffered to the sink, if there is one, and advance the base.
    void drain();

    void write_column_chunk(const column_spec&, const column_data&, chunk_meta&,
                            int column_ordinal);

    bool encrypting() const { return _opt.encryption.enabled; }
    // Appends `plain` to _buf as an encrypted module and returns the number of bytes written.
    // Used for page headers, page bodies, offset indexes and the footer alike, which is why it
    // takes the module type and ordinals rather than knowing them.
    size_t emit_module(std::span<const uint8_t> plain, module_type,
                       int row_group = -1, int column = -1, int page = -1,
                       bool ctr_body = false, const encryption_key* key = nullptr);
    // The key protecting one leaf: its own if it has one, otherwise the footer key.
    const encryption_key& key_for_column(const std::string& leaf_name) const;
    const writer_options::encryption_options::column_key*
            column_key_for(const std::string& leaf_name) const;
    // Size the envelope will occupy, needed because a page header has to state its own
    // compressed size before the body is encrypted.
    size_t envelope_size(size_t plain_len, bool ctr_body) const {
        return length_prefix_len + nonce_len + plain_len
               + ((ctr_body && _opt.encryption.algo == cipher::aes_gcm_ctr_v1) ? 0 : tag_len);
    }
    std::string _aad_file_unique;
    // Emitted after all row groups and before the footer, which is where the
    // spec puts it: the footer has to carry the offsets of these blobs.
    void write_page_indexes();
    void write_footer();

public:
    // Flat schema: one leaf per column, no nesting.
    parquet_file_writer(std::vector<column_spec> schema, writer_options opt = {});

    // Nested schema. `tree` is depth-first with the root at index 0, which is how
    // the footer stores it; the leaf specs and their Dremel levels are derived
    // from it by walk_leaves(), so writer and reader agree by construction.
    // A schema given as a tree, for anything with repeated groups in it.
    //
    // `preferred` carries the per-leaf encoding hints, in leaf order -- the order
    // walk_leaves() returns, which is also the order the mapping builds its leaves
    // in. Empty means no hints, which is what a test that only has a tree passes.
    //
    // They cannot travel inside the tree: `schema_element` mirrors the Parquet
    // Thrift SchemaElement, where an encoding is a property of a column chunk, not
    // of the schema. So structure comes from the tree and encoding comes from here,
    // and the two stay separate on purpose. Before this field existed the hints
    // were silently dropped on the nested path and every column was written PLAIN
    // -- including monotonic clustering keys the mapping had explicitly asked to be
    // DELTA_BINARY_PACKED. See docs/dev/parquet-storage-format.md section 10.1g.
    struct nested_schema {
        std::vector<schema_element> tree;
        std::vector<std::optional<encoding>> preferred;
    };
    parquet_file_writer(nested_schema tree, writer_options opt = {});

    // The leaves the tree produced, in column order. add_row_group() wants one
    // column_data per entry.
    const std::vector<column_spec>& leaves() const { return _schema; }

    // Scylla-private metadata (folding level, source table, dictionary id, ...).
    // External readers ignore it; ours uses it to know what was omitted.
    void add_key_value(std::string k, std::string v) { _kv.emplace_back(std::move(k), std::move(v)); }

    // All columns must carry the same number of values.
    void add_row_group(std::span<const column_data> cols);

    // Stream the file out as it is produced instead of accumulating it. Completed row
    // groups are handed to the sink and dropped, so peak memory becomes one row group
    // plus the footer rather than the whole output -- which for a 256 MB bottom-tier
    // sstable is the difference between ~253 MB resident and a few MB (design doc 7.2).
    //
    // The footer still has to be held: it carries one column-chunk entry per leaf per row
    // group and cannot be written until the last row group is known. That cost is
    // inherent to the format, and it is small -- measured at 1 % of the file.
    //
    // Must be set before the first add_row_group(). With a sink, finish() returns an
    // empty vector, because the file has already gone to the sink.
    void set_sink(std::function<void(std::span<const uint8_t>)> sink) {
        _sink = std::move(sink);
    }

    // Finalises the footer and returns the file image, or an empty vector if a sink is set.
    std::vector<uint8_t> finish();

    // Sum of every column chunk's uncompressed size, i.e. the serialised volume before the
    // codec. This is what a compression ratio for a `pq` sstable has to be measured against:
    // Parquet compresses internally, so there is no CompressionInfo component and Scylla would
    // otherwise report no ratio at all for a Parquet table.
    int64_t uncompressed_bytes() const {
        int64_t n = 0;
        for (const auto& rg : _rgs) { n += rg.total_byte_size; }
        return n;
    }

    // Total file bytes produced so far, flushed or not.
    size_t size_so_far() const { return size_t(pos()); }
    // Bytes currently held in memory. Equals size_so_far() without a sink; bounded by one
    // row group plus the footer with one.
    size_t buffered_bytes() const { return _buf.size(); }
};

} // namespace sstables::parquet::format
