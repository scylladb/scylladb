/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The bridge between Scylla's mutation-fragment stream and the Parquet
// shredder: an sstable_writer::writer_impl that accumulates rows and emits a
// Parquet file image at end of stream.
//
// This is the Phase 3 gate. Everything below it -- folding, shredding, encoding,
// file assembly -- already exists and is tested in schema_mapping.hh and
// format/. This class only translates types.
//
// Scope of this step: the fragment stream is consumed for real, and a valid
// Parquet image is produced. Writing that image into the sstable's Data
// component (and the matching reader and index components) is the next step, so
// the image is handed to a caller-supplied sink rather than to sstable storage.
// That also lets the whole path be driven from a unit test without an sstable.

#include "sstables/parquet/schema_mapping.hh"
#include <map>
#include <set>
#include <seastar/core/sstring.hh>
#include "sstables/metadata_collector.hh"
#include "sstables/types.hh"
#include "mutation/collection_mutation.hh"
#include "sstables/writer_impl.hh"
#include "sstables/writer.hh"
#include "sstables/parquet/encryption_keys.hh"

#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <vector>

namespace sstables::parquet {

struct pq_writer_config {
    folding_level      level = folding_level::row_folded;
    exception_encoding exc   = exception_encoding::sparse;
    format::writer_options wopt{};
    // Per-column encodings an operator asked for, already translated from the CQL enum to the
    // Parquet one. Empty for every table that does not set them, which is the common case.
    std::map<std::string, format::encoding> column_encodings{};
    // Parquet Modular Encryption. `encryption_enabled` is the selector; the key itself is never
    // held here, only the options that tell a key provider how to find it, so a config round-trip
    // (to_map) cannot leak key material.
    bool             encryption_enabled = false;
    format::cipher   encryption_algo = format::cipher::aes_gcm_v1;
    key_metadata_format encryption_key_metadata = key_metadata_format::provider;
    // ent/encryption's option vocabulary, forwarded verbatim to key_source. Empty unless
    // encryption is on.
    key_options      key_opts{};
    // Per-column keys, by CQL column name: the provider options that locate *that column's* key,
    // already overlaid onto `key_opts` so each entry is a complete option set a provider can be
    // asked with. A column listed here is encrypted under its own key instead of the footer key,
    // and its ColumnMetaData moves out of the footer -- so a reader holding only the footer key
    // learns that the column exists and nothing else about it.
    //
    // Complete option sets rather than deltas because that is what `key_source` takes: a key is
    // identified by an option map, and ent/encryption's `get_provider()` resolves a provider from
    // one. Storing the overlay result here means the writer and the reader never have to agree on
    // how a delta is applied -- they both just hand a map to the provider.
    //
    // Still no key material, exactly as with `key_opts`: only the options that locate it.
    std::map<sstring, key_options> column_key_opts{};
    // A row group is cut when either limit trips.
    //
    // `row_group_buffer_bytes` is **buffered shredder memory**, not encoded output, and
    // the two differ by about 343x -- 1 887 B/row held in memory against 5.2 B/row
    // written. It is named for what it measures because the obvious name invites a 343x
    // misconfiguration: someone setting "64 MiB" expecting 64 MiB of Parquet gets about
    // 35 600 rows, which is roughly 185 kB of actual output on a narrow table. Its job is
    // to stop a shard running out of memory (R-13), so it is charged against
    // fragment_shredder::buffered_bytes(), which errs ~4% high on purpose.
    //
    // `rows_per_row_group` is the read-granularity knob, and at 5 000 it is the one that
    // actually cuts: a row group is then about 9 MB of shredder buffer, far under the
    // byte budget, which reverts to being purely a safety net against a pathological
    // partition. That is the right division of labour -- bytes is a guard rail that
    // protects the shard, rows is the dial that tunes read cost -- and it is the opposite
    // of the original default, where the byte budget did all the cutting at an incidental
    // ~35 600 rows.
    //
    // Note the direction: this is rows *per group*, so raising it makes row groups larger
    // and therefore *fewer*. 8 M rows at 1 000 gives ~8 000 groups; at 20 000, ~400. The
    // setting and the row-group count move opposite ways, which is what the older name
    // `row_group_rows` obscured (see ROWS_PER_ROW_GROUP below).
    //
    // 5 000 comes from the sweep in design doc 10.4c (20 000 partitions x 5 rows, 2 000
    // random point reads): against one row group per file it is **2.1x lower point-read
    // latency and 3.9x less scan memory for +10 % size**, with write and scan throughput
    // flat across the whole sweep. Point-read latency and resident memory are this
    // format's two weakest metrics and disk is its strongest, so spending disk on both is
    // the right direction -- and it buys more than a cache would, without adding a cached
    // component or a line of new state.
    //
    // Going further costs disproportionately: 1 000 rows buys another 14 % of latency for
    // another 26 % of size. Per-table override via the `parquet` property (5.5a, 8.2).
    size_t rows_per_row_group     = 5'000;
    size_t row_group_buffer_bytes = 64u << 20;
};

// The user-facing `parquet = {...}` table property (design doc 8.2), parsed and
// validated into a pq_writer_config.
//
// Mirrors how `compression = {...}` becomes compression_parameters: a map of strings
// from CQL, validated once at ALTER/CREATE time so a bad value is a configuration
// error rather than a broken sstable discovered later.
//
// Only the knobs that are actually implemented are accepted. Silently ignoring a
// recognised-looking option is worse than rejecting it -- a user who sets
// `compression: 'gzip'` and gets zstd has been lied to -- so anything the writer
// cannot honour is an error with the supported set named.
// The CQL type as the writer sees it. Anything it does not special-case travels as an opaque
// BYTE_ARRAY, which is why this returns `blob` rather than failing: a blob column genuinely supports
// the byte_array encodings, so an override naming one is legitimate.
cql_type cql_type_of(const abstract_type&);

class parquet_parameters {
public:
    // How many rows go into one row group. Named for the ratio it sets, because the
    // previous name -- `row_group_rows`, kept below as an alias -- reads as if it set the
    // row-group *count*, and was in fact misread that way. The two move in opposite
    // directions: 8 M rows at 1 000 rows/group is ~8 000 groups, at 20 000 it is ~400, so
    // a reader who has the direction backwards tunes the wrong way and concludes the knob
    // does nothing.
    static constexpr const char* ROWS_PER_ROW_GROUP     = "rows_per_row_group";
    // The old spelling of ROWS_PER_ROW_GROUP. **Permanent, not deprecated-with-a-removal-
    // date.** The `parquet` map is persisted in schema (schema::parquet_options(), stored
    // by db/schema_tables.cc) and echoed verbatim by DESCRIBE, so tables created before
    // the rename carry this key in their stored property. Dropping it from the parser
    // would not merely warn -- parquet_parameters is constructed when the schema is read
    // back (writer_impl.cc, reader.cc, compaction_manager.cc) and on every subsequent
    // DDL, so those tables would fail to load. Both spellings therefore parse to the same
    // setting, for as long as any such table can exist.
    static constexpr const char* ROW_GROUP_ROWS_LEGACY  = "row_group_rows";
    static constexpr const char* ROW_GROUP_BUFFER_BYTES = "row_group_buffer_bytes";
    static constexpr const char* PAGE_ROWS              = "page_rows";
    static constexpr const char* COMPRESSION            = "compression";
    static constexpr const char* COMPRESSION_LEVEL      = "compression_level";
    static constexpr const char* METADATA_FOLDING       = "metadata_folding";
    static constexpr const char* DICTIONARY            = "dictionary";
    // Per-column encoding override: `parquet = {'encoding.<column>': '<enum>'}`.
    //
    // The writer's own choice is two-stage -- the schema proposes a hint from the column's kind and
    // type, and the data decides whether a dictionary beats it -- and both stages are deliberately
    // conservative. This is the escape hatch for the cases they get wrong, of which there are known
    // ones: a *wide scan-only* table wants encodings tuned for size at the cost of point-read
    // latency, and a text partition key that happens to be sorted in token order would benefit from
    // front coding that the structural rule refuses to apply (§10.13).
    //
    // A prefix rather than a nested map because a CQL table property is map<text,text>; there is no
    // nesting to be had. The column name is taken verbatim, so a quoted CQL identifier keeps its
    // case.
    static constexpr const char* ENCODING_PREFIX        = "encoding.";
    // Parquet Modular Encryption. `encryption` selects the algorithm -- and, being in the
    // `parquet` map rather than in `scylla_encryption_options`, says specifically "encrypt
    // *inside the format*".
    //
    // That separation is not cosmetic. `scylla_encryption_options` drives an sstable file-io
    // extension that encrypts the whole Data component, which would (a) double-encrypt and (b)
    // leave a file no external reader can open -- the exact outcome modular encryption exists to
    // prevent. So the two are mutually exclusive, enforced at DDL time in cf_prop_defs.
    //
    // The *key* still comes from ent/encryption, using its own option vocabulary (see
    // KEY_OPTIONS below), so every provider it supports -- including BYOK through KMIP, AWS KMS,
    // GCP and Azure -- works here. No key material enters the schema; only the options that
    // locate it, exactly as with scylla_encryption_options.
    static constexpr const char* ENCRYPTION            = "encryption";
    // What goes into FileCryptoMetaData.key_metadata: 'provider' (the key id verbatim, the
    // default, and what works with every provider including BYOK) or 'parquet_kms'
    // (parquet-java's key-material JSON, needed only by pyarrow's and Spark's KMS-mediated
    // readers). See encryption_keys.hh for why this is the operator's choice.
    static constexpr const char* ENCRYPTION_KEY_METADATA = "encryption_key_metadata";
    // Per-column encryption key: `parquet = {'encryption_key.<column>': '<opt>=<v>[,<opt>=<v>]'}`.
    //
    // The value is a comma-separated list of key-provider options -- the same vocabulary as
    // key_option_names() -- overlaid on the table's own key options. Usually one pair, because a
    // per-column key normally differs from the table key in exactly the one option that *names* a
    // key, and which option that is depends on the provider: `secret_key_file` for the local-file
    // provider, `master_key` for the cloud ones, `template_name`/`key_namespace` for KMIP. There is
    // no provider-neutral single name to hard-code, which is why this takes options rather than a
    // bare key identifier.
    //
    //   parquet = {'encryption': 'aes_gcm_v1',
    //              'secret_key_file': '/etc/scylla/keys/t.key',
    //              'encryption_key.ssn': 'secret_key_file=/etc/scylla/keys/pii.key'}
    //
    // A prefix rather than a nested map for the same reason as ENCODING_PREFIX: a CQL table
    // property is map<text,text> and there is no nesting to be had.
    //
    // Only a column that owns a uniquely-named Parquet leaf can be keyed -- see
    // keyable_column_error(), which is the whole of the restriction and the reason it exists.
    static constexpr const char* ENCRYPTION_KEY_PREFIX  = "encryption_key.";
    // The sub-options forwarded verbatim to the key provider. A closed list on purpose: an
    // unrecognised sub-option is an error (see the `else` in the parser), and providers ignore
    // options they do not know, so an open-ended pass-through would turn a typo'd
    // `secret_key_fil` into a silent fall back to the default key file. Adding a provider option
    // means adding it here -- which is the cost of having typos be errors.
    static const std::set<sstring>& key_option_names();

    // Guard rails. The lower bound on rows is not arbitrary: below ~1 000 rows the
    // fixed per-row-group metadata (~225 B per leaf) starts to dominate the file --
    // at 100 rows on a 20-leaf table it is 45 B/row against a 5.2 B/row total, so the
    // file grows about ninefold (design doc 10.4c).
    static constexpr size_t min_rows_per_row_group = 1'000;
    static constexpr size_t max_rows_per_row_group = 100'000'000;
    static constexpr size_t min_buffer_bytes   = 1u << 20;    // 1 MiB
    static constexpr size_t max_buffer_bytes   = 1024ull << 20; // 1 GiB

    parquet_parameters() = default;
    explicit parquet_parameters(const std::map<sstring, sstring>& opts);

    // Only entries that differ from the defaults, so DESCRIBE stays terse.
    std::map<sstring, sstring> to_map() const;

    const pq_writer_config& config() const { return _cfg; }

    // Every value the enum accepts, in the order the error message lists them. `auto` means "let the
    // writer decide", which is the default and the only way to spell "undo an override" in an ALTER.
    enum class column_encoding {
        automatic, plain, dictionary, delta_binary_packed, delta_byte_array,
        delta_length_byte_array, byte_stream_split,
    };
    static std::optional<column_encoding> parse_column_encoding(std::string_view);
    static const char* to_string(column_encoding);
    // Which encodings are legal for a physical type. Checked at DDL time rather than at write time,
    // because a write-time rejection would take the table down on a setting that looked accepted.
    static bool applies_to(column_encoding, cql_type);

    const std::map<sstring, column_encoding>& column_encodings() const { return _column_encodings; }

    // For the read path, which needs the same provider options the writer used but none of the
    // rest of the config.
    const key_options& key_opts() const { return _cfg.key_opts; }
    bool encryption_enabled() const { return _cfg.encryption_enabled; }
    // Per-column key options, by CQL column name, each already overlaid on key_opts(). The read
    // path indexes this by the leaf name a file says has its own key.
    const std::map<sstring, key_options>& column_key_opts() const { return _cfg.column_key_opts; }

    // Why `column` cannot carry its own encryption key, or nullopt if it can. Checked at DDL time,
    // where the operator is present, and exposed here so there is exactly one definition of the
    // rule.
    //
    // The restriction is a real property of the format layer, not caution: Parquet's
    // ColumnCryptoMetaData names a column by its path, but a key is looked up by the *last*
    // element of that path (`format::read_crypto::key_for`, `parquet_file_writer::column_key_for`),
    // and Scylla's mapping does not give every CQL column a uniquely-named leaf. A non-frozen
    // collection becomes five leaves called `key`, `value`, `__ts`, `__ttl`, `__ldt` under a group
    // named after the column (schema_mapping.cc), so:
    //
    //   * a collection column owns no leaf bearing its own name -- there is nothing to key; and
    //   * a *scalar* column that happens to be called `key` or `value` shares its leaf name with
    //     every collection in the table, so keying it would silently key those leaves too.
    //
    // The second case is rejected whether or not the table currently has a collection, because
    // ALTER TABLE ... ADD can introduce one later: an accepted key on a column named `value` would
    // then quietly widen to cover a new collection's values, which is a security change arriving
    // through an unrelated DDL.
    static std::optional<sstring> keyable_column_error(const sstring& column, bool multi_cell);

private:
    // Runs once the whole map is parsed: the interesting checks are about combinations (an
    // algorithm the format cannot honour, provider options with encryption off) rather than
    // single values, and CQL map order is not the operator's.
    void validate_key_options();
    // Shared by the table's own key options and by every per-column set: an algorithm the format
    // cannot honour and a key length AES does not have are errors wherever they appear, and a
    // per-column set that skipped the check would be a table that took its DDL and then failed
    // every flush of that one column. `what` names the option in the error message.
    static void validate_one_key_option_set(key_options&, std::string_view what);

    pq_writer_config _cfg;
    std::map<sstring, column_encoding> _column_encodings;
    // The per-column key overrides exactly as the operator wrote them, so to_map() round-trips the
    // text rather than a re-serialisation of the overlay result. Emitting the overlay would put
    // the table's own options back into every column's entry, which parses to the same thing but
    // reads, in DESCRIBE, as if each column had been configured from scratch.
    std::map<sstring, sstring> _column_key_raw;
};

// Builds the layer-2 column description for a Scylla schema. Exposed because
// the reader will need exactly the same mapping to invert it.
std::vector<cql_column> columns_of(const ::schema& s);

// Index, within the value columns produced by columns_of(), of the first static
// column. Everything at or after it is static; everything before is regular.
inline size_t static_base(const ::schema& s) { return s.regular_columns_count(); }

// Converts a mutation-fragment stream into rows. Split out from the
// writer_impl so it can be unit-tested without constructing an sstable.
class fragment_shredder {
    const ::schema& _schema;
    std::vector<cql_column> _cols;
    std::vector<row> _rows;
    size_t _buffered_bytes = 0;
    std::vector<value> _pk;      // current partition's key components
    std::optional<deletion_info> _part_del;
    std::map<size_t, cell> _static_cells;     // indexed as value columns
    std::map<size_t, collection_cell> _static_collections;
    size_t _static_base = 0;
    bool _saw_clustering_row = false;
    size_t _n_pk = 0, _n_ck = 0;

public:
    explicit fragment_shredder(const ::schema& s);

    void new_partition(const dht::decorated_key& dk);
    // Applies to every row of the current partition until the next one.
    void set_partition_tombstone(tombstone);
    // Closes the open partition. A partition whose only content is a static row
    // has no clustering row to attach it to, so one placeholder row is emitted
    // and marked with __no_ck.
    void end_partition();
    void add_clustering_row(const clustering_row& cr);
    void add_static_row(const static_row& sr);
    void add_range_tombstone_change(const range_tombstone_change& rtc);

    const std::vector<cql_column>& columns() const { return _cols; }
    const std::vector<row>& rows() const { return _rows; }
    size_t size() const { return _rows.size(); }
    void clear() { _rows.clear(); _buffered_bytes = 0; }

    // Estimated heap held by the buffered rows, for the write-side memory budget
    // (R-13, design doc 5.5a). Errs high on purpose -- see heap_bytes(). Accumulated
    // as rows are appended rather than walked on demand, because the writer needs to
    // consult it after every row.
    size_t buffered_bytes() const { return _buffered_bytes; }

    // Schema + shred + encode the accumulated rows into a Parquet file image.
    // Accepts any folding level, including the lossy export-only L3.
    std::vector<uint8_t> to_parquet(const pq_writer_config&) const;

    // Same, but refuses a lossy folding level. Everything on the storage path
    // must go through this: writing L3 into an sstable would silently discard
    // write times, TTLs and deletions.
    std::vector<uint8_t> to_parquet_for_storage(const pq_writer_config&) const;

private:
    // Static content rides on every row of the partition, because the reader
    // rebuilds the static row from whichever row it sees first -- and that may be
    // a range tombstone change or a placeholder, not a clustering row. Both the
    // atomic cells and the collections have to go on, together: replaying only
    // the cells drops every static collection in any partition whose first row is
    // not a clustering row.
    void replay_statics(row& r) const;

    // The one place a row enters the buffer, so the memory accounting cannot drift
    // away from the buffer the way the static-cell replay once did.
    void push_row(row&& r);
};

class pq_writer_impl : public sstables::sstable_writer::writer_impl {
public:
    using sink_type = std::function<void(std::vector<uint8_t>)>;

    // Close whatever file writers are still open.
    //
    // Needed because the success path is not the only path. consume_end_of_stream() closes
    // _data_writer and _index_writer explicitly, but a cancelled compaction never calls it: the
    // compaction_writer optional is destroyed instead, and with it this object. Leaving the
    // writers to their own destructors is a use-after-free, not merely untidy --
    // checksummed_file_writer keeps `checksum _c` as a DERIVED member while the base file_writer
    // owns the output_stream whose sink holds a reference to it (sstables/writer.hh:160-172).
    // Derived members are destroyed before the base destructor runs, and ~file_writer()
    // best-effort auto-closes an unclosed stream, so the flush appends a CRC to a checksum
    // struct that is already gone. That is the #18 segfault: si_addr 0x8 in
    // chunked_vector::emplace_back, reached from ~pq_writer_impl.
    //
    // Closing here instead runs while *_data_writer is fully alive, and close() sets the base's
    // _closed flag so ~file_writer() then does nothing.
    ~pq_writer_impl();

private:
    fragment_shredder _shredder;
    pq_writer_config  _pcfg;
    // The shard the new sstable will BELONG to, which during resharding is not the shard doing
    // the work: compaction runs on one shard and calls the creator once per *target* shard. mx
    // keeps this as `_shard` for exactly this reason (mx/writer.cc:542). Passing this_shard_id()
    // to write_scylla_metadata() instead made every resharded `pq` output claim the coordinating
    // shard, so two of them reported the same owner and the sstable's shard set was wrong on
    // disk -- invisible on a flush, where the two values are equal.
    shard_id          _shard;
    encoding_stats    _enc_stats;
    sink_type         _sink;
    uint64_t          _pos = 0;
    // True once the file writer is streaming into _data_writer, in which case
    // consume_end_of_stream must not write the image again -- finish() returns nothing.
    bool              _streaming = false;

    // Partition index. Entries carry a *row ordinal*, not a byte offset -- see
    // design doc 5.4 option A. The reader turns that ordinal into a page via the
    // Parquet OffsetIndex.
    // Held as the base type: the write() overload for keys takes file_writer&.
    std::unique_ptr<file_writer> _index_writer;
    // Opened up front, like mx does: create_data() hands the sstable's data and
    // index files to the storage layer, and nothing can be written before it.
    std::unique_ptr<crc32_checksummed_file_writer> _data_writer;
    sstables::index_sampling_state _index_sampling_state;
    std::optional<key> _first_key, _last_key;
    uint64_t _num_partitions = 0;
    // Ordinal of the first row of the partition currently being consumed.
    uint64_t _partition_first_row = 0;
    bool _in_partition = false;

    // Statistics metadata, collected with the same semantics as mx -- see
    // sstables/mx/writer.cc write_cell() and write_liveness_info().
    //
    // This is correctness, not reporting. The min/max timestamps, the
    // local-deletion-time range and the tombstone drop-time histogram are what
    // compaction and tombstone garbage collection read; an sstable that
    // under-reports them can have a tombstone dropped while data it shadows is
    // still live. Before this existed the pq writer fed its metadata_collector
    // only add_key().
    sstables::column_stats _c_stats;

    // ------------------------------------------------ large-data reporting
    //
    // system.large_partitions / large_rows / large_cells. Without these an
    // operator has no way to find a pathological partition in a `pq` table, and
    // -- worse -- cannot tell "no large partitions" from "this format does not
    // report them", because both read as an empty table.
    //
    // The measure is a *logical* size, not an on-disk one, and that is the one
    // real semantic difference from mx. mx reports bytes written to the Data
    // component for that row/cell/partition, which it can do because it encodes
    // one row at a time into a byte stream. A Parquet row has no byte extent:
    // its values are scattered across column chunks, interleaved with every
    // other row's, then dictionary-encoded and compressed as a chunk. So what is
    // reported here is the serialised size of the *content* -- the cell's
    // in-memory serialised form (flags, timestamp, ttl/expiry or deletion time,
    // value bytes), summed up the row and the partition. Encoding-independent,
    // and arguably the more useful number for the diagnostic: a 10 MB blob is a
    // 10 MB blob whether or not the codec squeezed it.
    //
    // Two further deliberate deviations, both consequences of the columnar
    // layout rather than approximations:
    //
    //  - Static cells are charged to the *partition*, once, and not to any row.
    //    The shredder replays static cells onto every storage row of the
    //    partition (see fragment_shredder::replay_statics), so charging them per
    //    row would multiply one static blob by the row count and report a
    //    partition of small rows as a partition of large rows. They are still
    //    reported as cells, once, exactly as mx does from its single static row.
    //
    //  - rows_in_partition counts *storage* rows: clustering rows, range
    //    tombstone change markers, and the placeholder row a static-only
    //    partition gets. There is no separate static row in a Parquet file, so
    //    a partition that mx reports as N+1 rows (it writes an empty static row
    //    whenever the schema declares static columns) is reported here as N.
    //
    // Everything below mirrors sstables/mx/writer.cc: the same five
    // large_data_stats_entry aggregates and the same five bounded top-N heaps.
    //
    // The accounting lives in the fragment consumers, upstream of both write
    // paths -- cut_row_group() and to_parquet_for_storage() -- so it cannot
    // diverge between them: a row is counted when it is consumed, whatever the
    // encoder later does with it.
    std::optional<sstables::key> _partition_key;
    // Logical bytes attributable to the partition being consumed: its key, its
    // tombstone, its static cells (once), and the sum of its rows' sizes.
    uint64_t _partition_logical_size = 0;

    large_data_stats_entry _partition_size_entry;
    large_data_stats_entry _rows_in_partition_entry;
    large_data_stats_entry _row_size_entry;
    large_data_stats_entry _cell_size_entry;
    large_data_stats_entry _elements_in_collection_entry;

    // Bounded min-heaps for top-N large data records, one per large_data_type.
    // Size-type heaps (partition_size, row_size, cell_size) compare by `value`;
    // element-count-type heaps (rows_in_partition, elements_in_collection)
    // compare by `elements_count`.
    struct large_data_record_cmp_by_value {
        bool operator()(const large_data_record& a, const large_data_record& b) const {
            return a.value > b.value; // min-heap: smallest value on top
        }
    };
    struct large_data_record_cmp_by_elements {
        bool operator()(const large_data_record& a, const large_data_record& b) const {
            return a.elements_count > b.elements_count; // min-heap: smallest elements_count on top
        }
    };
    using ld_size_heap = std::priority_queue<large_data_record, std::vector<large_data_record>, large_data_record_cmp_by_value>;
    using ld_elements_heap = std::priority_queue<large_data_record, std::vector<large_data_record>, large_data_record_cmp_by_elements>;
    ld_size_heap _ld_partition_size_records;
    ld_elements_heap _ld_rows_in_partition_records;
    ld_size_heap _ld_row_size_records;
    ld_size_heap _ld_cell_size_records;
    ld_elements_heap _ld_elements_in_collection_records;

    // Insert a record into a bounded min-heap, keeping at most N entries.
    // comp(rec, top) is true exactly when rec outranks the weakest kept entry.
    template <typename Heap>
    void insert_into_ld_heap(Heap& heap, large_data_record rec) {
        auto max_records = _cfg.large_data_records_per_sstable;
        if (max_records == 0) {
            return;
        }
        if (heap.size() < max_records) {
            heap.push(std::move(rec));
        } else if (typename Heap::value_compare{}(rec, heap.top())) {
            heap.pop();
            heap.push(std::move(rec));
        }
    }

    void maybe_record_large_partitions(uint64_t partition_size, uint64_t rows,
            uint64_t range_tombstones, uint64_t dead_rows);
    void maybe_record_large_rows(const clustering_key_prefix* clustering_key, uint64_t row_size);
    void maybe_record_large_cells(const clustering_key_prefix* clustering_key,
            const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements);

    // The three components of a logical size. Kept separate because the row and
    // the partition need different subsets -- see the note above on statics.
    static uint64_t cell_logical_size(const atomic_cell_view& cell);
    static uint64_t collection_logical_size(collection_mutation_view cmv);
    static uint64_t row_metadata_logical_size(const clustering_row& cr);

    void collect_atomic_cell(const atomic_cell_view& cell);
    // Returns the cell's logical size, and reports it to the large-data handler
    // under `clustering_key` (null for a static cell).
    uint64_t collect_cell(const column_definition& cdef, const atomic_cell_or_collection& acoc,
            const clustering_key_prefix* clustering_key);
    uint64_t collect_cells(const ::row& cells, ::column_kind kind,
            const clustering_key_prefix* clustering_key);
    void collect_marker(const row_marker& marker);

    // Streaming row groups. Both stay empty for an sstable that fits inside the
    // budget, which then takes the single-shot path in consume_end_of_stream() and is
    // byte-for-byte what it was before -- so every size measured in design doc 10 is
    // unaffected.
    //
    // Once a cut happens the leaf set has to be fixed, and it cannot be derived from
    // rows not yet seen, so it becomes the conservative one (design doc 5.5a).
    std::optional<mapped_schema> _ms;
    std::unique_ptr<format::parquet_file_writer> _pq;
    // Rows already flushed into earlier row groups. The index entry is a file-global
    // row ordinal (option A), so it cannot come from the shredder's own size once the
    // shredder is being cleared at each cut.
    uint64_t _rows_flushed = 0;

    void cut_row_group();
    void finish_open_partition();
    void write_components();

public:
    pq_writer_impl(sstables::sstable& sst, const ::schema& s,
                   uint64_t estimated_partitions,
                   const sstables::sstable_writer_config& cfg,
                   pq_writer_config pcfg, encoding_stats enc_stats,
                   shard_id shard, sink_type sink);

    void consume_new_partition(const dht::decorated_key& dk) override;
    void consume(tombstone t) override;
    stop_iteration consume(static_row&& sr) override;
    stop_iteration consume(clustering_row&& cr) override;
    stop_iteration consume(range_tombstone_change&& rtc) override;
    stop_iteration consume_end_of_partition() override;
    void consume_end_of_stream() override;
    uint64_t data_file_position_for_tests() const override { return _pos; }
};

// Factory used by sstable_writer when the sstable's version is `pq`. Mirrors
// mc::make_writer so the dispatch in writer.cc stays a one-line choice.
//
// The pq_writer_config is defaulted here (L1, sparse exceptions). Deriving it
// from the table's own storage-format properties -- rows per row group and so
// on, per design doc section 6 -- is the next step; nothing about that changes
// this signature.
std::unique_ptr<sstables::sstable_writer::writer_impl> make_writer(
        sstables::sstable& sst,
        const ::schema& s,
        uint64_t estimated_partitions,
        const sstables::sstable_writer_config& cfg,
        encoding_stats enc_stats,
        shard_id shard);

} // namespace sstables::parquet
