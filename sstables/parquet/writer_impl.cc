/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/encryption_keys.hh"

#include "exceptions/exceptions.hh"

#include "mutation/mutation_fragment.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "sstables/sstables.hh"
#include "sstables/storage.hh"

#include <seastar/core/fstream.hh>

#include "sstables/writer.hh"
#include "sstables/storage.hh"
#include "keys/keys.hh"
#include "dht/i_partitioner.hh"
#include "sstables/mx/writer.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "types/collection.hh"
#include "db/large_data_handler.hh"
#include "utils/chunked_vector.hh"

#include <cstdlib>
#include <cstring>

namespace sstables::parquet {

// Nothing in this file logged before; the teardown path below needs to be able to
// report a failed close without throwing out of a destructor.
static seastar::logger pqwlog("pq_writer");


namespace {

// Scylla serialises fixed-width scalars big-endian, so the physical mapping can
// read them straight out of the cell without going through deserialize().
// Anything not handled here keeps its serialised form and travels as an opaque
// BYTE_ARRAY, which is lossless but gives up type-specific encoding.
} // namespace

cql_type cql_type_of(const abstract_type& t) {
    if (&t == int32_type.get())                            { return cql_type::int32; }
    if (&t == long_type.get())                             { return cql_type::bigint; }
    if (&t == timestamp_type.get())                        { return cql_type::timestamp; }
    if (&t == double_type.get())                           { return cql_type::dbl; }
    if (&t == utf8_type.get() || &t == ascii_type.get())   { return cql_type::text; }
    return cql_type::blob;
}

namespace {

int64_t be64(const bytes_view& b) {
    uint64_t v = 0;
    const size_t n = std::min<size_t>(8, b.size());
    for (size_t i = 0; i < n; ++i) { v = (v << 8) | uint8_t(b[i]); }
    return int64_t(v);
}
int32_t be32(const bytes_view& b) {
    uint32_t v = 0;
    const size_t n = std::min<size_t>(4, b.size());
    for (size_t i = 0; i < n; ++i) { v = (v << 8) | uint8_t(b[i]); }
    return int32_t(v);
}

// A non-frozen collection as the mapping sees it: the collection-level tombstone
// plus one entry per (key, cell) pair. Keys and values stay serialised -- the
// mapping treats both as opaque bytes, which is what makes one code path work for
// sets, lists, maps and non-frozen UDTs alike.
// A counter cell, in the collection representation: one element per shard, keyed
// by the shard's id, with its value and logical clock packed into the element
// value. Counters are atomic cells, so without this they would be stored as an
// opaque blob and lose the shard structure that makes them mergeable.
//
// The cell's own timestamp is repeated on every element -- it is the same for all
// of them, so it costs nothing once compressed -- and a dead counter cell becomes
// an element-less collection carrying the deletion in the collection tombstone
// slot, which is how absent is told apart from deleted on the way back.
static collection_cell read_counter_cell(const atomic_cell_view& av) {
    collection_cell cc;
    if (!av.is_live()) {
        cc.tomb = deletion_info{av.timestamp(),
                int32_t(av.deletion_time().time_since_epoch().count())};
        return cc;
    }
    counter_cell_view ccv(av);
    for (auto&& cs : ccv.shards()) {
        const auto u = cs.id().uuid();
        collection_element e;
        e.key = pack_i64_pair(u.get_most_significant_bits(), u.get_least_significant_bits());
        e.value = pack_i64_pair(cs.value(), cs.logical_clock());
        e.timestamp = av.timestamp();
        cc.elements.push_back(std::move(e));
    }
    return cc;
}

collection_cell read_collection_mutation(const atomic_cell_or_collection& acoc) {
    auto cmv = acoc.as_collection_mutation();
    collection_cell cc;
    if (cmv.tomb()) {
        cc.tomb = deletion_info{cmv.tomb().timestamp,
                int32_t(cmv.tomb().deletion_time.time_since_epoch().count())};
    }
    for (auto&& kv : cmv) {
        auto kb = linearized(kv.first);
        collection_element e;
        e.key.assign(reinterpret_cast<const char*>(kb.data()), kb.size());
        e.timestamp = kv.second.timestamp();
        if (kv.second.is_live()) {
            auto lv = kv.second.value().linearize();
            e.value = std::string(reinterpret_cast<const char*>(lv.data()), lv.size());
            if (kv.second.is_live_and_has_ttl()) {
                e.ttl = int32_t(kv.second.ttl().count());
                e.local_deletion_time = int32_t(kv.second.expiry().time_since_epoch().count());
            }
        } else {
            e.local_deletion_time =
                    int32_t(kv.second.deletion_time().time_since_epoch().count());
        }
        cc.elements.push_back(std::move(e));
    }
    return cc;
}

value decode(cql_type t, bytes_view b) {
    switch (t) {
    case cql_type::int32:     return be32(b);
    case cql_type::bigint:
    case cql_type::timestamp: return be64(b);
    case cql_type::dbl: {
        uint64_t bits = uint64_t(be64(b));
        double d;
        std::memcpy(&d, &bits, 8);
        return d;
    }
    default:
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }
}

// Key components arrive already serialised, one per key column.
void explode_key(const std::vector<bytes>& parts,
                 const std::vector<cql_type>& types,
                 std::vector<value>& out) {
    for (size_t i = 0; i < types.size(); ++i) {
        out.push_back(i < parts.size() ? decode(types[i], bytes_view(parts[i]))
                                       : decode(types[i], bytes_view()));
    }
}

// Anything the shredder cannot represent must stop the write. Dropping a
// fragment here does not corrupt the file -- it produces a *valid* Parquet file
// that is quietly missing data, which is the worse failure: a dropped partition
// tombstone resurrects deleted rows. Refusing is recoverable; silence is not.
[[noreturn]] void unrepresentable(const char* what) {
    throw std::runtime_error(
            std::string("pq: cannot represent ") + what + " yet; this sstable would "
            "silently lose data. See docs/dev/parquet-storage-format.md section 11.");
}

} // namespace

std::vector<cql_column> columns_of(const ::schema& s) {
    std::vector<cql_column> cols;
    for (const auto& c : s.partition_key_columns()) {
        cols.push_back({c.name_as_text(), cql_type_of(*c.type), column_kind::partition_key});
    }
    for (const auto& c : s.clustering_key_columns()) {
        cols.push_back({c.name_as_text(), cql_type_of(*c.type), column_kind::clustering_key});
    }
    for (const auto& c : s.regular_columns()) {
        cql_column col{c.name_as_text(), cql_type_of(*c.type), column_kind::regular};
        col.multi_cell = !c.is_atomic() || c.is_counter();
        col.counter = c.is_counter();
        cols.push_back(std::move(col));
    }
    // Static columns ride along as ordinary value columns, appended after the
    // regular ones. That gets them the whole cell machinery -- timestamps, TTLs,
    // the divergence channel -- for free, and costs nothing on disk because a
    // static value is constant within its partition and so compresses away.
    // The reader splits them back out by index; see static_base().
    for (const auto& c : s.static_columns()) {
        cql_column col{"__s_" + c.name_as_text(), cql_type_of(*c.type), column_kind::regular};
        col.multi_cell = !c.is_atomic() || c.is_counter();
        cols.push_back(std::move(col));
    }
    return cols;
}

// ------------------------------------------------------- the `parquet = {...}` property
namespace {

size_t parse_count(const sstring& key, const sstring& v, size_t lo, size_t hi) {
    size_t out = 0;
    try {
        size_t pos = 0;
        unsigned long long n = std::stoull(std::string(v), &pos);
        if (pos != v.size()) { throw std::invalid_argument("trailing"); }
        out = size_t(n);
    } catch (...) {
        throw exceptions::configuration_exception(
                seastar::format("Invalid value '{}' for '{}' in the 'parquet' option; expected a "
                       "whole number", v, key));
    }
    if (out < lo || out > hi) {
        throw exceptions::configuration_exception(
                seastar::format("'{}' must be between {} and {} (got {})", key, lo, hi, out));
    }
    return out;
}

// Accepts a plain byte count or a KiB/MiB/GiB suffix, as design doc 8.2 shows.
size_t parse_bytes(const sstring& key, const sstring& v, size_t lo, size_t hi) {
    static const std::pair<const char*, size_t> units[] = {
        {"GiB", 1024ull * 1024 * 1024}, {"MiB", 1024 * 1024}, {"KiB", 1024},
    };
    for (auto [suffix, mult] : units) {
        const size_t sl = std::strlen(suffix);
        if (v.size() > sl && v.compare(v.size() - sl, sl, suffix) == 0) {
            auto head = v.substr(0, v.size() - sl);
            return parse_count(key, head, (lo + mult - 1) / mult, hi / mult) * mult;
        }
    }
    return parse_count(key, v, lo, hi);
}

} // namespace

namespace {

// The CQL enum to Parquet's. `dictionary` becomes RLE_DICTIONARY, the only dictionary encoding this
// writer emits -- PLAIN_DICTIONARY is the deprecated v1 spelling and nothing here produces it.
format::encoding to_format_encoding(parquet_parameters::column_encoding e) {
    using ce = parquet_parameters::column_encoding;
    switch (e) {
    case ce::plain:                   return format::encoding::plain;
    case ce::dictionary:              return format::encoding::rle_dictionary;
    case ce::delta_binary_packed:     return format::encoding::delta_binary_packed;
    case ce::delta_byte_array:        return format::encoding::delta_byte_array;
    case ce::delta_length_byte_array: return format::encoding::delta_length_byte_array;
    case ce::byte_stream_split:       return format::encoding::byte_stream_split;
    case ce::automatic:               break;
    }
    return format::encoding::plain;   // unreachable: 'auto' is filtered before this is called
}

} // namespace

std::optional<parquet_parameters::column_encoding>
parquet_parameters::parse_column_encoding(std::string_view v) {
    using ce = column_encoding;
    // 'dictionary' rather than 'rle_dictionary': the Parquet name is an implementation detail of the
    // index stream, and 'plain_dictionary' is the deprecated v1 spelling. One name for the concept.
    if (v == "auto")                    { return ce::automatic; }
    if (v == "plain")                   { return ce::plain; }
    if (v == "dictionary")              { return ce::dictionary; }
    if (v == "delta_binary_packed")     { return ce::delta_binary_packed; }
    if (v == "delta_byte_array")        { return ce::delta_byte_array; }
    if (v == "delta_length_byte_array") { return ce::delta_length_byte_array; }
    if (v == "byte_stream_split")       { return ce::byte_stream_split; }
    return std::nullopt;
}

const char* parquet_parameters::to_string(column_encoding e) {
    using ce = column_encoding;
    switch (e) {
    case ce::automatic:               return "auto";
    case ce::plain:                   return "plain";
    case ce::dictionary:              return "dictionary";
    case ce::delta_binary_packed:     return "delta_binary_packed";
    case ce::delta_byte_array:        return "delta_byte_array";
    case ce::delta_length_byte_array: return "delta_length_byte_array";
    case ce::byte_stream_split:       return "byte_stream_split";
    }
    return "auto";
}

bool parquet_parameters::applies_to(column_encoding e, cql_type t) {
    using ce = column_encoding;
    // PLAIN, a dictionary and 'auto' are legal for every physical type. The rest are type-specific,
    // and rejecting the mismatch at DDL time is the whole point: an encoding the writer would have to
    // ignore is a setting that lies, and one it would have to fail on is an outage.
    switch (e) {
    case ce::automatic:
    case ce::plain:
    case ce::dictionary:
        return true;
    case ce::delta_binary_packed:
        // Integer deltas. Timestamps are int64 underneath, which is what makes them the best case.
        return t == cql_type::int32 || t == cql_type::bigint || t == cql_type::timestamp;
    case ce::delta_byte_array:
    case ce::delta_length_byte_array:
        return t == cql_type::text || t == cql_type::blob;
    case ce::byte_stream_split:
        // Measured to *cost* 55 % on real doubles (design doc 10.3f). Accepted because a column
        // whose values do not repeat can still benefit, and refusing a legal Parquet encoding on the
        // strength of one corpus would be overreach -- but it is never chosen automatically.
        return t == cql_type::dbl;
    }
    return false;
}

parquet_parameters::parquet_parameters(const std::map<sstring, sstring>& opts) {
    // Both spellings of the same setting in one map is a mistake worth naming. Letting map
    // order decide would make the outcome depend on the fact that "row_group_rows" happens
    // to sort before "rows_per_row_group" -- an operator who wrote both almost certainly
    // meant the second to replace the first, and would get the first.
    if (opts.contains(ROWS_PER_ROW_GROUP) && opts.contains(ROW_GROUP_ROWS_LEGACY)) {
        throw exceptions::configuration_exception(seastar::format(
                "The 'parquet' option sets both '{}' and '{}', which are two names for the same "
                "setting ('{}' is the old name, still accepted). Set only '{}'.",
                ROWS_PER_ROW_GROUP, ROW_GROUP_ROWS_LEGACY, ROW_GROUP_ROWS_LEGACY,
                ROWS_PER_ROW_GROUP));
    }
    for (const auto& [k, v] : opts) {
        // `row_group_rows` is the pre-rename spelling and stays accepted permanently: it is
        // persisted in schema, so refusing it would stop existing tables loading. `k` is passed
        // to parse_count so a bad value is reported against the name the operator actually wrote.
        if (k == ROWS_PER_ROW_GROUP || k == ROW_GROUP_ROWS_LEGACY) {
            _cfg.rows_per_row_group =
                    parse_count(k, v, min_rows_per_row_group, max_rows_per_row_group);
        } else if (k == ROW_GROUP_BUFFER_BYTES) {
            _cfg.row_group_buffer_bytes = parse_bytes(k, v, min_buffer_bytes, max_buffer_bytes);
        } else if (k == PAGE_ROWS) {
            // A page is the unit a point read decodes, so the same reasoning as row
            // groups applies one level down.
            _cfg.wopt.page_values = parse_count(k, v, 128, 1'000'000);
        } else if (k == COMPRESSION) {
            // Only what the writer can actually emit. See the note on the class.
            if (v == "zstd") {
                _cfg.wopt.compression = format::codec::zstd;
            } else if (v == "lz4") {
                // LZ4_RAW on the wire (codec 7), which is what every current Parquet
                // implementation means by LZ4. Spelled "lz4" here because that is what the rest
                // of Scylla calls this codec, and an operator comparing a pq table against the
                // row format's LZ4WithDictsCompressor should not have to know the difference.
                _cfg.wopt.compression = format::codec::lz4_raw;
            } else if (v == "none") {
                _cfg.wopt.compression = format::codec::uncompressed;
            } else {
                throw exceptions::configuration_exception(
                        seastar::format("Unsupported 'compression' value '{}' in the 'parquet' "
                               "option; supported: none, lz4, zstd", v));
            }
        } else if (k == COMPRESSION_LEVEL) {
            _cfg.wopt.zstd_level = int(parse_count(k, v, 1, 22));
        } else if (k == DICTIONARY) {
            // Which columns may be dictionary-encoded. 'text' is the default: strings
            // benefit and numerics cost more point-read latency than they save disk
            // (+10.5% for -3.9%, see writer_options::numeric_dictionary). 'all' is for a
            // bottom tier that is scanned rather than point-read.
            if (v == "text") {
                _cfg.wopt.use_dictionary = true;
                _cfg.wopt.numeric_dictionary = false;
            } else if (v == "all") {
                _cfg.wopt.use_dictionary = true;
                _cfg.wopt.numeric_dictionary = true;
            } else if (v == "none") {
                _cfg.wopt.use_dictionary = false;
                _cfg.wopt.numeric_dictionary = false;
            } else {
                throw exceptions::configuration_exception(
                        seastar::format("Unsupported 'dictionary' value '{}' in the 'parquet' "
                                        "option; supported: text, all, none", v));
            }
        } else if (k == METADATA_FOLDING) {
            if (v == "auto" || v == "row") {
                _cfg.level = folding_level::row_folded;
            } else if (v == "verbatim") {
                _cfg.level = folding_level::verbatim;
            } else if (v == "uniform") {
                _cfg.level = folding_level::uniform;
            } else {
                // 'logical' (L3) is export-only: it discards write times and TTLs, so it
                // must never be reachable as a storage setting.
                throw exceptions::configuration_exception(
                        seastar::format("Unsupported 'metadata_folding' value '{}' in the 'parquet' "
                               "option; supported: auto, verbatim, row, uniform", v));
            }
        } else if (k == ENCRYPTION) {
            if (v == "none") {
                _cfg.encryption_enabled = false;
            } else if (v == "aes_gcm_v1") {
                _cfg.encryption_enabled = true;
                _cfg.encryption_algo = format::cipher::aes_gcm_v1;
            } else if (v == "aes_gcm_ctr_v1") {
                _cfg.encryption_enabled = true;
                // Page *bodies* are AES-CTR and carry no authentication tag, so tampering with
                // one is not detected by the format. It exists because it is measurably faster
                // and because other writers produce it, not because it is a good default.
                _cfg.encryption_algo = format::cipher::aes_gcm_ctr_v1;
            } else {
                throw exceptions::configuration_exception(seastar::format(
                        "Unsupported 'encryption' value '{}' in the 'parquet' option; supported: "
                        "none, aes_gcm_v1, aes_gcm_ctr_v1", v));
            }
        } else if (key_option_names().contains(k)) {
            // Forwarded to ent/encryption untouched. Validated below, once the whole map is in
            // hand, because the checks are about combinations rather than single values.
            _cfg.key_opts[k] = v;
        } else if (k == ENCRYPTION_KEY_METADATA) {
            auto f = parse_key_metadata_format(v);
            if (!f) {
                throw exceptions::configuration_exception(seastar::format(
                        "Unsupported 'encryption_key_metadata' value '{}' in the 'parquet' option; "
                        "supported: provider, parquet_kms", v));
            }
            _cfg.encryption_key_metadata = *f;
        } else if (k.size() > std::strlen(ENCRYPTION_KEY_PREFIX)
                   && k.compare(0, std::strlen(ENCRYPTION_KEY_PREFIX), ENCRYPTION_KEY_PREFIX) == 0) {
            // Stored raw and resolved in validate_key_options(), which is the first point that has
            // the whole map: the overlay needs the table's own key options, and CQL map order is
            // not the operator's, so 'encryption_key.ssn' may well be parsed before
            // 'secret_key_file'.
            const sstring col = k.substr(std::strlen(ENCRYPTION_KEY_PREFIX));
            if (col.empty()) {
                throw exceptions::configuration_exception(
                        "The 'encryption_key.' sub-option needs a column name, e.g. "
                        "'encryption_key.my_col'");
            }
            _column_key_raw[col] = v;
        } else if (k.size() > std::strlen(ENCODING_PREFIX)
                   && k.compare(0, std::strlen(ENCODING_PREFIX), ENCODING_PREFIX) == 0) {
            const sstring col = k.substr(std::strlen(ENCODING_PREFIX));
            if (col.empty()) {
                throw exceptions::configuration_exception(
                        "The 'encoding.' sub-option needs a column name, e.g. 'encoding.my_col'");
            }
            auto enc = parse_column_encoding(v);
            if (!enc) {
                throw exceptions::configuration_exception(seastar::format(
                        "Unsupported '{}' value '{}' in the 'parquet' option; supported: "
                        "auto, plain, dictionary, delta_binary_packed, delta_byte_array, "
                        "delta_length_byte_array, byte_stream_split", k, v));
            }
            _column_encodings.emplace(col, *enc);
            // Translated once, here, so the writer and the mapping only ever deal in Parquet's own
            // enum. `auto` records the user's intent for DESCRIBE but contributes no hint, which is
            // what makes 'auto' the way to cancel an override in an ALTER.
            if (*enc != column_encoding::automatic) {
                _cfg.column_encodings[std::string(col)] = to_format_encoding(*enc);
            }
        } else {
            throw exceptions::configuration_exception(
                    seastar::format("Unknown sub-option '{}' for the 'parquet' option; supported: "
                                    "rows_per_row_group, row_group_buffer_bytes, page_rows, compression, "
                                    "compression_level, metadata_folding, dictionary, encryption, "
                                    "encryption_key_metadata, 'encoding.<column>', "
                                    "'encryption_key.<column>', and the key "
                                    "provider options ({})",
                                    k, fmt::join(key_option_names(), ", ")));
        }
    }
    validate_key_options();
}

const std::set<sstring>& parquet_parameters::key_option_names() {
    // ent/encryption's own vocabulary, spelled exactly as `scylla_encryption_options` spells it,
    // so an operator who has configured encryption at rest already knows this list. The names are
    // duplicated rather than pulled from ent/encryption/encryption.hh deliberately: sstables must
    // not depend on scylla_encryption, which depends on it (see encryption_keys.hh).
    static const std::set<sstring> names{
        // generic
        "key_provider", "secret_key_provider_factory_class",
        "cipher_algorithm", "secret_key_strength",
        // local file / replicated
        "secret_key_file", "system_key_file",
        // KMIP
        "kmip_host", "template_name", "key_namespace",
        // AWS KMS
        "kms_host", "aws_assume_role_arn",
        // GCP
        "gcp_host", "gcp_credentials_file", "gcp_impersonate_service_account",
        "gcp_iam_endpoint_override",
        // Azure
        "azure_host",
        // shared by the cloud providers
        "master_key",
    };
    return names;
}

std::optional<sstring> parquet_parameters::keyable_column_error(const sstring& column,
                                                                bool multi_cell) {
    // See the long note on the declaration for why each of these is a hard restriction of the
    // format layer's leaf-name key lookup rather than caution.
    if (multi_cell) {
        return seastar::format(
                "column '{}' is a non-frozen collection, which Parquet cannot give its own "
                "encryption key: the column becomes a group of leaves named 'key', 'value', "
                "'__ts', '__ttl' and '__ldt', so there is no leaf bearing the column's name for a "
                "key to attach to. Freeze the column, or encrypt the whole table by setting "
                "'encryption' without 'encryption_key.{}'", column, column);
    }
    // The names the mapping uses for collection and metadata leaves. A scalar column with one of
    // these names shares its leaf name with those leaves, and a key looked up by leaf name would
    // cover both.
    static const std::set<sstring> reserved{"key", "value", "clock"};
    if (reserved.contains(column)) {
        return seastar::format(
                "column '{}' cannot have its own encryption key: '{}' is also the name Parquet "
                "gives a leaf inside every non-frozen collection, and a key is looked up by leaf "
                "name -- so the key would silently cover those leaves too, including in a "
                "collection added by a later ALTER TABLE. Rename the column, or encrypt the whole "
                "table by setting 'encryption' without 'encryption_key.{}'", column, column,
                column);
    }
    if (column.size() >= 2 && column[0] == '_' && column[1] == '_') {
        return seastar::format(
                "column '{}' cannot have its own encryption key: names beginning '__' are what "
                "Parquet's mapping calls its synthetic timestamp, TTL and deletion leaves, so a "
                "key looked up by leaf name could not be told apart from one of those",
                column);
    }
    return std::nullopt;
}

// Split 'a=b,c=d' into provider options. Comma between pairs and the *first* '=' within a pair:
// a provider option value may contain '=' (base64 padding) and ':' (a KMS ARN) but not ',', which
// no provider's key locator uses.
static std::map<sstring, sstring> parse_key_option_overrides(const sstring& column,
                                                             const sstring& spec) {
    std::map<sstring, sstring> out;
    size_t i = 0;
    while (i <= spec.size()) {
        const auto comma = spec.find(',', i);
        const auto end = comma == sstring::npos ? spec.size() : comma;
        std::string_view pair(spec.data() + i, end - i);
        // Trim, so 'a=b, c=d' works like every other comma list an operator has ever written.
        while (!pair.empty() && std::isspace(static_cast<unsigned char>(pair.front()))) {
            pair.remove_prefix(1);
        }
        while (!pair.empty() && std::isspace(static_cast<unsigned char>(pair.back()))) {
            pair.remove_suffix(1);
        }
        if (pair.empty()) {
            throw exceptions::configuration_exception(seastar::format(
                    "The 'parquet' option's 'encryption_key.{}' has an empty entry; it takes key "
                    "provider options, e.g. 'secret_key_file=/etc/scylla/keys/pii.key'", column));
        }
        const auto eq = pair.find('=');
        if (eq == std::string_view::npos || eq == 0) {
            throw exceptions::configuration_exception(seastar::format(
                    "The 'parquet' option's 'encryption_key.{}' entry '{}' is not "
                    "'<option>=<value>'; it takes key provider options, e.g. "
                    "'secret_key_file=/etc/scylla/keys/pii.key'", column, pair));
        }
        const sstring name(pair.substr(0, eq));
        const sstring value(pair.substr(eq + 1));
        if (!parquet_parameters::key_option_names().contains(name)) {
            // The same reasoning as the closed sub-option list: a provider ignores options it does
            // not know, so a typo'd 'secret_key_fil' here would fall back to the *table's* key and
            // leave the column looking separately encrypted when it is not.
            throw exceptions::configuration_exception(seastar::format(
                    "The 'parquet' option's 'encryption_key.{}' names '{}', which is not a key "
                    "provider option; supported: {}", column, name,
                    fmt::join(parquet_parameters::key_option_names(), ", ")));
        }
        if (value.empty()) {
            throw exceptions::configuration_exception(seastar::format(
                    "The 'parquet' option's 'encryption_key.{}' sets '{}' to the empty string",
                    column, name));
        }
        out[name] = value;
        if (comma == sstring::npos) { break; }
        i = comma + 1;
    }
    if (out.empty()) {
        throw exceptions::configuration_exception(seastar::format(
                "The 'parquet' option's 'encryption_key.{}' is empty; it takes key provider "
                "options, e.g. 'secret_key_file=/etc/scylla/keys/pii.key'", column));
    }
    return out;
}

void parquet_parameters::validate_key_options() {
    if (!_cfg.encryption_enabled) {
        // Provider options without `encryption` do nothing, and an inert security setting is
        // worse than a rejected one: it reads, in DESCRIBE and in a review, as if the table were
        // encrypted.
        if (!_cfg.key_opts.empty()) {
            throw exceptions::configuration_exception(seastar::format(
                    "The 'parquet' option sets key provider option '{}' but no 'encryption'; "
                    "the key options only apply when encryption is on",
                    _cfg.key_opts.begin()->first));
        }
        // Worse than inert in this case: `encryption_key.<column>` reads as "this column is
        // encrypted under its own key", which without `encryption` is the opposite of the truth.
        if (!_column_key_raw.empty()) {
            throw exceptions::configuration_exception(seastar::format(
                    "The 'parquet' option sets 'encryption_key.{}' but no 'encryption'; a "
                    "per-column key only applies when encryption is on, and nothing is encrypted "
                    "here", _column_key_raw.begin()->first));
        }
        return;
    }

    validate_one_key_option_set(_cfg.key_opts, "the 'parquet' option");

    // Each per-column set starts from the table's own options -- already defaulted and validated
    // above -- and applies the operator's overrides on top. So an override naming only the one
    // option that locates a key inherits the provider, the algorithm and the key length, which is
    // what makes the one-pair form the normal one.
    for (const auto& [col, spec] : _column_key_raw) {
        if (auto why = keyable_column_error(col, /*multi_cell=*/false)) {
            // The collection case cannot be seen from here -- parquet_parameters is constructed
            // wherever the schema is read and has no columns -- so it is checked in
            // cf_prop_defs::apply_to_builder. The name-based cases are checkable here, and are
            // checked here so they also hold for a schema arriving from another node.
            throw exceptions::configuration_exception(seastar::format(
                    "The 'parquet' option asks for a per-column encryption key, but {}", *why));
        }
        key_options ko = _cfg.key_opts;
        for (const auto& [name, value] : parse_key_option_overrides(col, spec)) {
            ko[name] = value;
        }
        validate_one_key_option_set(
                ko, seastar::format("the 'parquet' option's 'encryption_key.{}'", col));
        _cfg.column_key_opts[col] = std::move(ko);
    }
}

void parquet_parameters::validate_one_key_option_set(key_options& opts, std::string_view what) {
    // Parquet Modular Encryption permits AES-GCM and AES-GCM-CTR and nothing else, and even
    // AES_GCM_CTR_V1 uses GCM for every metadata module -- so the key is always an AES-GCM key.
    // ent/encryption's default is `AES/CBC/PKCS5Padding`, which cannot be honoured at all.
    //
    // Two rules follow, and they are different on purpose. An *absent* cipher_algorithm is
    // defaulted here to the only thing the format can use -- overriding a default is not
    // overriding a request. An *explicit* one that names another mode is refused outright,
    // because quietly giving a table AES-GCM when its DDL says CBC would be the worst kind of
    // help.
    //
    // The default is applied on every parse rather than persisted into the schema, which echoes
    // what the operator wrote (see schema.cc's parquet_options). So the writer and the reader
    // derive the same value from the same stored map, and DESCRIBE stays as terse as it is for
    // every other defaulted option. Ordinarily a default that is not written down is a hazard --
    // change it later and old files stop opening -- but not here: AES-GCM is the only thing the
    // format permits, so there is no other value for it to become.
    static constexpr const char* CIPHER = "cipher_algorithm";
    static constexpr const char* required_cipher = "AES/GCM/NoPadding";
    if (auto it = opts.find(CIPHER); it == opts.end()) {
        opts[CIPHER] = required_cipher;
    } else {
        // JCE spelling: transformation/mode/padding, case-insensitive, mode and padding optional
        // (and an absent mode means CBC to both OpenSSL and ent/encryption -- so it is refused
        // rather than treated as unspecified).
        std::string spec(it->second);
        std::transform(spec.begin(), spec.end(), spec.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        const auto first = spec.find('/');
        const std::string alg  = spec.substr(0, first);
        const std::string mode = first == std::string::npos
                               ? std::string()
                               : spec.substr(first + 1, spec.find('/', first + 1) - first - 1);
        if (alg != "aes" || mode != "gcm") {
            throw exceptions::configuration_exception(seastar::format(
                    "{} asks for cipher_algorithm '{}', which Parquet Modular "
                    "Encryption cannot honour: the format permits only AES-GCM (and AES-GCM-CTR, "
                    "whose metadata is still GCM). Use '{}', or encrypt the whole component with "
                    "scylla_encryption_options instead -- at the cost of a file no external "
                    "reader can open", what, it->second, required_cipher));
        }
    }

    // 128/192/256 -> 16/24/32 bytes, which is what format::encryption_key accepts. Anything else
    // would be a table that takes its DDL and then fails every flush.
    static constexpr const char* STRENGTH = "secret_key_strength";
    if (auto it = opts.find(STRENGTH); it != opts.end()) {
        if (it->second != "128" && it->second != "192" && it->second != "256") {
            throw exceptions::configuration_exception(seastar::format(
                    "{} asks for secret_key_strength '{}'; AES keys are 128, "
                    "192 or 256 bits", what, it->second));
        }
    }
}

std::map<sstring, sstring> parquet_parameters::to_map() const {
    const pq_writer_config def;
    std::map<sstring, sstring> m;
    // Always the canonical name, never the alias the operator may have written. Two reasons.
    // First, this is a serializer keyed off the parsed config, not off the input text: it holds
    // no record of which spelling arrived, and adding one would be state whose only purpose is
    // to reproduce a name we are trying to retire. Second, the usual objection -- "emitting the
    // new name rewrites the user's stored property" -- does not apply here, because to_map() is
    // not on the persistence path. cf_prop_defs stores the raw map the operator wrote and
    // schema.cc's DESCRIBE echoes that map verbatim (see the comment there), so an ALTER using
    // the old name keeps the old name in the schema. What to_map() must guarantee is that its
    // output parses back to an equal config, and the canonical name does.
    if (_cfg.rows_per_row_group != def.rows_per_row_group) {
        m[ROWS_PER_ROW_GROUP] = seastar::format("{}", _cfg.rows_per_row_group);
    }
    if (_cfg.row_group_buffer_bytes != def.row_group_buffer_bytes) {
        m[ROW_GROUP_BUFFER_BYTES] = seastar::format("{}", _cfg.row_group_buffer_bytes);
    }
    if (_cfg.wopt.page_values != def.wopt.page_values) {
        m[PAGE_ROWS] = seastar::format("{}", _cfg.wopt.page_values);
    }
    if (_cfg.wopt.compression != def.wopt.compression) {
        m[COMPRESSION] = _cfg.wopt.compression == format::codec::zstd ? "zstd"
                       : _cfg.wopt.compression == format::codec::lz4_raw ? "lz4" : "none";
    }
    if (_cfg.wopt.zstd_level != def.wopt.zstd_level) {
        m[COMPRESSION_LEVEL] = seastar::format("{}", _cfg.wopt.zstd_level);
    }
    if (_cfg.wopt.use_dictionary != def.wopt.use_dictionary ||
        _cfg.wopt.numeric_dictionary != def.wopt.numeric_dictionary) {
        m[DICTIONARY] = !_cfg.wopt.use_dictionary ? "none"
                      : (_cfg.wopt.numeric_dictionary ? "all" : "text");
    }
    if (_cfg.level != def.level) {
        // The user-facing vocabulary, not to_string()'s internal "L0"/"L1"/"L2". These
        // have to be the words the parser accepts or the property does not survive a
        // round trip through persistence -- which is exactly how this was caught.
        switch (_cfg.level) {
        case folding_level::verbatim:   m[METADATA_FOLDING] = "verbatim"; break;
        case folding_level::row_folded: m[METADATA_FOLDING] = "row";      break;
        case folding_level::uniform:    m[METADATA_FOLDING] = "uniform";  break;
        default: break;   // L3 is export-only and unreachable as a stored setting
        }
    }
    // Overrides are round-tripped verbatim, and unlike the scalar options they are *not* elided when
    // they equal the default: an explicit 'auto' is how a user cancels an override in an ALTER, and
    // dropping it from DESCRIBE would make the schema unreproducible.
    for (const auto& [col, enc] : _column_encodings) {
        m[sstring(ENCODING_PREFIX) + col] = to_string(enc);
    }
    // The provider *options* round-trip; there is no key material here to leak, exactly as with
    // scylla_encryption_options. Note that cipher_algorithm comes back out even when the operator
    // did not write it, because validate_key_options() defaulted it -- DESCRIBE showing the
    // algorithm actually in force is the point.
    if (_cfg.encryption_enabled) {
        m[ENCRYPTION] = _cfg.encryption_algo == format::cipher::aes_gcm_v1
                      ? "aes_gcm_v1" : "aes_gcm_ctr_v1";
        for (const auto& [ko, kv] : _cfg.key_opts) {
            m[ko] = kv;
        }
        if (_cfg.encryption_key_metadata != key_metadata_format::provider) {
            // Qualified: the member to_string(column_encoding) would otherwise hide it.
            m[ENCRYPTION_KEY_METADATA] =
                    sstables::parquet::to_string(_cfg.encryption_key_metadata);
        }
        // The operator's text, not the overlay result -- see the note on _column_key_raw. These
        // are locators, like the table's own key options, so there is nothing here to leak either.
        for (const auto& [col, spec] : _column_key_raw) {
            m[sstring(ENCRYPTION_KEY_PREFIX) + col] = spec;
        }
    }
    return m;
}

// ---------------------------------------------------------------- shredder
fragment_shredder::fragment_shredder(const ::schema& s)
    : _schema(s), _cols(columns_of(s)) {
    _n_pk = s.partition_key_size();
    _n_ck = s.clustering_key_size();
    _static_base = static_base(s);
}

void fragment_shredder::set_partition_tombstone(tombstone t) {
    _part_del = deletion_info{t.timestamp,
                              int32_t(t.deletion_time.time_since_epoch().count())};
}

void fragment_shredder::new_partition(const dht::decorated_key& dk) {
    _pk.clear();
    _part_del.reset();
    _static_cells.clear();
    _static_collections.clear();
    _saw_clustering_row = false;
    std::vector<cql_type> types;
    for (const auto& c : _schema.partition_key_columns()) { types.push_back(cql_type_of(*c.type)); }
    std::vector<bytes> parts;
    for (auto&& v : dk.key().components(_schema)) { parts.push_back(linearized(v)); }
    explode_key(parts, types, _pk);
}

void fragment_shredder::add_clustering_row(const clustering_row& cr) {
    row r;
    r.key = _pk;
    std::vector<cql_type> ck_types;
    for (const auto& c : _schema.clustering_key_columns()) { ck_types.push_back(cql_type_of(*c.type)); }
    std::vector<bytes> parts;
    for (auto&& v : cr.key().components(_schema)) { parts.push_back(linearized(v)); }
    explode_key(parts, ck_types, r.key);

    // Row-level metadata. The marker is what makes a row with no live cells
    // exist at all, so losing it deletes the row.
    if (!cr.marker().is_missing()) {
        marker_info m;
        m.timestamp = cr.marker().timestamp();
        if (cr.marker().is_expiring()) {
            m.ttl    = int32_t(cr.marker().ttl().count());
            m.expiry = int32_t(cr.marker().expiry().time_since_epoch().count());
        }
        r.marker = m;
    }
    if (cr.tomb()) {
        const auto& sh = cr.tomb().tomb();          // the shadowable half
        r.row_del = deletion_info{sh.timestamp,
                                  int32_t(sh.deletion_time.time_since_epoch().count())};
        const auto& reg = cr.tomb().regular();
        if (reg) {
            r.row_del_regular = deletion_info{
                    reg.timestamp, int32_t(reg.deletion_time.time_since_epoch().count())};
        }
    }
    r.part_del = _part_del;
    _saw_clustering_row = true;
    replay_statics(r);

    // Regular cells. column_id indexes the regular columns, which is exactly the
    // index space schema_mapping uses for cells.
    cr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
        const column_definition& cdef = _schema.regular_column_at(id);
        if (cdef.is_counter()) {
            // Counter updates are a pre-shard-transformation form that never
            // reaches storage; a cell still in that form is a bug upstream, not
            // something to invent a representation for.
            auto av = acoc.as_atomic_cell(cdef);
            if (av.is_counter_update()) { unrepresentable("counter updates"); }
            r.collections.emplace(size_t(id), read_counter_cell(av));
            return;
        }
        if (cdef.is_atomic()) {
            auto av = acoc.as_atomic_cell(cdef);
            cell c;
            c.timestamp = av.timestamp();
            c.live = av.is_live();
            if (c.live) {
                auto lv = av.value().linearize();
                c.v = decode(cql_type_of(*cdef.type), bytes_view(lv));
                if (av.is_live_and_has_ttl()) {
                    c.ttl = int32_t(av.ttl().count());
                    c.local_deletion_time = int32_t(av.expiry().time_since_epoch().count());
                }
            } else {
                c.local_deletion_time = int32_t(av.deletion_time().time_since_epoch().count());
            }
            r.cells.emplace(size_t(id), std::move(c));
        }
        else {
            r.collections.emplace(size_t(id), read_collection_mutation(acoc));
        }
    });
    push_row(std::move(r));
}

void fragment_shredder::push_row(row&& r) {
    _buffered_bytes += heap_bytes(r);
    _rows.push_back(std::move(r));
}

void fragment_shredder::replay_statics(row& r) const {
    for (const auto& [k, c] : _static_cells) { r.cells.emplace(k, c); }
    for (const auto& [k, c] : _static_collections) { r.collections.emplace(k, c); }
}

void fragment_shredder::add_static_row(const static_row& sr) {
    // Held, not emitted: static cells are replayed onto every clustering row of
    // the partition, where they cost nothing because they are constant and
    // compress away. A partition with no clustering rows gets a placeholder from
    // end_partition().
    sr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
        const column_definition& cdef = _schema.static_column_at(id);
        if (cdef.is_counter()) {
            auto av = acoc.as_atomic_cell(cdef);
            if (av.is_counter_update()) { unrepresentable("counter updates"); }
            _static_collections.emplace(_static_base + size_t(id), read_counter_cell(av));
            return;
        }
        if (!cdef.is_atomic()) {
            _static_collections.emplace(_static_base + size_t(id),
                                        read_collection_mutation(acoc));
            return;
        }
        auto av = acoc.as_atomic_cell(cdef);
        cell c;
        c.timestamp = av.timestamp();
        c.live = av.is_live();
        if (c.live) {
            auto lv = av.value().linearize();
            c.v = decode(cql_type_of(*cdef.type), bytes_view(lv));
            if (av.is_live_and_has_ttl()) {
                c.ttl = int32_t(av.ttl().count());
                c.local_deletion_time = int32_t(av.expiry().time_since_epoch().count());
            }
        } else {
            c.local_deletion_time = int32_t(av.deletion_time().time_since_epoch().count());
        }
        _static_cells.emplace(_static_base + size_t(id), std::move(c));
    });
}

void fragment_shredder::add_range_tombstone_change(const range_tombstone_change& rtc) {
    // Carried as a row so it keeps its place in the clustering order. The
    // clustering-key columns hold the bound's prefix; anything past prefix_len is
    // padding and is ignored on the way back.
    const auto& pos = rtc.position();
    row r;
    r.key = _pk;

    std::vector<bytes> parts;
    if (pos.has_key()) {
        for (auto&& v : pos.key().components(_schema)) { parts.push_back(linearized(v)); }
    }
    for (size_t i = 0; i < _n_ck; ++i) {
        r.key.push_back(i < parts.size()
                ? decode(_cols[_n_pk + i].type, bytes_view(parts[i]))
                : decode(_cols[_n_pk + i].type, bytes_view()));
    }

    rtc_info ri;
    ri.weight     = int32_t(pos.get_bound_weight());
    ri.region     = int32_t(pos.region());
    ri.prefix_len = int32_t(parts.size()); // ok
    if (rtc.tombstone()) {
        ri.tomb = deletion_info{rtc.tombstone().timestamp,
                int32_t(rtc.tombstone().deletion_time.time_since_epoch().count())};
    }
    r.rtc = ri;
    r.part_del = _part_del;
    replay_statics(r);

    // The partition has content, so end_partition() must not add a placeholder.
    _saw_clustering_row = true;
    push_row(std::move(r));
}

void fragment_shredder::end_partition() {
    if (_saw_clustering_row || _pk.empty()) {
        _saw_clustering_row = false;
        return;
    }
    if (_static_cells.empty() && _static_collections.empty() && !_part_del) {
        return;                       // nothing to record
    }
    // Static-only (or tombstone-only) partition: one placeholder row, whose
    // clustering values are meaningless and are flagged as such.
    row r;
    r.key = _pk;
    for (size_t i = 0; i < _n_ck; ++i) {
        r.key.push_back(decode(_cols[_n_pk + i].type, bytes_view()));
    }
    r.no_ck = true;
    r.part_del = _part_del;
    replay_statics(r);
    push_row(std::move(r));
}

std::vector<uint8_t> fragment_shredder::to_parquet(const pq_writer_config& cfg) const {
    // The per-column overrides have to be handed over here too. There are two ways an sstable
    // gets written -- cut_row_group() when it outgrows the row-group budget, and this one-shot
    // path when the whole thing fits a single row group -- and for a while only the first passed
    // the overrides on. The effect was that `encoding.<col>` worked on large tables and did
    // nothing on small ones, which reads as an intermittent bug rather than a missing argument:
    // raising rows_per_row_group made it "start working" only because it forced a cut. Any per-column
    // writer setting added later has to travel down both paths for the same reason.
    return write_rows(_cols, _rows, cfg.level, cfg.wopt, cfg.exc, cfg.column_encodings);
}

std::vector<uint8_t> fragment_shredder::to_parquet_for_storage(const pq_writer_config& cfg) const {
    if (!folding_is_lossless(cfg.level)) {
        throw std::invalid_argument(
                std::string("folding level ") + to_string(cfg.level) +
                " discards cell metadata and cannot be used as a storage format; "
                "it is available for export only");
    }
    return to_parquet(cfg);
}

// ---------------------------------------------------------------- writer_impl
pq_writer_impl::pq_writer_impl(sstables::sstable& sst, const ::schema& s,
                               uint64_t estimated_partitions,
                               const sstables::sstable_writer_config& cfg,
                               pq_writer_config pcfg, encoding_stats enc_stats,
                               shard_id shard, sink_type sink)
    : sstables::sstable_writer::writer_impl(sst, s, cfg)
    , _shredder(s)
    , _pcfg(std::move(pcfg))
    , _shard(shard)
    , _enc_stats(enc_stats)
    , _sink(std::move(sink))
    // Thresholds are read once here, as mx does: they are what
    // get_large_data_stat() reports next to the max, so a threshold changed
    // mid-write would make the recorded aggregate self-inconsistent.
    , _partition_size_entry(large_data_stats_entry{
            .threshold = sst.get_large_data_handler().get_partition_threshold_bytes()})
    , _rows_in_partition_entry(large_data_stats_entry{
            .threshold = sst.get_large_data_handler().get_rows_count_threshold()})
    , _row_size_entry(large_data_stats_entry{
            .threshold = sst.get_large_data_handler().get_row_threshold_bytes()})
    , _cell_size_entry(large_data_stats_entry{
            .threshold = sst.get_large_data_handler().get_cell_threshold_bytes()})
    , _elements_in_collection_entry(large_data_stats_entry{
            .threshold = sst.get_large_data_handler().get_collection_elements_count_threshold()}) {
    if (_sink) {
        return;   // unit-test path: no sstable components at all
    }
    // Zero is benign here but not further down; mx clamps for the same reason.
    estimated_partitions = std::max(uint64_t(1), estimated_partitions);

    // create_data() is what actually opens the Data and Index files. Until it
    // runs, sst._data_file is null and make_data_or_index_sink dereferences it.
    sst.open_sstable(cfg.origin);
    sst.create_data().get();
    sst._shards = { shard };

    _index_sampling_state.summary_byte_cost = cfg.summary_byte_cost;
    _index_sampling_state.max_partitions_per_page = cfg.summary_max_partitions_per_page;

    {
        auto out = sst._storage->make_data_or_index_sink(sst, component_type::Data).get();
        _data_writer = std::make_unique<crc32_checksummed_file_writer>(
                std::move(out), sst.sstable_buffer_size, sst.get_filename());
    }
    if (sst.has_component(component_type::Index)) {
        auto out = sst._storage->make_data_or_index_sink(sst, component_type::Index).get();
        _index_writer = std::make_unique<crc32_digest_file_writer>(
                std::move(out), sst.sstable_buffer_size, sst.index_filename());
        sstables::prepare_summary(sst._components->summary, estimated_partitions,
                                  s.min_index_interval());
    }

    _cfg.monitor->on_write_started(_data_writer->offset_tracker());
    if (sst.has_component(component_type::Filter)) {
        sst._components->filter = utils::i_filter::get_filter(
                estimated_partitions, s.bloom_filter_fp_chance(), utils::filter_format::m_format);
    }
}

// The mc index entry is: key, then a vint position, then a vint promoted-index
// size. For pq the position is the row ordinal, and the promoted index is always
// absent because Parquet's ColumnIndex already provides intra-partition seeking
// (design doc open question 2).
void pq_writer_impl::finish_open_partition() {
    if (!_in_partition || !_index_writer) {
        _in_partition = false;
        return;
    }
    write_vint(*_index_writer, uint64_t(0));   // no promoted index
    _in_partition = false;
}


// ------------------------------------------------------ statistics collection
//
// Mirrors sstables/mx/writer.cc: write_cell() for atomic cells, write_liveness_info()
// for row markers, and collect_row_stats() / collect_range_tombstone_stats() for the
// counters. Keeping the same call order and the same is_live distinctions matters --
// min_live_timestamp and the tombstone drop-time histogram feed tombstone GC
// decisions, so "close enough" here is a correctness bug, not a reporting one.

void pq_writer_impl::collect_atomic_cell(const atomic_cell_view& cell) {
    if (!cell.is_live()) {
        _c_stats.update_timestamp(cell.timestamp(), is_live::no);
        _c_stats.update_local_deletion_time_and_tombstone_histogram(cell.deletion_time());
        return;
    }
    _c_stats.update_timestamp(cell.timestamp(), is_live::yes);
    if (cell.is_live_and_has_ttl()) {
        _c_stats.update_ttl(cell.ttl());
        // The histogram takes the *expiry*, not the TTL: if a long TTL were fed in
        // instead, an sstable whose data expires far in the future would look fully
        // expired now.
        _c_stats.update_local_deletion_time_and_tombstone_histogram(cell.expiry());
    } else {
        _c_stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

// The cell's own serialised form: flags, timestamp, then either expiry+ttl or a
// local deletion time, then the value bytes. This is the encoding-independent
// "how much content is this cell" number that stands in for mx's on-disk cell
// extent -- see the note in writer_impl.hh on why a Parquet cell has no extent.
uint64_t pq_writer_impl::cell_logical_size(const atomic_cell_view& cell) {
    return cell.serialize().size_bytes();
}

// A collection is reported as a whole, like mx does: the collection-wide
// tombstone plus every element's path and cell.
uint64_t pq_writer_impl::collection_logical_size(collection_mutation_view cmv) {
    uint64_t n = 0;
    if (cmv.tomb()) {
        n += sizeof(api::timestamp_type) + sizeof(int32_t);
    }
    for (auto&& kv : cmv) {
        n += kv.first.size_bytes() + cell_logical_size(kv.second);
    }
    return n;
}

// Row-level metadata: the marker (which is what makes a row with no live cells
// exist at all) and the two halves of the row tombstone. The partition
// tombstone is charged to the partition, not to every row it is replayed onto.
uint64_t pq_writer_impl::row_metadata_logical_size(const clustering_row& cr) {
    uint64_t n = 0;
    if (!cr.marker().is_missing()) {
        n += sizeof(api::timestamp_type);
        if (cr.marker().is_expiring()) {
            n += 2 * sizeof(int32_t);   // ttl + expiry
        }
    }
    if (cr.tomb().tomb()) {
        n += sizeof(api::timestamp_type) + sizeof(int32_t);
    }
    if (cr.tomb().regular()) {
        n += sizeof(api::timestamp_type) + sizeof(int32_t);
    }
    return n;
}

uint64_t pq_writer_impl::collect_cell(const column_definition& cdef,
                                  const atomic_cell_or_collection& acoc,
                                  const clustering_key_prefix* clustering_key) {
    if (!cdef.is_atomic()) {
        // A non-frozen collection counts as one column and one cell per element,
        // and carries its own collection-wide tombstone.
        auto cmv = acoc.as_collection_mutation();
        _c_stats.update(cmv.tomb());
        for (auto&& kv : cmv) {
            collect_atomic_cell(kv.second);
            ++_c_stats.cells_count;
        }
        ++_c_stats.column_count;
        auto size = collection_logical_size(cmv);
        maybe_record_large_cells(clustering_key, cdef, size, cmv.size());
        return size;
    }
    auto cell = acoc.as_atomic_cell(cdef);
    collect_atomic_cell(cell);
    ++_c_stats.cells_count;
    ++_c_stats.column_count;
    auto size = cell_logical_size(cell);
    maybe_record_large_cells(clustering_key, cdef, size, 0);
    return size;
}

uint64_t pq_writer_impl::collect_cells(const ::row& cells, ::column_kind kind,
                                       const clustering_key_prefix* clustering_key) {
    uint64_t total = 0;
    cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
        total += collect_cell(_schema.column_at(kind, id), acoc, clustering_key);
    });
    return total;
}

// The three recorders below mirror sstables/mx/writer.cc one for one: update the
// aggregate's max, ask the handler (which counts above-threshold occurrences and
// writes the system table row), and on a hit push a record into the bounded
// top-N heap for this type.
//
// `_sink` is the unit-test route with no sstable behind it; there is no handler
// to report to and nothing will ever read the metadata.

void pq_writer_impl::maybe_record_large_partitions(uint64_t partition_size, uint64_t rows,
        uint64_t range_tombstones, uint64_t dead_rows) {
    if (_sink || !_partition_key) {
        return;
    }
    auto& size_entry = _partition_size_entry;
    auto& row_count_entry = _rows_in_partition_entry;
    size_entry.max_value = std::max(size_entry.max_value, partition_size);
    row_count_entry.max_value = std::max(row_count_entry.max_value, rows);
    auto ret = _sst.get_large_data_handler().maybe_record_large_partitions(
            _sst, *_partition_key, partition_size, rows, range_tombstones, dead_rows).get();
    size_entry.above_threshold += unsigned(bool(ret.size));
    row_count_entry.above_threshold += unsigned(bool(ret.elements));

    if (!ret.size && !ret.elements) {
        return;
    }
    const auto& pk_bytes = _partition_key->get_bytes();
    auto make = [&] (large_data_type type) {
        return large_data_record{
            .type = type,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{bytes()},
            .column_name = disk_string<uint32_t>{bytes()},
            .value = partition_size,
            .elements_count = rows,
            .range_tombstones = range_tombstones,
            .dead_rows = dead_rows,
        };
    };
    if (ret.size) {
        insert_into_ld_heap(_ld_partition_size_records, make(large_data_type::partition_size));
    }
    if (ret.elements) {
        insert_into_ld_heap(_ld_rows_in_partition_records, make(large_data_type::rows_in_partition));
    }
}

void pq_writer_impl::maybe_record_large_rows(const clustering_key_prefix* clustering_key,
        uint64_t row_size) {
    if (_sink || !_partition_key) {
        return;
    }
    auto& entry = _row_size_entry;
    entry.max_value = std::max(entry.max_value, row_size);
    if (!_sst.get_large_data_handler().maybe_record_large_rows(
                _sst, *_partition_key, clustering_key, row_size).get()) {
        return;
    }
    ++entry.above_threshold;
    const auto& pk_bytes = _partition_key->get_bytes();
    auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
    insert_into_ld_heap(_ld_row_size_records, large_data_record{
        .type = large_data_type::row_size,
        .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
        .clustering_key = disk_string<uint32_t>{std::move(ck_bytes)},
        .column_name = disk_string<uint32_t>{bytes()},
        .value = row_size,
    });
}

void pq_writer_impl::maybe_record_large_cells(const clustering_key_prefix* clustering_key,
        const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) {
    if (_sink || !_partition_key) {
        return;
    }
    auto& cell_size_entry = _cell_size_entry;
    cell_size_entry.max_value = std::max(cell_size_entry.max_value, cell_size);
    auto& collection_elements_entry = _elements_in_collection_entry;
    collection_elements_entry.max_value =
            std::max(collection_elements_entry.max_value, collection_elements);
    auto ret = _sst.get_large_data_handler().maybe_record_large_cells(
            _sst, *_partition_key, clustering_key, cdef, cell_size, collection_elements).get();
    cell_size_entry.above_threshold += unsigned(bool(ret.size));
    collection_elements_entry.above_threshold += unsigned(bool(ret.elements));

    if (!ret.size && !ret.elements) {
        return;
    }
    const auto& pk_bytes = _partition_key->get_bytes();
    auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
    auto make = [&] (large_data_type type) {
        return large_data_record{
            .type = type,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{bytes(ck_bytes)},
            .column_name = disk_string<uint32_t>{to_bytes(cdef.name_as_text())},
            .value = cell_size,
            .elements_count = collection_elements,
        };
    };
    if (ret.size) {
        insert_into_ld_heap(_ld_cell_size_records, make(large_data_type::cell_size));
    }
    if (ret.elements) {
        insert_into_ld_heap(_ld_elements_in_collection_records,
                            make(large_data_type::elements_in_collection));
    }
}

void pq_writer_impl::collect_marker(const row_marker& marker) {
    if (marker.is_missing()) {
        return;
    }
    if (marker.is_live()) {
        _c_stats.update_timestamp(marker.timestamp(), is_live::yes);
        _c_stats.update_live_row_marker_timestamp(marker.timestamp());
    } else {
        _c_stats.update_timestamp(marker.timestamp(), is_live::no);
    }
    if (!marker.is_live()) {
        _c_stats.update_ttl(gc_clock::duration(sstables::expired_liveness_ttl));
        _c_stats.update_local_deletion_time_and_tombstone_histogram(marker.deletion_time());
    } else if (marker.is_expiring()) {
        _c_stats.update_ttl(marker.ttl());
        _c_stats.update_local_deletion_time_and_tombstone_histogram(marker.expiry());
    } else {
        _c_stats.update_ttl(0);
        _c_stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

// Emit the buffered rows as one row group and drop them.
//
// Cut only at a partition boundary, so a partition never spans row groups. That keeps the
// option-A index entry a single ordinal and keeps a point read inside one row group.
//
// A partition larger than the budget therefore overshoots it rather than being split, and
// that is deliberate (decided 2026-08-18): keeping a partition whole is worth more than
// holding the budget exactly. Splitting one would mean the index entry carrying
// (row group, ordinal) instead of a bare ordinal, and a point read spanning row groups --
// complexity paid on every read to bound a rare case. The budget is a target, not a
// guarantee; see design doc 5.5a for the residual exposure.
void pq_writer_impl::cut_row_group() {
    if (_shredder.size() == 0) { return; }
    if (!_pq) {
        // First cut. Parquet fixes one leaf set for the whole file, and we are only a
        // prefix of the way through the rows, so it has to cover every case a later row
        // might need rather than only what these rows use.
        _ms.emplace(map_schema(_shredder.columns(), _pcfg.level, _shredder.rows(),
                               _pcfg.exc, leaf_set::conservative, _pcfg.column_encodings));
        if (!folding_is_lossless(_ms->level)) {
            throw std::invalid_argument(
                    std::string("folding level ") + to_string(_ms->level) +
                    " discards cell metadata and cannot be used as a storage format");
        }
        std::vector<std::optional<format::encoding>> hints;
        hints.reserve(_ms->columns.size());
        for (const auto& c : _ms->columns) { hints.push_back(c.preferred); }
        _pq = std::make_unique<format::parquet_file_writer>(
                format::parquet_file_writer::nested_schema{_ms->tree, std::move(hints)},
                _pcfg.wopt);
        _pq->add_key_value("scylla.folding_level", to_string(_ms->level));
        // L2 keeps one timestamp for the whole file rather than a per-row column, so the value
        // lives in the footer and the reader requires it -- mapped_schema_from_footer() throws
        // "L2 file without scylla.uniform_timestamp" when it is missing. write_rows() emits it and
        // this path did not.
        //
        // That asymmetry is currently unreachable rather than a live bug, and the reason is worth
        // stating so nobody 'fixes' it by deleting this: the conservative leaf set this path uses
        // sets all_same_ts = false and turns every optional metadata flag on, which breaks L2's
        // precondition, so build_mapped_schema() falls the level back to L1 and never sets
        // uniform_ts. The guard stays because the invariant it protects -- an L2 footer carries its
        // timestamp -- belongs with the code that writes the footer, not with the leaf-set logic
        // three files away that happens to make it moot today.
        if (_ms->uniform_ts) {
            _pq->add_key_value("scylla.uniform_timestamp", std::to_string(*_ms->uniform_ts));
        }
        add_counter_metadata(*_pq, _shredder.columns());

        // Stream straight into the Data component instead of accumulating the file.
        // Without this, peak write memory is the whole output -- ~253 MB for a 256 MB
        // bottom-tier sstable, per concurrent compaction per shard (design doc 7.2).
        //
        // Only on the real sstable path. `_sink` is the unit-test route, which wants the
        // finished image handed back in one piece, and there is no _data_writer at all in
        // that case.
        //
        // Safe to write here even though finish_open_partition() and the index bookkeeping
        // run later: the Parquet index is by *row ordinal*, not by data-file offset
        // (section 5.4, option A), so nothing downstream depends on where the data lands.
        if (_data_writer && !_sink) {
            _streaming = true;
            _pq->set_sink([this] (std::span<const uint8_t> bytes) {
                _data_writer->write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                _pos += bytes.size();
            });
        }
    }
    auto data = shred(*_ms, _shredder.columns(), _shredder.rows());
    _pq->add_row_group(data);
    _rows_flushed += _shredder.size();
    _shredder.clear();
}

void pq_writer_impl::consume_new_partition(const dht::decorated_key& dk) {
    finish_open_partition();
    _shredder.new_partition(dk);
    _partition_first_row = _rows_flushed + _shredder.size();

    // Kept for the whole partition: every large-data record carries the binary
    // partition key, and it is the key the handler writes into the system table.
    // Held here rather than reused from _last_key because _last_key is only
    // populated when there is an Index component.
    _partition_key = key::from_partition_key(_schema, dk.key());
    // The key itself is content in a Parquet file: the shredder writes it into
    // the key columns of every row of the partition. Charged once.
    _partition_logical_size = _partition_key->get_bytes().size();

    if (_index_writer) {
        auto pk = key::from_partition_key(_schema, dk.key());
        // The filter and the min/max key statistics are fed per partition, as
        // mx does. Without the filter every point read on this sstable misses.
        if (_sst._components->filter) {
            _sst._components->filter->add(utils::make_hashed_key(bytes_view(pk)));
        }
        _collector.add_key(bytes_view(pk));
        sstables::maybe_add_summary_entry(_sst._components->summary, dk.token(),
                bytes_view(pk), _index_writer->offset(), _index_writer->offset(),
                _index_sampling_state);
        // Same on-disk shape as mc: a uint16-prefixed key, then a vint. Only the
        // meaning of the vint differs.
        auto p_key = disk_string_view<uint16_t>();
        p_key.value = bytes_view(pk);
        write(_sst.get_version(), *_index_writer, p_key);
        // Option A: the row ordinal of this partition's first row, not a byte
        // offset. The reader maps it to a page through the OffsetIndex.
        write_vint(*_index_writer, _partition_first_row);
        _in_partition = true;

        // After the write: p_key views into pk.
        if (!_first_key) { _first_key = pk; }
        _last_key = std::move(pk);
    }
    ++_num_partitions;
}

void pq_writer_impl::consume(tombstone t) {
    if (t) {
        _shredder.set_partition_tombstone(t);
        _c_stats.update(t);
        // Replayed onto every row of the partition, so charged to the partition
        // once rather than to each row -- same reasoning as static cells.
        _partition_logical_size += sizeof(api::timestamp_type) + sizeof(int32_t);
        // A partition tombstone spans the whole clustering range, so it widens the
        // min/max clustering key to both sentinels -- exactly what mx records.
        _collector.update_min_max_components(
                position_in_partition_view::before_all_clustered_rows());
        _collector.update_min_max_components(
                position_in_partition_view::after_all_clustered_rows());
    }
}

stop_iteration pq_writer_impl::consume(static_row&& sr) {
    // Static cells are reported as cells (with a null clustering key, as mx does
    // from its static row) but charged to the partition, not to any row: the
    // shredder replays them onto every storage row, and charging them per row
    // would report a partition of small rows as a partition of large rows.
    //
    // There is deliberately no maybe_record_large_rows() call here. A Parquet
    // file has no static row -- see the note in writer_impl.hh -- so reporting
    // one would be inventing a row that is not in the file.
    _partition_logical_size += collect_cells(sr.cells(), ::column_kind::static_column, nullptr);
    _shredder.add_static_row(sr);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume(clustering_row&& cr) {
    _collector.update_min_max_components(cr.position());
    collect_marker(cr.marker());
    _c_stats.update(cr.tomb().regular());
    _c_stats.update(cr.tomb().tomb());
    uint64_t row_size = cr.key().representation().size()
            + row_metadata_logical_size(cr)
            + collect_cells(cr.cells(), ::column_kind::regular_column, &cr.key());
    ++_c_stats.rows_count;
    if (cr.tomb()) {
        ++_c_stats.dead_rows_count;
    }
    maybe_record_large_rows(&cr.key(), row_size);
    _partition_logical_size += row_size;
    _shredder.add_clustering_row(cr);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume(range_tombstone_change&& rtc) {
    _collector.update_min_max_components(rtc.position());
    _c_stats.update(rtc.tombstone());
    // mx counts a range tombstone change as a row as well as a range tombstone,
    // because on its side the marker occupies a row slot in the data file.
    ++_c_stats.rows_count;
    ++_c_stats.range_tombstones_count;
    // A marker occupies a storage row, so its bound and tombstone are part of the
    // partition's volume. mx does not emit a row_size record for a marker either
    // (see collect_range_tombstone_stats), so neither do we: "large row" means a
    // row with data in it.
    _partition_logical_size += (rtc.position().has_key()
            ? rtc.position().key().representation().size() : 0)
            + sizeof(int32_t) * 2   // bound weight + region
            + (rtc.tombstone() ? sizeof(api::timestamp_type) + sizeof(int32_t) : 0);
    _shredder.add_range_tombstone_change(rtc);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume_end_of_partition() {
    _shredder.end_partition();
    // Reported before column_stats is drained into the collector, because it is
    // where the row and tombstone counts live.
    //
    // The size here is the partition's *logical* volume, not its on-disk length:
    // byte offsets do not exist per partition, since the Parquet image is encoded
    // once at end of stream. See the note in writer_impl.hh.
    maybe_record_large_partitions(_partition_logical_size, _c_stats.rows_count,
            _c_stats.range_tombstones_count, _c_stats.dead_rows_count);
    // column_stats::partition_size still stays 0 for the same byte-offset reason.
    // It only feeds the estimated-partition-size histogram, not any GC decision,
    // and is deliberately left alone rather than fed the logical size: the
    // histogram is consumed as on-disk bytes.
    _collector.update(std::move(_c_stats));
    _c_stats.reset();
    _partition_key.reset();
    _partition_logical_size = 0;
    // A partition boundary is the only place a cut is allowed, so this is where the
    // budget is checked.
    if (_shredder.buffered_bytes() >= _pcfg.row_group_buffer_bytes ||
        _shredder.size() >= _pcfg.rows_per_row_group) {
        cut_row_group();
    }
    return stop_iteration::no;
}

pq_writer_impl::~pq_writer_impl() {
    // Only reached with writers still held when the stream did not end normally -- a cancelled or
    // failed compaction. Must not throw: this is a destructor, and the abort that got us here may
    // well have poisoned the sink, so a failing flush is expected rather than exceptional.
    auto shut = [] (auto& w, const char* what) noexcept {
        if (!w) {
            return;
        }
        try {
            w->close();
        } catch (...) {
            pqwlog.warn("closing {} during teardown failed: {}. Ignored.",
                      what, std::current_exception());
        }
        w.reset();
    };
    shut(_data_writer, "the data component");
    shut(_index_writer, "the index component");
}

void pq_writer_impl::consume_end_of_stream() {
    // Two paths on purpose. If no cut ever happened the whole sstable fits the budget and
    // goes out as a single row group with the *derived* leaf set -- identical to what this
    // writer produced before row-group cutting existed. Only once a cut has forced the
    // conservative leaf set does the streaming path take over.
    std::vector<uint8_t> img;
    if (_pq) {
        cut_row_group();            // the tail
        img = _pq->finish();        // empty when streaming: already in the Data component
    } else {
        // No cut ever happened, so the whole sstable fitted the row-group budget and the
        // image is bounded by it. Materialising here costs at most one row group.
        img = _shredder.to_parquet_for_storage(_pcfg);
    }
    if (!_streaming) {
        _pos = img.size();          // streaming keeps _pos as it goes
    }

    // Compression ratio, for `nodetool` and the REST API. Without this a Parquet table reports
    // no ratio at all, because sstable::get_compression_ratio() looks for a CompressionInfo
    // component and Parquet has none -- it compresses inside the file. The honest numerator is
    // the file we wrote and the denominator is the sum of the column chunks' uncompressed sizes,
    // which is the serialised volume before the codec.
    const int64_t uncompressed = _pq ? _pq->uncompressed_bytes() : [&] {
        // No cut happened, so the whole file is in `img` and its footer is the only place the
        // per-chunk uncompressed sizes exist.
        if (img.empty()) { return int64_t(0); }
        try {
            auto md = format::parse_footer(img);
            int64_t n = 0;
            for (const auto& rg : md.row_groups) { n += rg.total_byte_size; }
            return n;
        } catch (...) {
            return int64_t(0);      // reporting must never fail a write
        }
    }();
    if (uncompressed > 0 && _pos > 0) {
        _collector.add_compression_ratio(_pos, uint64_t(uncompressed));
    }

    // A sink is the unit-test path: it lets the whole fragment -> Parquet route be
    // driven without constructing an sstable.
    if (_sink) {
        _sink(std::move(img));
        return;
    }

    // Otherwise the image becomes the Data component. consume_end_of_stream runs
    // in a seastar thread (mx::writer relies on the same), so blocking here is
    // allowed.
    finish_open_partition();
    if (!_streaming) {
        _data_writer->write(reinterpret_cast<const char*>(img.data()), img.size());
    }
    _data_writer->close();
    _sst.write_digest(_data_writer->full_checksum());
    _sst.write_crc(_data_writer->finalize_checksum());
    _data_writer.reset();
    _cfg.monitor->on_data_write_completed();
    write_components();
}

void pq_writer_impl::write_components() {
    if (_index_writer) {
        sstables::seal_summary(_sst._components->summary, std::move(_first_key),
                               std::move(_last_key), _index_sampling_state).get();
        _index_writer->close();
        _index_writer.reset();
    }
    _sst.set_first_and_last_keys();

    // The mc serialization header. Without it sstable::get_column_translation()
    // is empty, and the index parser reads that as "not mc format" and decodes
    // our vint entries as fixed-width big-endian -- every lookup then misses.
    _sst._components->statistics.contents[metadata_type::Serialization] =
            std::make_unique<serialization_header>(
                    mc::make_serialization_header(_schema, _enc_stats, _cfg));

    sstables::seal_statistics(_sst.get_version(), _sst._components->statistics, _collector,
            _schema.get_partitioner().name(), _schema.bloom_filter_fp_chance(),
            _sst.get_schema(), _sst.get_first_decorated_key(), _sst.get_last_decorated_key(),
            _enc_stats);

    _sst.maybe_rebuild_filter_from_index(_num_partitions);
    _sst.write_summary();
    _sst.write_filter();
    _sst.write_statistics();
    // Large-data metadata. Passing nullopt here -- which this did until B1 was
    // closed -- leaves system.large_partitions / large_rows / large_cells silently
    // empty for every `pq` table, which is worse than a visible gap: the table
    // looks healthy.
    std::optional<scylla_metadata::large_data_stats> ld_stats(scylla_metadata::large_data_stats{
        .map = {
            { large_data_type::partition_size, std::move(_partition_size_entry) },
            { large_data_type::rows_in_partition, std::move(_rows_in_partition_entry) },
            { large_data_type::row_size, std::move(_row_size_entry) },
            { large_data_type::cell_size, std::move(_cell_size_entry) },
            { large_data_type::elements_in_collection, std::move(_elements_in_collection_entry) },
        }
    });
    // Not merely reporting: compaction/compaction.cc reads min_live_timestamp to
    // decide what a tombstone may purge, and falls back to the (more
    // conservative, so safe but coarser) min_timestamp when it is absent.
    std::optional<scylla_metadata::ext_timestamp_stats> ts_stats(scylla_metadata::ext_timestamp_stats{
        .map = _collector.get_ext_timestamp_stats()
    });
    // Drain all per-type min-heaps into a single large_data_records array.
    std::optional<scylla_metadata::large_data_records> ld_records;
    {
        utils::chunked_vector<large_data_record> records;
        auto drain = [&records] (auto& heap) {
            while (!heap.empty()) {
                records.push_back(std::move(const_cast<large_data_record&>(heap.top())));
                heap.pop();
            }
        };
        drain(_ld_partition_size_records);
        drain(_ld_rows_in_partition_records);
        drain(_ld_row_size_records);
        drain(_ld_cell_size_records);
        drain(_ld_elements_in_collection_records);
        if (!records.empty()) {
            ld_records = scylla_metadata::large_data_records{.elements = std::move(records)};
        }
    }
    _sst.write_scylla_metadata(_shard, run_identifier{_cfg.run_identifier},
                               std::move(ld_stats), std::move(ts_stats), std::move(ld_records));
    if (!_cfg.leave_unsealed) {
        _sst.seal_sstable(_cfg.backup).get();
    }
}

std::unique_ptr<sstables::sstable_writer::writer_impl> make_writer(
        sstables::sstable& sst,
        const ::schema& s,
        uint64_t estimated_partitions,
        const sstables::sstable_writer_config& cfg,
        encoding_stats enc_stats,
        shard_id shard) {
    // From the table's `parquet = {...}` property. Already validated at CREATE/ALTER
    // time, so anything stored here parses; an empty map yields the defaults.
    pq_writer_config pcfg = parquet_parameters(s.parquet_options()).config();
    // The key is resolved here, at the last moment before writing, so a rotated or newly reachable
    // key takes effect on the next sstable rather than needing a restart -- and so a provider that
    // cannot be reached is a loud failure at write time rather than a file written in the clear.
    // Writing an unencrypted file for a table that asked for encryption would be the worst outcome
    // available, since nothing downstream would ever notice.
    //
    // Blocking is legal here: this runs in the same seastar thread as pq_writer_impl's constructor,
    // which already waits on sst.create_data().
    if (pcfg.encryption_enabled) {
        auto* ks = key_source_ptr();
        if (!ks) {
            throw std::runtime_error(seastar::format(
                    "{}.{}: the 'parquet' option asks for encryption but this node has no "
                    "encryption key provider registered", s.ks_name(), s.cf_name()));
        }
        resolved_key rk;
        try {
            rk = ks->key_for_write(pcfg.key_opts).get();
        } catch (...) {
            std::throw_with_nested(std::runtime_error(seastar::format(
                    "{}.{}: could not obtain a parquet encryption key from the key provider",
                    s.ks_name(), s.cf_name())));
        }
        if (!rk.key.valid()) {
            throw std::runtime_error(seastar::format(
                    "{}.{}: the key provider returned a {}-byte key; AES needs 16, 24 or 32",
                    s.ks_name(), s.cf_name(), rk.key.bytes.size()));
        }
        pcfg.wopt.encryption.enabled = true;
        pcfg.wopt.encryption.algo = pcfg.encryption_algo;
        pcfg.wopt.encryption.footer_key = rk.key;
        // Binds the file to the table it belongs to: a Data.db moved between tables, or replayed
        // from a backup into a different one, fails authentication instead of decoding.
        pcfg.wopt.encryption.aad_prefix = seastar::format("{}.{}", s.ks_name(), s.cf_name());
        pcfg.wopt.encryption.store_aad_prefix = true;
        // The reader needs to be able to get the same key back. That is what the provider's id is
        // for, and key_metadata is where Parquet puts it. Some providers issue none (their options
        // alone identify the key), in which case this is the empty string in the `provider` shape
        // and an empty masterKeyID in the `parquet_kms` one -- both of which the read path
        // understands as "no id".
        pcfg.wopt.encryption.key_metadata = std::string(
                make_key_metadata(rk.id, pcfg.encryption_key_metadata));

        // Per-column keys. Resolved through the same provider interface as the footer key, so
        // BYOK works identically and no key is ever derived from another or held in the schema.
        //
        // Deduplicated by option set, which is what makes the common shape cheap: "encrypt the PII
        // columns under the PII key" names one key for several columns, and that must be one
        // provider round trip, not one per column. It also matters on the read side, where each
        // distinct set costs a nested key lookup under a reader permit (§11.1 B3).
        std::map<key_options, resolved_key> by_opts;
        for (const auto& [col, kopts] : pcfg.column_key_opts) {
            auto it = by_opts.find(kopts);
            if (it == by_opts.end()) {
                resolved_key crk;
                try {
                    crk = ks->key_for_write(kopts).get();
                } catch (...) {
                    std::throw_with_nested(std::runtime_error(seastar::format(
                            "{}.{}: could not obtain the parquet encryption key for column '{}' "
                            "from the key provider", s.ks_name(), s.cf_name(), col)));
                }
                if (!crk.key.valid()) {
                    throw std::runtime_error(seastar::format(
                            "{}.{}: the key provider returned a {}-byte key for column '{}'; AES "
                            "needs 16, 24 or 32",
                            s.ks_name(), s.cf_name(), crk.key.bytes.size(), col));
                }
                it = by_opts.emplace(kopts, std::move(crk)).first;
            }
            // The column's own key_metadata, so the reader can ask the provider for *that* key by
            // id -- which is what lets one column's key rotate without touching another's.
            pcfg.wopt.encryption.column_keys[std::string(col)] =
                    format::writer_options::encryption_options::column_key{
                            it->second.key,
                            std::string(make_key_metadata(it->second.id,
                                                          pcfg.encryption_key_metadata))};
        }
    }
    return std::make_unique<pq_writer_impl>(sst, s, estimated_partitions, cfg,
                                            std::move(pcfg), enc_stats, shard, nullptr);
}

future<> validate_encryption(const ::schema& s) {
    if (s.parquet_options().empty()) {
        co_return;
    }
    const parquet_parameters pp{s.parquet_options()};
    if (!pp.encryption_enabled()) {
        co_return;
    }
    // cf_prop_defs::validate() rejects setting both in one statement, but it sees only that
    // statement's extensions: two separate ALTERs (one setting scylla_encryption_options, one
    // setting parquet encryption) merge at the schema builder and meet for the first time here,
    // on the merged schema. Encrypting the encrypted -- the file-io layer under the format's own
    // modular encryption -- doubles the cost and makes key rotation ambiguous, so it is refused
    // wherever it is first visible.
    if (s.extensions().contains("scylla_encryption_options")) {
        throw exceptions::configuration_exception(seastar::format(
                "{}.{}: a table cannot combine scylla_encryption_options with parquet "
                "encryption; they are separate encryption layers", s.ks_name(), s.cf_name()));
    }
    auto* ks = key_source_ptr();
    if (!ks) {
        throw exceptions::configuration_exception(seastar::format(
                "{}.{}: the 'parquet' option asks for encryption but this node has no encryption "
                "key provider registered", s.ks_name(), s.cf_name()));
    }
    try {
        co_await ks->validate(pp.key_opts());
    } catch (...) {
        // Nested rather than flattened: the provider's own message ("Could not read
        // '/etc/scylla/foo'", "kmip host bar not configured") is the actionable part, and it is
        // several layers down.
        std::throw_with_nested(exceptions::configuration_exception(seastar::format(
                "{}.{}: the 'parquet' option asks for encryption, but no key could be obtained "
                "from the key provider", s.ks_name(), s.cf_name())));
    }
    // Every per-column key too, and for the same reason: a per-column key that cannot be resolved
    // is a table that accepts its DDL and then fails every flush, whose first symptom is a
    // compaction error hours later. Deduplicated by option set so that keying twenty columns under
    // one PII key is one provider round trip at DDL time, not twenty.
    std::set<key_options> seen;
    for (const auto& [col, kopts] : pp.column_key_opts()) {
        if (!seen.insert(kopts).second) { continue; }
        try {
            co_await ks->validate(kopts);
        } catch (...) {
            std::throw_with_nested(exceptions::configuration_exception(seastar::format(
                    "{}.{}: the 'parquet' option asks for a separate encryption key for column "
                    "'{}', but no key could be obtained from the key provider",
                    s.ks_name(), s.cf_name(), col)));
        }
    }
}

} // namespace sstables::parquet
