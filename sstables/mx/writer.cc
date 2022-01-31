/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "sstables/mx/writer.hh"
#include "sstables/writer.hh"
#include "encoding_stats.hh"
#include "schema.hh"
#include "mutation_fragment.hh"
#include "vint-serialization.hh"
#include "sstables/types.hh"
#include "sstables/mx/types.hh"
#include "db/config.hh"
#include "atomic_cell.hh"
#include "utils/exceptions.hh"

#include <functional>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/range/adaptor/indexed.hpp>
#include <boost/range/algorithm/stable_partition.hpp>

logging::logger slogger("mc_writer");

namespace sstables {
namespace mc {

using indexed_columns = std::vector<std::reference_wrapper<const column_definition>>;

// There is a special case when we need to treat a non-full clustering key prefix as a full one
// for serialization purposes. This is the case that may occur with a compact table.
// For historical reasons a compact table may have rows with missing trailing clustering columns in their clustering keys.
// Consider:
//    cqlsh:test> CREATE TABLE cf (pk int, ck1 int, ck2 int, rc int, primary key (pk, ck1, ck2)) WITH COMPACT STORAGE;
//    cqlsh:test> INSERT INTO cf (pk, ck1, rc) VALUES (1, 1, 1);
//    cqlsh:test> SELECT * FROM cf;
//
//     pk | ck1 | ck2  | rc
//    ----+-----+------+----
//      1 |   1 | null |  1
//
//    (1 rows)
// In this case, the clustering key of the row will have length 1, but for serialization purposes we want to treat
// it as a full prefix of length 2.
// So we use ephemerally_full_prefix to distinguish this kind of clustering keys
using ephemerally_full_prefix = seastar::bool_class<struct ephemerally_full_prefix_tag>;

// A helper CRTP base class for input ranges.
// Derived classes should implement the following functions:
//      bool next() const;
//          generates the next value, if possible;
//          returns true if the next value has been evaluated, false otherwise
//      explicit operator bool() const;
//          tells whether the range can produce more items
// TODO: turn description into a concept
template <typename InputRange, typename ValueType>
struct input_range_base {
private:

    InputRange& self() {
        return static_cast<InputRange&>(*this);
    }

    const InputRange& self() const {
        return static_cast<const InputRange&>(*this);
    }

public:
    // Use the same type for iterator and const_iterator
    using const_iterator = class iterator
        : public boost::iterator_facade<
            iterator,
            const ValueType,
            std::input_iterator_tag,
            const ValueType
        >
    {
    private:
        const InputRange* _range;

        friend class input_range_base;
        friend class boost::iterator_core_access;

        explicit iterator(const InputRange& range)
            : _range(range.next() ? &range : nullptr)
        {}

        void increment() {
            assert(_range);
            if (!_range->next()) {
                _range = nullptr;
            }
        }

        bool equal(iterator that) const {
            return (_range == that._range);
        }

        const ValueType dereference() const {
            assert(_range);
            return _range->get_value();
        }

    public:
        iterator() : _range{} {}

    };

    iterator begin() const { return iterator{self()}; }
    iterator end() const   { return iterator{}; }
};

struct clustering_block {
    constexpr static size_t max_block_size = 32;
    uint64_t header = 0;
    struct described_value {
        managed_bytes_view value;
        std::reference_wrapper<const abstract_type> type;
    };
    boost::container::static_vector<described_value, clustering_block::max_block_size> values;
};

class clustering_blocks_input_range
    : public input_range_base<clustering_blocks_input_range, clustering_block> {
private:
    const schema& _schema;
    const clustering_key_prefix& _prefix;
    size_t _serialization_limit_size;
    mutable clustering_block _current_block;
    mutable uint32_t _offset = 0;

public:
    clustering_blocks_input_range(const schema& s, const clustering_key_prefix& prefix, ephemerally_full_prefix is_ephemerally_full)
        : _schema(s)
        , _prefix(prefix) {
        _serialization_limit_size = is_ephemerally_full == ephemerally_full_prefix::yes
                                    ? _schema.clustering_key_size()
                                    : _prefix.size(_schema);
    }

    bool next() const {
        if (_offset == _serialization_limit_size) {
            // No more values to encode
            return false;
        }

        // Each block contains up to max_block_size values
        auto limit = std::min(_serialization_limit_size, _offset + clustering_block::max_block_size);

        _current_block = {};
        assert (_offset % clustering_block::max_block_size == 0);
        while (_offset < limit) {
            auto shift = _offset % clustering_block::max_block_size;
            if (_offset < _prefix.size(_schema)) {
                managed_bytes_view value = _prefix.get_component(_schema, _offset);
                if (value.empty()) {
                    _current_block.header |= (uint64_t(1) << (shift * 2));
                } else {
                    const auto& compound_type = _prefix.get_compound_type(_schema);
                    const auto& types = compound_type->types();
                    if (_offset < types.size()) {
                        const auto& type = *types[_offset];
                        _current_block.values.push_back({value, type});
                    } else {
                        // FIXME: might happen due to bad thrift key.
                        // See https://github.com/scylladb/scylla/issues/7568
                        //
                        // Consider turning into exception when the issue is fixed
                        // and the key is rejected in thrift handler layer, and if
                        // the bad key could not find its way to existing sstables.
                        slogger.warn("prefix {} (size={}): offset {} >= types.size {}", _prefix, _prefix.size(_schema), _offset, types.size());
                        _current_block.header |= (uint64_t(1) << ((shift * 2) + 1));
                    }
                }
            } else {
                // This (and all subsequent) values of the prefix are missing (null)
                // This branch is only ever taken for an ephemerally_full_prefix
                _current_block.header |= (uint64_t(1) << ((shift * 2) + 1));
            }
            ++_offset;
        }
        return true;
    }

    clustering_block get_value() const { return _current_block; };

    explicit operator bool() const {
        return (_offset < _serialization_limit_size);
    }
};

template <typename W>
requires Writer<W>
static void write(sstable_version_types v, W& out, const clustering_block& block) {
    write_vint(out, block.header);
    for (const auto& block_value: block.values) {
        write_cell_value(v, out, block_value.type, block_value.value);
    }
}

template <typename W>
requires Writer<W>
void write_clustering_prefix(sstable_version_types v, W& out, const schema& s,
    const clustering_key_prefix& prefix, ephemerally_full_prefix is_ephemerally_full) {
    clustering_blocks_input_range range{s, prefix, is_ephemerally_full};
    for (const auto block: range) {
        write(v, out, block);
    }
}

// This range generates a sequence of values that represent information
// about missing columns for SSTables 3.0 format.
class missing_columns_input_range
    : public input_range_base<missing_columns_input_range, uint64_t> {
private:
    const indexed_columns& _columns;
    const row& _row;
    mutable uint64_t _current_value = 0;
    mutable size_t _current_index = 0;
    mutable bool _large_mode_produced_size = false;

    enum class encoding_mode {
        small,
        large_encode_present,
        large_encode_missing,
    } _mode;

public:
    missing_columns_input_range(const indexed_columns& columns, const row& row)
        : _columns(columns)
        , _row(row) {

        auto row_size = _row.size();
        auto total_size = _columns.size();

        _current_index = row_size < total_size ? 0 : total_size;
        _mode = (total_size < 64)           ? encoding_mode::small :
                (row_size < total_size / 2) ? encoding_mode::large_encode_present :
                encoding_mode::large_encode_missing;
    }

    bool next() const {
        auto total_size = _columns.size();
        if (_current_index == total_size) {
            // No more values to encode
            return false;
        }

        if (_mode ==  encoding_mode::small) {
            // Set bit for every missing column
            for (const auto& element: _columns | boost::adaptors::indexed()) {
                auto cell = _row.find_cell(element.value().get().id);
                if (!cell) {
                    _current_value |= (uint64_t(1) << element.index());
                }
            }
            _current_index = total_size;
            return true;
        } else {
            // For either of large modes, output the difference between total size and row size first
            if (!_large_mode_produced_size) {
                _current_value = total_size - _row.size();
                _large_mode_produced_size = true;
                return true;
            }

            if (_mode == encoding_mode::large_encode_present) {
                while (_current_index < total_size) {
                    auto cell = _row.find_cell(_columns[_current_index].get().id);
                    if (cell) {
                        _current_value = _current_index;
                        ++_current_index;
                        return true;
                    }
                    ++_current_index;
                }
            } else {
                assert(_mode == encoding_mode::large_encode_missing);
                while (_current_index < total_size) {
                    auto cell = _row.find_cell(_columns[_current_index].get().id);
                    if (!cell) {
                        _current_value = _current_index;
                        ++_current_index;
                        return true;
                    }
                    ++_current_index;
                }
            }
        }

        return false;
    }

    uint64_t get_value() const { return _current_value; }

    explicit operator bool() const
    {
        return (_current_index < _columns.size());
    }
};

template <typename W>
requires Writer<W>
void write_missing_columns(W& out, const indexed_columns& columns, const row& row) {
    for (const auto value: missing_columns_input_range{columns, row}) {
        write_vint(out, value);
    }
}

template <typename T, typename W>
requires Writer<W>
void write_unsigned_delta_vint(W& out, T value, T base) {
    using unsigned_type = std::make_unsigned_t<T>;
    unsigned_type unsigned_delta = static_cast<unsigned_type>(value) - static_cast<unsigned_type>(base);
    // sign-extend to 64-bits
    using signed_type = std::make_signed_t<T>;
    int64_t delta = static_cast<int64_t>(static_cast<signed_type>(unsigned_delta));
    // write as unsigned 64-bit varint
    write_vint(out, static_cast<uint64_t>(delta));
}

template <typename W>
requires Writer<W>
void write_delta_timestamp(W& out, api::timestamp_type timestamp, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, timestamp, enc_stats.min_timestamp);
}

template <typename W>
requires Writer<W>
void write_delta_ttl(W& out, gc_clock::duration ttl, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, ttl.count(), enc_stats.min_ttl.count());
}

template <typename W>
requires Writer<W>
void write_delta_local_deletion_time(W& out, int64_t local_deletion_time, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, local_deletion_time, enc_stats.min_local_deletion_time.time_since_epoch().count());
}

template <typename W>
requires Writer<W>
void write_delta_local_deletion_time(W& out, gc_clock::time_point ldt, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, ldt.time_since_epoch().count(), enc_stats.min_local_deletion_time.time_since_epoch().count());
}

static bytes_array_vint_size to_bytes_array_vint_size(bytes b) {
    bytes_array_vint_size result;
    result.value = std::move(b);
    return result;
}

static bytes_array_vint_size to_bytes_array_vint_size(const sstring& s) {
    bytes_array_vint_size result;
    result.value = to_bytes(s);
    return result;
}

static sstring pk_type_to_string(const schema& s) {
    if (s.partition_key_size() == 1) {
        return s.partition_key_columns().begin()->type->name();
    } else {
        sstring type_params = ::join(",", s.partition_key_columns()
                                          | boost::adaptors::transformed(std::mem_fn(&column_definition::type))
                                          | boost::adaptors::transformed(std::mem_fn(&abstract_type::name)));
        return "org.apache.cassandra.db.marshal.CompositeType(" + type_params + ")";
    }
}

struct sstable_schema {
    serialization_header header;
    indexed_columns regular_columns;
    indexed_columns static_columns;
};

static
sstable_schema make_sstable_schema(const schema& s, const encoding_stats& enc_stats, const sstable_writer_config& cfg) {
    sstable_schema sst_sch;
    serialization_header& header = sst_sch.header;
    // mc serialization header minimum values are delta-encoded based on the default timestamp epoch times
    // Note: We rely on implicit conversion to uint64_t when subtracting the signed epoch values below
    // for preventing signed integer overflow.
    header.min_timestamp_base.value = static_cast<uint64_t>(enc_stats.min_timestamp) - encoding_stats::timestamp_epoch;
    header.min_local_deletion_time_base.value = static_cast<uint64_t>(enc_stats.min_local_deletion_time.time_since_epoch().count()) - encoding_stats::deletion_time_epoch;
    header.min_ttl_base.value = static_cast<uint64_t>(enc_stats.min_ttl.count()) - encoding_stats::ttl_epoch;

    header.pk_type_name = to_bytes_array_vint_size(pk_type_to_string(s));

    header.clustering_key_types_names.elements.reserve(s.clustering_key_size());
    for (const auto& ck_column : s.clustering_key_columns()) {
        auto ck_type_name = to_bytes_array_vint_size(ck_column.type->name());
        header.clustering_key_types_names.elements.push_back(std::move(ck_type_name));
    }

    auto add_column = [&] (const column_definition& column) {
        serialization_header::column_desc cd;
        cd.name = to_bytes_array_vint_size(column.name());
        cd.type_name = to_bytes_array_vint_size(column.type->name());
        if (column.is_static()) {
            header.static_columns.elements.push_back(std::move(cd));
            sst_sch.static_columns.push_back(column);
        } else if (column.is_regular()) {
            header.regular_columns.elements.push_back(std::move(cd));
            sst_sch.regular_columns.push_back(column);
        }
    };

    for (const auto& column : s.v3().all_columns()) {
        add_column(column);
    }


    // For static and regular columns, we write all simple columns first followed by collections
    // These containers have columns partitioned by atomicity
    auto pred = [] (const std::reference_wrapper<const column_definition>& column) { return column.get().is_atomic(); };
    boost::range::stable_partition(sst_sch.regular_columns, pred);
    boost::range::stable_partition(sst_sch.static_columns, pred);

    return sst_sch;
}

enum class cell_flags : uint8_t {
    none = 0x00,
    is_deleted_mask = 0x01, // Whether the cell is a tombstone or not.
    is_expiring_mask = 0x02, // Whether the cell is expiring.
    has_empty_value_mask = 0x04, // Whether the cell has an empty value. This will be the case for a tombstone in particular.
    use_row_timestamp_mask = 0x08, // Whether the cell has the same timestamp as the row this is a cell of.
    use_row_ttl_mask = 0x10, // Whether the cell has the same TTL as the row this is a cell of.
};

inline cell_flags operator& (cell_flags lhs, cell_flags rhs) {
    return cell_flags(static_cast<uint8_t>(lhs) & static_cast<uint8_t>(rhs));
}

inline cell_flags& operator |= (cell_flags& lhs, cell_flags rhs) {
    lhs = cell_flags(static_cast<uint8_t>(lhs) | static_cast<uint8_t>(rhs));
    return lhs;
}

enum class row_flags : uint8_t {
    none = 0x00,
    // Signal the end of the partition. Nothing follows a <flags> field with that flag.
        end_of_partition = 0x01,
    // Whether the encoded unfiltered is a marker or a row. All following flags apply only to rows.
        is_marker = 0x02,
    // Whether the encoded row has a timestamp (i.e. its liveness_info is not empty).
        has_timestamp = 0x04,
    // Whether the encoded row has some expiration info (i.e. if its liveness_info contains TTL and local_deletion).
        has_ttl = 0x08,
    // Whether the encoded row has some deletion info.
        has_deletion = 0x10,
    // Whether the encoded row has all of the columns from the header present.
        has_all_columns = 0x20,
    // Whether the encoded row has some complex deletion for at least one of its complex columns.
        has_complex_deletion = 0x40,
    // If present, another byte is read containing the "extended flags" below.
        extension_flag = 0x80
};

inline row_flags operator& (row_flags lhs, row_flags rhs) {
    return row_flags(static_cast<uint8_t>(lhs) & static_cast<uint8_t>(rhs));
}

inline row_flags& operator |= (row_flags& lhs, row_flags rhs) {
    lhs = row_flags(static_cast<uint8_t>(lhs) | static_cast<uint8_t>(rhs));
    return lhs;
}

enum class row_extended_flags : uint8_t {
    none = 0x00,
    // Whether the encoded row is a static. If there is no extended flag, the row is assumed not static.
        is_static = 0x01,
    // Cassandra-specific flag, indicates whether the row deletion is shadowable.
    // This flag is deprecated in Origin - see CASSANDRA-11500.
    // This flag is never set by Scylla and it fails to read files that have it set.
        has_shadowable_deletion_cassandra = 0x02,
    // Scylla-specific flag, indicates whether the row deletion is shadowable.
    // If set, the shadowable tombstone is writen right after the row deletion.
    // This is only used by Materialized Views that are not supposed to be exported.
        has_shadowable_deletion_scylla = 0x80,
};

// A range tombstone marker (RT marker) represents a bound of a range tombstone
// in a SSTables 3.x ('m') data file.
// RT markers can be of two types called "bounds" and "boundaries" in Origin nomenclature.
//
// A bound simply represents either a start or an end bound of a full range tombstone.
//
// A boundary can be thought of as two merged adjacent bounds and is used to represent adjacent
// range tombstones. An RT marker of a boundary type has two tombstones corresponding to two
// range tombstones this boundary belongs to.
struct rt_marker {
    clustering_key_prefix clustering;
    bound_kind_m kind;
    tombstone tomb;
    std::optional<tombstone> boundary_tomb; // only engaged for rt_marker of a boundary type

    position_in_partition_view position() const {
        if (kind == bound_kind_m::incl_start || kind == bound_kind_m::excl_end_incl_start) {
            return position_in_partition_view::before_key(clustering);
        }
        return position_in_partition_view::after_key(clustering);
    }

    // We need this one to uniformly write rows and RT markers inside write_clustered().
    const clustering_key_prefix& key() const { return clustering; }
};

static bound_kind_m get_kind(const clustering_row&) {
    return bound_kind_m::clustering;
}

static bound_kind_m get_kind(const rt_marker& marker) {
    return marker.kind;
}

template<typename T>
concept Clustered = requires(T t) {
    { t.key() } -> std::convertible_to<const clustering_key_prefix&>;
    { get_kind(t) } -> std::same_as<bound_kind_m>;
};

// Used for writing SSTables in 'mc' format.
class writer : public sstable_writer::writer_impl {
private:
    const encoding_stats _enc_stats;
    shard_id _shard; // Specifies which shard the new SStable will belong to.
    bool _compression_enabled = false;
    std::unique_ptr<file_writer> _data_writer;
    std::unique_ptr<file_writer> _index_writer;
    bool _tombstone_written = false;
    bool _static_row_written = false;
    // The length of partition header (partition key, partition deletion and static row, if present)
    // as written to the data file
    // Used for writing promoted index
    uint64_t _partition_header_length = 0;
    uint64_t _prev_row_start = 0;
    std::optional<key> _partition_key;
    std::optional<key> _first_key, _last_key;
    index_sampling_state _index_sampling_state;
    bytes_ostream _tmp_bufs;

    const sstable_schema _sst_schema;

    struct cdef_and_collection {
        const column_definition* cdef;
        std::reference_wrapper<const atomic_cell_or_collection> collection;
    };

    // Used to defer writing collections until all atomic cells are written
    std::vector<cdef_and_collection> _collections;

    tombstone _current_tombstone;

    struct clustering_info {
        clustering_key_prefix clustering;
        bound_kind_m kind;
    };
    struct pi_block {
        clustering_info first;
        clustering_info last;
        uint64_t offset;
        uint64_t width;
        std::optional<tombstone> open_marker;
    };
    // _pi_write_m is used temporarily for building the promoted
    // index (column sample) of one partition when writing a new sstable.
    struct {
        // Unfortunately we cannot output the promoted index directly to the
        // index file because it needs to be prepended by its size.
        // first_entry is used for deferring serialization into blocks for small partitions.
        std::optional<pi_block> first_entry;
        bytes_ostream blocks; // Serialized pi_blocks.
        bytes_ostream offsets; // Serialized block offsets (uint32_t) relative to the start of "blocks".
        uint64_t promoted_index_size = 0; // Number of pi_blocks inside blocks and first_entry;
        tombstone tomb;
        uint64_t block_start_offset;
        uint64_t block_next_start_offset;
        std::optional<clustering_info> first_clustering;
        std::optional<clustering_info> last_clustering;
        size_t desired_block_size;
    } _pi_write_m;
    utils::UUID _run_identifier;
    bool _write_regular_as_static; // See #4139
    scylla_metadata::large_data_stats _large_data_stats;

    void init_file_writers();

    // Returns the closed writer
    std::unique_ptr<file_writer> close_writer(std::unique_ptr<file_writer>& w);

    void close_data_writer();
    void ensure_tombstone_is_written() {
        if (!_tombstone_written) {
            consume(tombstone());
        }
    }

    void ensure_static_row_is_written_if_needed() {
        if (!_sst_schema.static_columns.empty() && !_static_row_written) {
            consume(static_row{});
        }
    }

    void drain_tombstones(std::optional<position_in_partition_view> pos = {});

    void maybe_add_summary_entry(const dht::token& token, bytes_view key) {
        return sstables::maybe_add_summary_entry(
            _sst._components->summary, token, key, get_data_offset(),
            _index_writer->offset(), _index_sampling_state);
    }

    void maybe_set_pi_first_clustering(const clustering_info& info);
    void maybe_add_pi_block();
    void add_pi_block();
    void write_pi_block(const pi_block&);

    uint64_t get_data_offset() const {
        if (_sst.has_component(component_type::CompressionInfo)) {
            // Variable returned by compressed_file_length() is constantly updated by compressed output stream.
            return _sst._components->compression.compressed_file_length();
        } else {
            return _data_writer->offset();
        }
    }

    void write_delta_timestamp(bytes_ostream& writer, api::timestamp_type timestamp) {
        sstables::mc::write_delta_timestamp(writer, timestamp, _enc_stats);
    }
    void write_delta_ttl(bytes_ostream& writer, gc_clock::duration ttl) {
        sstables::mc::write_delta_ttl(writer, ttl, _enc_stats);
    }
    void write_delta_local_deletion_time(bytes_ostream& writer, gc_clock::time_point ldt) {
        sstables::mc::write_delta_local_deletion_time(writer, ldt, _enc_stats);
    }
    void do_write_delta_deletion_time(bytes_ostream& writer, const tombstone& t) {
        sstables::mc::write_delta_timestamp(writer, t.timestamp, _enc_stats);
        sstables::mc::write_delta_local_deletion_time(writer, t.deletion_time, _enc_stats);
    }
    void write_delta_deletion_time(bytes_ostream& writer, const tombstone& t) {
        if (t) {
            do_write_delta_deletion_time(writer, t);
        } else {
            sstables::mc::write_delta_timestamp(writer, api::missing_timestamp, _enc_stats);
            sstables::mc::write_delta_local_deletion_time(writer, no_deletion_time, _enc_stats);
        }
    }

    deletion_time to_deletion_time(tombstone t) {
        deletion_time dt;
        if (t) {
            bool capped;
            int32_t ldt = adjusted_local_deletion_time(t.deletion_time, capped);
            if (capped) {
                slogger.warn("Capping tombstone local_deletion_time {} to max {}", t.deletion_time.time_since_epoch().count(), ldt);
                _sst.get_stats().on_capped_tombstone_deletion_time();
            }
            dt.local_deletion_time = ldt;
            dt.marked_for_delete_at = t.timestamp;
        } else {
            // Default values for live, non-deleted rows.
            dt.local_deletion_time = no_deletion_time;
            dt.marked_for_delete_at = api::missing_timestamp;
        }
        return dt;
    }

    struct row_time_properties {
        std::optional<api::timestamp_type> timestamp;
        std::optional<gc_clock::duration> ttl;
        std::optional<gc_clock::time_point> local_deletion_time;
    };

    void maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows);
    void maybe_record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const uint64_t row_size);
    void maybe_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size);

    // Writes single atomic cell
    void write_cell(bytes_ostream& writer, const clustering_key_prefix* clustering_key, atomic_cell_view cell, const column_definition& cdef,
        const row_time_properties& properties, std::optional<bytes_view> cell_path = {});

    // Writes information about row liveness (formerly 'row marker')
    void write_liveness_info(bytes_ostream& writer, const row_marker& marker);

    // Writes a collection of cells, representing either a CQL collection or fields of a non-frozen user type
    void write_collection(bytes_ostream& writer, const clustering_key_prefix* clustering_key, const column_definition& cdef, collection_mutation_view collection,
        const row_time_properties& properties, bool has_complex_deletion);

    void write_cells(bytes_ostream& writer, const clustering_key_prefix* clustering_key, column_kind kind,
        const row& row_body, const row_time_properties& properties, bool has_complex_deletion);
    void write_row_body(bytes_ostream& writer, const clustering_row& row, bool has_complex_deletion);
    void write_static_row(const row&, column_kind);
    void collect_row_stats(uint64_t row_size, const clustering_key_prefix* clustering_key) {
        ++_c_stats.rows_count;
        maybe_record_large_rows(_sst, *_partition_key, clustering_key, row_size);
    }

    // Clustered is a term used to denote an entity that has a clustering key prefix
    // and constitutes an entry of a partition.
    // Both clustered_rows and rt_markers are instances of Clustered
    void write_clustered(const clustering_row& clustered_row, uint64_t prev_row_size);
    void write_clustered(const rt_marker& marker, uint64_t prev_row_size);

    template <typename T>
    requires Clustered<T>
    void write_clustered(const T& clustered) {
        clustering_info info {clustered.key(), get_kind(clustered)};
        maybe_set_pi_first_clustering(info);
        uint64_t pos = _data_writer->offset();
        write_clustered(clustered, pos - _prev_row_start);
        _pi_write_m.last_clustering = info;
        _prev_row_start = pos;
        maybe_add_pi_block();
    }
    void write_promoted_index();
    void consume(rt_marker&& marker);

    // Must be called in a seastar thread.
    void flush_tmp_bufs(file_writer& writer) {
        for (auto&& buf : _tmp_bufs) {
            thread::maybe_yield();
            writer.write(buf);
        }
        _tmp_bufs.clear();
    }

    void flush_tmp_bufs() {
        flush_tmp_bufs(*_data_writer);
    }
public:

    writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats,
        const io_priority_class& pc, shard_id shard = this_shard_id())
        : sstable_writer::writer_impl(sst, s, pc, cfg)
        , _enc_stats(enc_stats)
        , _shard(shard)
        , _tmp_bufs(_sst.sstable_buffer_size)
        , _sst_schema(make_sstable_schema(s, _enc_stats, _cfg))
        , _run_identifier(cfg.run_identifier)
        , _write_regular_as_static(s.is_static_compact_table())
        , _large_data_stats({{
                {
                    large_data_type::partition_size,
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_partition_threshold_bytes(),
                    }
                },
                {
                    large_data_type::row_size,
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_row_threshold_bytes(),
                    }
                },
                {
                    large_data_type::cell_size,
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_cell_threshold_bytes(),
                    }
                },
                {
                    large_data_type::rows_in_partition,
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_rows_count_threshold(),
                    }
                },
            }})
    {
        // This can be 0 in some cases, which is albeit benign, can wreak havoc
        // in lower-level writer code, so clamp it to [1, +inf) here, which is
        // exactly what callers used to do anyway.
        estimated_partitions = std::max(uint64_t(1), estimated_partitions);

        _sst.generate_toc(_schema.get_compressor_params().get_compressor(), _schema.bloom_filter_fp_chance());
        _sst.write_toc(_pc);
        _sst.create_data().get();
        _compression_enabled = !_sst.has_component(component_type::CRC);
        init_file_writers();
        _sst._shards = { shard };

        _cfg.monitor->on_write_started(_data_writer->offset_tracker());
        _sst._components->filter = utils::i_filter::get_filter(estimated_partitions, _schema.bloom_filter_fp_chance(), utils::filter_format::m_format);
        _pi_write_m.desired_block_size = cfg.promoted_index_block_size;
        _index_sampling_state.summary_byte_cost = _cfg.summary_byte_cost;
        prepare_summary(_sst._components->summary, estimated_partitions, _schema.min_index_interval());
    }

    ~writer();
    writer(writer&& o) = default;
    void consume_new_partition(const dht::decorated_key& dk) override;
    void consume(tombstone t) override;
    stop_iteration consume(static_row&& sr) override;
    stop_iteration consume(clustering_row&& cr) override;
    stop_iteration consume(range_tombstone_change&& rtc) override;
    stop_iteration consume_end_of_partition() override;
    void consume_end_of_stream() override;
};

writer::~writer() {
    auto close_writer = [](auto& writer) {
        if (writer) {
            try {
                writer->close();
            } catch (...) {
                sstlog.error("writer failed to close file: {}", std::current_exception());
            }
        }
    };
    close_writer(_index_writer);
    close_writer(_data_writer);
}

void writer::maybe_set_pi_first_clustering(const writer::clustering_info& info) {
    uint64_t pos = _data_writer->offset();
    if (!_pi_write_m.first_clustering) {
        _pi_write_m.first_clustering = info;
        _pi_write_m.block_start_offset = pos;
        _pi_write_m.block_next_start_offset = pos + _pi_write_m.desired_block_size;
    }
}

void writer::add_pi_block() {
    auto block = pi_block{
        *_pi_write_m.first_clustering,
        *_pi_write_m.last_clustering,
        _pi_write_m.block_start_offset - _c_stats.start_offset,
        _data_writer->offset() - _pi_write_m.block_start_offset,
        (_current_tombstone ? std::make_optional(_current_tombstone) : std::optional<tombstone>{})};

    if (_pi_write_m.blocks.empty()) {
        if (!_pi_write_m.first_entry) {
            _pi_write_m.first_entry.emplace(std::move(block));
            ++_pi_write_m.promoted_index_size;
            return;
        } else {
            write_pi_block(*_pi_write_m.first_entry);
        }
    }

    write_pi_block(block);
    ++_pi_write_m.promoted_index_size;
}

void writer::maybe_add_pi_block() {
    uint64_t pos = _data_writer->offset();
    if (pos >= _pi_write_m.block_next_start_offset) {
        add_pi_block();
        _pi_write_m.first_clustering.reset();
        _pi_write_m.block_next_start_offset = pos + _pi_write_m.desired_block_size;
    }
}

void writer::init_file_writers() {
    file_output_stream_options options;
    options.io_priority_class = _pc;
    options.buffer_size = _sst.sstable_buffer_size;
    options.write_behind = 10;

    if (!_compression_enabled) {
        auto out = make_file_data_sink(std::move(_sst._data_file), options).get0();
        _data_writer = std::make_unique<crc32_checksummed_file_writer>(std::move(out), options.buffer_size, _sst.filename(component_type::Data));
    } else {
        auto out = make_file_output_stream(std::move(_sst._data_file), options).get0();
        _data_writer = std::make_unique<file_writer>(
            make_compressed_file_m_format_output_stream(
                std::move(out),
                &_sst._components->compression,
                _schema.get_compressor_params()), _sst.filename(component_type::Data));
    }
    auto w = file_writer::make(std::move(_sst._index_file), std::move(options), _sst.filename(component_type::Index));
    _index_writer = std::make_unique<file_writer>(w.get0());
}

std::unique_ptr<file_writer> writer::close_writer(std::unique_ptr<file_writer>& w) {
    auto writer = std::move(w);
    writer->close();
    return writer;
}

void writer::close_data_writer() {
    auto writer = close_writer(_data_writer);
    if (!_compression_enabled) {
        auto chksum_wr = static_cast<crc32_checksummed_file_writer*>(writer.get());
        _sst.write_digest(chksum_wr->full_checksum());
        _sst.write_crc(chksum_wr->finalize_checksum());
    } else {
        _sst.write_digest(_sst._components->compression.get_full_checksum());
    }
}

void writer::consume_new_partition(const dht::decorated_key& dk) {
    _c_stats.start_offset = _data_writer->offset();
    _prev_row_start = _data_writer->offset();

    _partition_key = key::from_partition_key(_schema, dk.key());
    maybe_add_summary_entry(dk.token(), bytes_view(*_partition_key));

    _sst._components->filter->add(bytes_view(*_partition_key));
    _collector.add_key(bytes_view(*_partition_key));

    auto p_key = disk_string_view<uint16_t>();
    p_key.value = bytes_view(*_partition_key);

    // Write index file entry from partition key into index file.
    // Write an index entry minus the "promoted index" (sample of columns)
    // part. We can only write that after processing the entire partition
    // and collecting the sample of columns.
    write(_sst.get_version(), *_index_writer, p_key);
    write_vint(*_index_writer, _data_writer->offset());

    _pi_write_m.first_entry.reset();
    _pi_write_m.blocks.clear();
    _pi_write_m.offsets.clear();
    _pi_write_m.promoted_index_size = 0;
    _pi_write_m.tomb = {};
    _pi_write_m.first_clustering.reset();
    _pi_write_m.last_clustering.reset();

    write(_sst.get_version(), *_data_writer, p_key);
    _partition_header_length = _data_writer->offset() - _c_stats.start_offset;

    _tombstone_written = false;
    _static_row_written = false;
}

void writer::consume(tombstone t) {
    uint64_t current_pos = _data_writer->offset();
    auto dt = to_deletion_time(t);
    write(_sst.get_version(), *_data_writer, dt);
    _partition_header_length += (_data_writer->offset() - current_pos);
    _c_stats.update(t);

    _pi_write_m.tomb = t;
    _tombstone_written = true;

    if (t) {
        _collector.update_min_max_components(position_in_partition_view::before_all_clustered_rows());
        _collector.update_min_max_components(position_in_partition_view::after_all_clustered_rows());
    }
}

void writer::maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key,
                                           uint64_t partition_size, uint64_t rows) {
    auto& size_entry = _large_data_stats.map.at(large_data_type::partition_size);
    auto& row_count_entry = _large_data_stats.map.at(large_data_type::rows_in_partition);
    size_entry.max_value = std::max(size_entry.max_value, partition_size);
    row_count_entry.max_value = std::max(row_count_entry.max_value, rows);
    auto ret = _sst.get_large_data_handler().maybe_record_large_partitions(sst, partition_key, partition_size, rows).get0();
    size_entry.above_threshold += unsigned(bool(ret.size));
    row_count_entry.above_threshold += unsigned(bool(ret.rows));
}

void writer::maybe_record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) {
    auto& entry = _large_data_stats.map.at(large_data_type::row_size);
    if (entry.max_value < row_size) {
        entry.max_value = row_size;
    }
    if (_sst.get_large_data_handler().maybe_record_large_rows(sst, partition_key, clustering_key, row_size).get0()) {
        entry.above_threshold++;
    };
}

void writer::maybe_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size) {
    auto& entry = _large_data_stats.map.at(large_data_type::cell_size);
    if (entry.max_value < cell_size) {
        entry.max_value = cell_size;
    }
    if (_sst.get_large_data_handler().maybe_record_large_cells(_sst, *_partition_key, clustering_key, cdef, cell_size).get0()) {
        entry.above_threshold++;
    };
}

void writer::write_cell(bytes_ostream& writer, const clustering_key_prefix* clustering_key, atomic_cell_view cell,
         const column_definition& cdef, const row_time_properties& properties, std::optional<bytes_view> cell_path) {

    uint64_t current_pos = writer.size();
    bool is_deleted = !cell.is_live();
    bool has_value = !is_deleted && !cell.value().empty();
    bool use_row_timestamp = (properties.timestamp == cell.timestamp());
    bool is_row_expiring = properties.ttl.has_value();
    bool is_cell_expiring = cell.is_live_and_has_ttl();
    bool use_row_ttl = is_row_expiring && is_cell_expiring &&
                       properties.ttl == cell.ttl() &&
                       properties.local_deletion_time == cell.deletion_time();

    cell_flags flags = cell_flags::none;
    if ((!has_value && !cdef.is_counter()) || is_deleted) {
        flags |= cell_flags::has_empty_value_mask;
    }
    if (is_deleted) {
        flags |= cell_flags::is_deleted_mask;
    } else if (is_cell_expiring) {
        flags |= cell_flags::is_expiring_mask;
    }
    if (use_row_timestamp) {
        flags |= cell_flags::use_row_timestamp_mask;
    }
    if (use_row_ttl) {
        flags |= cell_flags::use_row_ttl_mask;
    }
    write(_sst.get_version(), writer, flags);

    if (!use_row_timestamp) {
        write_delta_timestamp(writer, cell.timestamp());
    }

    if (!use_row_ttl) {
        if (is_deleted) {
            write_delta_local_deletion_time(writer, cell.deletion_time());
        } else if (is_cell_expiring) {
            write_delta_local_deletion_time(writer, cell.expiry());
            write_delta_ttl(writer, cell.ttl());
        }
    }

    if (bool(cell_path)) {
        write_vint(writer, cell_path->size());
        write(_sst.get_version(), writer, *cell_path);
    }

    if (cdef.is_counter()) {
        if (!is_deleted) {
            assert(!cell.is_counter_update());
            auto ccv = counter_cell_view(cell);
            write_counter_value(ccv, writer, _sst.get_version(), [] (bytes_ostream& out, uint32_t value) {
                return write_vint(out, value);
            });
        }
    } else {
        if (has_value) {
            write_cell_value(_sst.get_version(), writer, *cdef.type, cell.value());
        }
    }

    // Collect cell statistics
    // We record collections in write_collection, so ignore them here
    if (cdef.is_atomic()) {
        uint64_t size = writer.size() - current_pos;
        maybe_record_large_cells(_sst, *_partition_key, clustering_key, cdef, size);
    }

    _c_stats.update_timestamp(cell.timestamp());
    if (is_deleted) {
        _c_stats.update_local_deletion_time_and_tombstone_histogram(cell.deletion_time());
        _sst.get_stats().on_cell_tombstone_write();
        return;
    }

    if (is_cell_expiring) {
        _c_stats.update_ttl(cell.ttl());
        // tombstone histogram is updated with expiration time because if ttl is longer
        // than gc_grace_seconds for all data, sstable will be considered fully expired
        // when actually nothing is expired.
        _c_stats.update_local_deletion_time_and_tombstone_histogram(cell.expiry());
    } else { // regular live cell
        _c_stats.update_local_deletion_time(std::numeric_limits<int>::max());
    }
    _sst.get_stats().on_cell_write();
}

void writer::write_liveness_info(bytes_ostream& writer, const row_marker& marker) {
    if (marker.is_missing()) {
        return;
    }

    api::timestamp_type timestamp = marker.timestamp();
    _c_stats.update_timestamp(timestamp);
    write_delta_timestamp(writer, timestamp);

    auto write_expiring_liveness_info = [this, &writer] (gc_clock::duration ttl, gc_clock::time_point ldt) {
        _c_stats.update_ttl(ttl);
        _c_stats.update_local_deletion_time_and_tombstone_histogram(ldt);
        write_delta_ttl(writer, ttl);
        write_delta_local_deletion_time(writer, ldt);
    };
    if (!marker.is_live()) {
        write_expiring_liveness_info(gc_clock::duration(expired_liveness_ttl), marker.deletion_time());
    } else if (marker.is_expiring()) {
        write_expiring_liveness_info(marker.ttl(), marker.expiry());
    } else {
        _c_stats.update_ttl(0);
        _c_stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

void writer::write_collection(bytes_ostream& writer, const clustering_key_prefix* clustering_key,
        const column_definition& cdef, collection_mutation_view collection, const row_time_properties& properties,
        bool has_complex_deletion) {
    uint64_t current_pos = writer.size();
    collection.with_deserialized(*cdef.type, [&] (collection_mutation_view_description mview) {
        if (has_complex_deletion) {
            write_delta_deletion_time(writer, mview.tomb);
            _c_stats.update(mview.tomb);
        }

        write_vint(writer, mview.cells.size());
        if (!mview.cells.empty()) {
            ++_c_stats.column_count;
        }
        for (const auto& [cell_path, cell]: mview.cells) {
            ++_c_stats.cells_count;
            write_cell(writer, clustering_key, cell, cdef, properties, cell_path);
        }
    });
    uint64_t size = writer.size() - current_pos;
    maybe_record_large_cells(_sst, *_partition_key, clustering_key, cdef, size);
}

void writer::write_cells(bytes_ostream& writer, const clustering_key_prefix* clustering_key, column_kind kind, const row& row_body,
    const row_time_properties& properties, bool has_complex_deletion) {
    // Note that missing columns are written based on the whole set of regular columns as defined by schema.
    // This differs from Origin where all updated columns are tracked and the set of filled columns of a row
    // is compared with the set of all columns filled in the memtable. So our encoding may be less optimal in some cases
    // but still valid.
    write_missing_columns(writer, kind == column_kind::static_column ? _sst_schema.static_columns : _sst_schema.regular_columns, row_body);
    row_body.for_each_cell([this, &writer, kind, &properties, has_complex_deletion, clustering_key] (column_id id, const atomic_cell_or_collection& c) {
        auto&& column_definition = _schema.column_at(kind, id);
        if (!column_definition.is_atomic()) {
            _collections.push_back({&column_definition, c});
            return;
        }
        atomic_cell_view cell = c.as_atomic_cell(column_definition);
        ++_c_stats.cells_count;
        ++_c_stats.column_count;
        write_cell(writer, clustering_key, cell, column_definition, properties);
    });

    for (const auto& col: _collections) {
        write_collection(writer, clustering_key, *col.cdef, col.collection.get().as_collection_mutation(), properties, has_complex_deletion);
    }
    _collections.clear();
}

void writer::write_row_body(bytes_ostream& writer, const clustering_row& row, bool has_complex_deletion) {
    write_liveness_info(writer, row.marker());
    auto write_tombstone_and_update_stats = [this, &writer] (const tombstone& t) {
        _c_stats.do_update(t);
        do_write_delta_deletion_time(writer, t);
    };
    if (row.tomb().regular()) {
        write_tombstone_and_update_stats(row.tomb().regular());
    }
    if (row.tomb().is_shadowable()) {
        write_tombstone_and_update_stats(row.tomb().tomb());
    }
    row_time_properties properties;
    if (!row.marker().is_missing()) {
        properties.timestamp = row.marker().timestamp();
        if (row.marker().is_expiring()) {
            properties.ttl = row.marker().ttl();
            properties.local_deletion_time = row.marker().deletion_time();
        }
    }

    return write_cells(writer, &row.key(), column_kind::regular_column, row.cells(), properties, has_complex_deletion);
}

// Find if any complex column (collection or non-frozen user type) in the row contains a column-wide tombstone
static bool row_has_complex_deletion(const schema& s, const row& r, column_kind kind) {
    bool result = false;
    r.for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& c) {
        auto&& cdef = s.column_at(kind, id);
        if (cdef.is_atomic()) {
            return stop_iteration::no;
        }
        return c.as_collection_mutation().with_deserialized(*cdef.type, [&] (collection_mutation_view_description mview) {
            if (mview.tomb) {
                result = true;
            }
            return stop_iteration(static_cast<bool>(mview.tomb));
        });
    });

    return result;
}

void writer::write_static_row(const row& static_row, column_kind kind) {
    uint64_t current_pos = _data_writer->offset();
    // Static row flag is stored in extended flags so extension_flag is always set for static rows
    row_flags flags = row_flags::extension_flag;
    if (static_row.size() == _sst_schema.static_columns.size()) {
        flags |= row_flags::has_all_columns;
    }
    bool has_complex_deletion = row_has_complex_deletion(_schema, static_row, kind);
    if (has_complex_deletion) {
        flags |= row_flags::has_complex_deletion;
    }
    write(_sst.get_version(), *_data_writer, flags);
    write(_sst.get_version(), *_data_writer, row_extended_flags::is_static);

    write_vint(_tmp_bufs, 0); // as the static row always comes first, the previous row size is always zero
    write_cells(_tmp_bufs, nullptr, column_kind::static_column, static_row, row_time_properties{}, has_complex_deletion);
    write_vint(*_data_writer, _tmp_bufs.size());
    flush_tmp_bufs();

    _partition_header_length += (_data_writer->offset() - current_pos);

    collect_row_stats(_data_writer->offset() - current_pos, nullptr);
    _static_row_written = true;
}

stop_iteration writer::consume(static_row&& sr) {
    ensure_tombstone_is_written();
    write_static_row(sr.cells(), column_kind::static_column);
    return stop_iteration::no;
}

void writer::write_clustered(const clustering_row& clustered_row, uint64_t prev_row_size) {
    uint64_t current_pos = _data_writer->offset();
    row_flags flags = row_flags::none;
    row_extended_flags ext_flags = row_extended_flags::none;
    const row_marker& marker = clustered_row.marker();
    if (!marker.is_missing()) {
        flags |= row_flags::has_timestamp;
        if (!marker.is_live() || marker.is_expiring()) {
            flags |= row_flags::has_ttl;
        }
    }

    if (clustered_row.tomb().regular()) {
        flags |= row_flags::has_deletion;
    }
    if (clustered_row.tomb().is_shadowable()) {
        flags |= row_flags::extension_flag;
        ext_flags = row_extended_flags::has_shadowable_deletion_scylla;
    }

    if (clustered_row.cells().size() == _sst_schema.regular_columns.size()) {
        flags |= row_flags::has_all_columns;
    }
    bool has_complex_deletion = row_has_complex_deletion(_schema, clustered_row.cells(), column_kind::regular_column);
    if (has_complex_deletion) {
        flags |= row_flags::has_complex_deletion;
    }
    write(_sst.get_version(), *_data_writer, flags);
    if (ext_flags != row_extended_flags::none) {
        write(_sst.get_version(), *_data_writer, ext_flags);
    }

    write_clustering_prefix(_sst.get_version(), *_data_writer, _schema, clustered_row.key(), ephemerally_full_prefix{_schema.is_compact_table()});

    write_vint(_tmp_bufs, prev_row_size);
    write_row_body(_tmp_bufs, clustered_row, has_complex_deletion);

    uint64_t row_body_size = _tmp_bufs.size();
    write_vint(*_data_writer, row_body_size);
    flush_tmp_bufs();

    // Collect statistics
    _collector.update_min_max_components(clustered_row.position());
    collect_row_stats(_data_writer->offset() - current_pos, &clustered_row.key());
}

stop_iteration writer::consume(clustering_row&& cr) {
    if (_write_regular_as_static) {
        ensure_tombstone_is_written();
        write_static_row(cr.cells(), column_kind::regular_column);
        return stop_iteration::no;
    }
    ensure_tombstone_is_written();
    ensure_static_row_is_written_if_needed();
    write_clustered(cr);
    return stop_iteration::no;
}

// Write clustering prefix along with its bound kind and, if not full, its size
template <typename W>
requires Writer<W>
static void write_clustering_prefix(sstable_version_types v, W& writer, bound_kind_m kind,
    const schema& s, const clustering_key_prefix& clustering) {
    assert(kind != bound_kind_m::static_clustering);
    write(v, writer, kind);
    auto is_ephemerally_full = ephemerally_full_prefix{s.is_compact_table()};
    if (kind != bound_kind_m::clustering) {
        // Don't use ephemerally full for RT bounds as they're always non-full
        is_ephemerally_full = ephemerally_full_prefix::no;
        write(v, writer, static_cast<uint16_t>(clustering.size(s)));
    }
    write_clustering_prefix(v, writer, s, clustering, is_ephemerally_full);
}

void writer::write_promoted_index() {
    if (_pi_write_m.promoted_index_size < 2) {
        write_vint(*_index_writer, uint64_t(0));
        return;
    }
    write_vint(_tmp_bufs, _partition_header_length);
    write(_sst.get_version(), _tmp_bufs, to_deletion_time(_pi_write_m.tomb));
    write_vint(_tmp_bufs, _pi_write_m.promoted_index_size);
    uint64_t pi_size = _tmp_bufs.size() + _pi_write_m.blocks.size() + _pi_write_m.offsets.size();
    write_vint(*_index_writer, pi_size);
    flush_tmp_bufs(*_index_writer);
    write(_sst.get_version(), *_index_writer, _pi_write_m.blocks);
    write(_sst.get_version(), *_index_writer, _pi_write_m.offsets);
}

void writer::write_pi_block(const pi_block& block) {
    static constexpr size_t width_base = 65536;
    bytes_ostream& blocks = _pi_write_m.blocks;
    uint32_t offset = blocks.size();
    write(_sst.get_version(), _pi_write_m.offsets, offset);
    write_clustering_prefix(_sst.get_version(), blocks, block.first.kind, _schema, block.first.clustering);
    write_clustering_prefix(_sst.get_version(), blocks, block.last.kind, _schema, block.last.clustering);
    write_vint(blocks, block.offset);
    write_signed_vint(blocks, block.width - width_base);
    write(_sst.get_version(), blocks, static_cast<std::byte>(block.open_marker ? 1 : 0));
    if (block.open_marker) {
        write(_sst.get_version(), blocks, to_deletion_time(*block.open_marker));
    }
}

void writer::write_clustered(const rt_marker& marker, uint64_t prev_row_size) {
    write(_sst.get_version(), *_data_writer, row_flags::is_marker);
    write_clustering_prefix(_sst.get_version(), *_data_writer, marker.kind, _schema, marker.clustering);
    auto write_marker_body = [this, &marker] (bytes_ostream& writer) {
        write_delta_deletion_time(writer, marker.tomb);
        _c_stats.update(marker.tomb);
        if (marker.boundary_tomb) {
            do_write_delta_deletion_time(writer, *marker.boundary_tomb);
            _c_stats.do_update(*marker.boundary_tomb);
        }
    };

    write_vint(_tmp_bufs, prev_row_size);
    write_marker_body(_tmp_bufs);
    write_vint(*_data_writer, _tmp_bufs.size());
    flush_tmp_bufs();
    _collector.update_min_max_components(marker.position());
}

void writer::consume(rt_marker&& marker) {
    write_clustered(marker);
}

stop_iteration writer::consume(range_tombstone_change&& rtc) {
    ensure_tombstone_is_written();
    ensure_static_row_is_written_if_needed();
    position_in_partition_view pos = rtc.position();
    tombstone prev_tombstone = std::exchange(_current_tombstone, rtc.tombstone());
    if (!prev_tombstone) { // start bound
        auto bv = pos.as_start_bound_view();
        consume(rt_marker{pos.key(), to_bound_kind_m(bv.kind()), rtc.tombstone(), {}});
    } else if (!rtc.tombstone()) { // end bound
        auto bv = pos.as_end_bound_view();
        consume(rt_marker{pos.key(), to_bound_kind_m(bv.kind()), prev_tombstone, {}});
    } else { // boundary
        auto bk = pos.get_bound_weight() == bound_weight::before_all_prefixed
            ? bound_kind_m::excl_end_incl_start
            : bound_kind_m::incl_end_excl_start;
        consume(rt_marker{pos.key(), bk, prev_tombstone, rtc.tombstone()});
    }
    return stop_iteration::no;
}

stop_iteration writer::consume_end_of_partition() {
    ensure_tombstone_is_written();
    ensure_static_row_is_written_if_needed();

    write(_sst.get_version(), *_data_writer, row_flags::end_of_partition);

    if (_pi_write_m.promoted_index_size && _pi_write_m.first_clustering) {
        add_pi_block();
    }

    write_promoted_index();

    // compute size of the current row.
    _c_stats.partition_size = _data_writer->offset() - _c_stats.start_offset;

    maybe_record_large_partitions(_sst, *_partition_key, _c_stats.partition_size, _c_stats.rows_count);


    // update is about merging column_stats with the data being stored by collector.
    _collector.update(std::move(_c_stats));
    _c_stats.reset();

    if (!_first_key) {
        _first_key = *_partition_key;
    }
    _last_key = std::move(*_partition_key);
    _partition_key = std::nullopt;
    return get_data_offset() < _cfg.max_sstable_size ? stop_iteration::no : stop_iteration::yes;
}

void writer::consume_end_of_stream() {
    _cfg.monitor->on_data_write_completed();

    seal_summary(_sst._components->summary, std::move(_first_key), std::move(_last_key), _index_sampling_state).get();

    if (_sst.has_component(component_type::CompressionInfo)) {
        _collector.add_compression_ratio(_sst._components->compression.compressed_file_length(), _sst._components->compression.uncompressed_file_length());
    }

    close_writer(_index_writer);
    _sst.set_first_and_last_keys();

    _sst._components->statistics.contents[metadata_type::Serialization] = std::make_unique<serialization_header>(std::move(_sst_schema.header));
    seal_statistics(_sst.get_version(), _sst._components->statistics, _collector, _sst.compaction_ancestors(),
        _sst._schema->get_partitioner().name(), _schema.bloom_filter_fp_chance(),
        _sst._schema, _sst.get_first_decorated_key(), _sst.get_last_decorated_key(), _enc_stats);
    close_data_writer();
    _sst.write_summary(_pc);
    _sst.write_filter(_pc);
    _sst.write_statistics(_pc);
    _sst.write_compression(_pc);
    auto features = sstable_enabled_features::all();
    run_identifier identifier{_run_identifier};
    std::optional<scylla_metadata::large_data_stats> ld_stats(std::move(_large_data_stats));
    _sst.write_scylla_metadata(_pc, _shard, std::move(features), std::move(identifier), std::move(ld_stats), _cfg.origin);
    if (!_cfg.leave_unsealed) {
        _sst.seal_sstable(_cfg.backup).get();
    }
}

std::unique_ptr<sstable_writer::writer_impl> make_writer(sstable& sst,
        const schema& s,
        uint64_t estimated_partitions,
        const sstable_writer_config& cfg,
        encoding_stats enc_stats,
        const io_priority_class& pc,
        shard_id shard) {
    return std::make_unique<writer>(sst, s, estimated_partitions, cfg, enc_stats, pc, shard);
}

}
}
