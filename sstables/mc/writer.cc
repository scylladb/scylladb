/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "sstables/mc/writer.hh"
#include "sstables/writer.hh"
#include "encoding_stats.hh"
#include "schema.hh"
#include "mutation_fragment.hh"
#include "vint-serialization.hh"
#include "sstables/types.hh"
#include "sstables/mc/types.hh"
#include "db/config.hh"
#include "atomic_cell.hh"

#include <functional>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/container/static_vector.hpp>

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
        bytes_view value;
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
                bytes_view value = _prefix.get_component(_schema, _offset);
                if (value.empty()) {
                    _current_block.header |= (uint64_t(1) << (shift * 2));
                } else {
                    _current_block.values.push_back({value, *_prefix.get_compound_type(_schema)->types()[_offset]});
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
GCC6_CONCEPT(requires Writer<W>())
static void write(W& out, const clustering_block& block) {
    write_vint(out, block.header);
    for (const auto& [value, type]: block.values) {
        write_cell_value(out, type, value);
    }
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_clustering_prefix(W& out, const schema& s,
    const clustering_key_prefix& prefix, ephemerally_full_prefix is_ephemerally_full) {
    clustering_blocks_input_range range{s, prefix, is_ephemerally_full};
    for (const auto block: range) {
        write(out, block);
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
GCC6_CONCEPT(requires Writer<W>())
void write_missing_columns(W& out, const indexed_columns& columns, const row& row) {
    for (const auto value: missing_columns_input_range{columns, row}) {
        write_vint(out, value);
    }
}

template <typename T, typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_unsigned_delta_vint(W& out, T value, T base) {
    // sign-extend to 64-bits
    using signed_type = std::make_signed_t<T>;
    int64_t delta = static_cast<signed_type>(value) - static_cast<signed_type>(base);
    // write as unsigned 64-bit varint
    write_vint(out, static_cast<uint64_t>(delta));
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_timestamp(W& out, api::timestamp_type timestamp, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, timestamp, enc_stats.min_timestamp);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_ttl(W& out, int32_t ttl, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, ttl, enc_stats.min_ttl);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_local_deletion_time(W& out, int32_t local_deletion_time, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, local_deletion_time, enc_stats.min_local_deletion_time);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_deletion_time(W& out, deletion_time dt, const encoding_stats& enc_stats) {
    write_delta_timestamp(out, dt.marked_for_delete_at, enc_stats);
    write_delta_local_deletion_time(out, dt.local_deletion_time, enc_stats);
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

sstring type_name_with_udt_frozen(data_type type) {
    if (type->is_user_type()) {
        return "org.apache.cassandra.db.marshal.FrozenType(" + type->name() + ")";
    }
    return type->name();
}

static sstring pk_type_to_string(const schema& s) {
    if (s.partition_key_size() == 1) {
        return type_name_with_udt_frozen(s.partition_key_columns().begin()->type);
    } else {
        sstring type_params = ::join(",", s.partition_key_columns()
                                          | boost::adaptors::transformed(std::mem_fn(&column_definition::type))
                                          | boost::adaptors::transformed(type_name_with_udt_frozen));
        return "org.apache.cassandra.db.marshal.CompositeType(" + type_params + ")";
    }
}

serialization_header make_serialization_header(const schema& s, const encoding_stats& enc_stats) {
    serialization_header header;
    // mc serialization header minimum values are delta-encoded based on the default timestamp epoch times
    header.min_timestamp_base.value = static_cast<uint64_t>(enc_stats.min_timestamp - encoding_stats::timestamp_epoch);
    header.min_local_deletion_time_base.value = static_cast<uint64_t>(enc_stats.min_local_deletion_time - encoding_stats::deletion_time_epoch);
    header.min_ttl_base.value = static_cast<uint64_t>(enc_stats.min_ttl - encoding_stats::ttl_epoch);

    header.pk_type_name = to_bytes_array_vint_size(pk_type_to_string(s));

    header.clustering_key_types_names.elements.reserve(s.clustering_key_size());
    for (const auto& ck_column : s.clustering_key_columns()) {
        auto ck_type_name = to_bytes_array_vint_size(type_name_with_udt_frozen(ck_column.type));
        header.clustering_key_types_names.elements.push_back(std::move(ck_type_name));
    }

    header.static_columns.elements.reserve(s.static_columns_count());
    for (const auto& static_column : s.static_columns()) {
        serialization_header::column_desc cd;
        cd.name = to_bytes_array_vint_size(static_column.name());
        cd.type_name = to_bytes_array_vint_size(type_name_with_udt_frozen(static_column.type));
        header.static_columns.elements.push_back(std::move(cd));
    }

    header.regular_columns.elements.reserve(s.regular_columns_count());
    for (const auto& regular_column : s.regular_columns()) {
        serialization_header::column_desc cd;
        cd.name = to_bytes_array_vint_size(regular_column.name());
        cd.type_name = to_bytes_array_vint_size(type_name_with_udt_frozen(regular_column.type));
        header.regular_columns.elements.push_back(std::move(cd));
    }

    return header;
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
        return {
            position_in_partition_view::range_tombstone_tag_t(),
            bound_view{clustering, to_bound_kind(kind)}
        };
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

GCC6_CONCEPT(
    template<typename T>
    concept bool Clustered = requires(T t) {
        { t.key() } -> const clustering_key_prefix&;
        { get_kind(t) } -> bound_kind_m;
    };
)

static indexed_columns get_indexed_columns_partitioned_by_atomicity(schema::const_iterator_range_type columns) {
    indexed_columns result;
    result.reserve(columns.size());
    for (const auto& col: columns) {
        result.emplace_back(col);
    }
    boost::range::stable_partition(
            result,
            [](const std::reference_wrapper<const column_definition>& column) { return column.get().is_atomic();});
    return result;
}

// Used for writing SSTables in 'mc' format.
class writer : public sstable_writer::writer_impl {
private:
    encoding_stats _enc_stats;
    shard_id _shard; // Specifies which shard the new SStable will belong to.
    bool _compression_enabled = false;
    std::unique_ptr<file_writer> _data_writer;
    std::optional<file_writer> _index_writer;
    bool _tombstone_written = false;
    bool _static_row_written = false;
    // The length of partition header (partition key, partition deletion and static row, if present)
    // as written to the data file
    // Used for writing promoted index
    uint64_t _partition_header_length = 0;
    uint64_t _prev_row_start = 0;
    std::optional<key> _partition_key;
    stdx::optional<key> _first_key, _last_key;
    index_sampling_state _index_sampling_state;
    range_tombstone_stream _range_tombstones;
    bytes_ostream _tmp_bufs;

    // For static and regular columns, we write all simple columns first followed by collections
    // These containers have columns partitioned by atomicity
    const indexed_columns _static_columns;
    const indexed_columns _regular_columns;

    struct cdef_and_collection {
        const column_definition* cdef;
        std::reference_wrapper<const atomic_cell_or_collection> collection;
    };

    // Used to defer writing collections until all atomic cells are written
    std::vector<cdef_and_collection> _collections;

    std::optional<rt_marker> _end_open_marker;

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
    column_stats _c_stats;

    void init_file_writers();
    void close_data_writer();
    void ensure_tombstone_is_written() {
        if (!_tombstone_written) {
            consume(tombstone());
        }
    }

    void ensure_static_row_is_written_if_needed() {
        if (!_static_columns.empty() && !_static_row_written) {
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

    void update_deletion_time_stats(deletion_time dt) {
        _c_stats.update_timestamp(dt.marked_for_delete_at);
        _c_stats.update_local_deletion_time(dt.local_deletion_time);
        _c_stats.tombstone_histogram.update(dt.local_deletion_time);
    }

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
    void write_delta_ttl(bytes_ostream& writer, int32_t ttl) {
        sstables::mc::write_delta_ttl(writer, ttl, _enc_stats);
    }
    void write_delta_local_deletion_time(bytes_ostream& writer, int32_t ldt) {
        sstables::mc::write_delta_local_deletion_time(writer, ldt, _enc_stats);
    }
    void write_delta_deletion_time(bytes_ostream& writer, deletion_time dt) {
        sstables::mc::write_delta_deletion_time(writer, dt, _enc_stats);
    }

    struct row_time_properties {
        std::optional<api::timestamp_type> timestamp;
        std::optional<int32_t> ttl;
        std::optional<int32_t> local_deletion_time;
    };

    // Writes single atomic cell
    void write_cell(bytes_ostream& writer, atomic_cell_view cell, const column_definition& cdef,
                    const row_time_properties& properties, bytes_view cell_path = {});

    // Writes information about row liveness (formerly 'row marker')
    void write_liveness_info(bytes_ostream& writer, const row_marker& marker);

    // Writes a CQL collection (list, set or map)
    void write_collection(bytes_ostream& writer, const column_definition& cdef, collection_mutation_view collection,
                          const row_time_properties& properties, bool has_complex_deletion);

    void write_cells(bytes_ostream& writer, column_kind kind, const row& row_body, const row_time_properties& properties, bool has_complex_deletion);
    void write_row_body(bytes_ostream& writer, const clustering_row& row, bool has_complex_deletion);
    void write_static_row(const row& static_row);

    // Clustered is a term used to denote an entity that has a clustering key prefix
    // and constitutes an entry of a partition.
    // Both clustered_rows and rt_markers are instances of Clustered
    void write_clustered(const clustering_row& clustered_row, uint64_t prev_row_size);
    void write_clustered(const rt_marker& marker, uint64_t prev_row_size);

    template <typename T>
    GCC6_CONCEPT(
        requires Clustered<T>
    )
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

    void flush_tmp_bufs(file_writer& writer) {
        for (auto&& buf : _tmp_bufs) {
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
                      const io_priority_class& pc, shard_id shard = engine().cpu_id())
        : writer_impl(sst, s, pc, cfg)
        , _enc_stats(enc_stats)
        , _shard(shard)
        , _range_tombstones(_schema)
        , _tmp_bufs(_sst.sstable_buffer_size)
        , _static_columns(get_indexed_columns_partitioned_by_atomicity(s.static_columns()))
        , _regular_columns(get_indexed_columns_partitioned_by_atomicity(s.regular_columns()))
    {
        _sst.generate_toc(_schema.get_compressor_params().get_compressor(), _schema.bloom_filter_fp_chance());
        _sst.write_toc(_pc);
        _sst.create_data().get();
        _compression_enabled = !_sst.has_component(component_type::CRC);
        init_file_writers();
        _sst._shards = { shard };

        _cfg.monitor->on_write_started(_data_writer->offset_tracker());
        _sst._components->filter = utils::i_filter::get_filter(estimated_partitions, _schema.bloom_filter_fp_chance(), utils::filter_format::m_format);
        _pi_write_m.desired_block_size = cfg.promoted_index_block_size.value_or(get_config().column_index_size_in_kb() * 1024);
        _sst._correctly_serialize_non_compound_range_tombstones = _cfg.correctly_serialize_non_compound_range_tombstones;
        _index_sampling_state.summary_byte_cost = summary_byte_cost();
        prepare_summary(_sst._components->summary, estimated_partitions, _schema.min_index_interval());
    }

    ~writer();
    writer(writer&& o) = default;
    void consume_new_partition(const dht::decorated_key& dk) override;
    void consume(tombstone t) override;
    stop_iteration consume(static_row&& sr) override;
    stop_iteration consume(clustering_row&& cr) override;
    stop_iteration consume(range_tombstone&& rt) override;
    stop_iteration consume_end_of_partition();
    void consume_end_of_stream() override;
};

writer::~writer() {
    auto close_writer = [](auto& writer) {
        if (writer) {
            try {
                writer->close();
            } catch (...) {
                sstlog.error("mc::writer failed to close file: {}", std::current_exception());
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

static deletion_time to_deletion_time(tombstone t) {
    deletion_time dt;
    if (t) {
        dt.local_deletion_time = t.deletion_time.time_since_epoch().count();
        dt.marked_for_delete_at = t.timestamp;
    } else {
        // Default values for live, non-deleted rows.
        dt.local_deletion_time = std::numeric_limits<int32_t>::max();
        dt.marked_for_delete_at = std::numeric_limits<int64_t>::min();
    }
    return dt;
}

void writer::add_pi_block() {
    auto block = pi_block{
        *_pi_write_m.first_clustering,
        *_pi_write_m.last_clustering,
        _pi_write_m.block_start_offset - _c_stats.start_offset,
        _data_writer->offset() - _pi_write_m.block_start_offset,
        (_end_open_marker ? std::make_optional(_end_open_marker->tomb) : std::optional<tombstone>{})};

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
        _data_writer = std::make_unique<crc32_checksummed_file_writer>(std::move(_sst._data_file), options);
    } else {
        _data_writer = std::make_unique<file_writer>(
            make_compressed_file_m_format_output_stream(
                    std::move(_sst._data_file),
                    options,
                    &_sst._components->compression,
                    _schema.get_compressor_params()));
    }
    _index_writer.emplace(std::move(_sst._index_file), options);
}

void writer::close_data_writer() {
    auto writer = std::move(_data_writer);
    writer->close();

    if (!_compression_enabled) {
        auto chksum_wr = static_cast<crc32_checksummed_file_writer*>(writer.get());
        _sst.write_digest(chksum_wr->full_checksum());
        _sst.write_crc(chksum_wr->finalize_checksum());
    } else {
        _sst.write_digest(_sst._components->compression.get_full_checksum());
    }
}

void writer::drain_tombstones(std::optional<position_in_partition_view> pos) {
    auto get_next_rt = [this, &pos] {
        return pos ? _range_tombstones.get_next(*pos) : _range_tombstones.get_next();
    };

    auto get_rt_start = [] (const range_tombstone& rt) {
        return rt_marker{rt.start, to_bound_kind_m(rt.start_kind), rt.tomb};
    };

    auto get_rt_end = [] (const range_tombstone& rt) {
        return rt_marker{rt.end, to_bound_kind_m(rt.end_kind), rt.tomb};
    };

    auto flush_end_open_marker = [this] {
        consume(*std::exchange(_end_open_marker, {}));
    };

    auto write_rt_boundary = [this, &get_rt_end] (const range_tombstone& rt) {
        auto boundary_kind = rt.start_kind == bound_kind::incl_start
                ? bound_kind_m::excl_end_incl_start
                : bound_kind_m::incl_end_excl_start;
        tombstone end_tomb{std::move(_end_open_marker->tomb)};
        _end_open_marker = get_rt_end(rt);
        consume(rt_marker{rt.start, boundary_kind, end_tomb, rt.tomb});
    };

    ensure_tombstone_is_written();
    ensure_static_row_is_written_if_needed();
    position_in_partition::less_compare less{_schema};
    position_in_partition::equal_compare eq{_schema};
    while (auto mfo = get_next_rt()) {
        range_tombstone rt {std::move(mfo)->as_range_tombstone()};
        bool need_write_start = true;
        if (_end_open_marker) {
            if (eq(_end_open_marker->position(), rt.position())) {
                write_rt_boundary(rt);
                need_write_start = false;
            } else if (less(rt.position(), _end_open_marker->position())) {
                if (_end_open_marker->tomb != rt.tomb) {
                    // Previous end marker has been superseded by a range tombstone that was added later
                    // so we end the currently open one and start the new one at once
                    write_rt_boundary(rt);
                    need_write_start = false;
                } else {
                    // The range tombstone is a continuation of the currently open one
                    // so don't need to write the start marker, just update the end
                    _end_open_marker = get_rt_end(rt);
                    need_write_start = false;
                }
            } else {
                // The new range tombstone lies entirely after the currently open one.
                // Simply close it.
                flush_end_open_marker();
            }
        }

        if (need_write_start) {
            _end_open_marker = get_rt_end(rt);
            consume(get_rt_start(rt));
        }

        if (pos && rt.trim_front(_schema, *pos)) {
            _range_tombstones.apply(std::move(rt));
        }
    }

    if (_end_open_marker && (!pos || less(_end_open_marker->position(), *pos))) {
        flush_end_open_marker();
    }
}

void writer::consume_new_partition(const dht::decorated_key& dk) {
    _c_stats.start_offset = _data_writer->offset();
    _prev_row_start = _data_writer->offset();

    _partition_key = key::from_partition_key(_schema, dk.key());
    maybe_add_summary_entry(dk.token(), bytes_view(*_partition_key));

    _sst._components->filter->add(bytes_view(*_partition_key));
    _sst.get_metadata_collector().add_key(bytes_view(*_partition_key));

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
    if (t) {
        update_deletion_time_stats(dt);
    }

    _pi_write_m.tomb = t;
    _tombstone_written = true;
}

void writer::write_cell(bytes_ostream& writer, atomic_cell_view cell, const column_definition& cdef,
        const row_time_properties& properties, bytes_view cell_path) {

    bool is_deleted = !cell.is_live();
    bool has_value = !is_deleted && !cell.value().empty();
    bool use_row_timestamp = (properties.timestamp == cell.timestamp());
    bool is_row_expiring = properties.ttl.has_value();
    bool is_cell_expiring = cell.is_live_and_has_ttl();
    bool use_row_ttl = is_row_expiring && is_cell_expiring &&
                       properties.ttl == cell.ttl().count() &&
                       properties.local_deletion_time == cell.deletion_time().time_since_epoch().count();

    cell_flags flags = cell_flags::none;
    if (!has_value) {
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
            write_delta_local_deletion_time(writer, cell.deletion_time().time_since_epoch().count());
        } else if (is_cell_expiring) {
            write_delta_local_deletion_time(writer, cell.expiry().time_since_epoch().count());
            write_delta_ttl(writer, cell.ttl().count());
        }
    }

    if (!cell_path.empty()) {
        write_vint(writer, cell_path.size());
        write(_sst.get_version(), writer, cell_path);
    }

    if (has_value) {
        if (cdef.is_counter()) {
            assert(!cell.is_counter_update());
          counter_cell_view::with_linearized(cell, [&] (counter_cell_view ccv) {
            write_counter_value(ccv, writer, sstable_version_types::mc, [] (bytes_ostream& out, uint32_t value) {
                return write_vint(out, value);
            });
          });
        } else {
            write_cell_value(writer, *cdef.type, cell.value());
        }
    }

    // Collect cell statistics
    _c_stats.update_timestamp(cell.timestamp());
    if (is_deleted) {
        auto ldt = cell.deletion_time().time_since_epoch().count();
        _c_stats.update_local_deletion_time(ldt);
        _c_stats.tombstone_histogram.update(ldt);
    } else if (is_cell_expiring) {
        auto expiration = cell.expiry().time_since_epoch().count();
        auto ttl = cell.ttl().count();
        _c_stats.update_ttl(ttl);
        _c_stats.update_local_deletion_time(expiration);
        // tombstone histogram is updated with expiration time because if ttl is longer
        // than gc_grace_seconds for all data, sstable will be considered fully expired
        // when actually nothing is expired.
        _c_stats.tombstone_histogram.update(expiration);
    } else { // regular live cell
        _c_stats.update_local_deletion_time(std::numeric_limits<int>::max());
    }
}

void writer::write_liveness_info(bytes_ostream& writer, const row_marker& marker) {
    if (marker.is_missing()) {
        return;
    }

    uint64_t timestamp = marker.timestamp();
    _c_stats.update_timestamp(timestamp);
    write_delta_timestamp(writer, timestamp);

    auto write_expiring_liveness_info = [this, &writer] (uint32_t ttl, uint64_t ldt) {
        _c_stats.update_ttl(ttl);
        _c_stats.update_local_deletion_time(ldt);
        write_delta_ttl(writer, ttl);
        write_delta_local_deletion_time(writer, ldt);
    };
    if (!marker.is_live()) {
        write_expiring_liveness_info(expired_liveness_ttl, marker.deletion_time().time_since_epoch().count());
    } else if (marker.is_expiring()) {
        write_expiring_liveness_info(marker.ttl().count(), marker.expiry().time_since_epoch().count());
    } else {
        _c_stats.update_ttl(0);
        _c_stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

void writer::write_collection(bytes_ostream& writer, const column_definition& cdef,
        collection_mutation_view collection, const row_time_properties& properties, bool has_complex_deletion) {
    auto& ctype = *static_pointer_cast<const collection_type_impl>(cdef.type);
    collection.data.with_linearized([&] (bytes_view collection_bv) {
        auto mview = ctype.deserialize_mutation_form(collection_bv);
        if (has_complex_deletion) {
            auto dt = to_deletion_time(mview.tomb);
            write_delta_deletion_time(writer, dt);
            if (mview.tomb) {
                _c_stats.update_timestamp(dt.marked_for_delete_at);
                _c_stats.update_local_deletion_time(dt.local_deletion_time);
            }
        }

        write_vint(writer, mview.cells.size());
        if (!mview.cells.empty()) {
            ++_c_stats.column_count;
        }
        for (const auto& [cell_path, cell]: mview.cells) {
            ++_c_stats.cells_count;
            write_cell(writer, cell, cdef, properties, cell_path);
        }
    });
}

void writer::write_cells(bytes_ostream& writer, column_kind kind, const row& row_body,
        const row_time_properties& properties, bool has_complex_deletion) {
    // Note that missing columns are written based on the whole set of regular columns as defined by schema.
    // This differs from Origin where all updated columns are tracked and the set of filled columns of a row
    // is compared with the set of all columns filled in the memtable. So our encoding may be less optimal in some cases
    // but still valid.
    write_missing_columns(writer, kind == column_kind::static_column ? _static_columns : _regular_columns, row_body);
    row_body.for_each_cell([this, &writer, kind, &properties, has_complex_deletion] (column_id id, const atomic_cell_or_collection& c) {
        auto&& column_definition = _schema.column_at(kind, id);
        if (!column_definition.is_atomic()) {
            _collections.push_back({&column_definition, c});
            return;
        }
        atomic_cell_view cell = c.as_atomic_cell(column_definition);
        ++_c_stats.cells_count;
        ++_c_stats.column_count;
        write_cell(writer, cell, column_definition, properties);
    });

    for (const auto& col: _collections) {
        write_collection(writer, *col.cdef, col.collection.get().as_collection_mutation(), properties, has_complex_deletion);
    }
    _collections.clear();
}

void writer::write_row_body(bytes_ostream& writer, const clustering_row& row, bool has_complex_deletion) {
    write_liveness_info(writer, row.marker());
    auto write_tombstone_and_update_stats = [this, &writer] (const tombstone& t) {
        auto dt = to_deletion_time(t);
        update_deletion_time_stats(dt);
        write_delta_deletion_time(writer, dt);
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
            properties.ttl = row.marker().ttl().count();
            properties.local_deletion_time = row.marker().deletion_time().time_since_epoch().count();
        }
    }

    return write_cells(writer, column_kind::regular_column, row.cells(), properties, has_complex_deletion);
}

// Find if any collection in the row contains a collection-wide tombstone
static bool row_has_complex_deletion(const schema& s, const row& r, column_kind kind) {
    bool result = false;
    r.for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& c) {
        auto&& cdef = s.column_at(kind, id);
        if (cdef.is_atomic()) {
            return stop_iteration::no;
        }
        auto t = static_pointer_cast<const collection_type_impl>(cdef.type);
        return c.as_collection_mutation().data.with_linearized([&] (bytes_view c_bv) {
            auto mview = t->deserialize_mutation_form(c_bv);
            if (mview.tomb) {
                result = true;
            }
            return stop_iteration(static_cast<bool>(mview.tomb));
        });
    });

    return result;
}

void writer::write_static_row(const row& static_row) {
    assert(_schema.is_compound());

    uint64_t current_pos = _data_writer->offset();
    // Static row flag is stored in extended flags so extension_flag is always set for static rows
    row_flags flags = row_flags::extension_flag;
    if (static_row.size() == _schema.static_columns_count()) {
        flags |= row_flags::has_all_columns;
    }
    bool has_complex_deletion = row_has_complex_deletion(_schema, static_row, column_kind::static_column);
    if (has_complex_deletion) {
        flags |= row_flags::has_complex_deletion;
    }
    write(_sst.get_version(), *_data_writer, flags);
    write(_sst.get_version(), *_data_writer, row_extended_flags::is_static);

    write_vint(_tmp_bufs, 0); // as the static row always comes first, the previous row size is always zero
    write_cells(_tmp_bufs, column_kind::static_column, static_row, row_time_properties{}, has_complex_deletion);
    write_vint(*_data_writer, _tmp_bufs.size());
    flush_tmp_bufs();

    _partition_header_length += (_data_writer->offset() - current_pos);

    // Collect statistics
    ++_c_stats.rows_count;
}

stop_iteration writer::consume(static_row&& sr) {
    ensure_tombstone_is_written();
    write_static_row(sr.cells());
    _static_row_written = true;
    return stop_iteration::no;
}

void writer::write_clustered(const clustering_row& clustered_row, uint64_t prev_row_size) {
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

    if (clustered_row.cells().size() == _schema.regular_columns_count()) {
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

    write_clustering_prefix(*_data_writer, _schema, clustered_row.key(), ephemerally_full_prefix{_schema.is_compact_table()});

    write_vint(_tmp_bufs, prev_row_size);
    write_row_body(_tmp_bufs, clustered_row, has_complex_deletion);

    uint64_t row_body_size = _tmp_bufs.size();
    write_vint(*_data_writer, row_body_size);
    flush_tmp_bufs();

    // Collect statistics
    if (_schema.clustering_key_size()) {
        column_name_helper::min_max_components(_schema, _sst.get_metadata_collector().min_column_names(),
            _sst.get_metadata_collector().max_column_names(), clustered_row.key().components());
    }
    ++_c_stats.rows_count;
}

stop_iteration writer::consume(clustering_row&& cr) {
    drain_tombstones(position_in_partition_view::after_key(cr.key()));
    write_clustered(cr);
    return stop_iteration::no;
}

// Write clustering prefix along with its bound kind and, if not full, its size
template <typename W>
GCC6_CONCEPT(requires Writer<W>())
static void write_clustering_prefix(W& writer, bound_kind_m kind,
        const schema& s, const clustering_key_prefix& clustering) {
    assert(kind != bound_kind_m::static_clustering);
    write(sstable_version_types::mc, writer, kind);
    auto is_ephemerally_full = ephemerally_full_prefix{s.is_compact_table()};
    if (kind != bound_kind_m::clustering) {
        // Don't use ephemerally full for RT bounds as they're always non-full
        is_ephemerally_full = ephemerally_full_prefix::no;
        write(sstable_version_types::mc, writer, static_cast<uint16_t>(clustering.size(s)));
    }
    write_clustering_prefix(writer, s, clustering, is_ephemerally_full);
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
    write_clustering_prefix(blocks, block.first.kind, _schema, block.first.clustering);
    write_clustering_prefix(blocks, block.last.kind, _schema, block.last.clustering);
    write_vint(blocks, block.offset);
    write_signed_vint(blocks, block.width - width_base);
    write(_sst.get_version(), blocks, static_cast<std::byte>(block.open_marker ? 1 : 0));
    if (block.open_marker) {
        write(sstable_version_types::mc, blocks, to_deletion_time(*block.open_marker));
    }
}

void writer::write_clustered(const rt_marker& marker, uint64_t prev_row_size) {
    write(sstable_version_types::mc, *_data_writer, row_flags::is_marker);
    write_clustering_prefix(*_data_writer, marker.kind, _schema, marker.clustering);
    auto write_marker_body = [this, &marker] (bytes_ostream& writer) {
        auto dt = to_deletion_time(marker.tomb);
        write_delta_deletion_time(writer, dt);
        update_deletion_time_stats(dt);
        if (marker.boundary_tomb) {
            auto dt_boundary = to_deletion_time(*marker.boundary_tomb);
            write_delta_deletion_time(writer, dt_boundary);
            update_deletion_time_stats(dt_boundary);
        }
    };

    write_vint(_tmp_bufs, prev_row_size);
    write_marker_body(_tmp_bufs);
    write_vint(*_data_writer, _tmp_bufs.size());
    flush_tmp_bufs();

    if (_schema.clustering_key_size()) {
        column_name_helper::min_max_components(_schema, _sst.get_metadata_collector().min_column_names(),
            _sst.get_metadata_collector().max_column_names(), marker.clustering.components());
    }
}

void writer::consume(rt_marker&& marker) {
    write_clustered(marker);
}

stop_iteration writer::consume(range_tombstone&& rt) {
    drain_tombstones(rt.position());
    _range_tombstones.apply(std::move(rt));
    return stop_iteration::no;
}

stop_iteration writer::consume_end_of_partition() {
    drain_tombstones();

    write(_sst.get_version(), *_data_writer, row_flags::end_of_partition);

    if (_pi_write_m.promoted_index_size && _pi_write_m.first_clustering) {
        add_pi_block();
    }

    write_promoted_index();

    // compute size of the current row.
    _c_stats.partition_size = _data_writer->offset() - _c_stats.start_offset;

    _cfg.large_partition_handler->maybe_update_large_partitions(_sst, *_partition_key, _c_stats.partition_size).get();

    // update is about merging column_stats with the data being stored by collector.
    _sst.get_metadata_collector().update(std::move(_c_stats));
    _c_stats.reset();

    if (!_first_key) {
        _first_key = *_partition_key;
    }
    _last_key = std::move(*_partition_key);
    return get_data_offset() < _cfg.max_sstable_size ? stop_iteration::no : stop_iteration::yes;
}

void writer::consume_end_of_stream() {
    _cfg.monitor->on_data_write_completed();

    seal_summary(_sst._components->summary, std::move(_first_key), std::move(_last_key), _index_sampling_state);

    if (_sst.has_component(component_type::CompressionInfo)) {
        _sst.get_metadata_collector().add_compression_ratio(_sst._components->compression.compressed_file_length(), _sst._components->compression.uncompressed_file_length());
    }

    _index_writer->close();
    _index_writer.reset();
    _sst.set_first_and_last_keys();
    seal_statistics(_sst.get_version(), _sst._components->statistics, _sst.get_metadata_collector(),
            dht::global_partitioner().name(), _schema.bloom_filter_fp_chance(),
            _sst._schema, _sst.get_first_decorated_key(), _sst.get_last_decorated_key(), _enc_stats);
    close_data_writer();
    _sst.write_summary(_pc);
    _sst.write_filter(_pc);
    _sst.write_statistics(_pc);
    _sst.write_compression(_pc);
    auto features = sstable_enabled_features::all();
    if (!_cfg.correctly_serialize_non_compound_range_tombstones) {
        features.disable(sstable_feature::NonCompoundRangeTombstones);
    }
    _sst.write_scylla_metadata(_pc, _shard, std::move(features));
    _cfg.monitor->on_write_completed();
    if (!_cfg.leave_unsealed) {
        _sst.seal_sstable(_cfg.backup).get();
    }
    _cfg.monitor->on_flush_completed();
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
