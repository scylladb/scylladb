/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstables/writer.hh"
#include "sstables/file_writer.hh"
#include "sstables/types.hh"
#include "sstables/mx/types.hh"
#include "encoding_stats.hh"
#include "schema/schema.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "mutation/atomic_cell.hh"
#include "vint-serialization.hh"
#include "bytes_ostream.hh"
#include "utils/assert.hh"

#include <iterator>
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
    using const_iterator = class iterator {
    private:
        const InputRange* _range;

        friend class input_range_base;

        explicit iterator(const InputRange& range)
            : _range(range.next() ? &range : nullptr)
        {}

    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = ValueType;
        using difference_type = std::ptrdiff_t;

        iterator() : _range{} {}

        const ValueType operator*() const {
            SCYLLA_ASSERT(_range);
            return _range->get_value();
        }

        iterator& operator++() {
            SCYLLA_ASSERT(_range);
            if (!_range->next()) {
                _range = nullptr;
            }
            return *this;
        }

        void operator++(int) {
            ++*this;
        }

        bool operator==(const iterator&) const = default;
    };
    static_assert(std::input_iterator<iterator>);

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

    // Defined in writer.cc, next to the logger it warns through.
    bool next() const;

    clustering_block get_value() const { return _current_block; };

    explicit operator bool() const {
        return (_offset < _serialization_limit_size);
    }
};

template <typename W>
requires Writer<W>
void write(sstable_version_types v, W& out, const clustering_block& block) {
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
            for (const auto& [index, column]: _columns | std::views::enumerate) {
                auto cell = _row.find_cell(column.get().id);
                if (!cell) {
                    _current_value |= (uint64_t(1) << index);
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
                SCYLLA_ASSERT(_mode == encoding_mode::large_encode_missing);
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

struct sstable_schema {
    serialization_header header;
    indexed_columns regular_columns;
    indexed_columns static_columns;
};

// The header's minimum values come from enc_stats. The column lists refer to the
// column_definitions of s, so s must outlive the result.
sstable_schema make_sstable_schema(const schema& s, const encoding_stats& enc_stats);

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
    // If set, the shadowable tombstone is written right after the row deletion.
    // This is only used by Materialized Views that are not supposed to be exported.
        has_shadowable_deletion_scylla = 0x80,
};

// Position of a row (bound_kind_m::clustering) or an RT marker.
inline position_in_partition_view position_of(const clustering_key_prefix& ck, bound_kind_m kind) {
    if (kind == bound_kind_m::clustering) {
        return position_in_partition_view(position_in_partition_view::clustering_row_tag_t(), ck);
    }
    if (kind == bound_kind_m::incl_start || kind == bound_kind_m::excl_end_incl_start) {
        return position_in_partition_view::before_key(ck);
    }
    return position_in_partition_view::after_all_prefixed(ck);
}

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
        return position_of(clustering, kind);
    }

    // We need this one to uniformly write rows and RT markers inside write_clustered().
    const clustering_key_prefix& key() const { return clustering; }
};

inline bound_kind_m get_kind(const clustering_row&) {
    return bound_kind_m::clustering;
}

inline bound_kind_m get_kind(const rt_marker& marker) {
    return marker.kind;
}

template<typename T>
concept Clustered = requires(T t) {
    { t.key() } -> std::convertible_to<const clustering_key_prefix&>;
    { get_kind(t) } -> std::same_as<bound_kind_m>;
};

// Write clustering prefix along with its bound kind and, if not full, its size
template <typename W>
requires Writer<W>
void write_clustering_prefix(sstable_version_types v, W& writer, bound_kind_m kind,
    const schema& s, const clustering_key_prefix& clustering) {
    SCYLLA_ASSERT(kind != bound_kind_m::static_clustering);
    write(v, writer, kind);
    auto is_ephemerally_full = ephemerally_full_prefix{s.is_compact_table()};
    if (kind != bound_kind_m::clustering) {
        // Don't use ephemerally full for RT bounds as they're always non-full
        is_ephemerally_full = ephemerally_full_prefix::no;
        write(v, writer, static_cast<uint16_t>(clustering.size(s)));
    }
    write_clustering_prefix(v, writer, s, clustering, is_ephemerally_full);
}

// Find if any complex column (collection or non-frozen user type) in the row contains a column-wide tombstone
inline bool row_has_complex_deletion(const schema& s, const row& r, column_kind kind) {
    bool result = false;
    r.for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& c) {
        auto&& cdef = s.column_at(kind, id);
        if (cdef.is_atomic()) {
            return stop_iteration::no;
        }
        auto tomb = c.as_collection_mutation().tomb();
        if (tomb) {
            result = true;
        }
        return stop_iteration(static_cast<bool>(tomb));
    });

    return result;
}

// Offsets the encoder measures row sizes and prev_row_size with.
inline uint64_t writer_offset(const file_writer& w) {
    return w.offset();
}

inline uint64_t writer_offset(const bytes_ostream& w) {
    return w.size();
}

// Writes the data-file (Data.db) encoding of partitions, for the sstable writer and
// for any other user of the format. Observer receives the statistics, index and
// logging hooks (see sstables::mc::writer), and Yield::maybe_yield() runs in long loops.
// The observer is a template parameter because write_cell() is on the flush hot path.
template <typename Observer, typename Yield>
class partition_encoder {
public:
    // The reader rejects a TTL above max_ttl (see parse_ttl()), so never write
    // one: clamp it to max_ttl. The write time (expiry - ttl) is kept, as
    // tombstone GC relies on it once the cell expires, so the expiry moves
    // earlier. Failing the write instead would fail the memtable flush, which
    // is retried forever. Every entry point validates the TTL now, but cells
    // with a larger one can still arrive from before that, via the commitlog,
    // hints or streaming (SCYLLADB-6065). Only live TTLs come here; the
    // expired-liveness sentinel of a dead row marker is written as is.
    // The serialization header's min_ttl is the base the TTLs are encoded
    // against. It comes from the unclamped input (the memtable's stats, or
    // the source sstables' on compaction), so clamp it too, otherwise a
    // clamped cell's delta is negative and goes out as a wrapped vint: still
    // read back correctly, but 10 bytes instead of 1, with a header min_ttl
    // above the limit the reader enforces on cells.
    static encoding_stats clamp_enc_stats_ttl(encoding_stats enc_stats) {
        enc_stats.min_ttl = std::min(enc_stats.min_ttl, max_ttl);
        return enc_stats;
    }

private:
    const schema& _schema;
    const sstable_schema& _sst_schema;
    const sstable_version_types _version;
    const encoding_stats _enc_stats;
    Observer& _observer;
    bool _write_regular_as_static; // See #4139
    bytes_ostream _tmp_bufs;

    struct cdef_and_collection {
        const column_definition* cdef;
        std::reference_wrapper<const atomic_cell_or_collection> collection;
    };

    // Used to defer writing collections until all atomic cells are written
    std::vector<cdef_and_collection> _collections;

    tombstone _current_tombstone;
    bool _tombstone_written = false;
    bool _static_row_written = false;
    uint64_t _prev_row_start = 0;

    struct row_time_properties {
        std::optional<api::timestamp_type> timestamp;
        std::optional<gc_clock::duration> ttl;
        std::optional<gc_clock::time_point> local_deletion_time;
    };

public:
    // sst_schema must outlive the encoder. tmp_buf_size is the chunk size of the
    // buffer row bodies are assembled in before their size prefix is known.
    partition_encoder(const schema& s, const sstable_schema& sst_schema, sstable_version_types version,
            const encoding_stats& enc_stats, Observer& observer, size_t tmp_buf_size)
        : _schema(s)
        , _sst_schema(sst_schema)
        , _version(version)
        , _enc_stats(clamp_enc_stats_ttl(enc_stats))
        , _observer(observer)
        , _write_regular_as_static(s.is_static_compact_table())
        , _tmp_bufs(tmp_buf_size)
    { }

    // The range tombstone open after the last written fragment.
    tombstone current_tombstone() const {
        return _current_tombstone;
    }

    bool writes_regular_as_static() const {
        return _write_regular_as_static;
    }

    // Shared with the sstable writer's promoted index, which encodes into the same buffer.
    bytes_ostream& tmp_bufs() {
        return _tmp_bufs;
    }

    template <typename W>
    requires Writer<W>
    void flush_tmp_bufs(W& writer) {
        for (auto&& buf : _tmp_bufs) {
            Yield::maybe_yield();
            writer.write(buf);
        }
        _tmp_bufs.clear();
    }

    deletion_time to_deletion_time(tombstone t) {
        deletion_time dt;
        if (t) {
            bool capped;
            int32_t ldt = adjusted_local_deletion_time(t.deletion_time, capped);
            if (capped) {
                _observer.on_capped_deletion_time(t, ldt);
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

    template <typename W>
    requires Writer<W>
    void consume_new_partition(W& out, bytes_view partition_key) {
        _prev_row_start = writer_offset(out);
        auto p_key = disk_string_view<uint16_t>();
        p_key.value = partition_key;
        write(_version, out, p_key);
        _tombstone_written = false;
        _static_row_written = false;
    }

    template <typename W>
    requires Writer<W>
    void consume(W& out, tombstone t) {
        uint64_t current_pos = writer_offset(out);
        auto dt = to_deletion_time(t);
        write(_version, out, dt);
        _tombstone_written = true;
        _observer.on_partition_tombstone(t, dt, writer_offset(out) - current_pos);
    }

    template <typename W>
    requires Writer<W>
    void consume(W& out, const static_row& sr) {
        ensure_tombstone_is_written(out);
        write_static_row(out, sr.cells(), column_kind::static_column);
    }

    template <typename W>
    requires Writer<W>
    void consume(W& out, const clustering_row& cr) {
        if (_write_regular_as_static) {
            ensure_tombstone_is_written(out);
            write_static_row(out, cr.cells(), column_kind::regular_column);
            return;
        }
        ensure_tombstone_is_written(out);
        ensure_static_row_is_written_if_needed(out);
        write_clustered(out, cr, _current_tombstone);
    }

    template <typename W>
    requires Writer<W>
    void consume(W& out, const range_tombstone_change& rtc) {
        ensure_tombstone_is_written(out);
        ensure_static_row_is_written_if_needed(out);
        position_in_partition_view pos = rtc.position();
        if (!_current_tombstone && !rtc.tombstone()) {
            return;
        }
        tombstone prev_tombstone = std::exchange(_current_tombstone, rtc.tombstone());
        if (!prev_tombstone) { // start bound
            auto bv = pos.as_start_bound_view();
            write_clustered(out,
                rt_marker{pos.key(), to_bound_kind_m(bv.kind()), rtc.tombstone(), {}},
                prev_tombstone);
        } else if (!rtc.tombstone()) { // end bound
            auto bv = pos.as_end_bound_view();
            write_clustered(out,
                rt_marker{pos.key(), to_bound_kind_m(bv.kind()), prev_tombstone, {}},
                prev_tombstone);
        } else { // boundary
            auto bk = pos.get_bound_weight() == bound_weight::before_all_prefixed
                ? bound_kind_m::excl_end_incl_start
                : bound_kind_m::incl_end_excl_start;
            write_clustered(out,
                rt_marker{pos.key(), bk, prev_tombstone, rtc.tombstone()},
                prev_tombstone);
        }
    }

    // Writes the partition tombstone and the empty static row if the partition has neither yet.
    template <typename W>
    requires Writer<W>
    void ensure_partition_header_is_written(W& out) {
        ensure_tombstone_is_written(out);
        ensure_static_row_is_written_if_needed(out);
    }

    template <typename W>
    requires Writer<W>
    void write_end_of_partition(W& out) {
        write(_version, out, row_flags::end_of_partition);
    }

private:
    template <typename W>
    requires Writer<W>
    void ensure_tombstone_is_written(W& out) {
        if (!_tombstone_written) {
            consume(out, tombstone());
        }
    }

    template <typename W>
    requires Writer<W>
    void ensure_static_row_is_written_if_needed(W& out) {
        if (!_sst_schema.static_columns.empty() && !_static_row_written) {
            consume(out, static_row{});
        }
    }

    void write_delta_timestamp(bytes_ostream& writer, api::timestamp_type timestamp) {
        sstables::mc::write_delta_timestamp(writer, timestamp, _enc_stats);
    }
    void write_delta_ttl(bytes_ostream& writer, gc_clock::duration ttl) {
        sstables::mc::write_delta_ttl(writer, ttl, _enc_stats);
    }
    std::pair<gc_clock::duration, gc_clock::time_point> clamp_ttl(gc_clock::duration ttl, gc_clock::time_point expiry) {
        if (ttl > max_ttl) [[unlikely]] {
            _observer.on_clamped_ttl(ttl);
            return {max_ttl, expiry - ttl + max_ttl};
        }
        return {ttl, expiry};
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

    // Writes single atomic cell
    void write_cell(bytes_ostream& writer, const clustering_key_prefix* clustering_key, atomic_cell_view cell,
            const column_definition& cdef, const row_time_properties& properties, std::optional<bytes_view> cell_path = {}) {

        uint64_t current_pos = writer.size();
        bool is_deleted = !cell.is_live();
        bool has_value = !is_deleted && !cell.value().empty();
        bool use_row_timestamp = (properties.timestamp == cell.timestamp());
        bool is_row_expiring = properties.ttl.has_value();
        bool is_cell_expiring = cell.is_live_and_has_ttl();
        // Clamped once here, for both the data and the stats below.
        const auto [cell_ttl, cell_expiry] = is_cell_expiring
                ? clamp_ttl(cell.ttl(), cell.expiry())
                : std::pair{gc_clock::duration::zero(), gc_clock::time_point()};
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
        write(_version, writer, flags);

        if (!use_row_timestamp) {
            write_delta_timestamp(writer, cell.timestamp());
        }

        if (!use_row_ttl) {
            if (is_deleted) {
                write_delta_local_deletion_time(writer, cell.deletion_time());
            } else if (is_cell_expiring) {
                write_delta_local_deletion_time(writer, cell_expiry);
                write_delta_ttl(writer, cell_ttl);
            }
        }

        if (bool(cell_path)) {
            write_vint(writer, cell_path->size());
            write(_version, writer, *cell_path);
        }

        if (cdef.is_counter()) {
            if (!is_deleted) {
                SCYLLA_ASSERT(!cell.is_counter_update());
                auto ccv = counter_cell_view(cell);
                write_counter_value(ccv, writer, _version, [] (bytes_ostream& out, uint32_t value) {
                    return write_vint(out, value);
                });
            }
        } else {
            if (has_value) {
                write_cell_value(_version, writer, *cdef.type, cell.value());
            }
        }

        _observer.on_cell_written(clustering_key, cdef, cell, cell_ttl, cell_expiry, writer.size() - current_pos);
    }

    // Writes information about row liveness (formerly 'row marker')
    void write_liveness_info(bytes_ostream& writer, const row_marker& marker) {
        if (marker.is_missing()) {
            return;
        }

        api::timestamp_type timestamp = marker.timestamp();
        write_delta_timestamp(writer, timestamp);

        auto write_expiring_liveness_info = [this, &writer, &marker] (gc_clock::duration ttl, gc_clock::time_point ldt) {
            write_delta_ttl(writer, ttl);
            write_delta_local_deletion_time(writer, ldt);
            _observer.on_liveness_info(marker, ttl, ldt);
        };
        if (!marker.is_live()) {
            write_expiring_liveness_info(gc_clock::duration(expired_liveness_ttl), marker.deletion_time());
        } else if (marker.is_expiring()) {
            auto [ttl, expiry] = clamp_ttl(marker.ttl(), marker.expiry());
            write_expiring_liveness_info(ttl, expiry);
        } else {
            _observer.on_liveness_info(marker, gc_clock::duration::zero(), gc_clock::time_point());
        }
    }

    // Writes a collection of cells, representing either a CQL collection or fields of a non-frozen user type
    void write_collection(bytes_ostream& writer, const clustering_key_prefix* clustering_key,
            const column_definition& cdef, collection_mutation_view collection, const row_time_properties& properties,
            bool has_complex_deletion) {
        uint64_t current_pos = writer.size();
        uint64_t collection_elements = 0;
        auto tomb = collection.tomb();
        if (has_complex_deletion) {
            write_delta_deletion_time(writer, tomb);
            _observer.on_tombstone(tomb);
        }
        collection_elements = collection.size();
        write_vint(writer, collection_elements);
        for (const auto& [cell_path, cell] : collection) {
            cell_path.with_linearized([&](bytes_view path) {
                write_cell(writer, clustering_key, cell, cdef, properties, path);
            });
            Yield::maybe_yield();
        }
        uint64_t size = writer.size() - current_pos;
        _observer.on_collection_written(clustering_key, cdef, collection_elements, size);
    }

    void write_cells(bytes_ostream& writer, const clustering_key_prefix* clustering_key, column_kind kind, const row& row_body,
            const row_time_properties& properties, bool has_complex_deletion) {
        // Note that missing columns are written based on the whole set of regular columns as defined by schema.
        // This differs from Origin where all updated columns are tracked and the set of filled columns of a row
        // is compared with the set of all columns filled in the memtable. So our encoding may be less optimal in some cases
        // but still valid.
        write_missing_columns(writer, kind == column_kind::static_column ? _sst_schema.static_columns : _sst_schema.regular_columns, row_body);
        row_body.for_each_cell([this, &writer, kind, &properties, clustering_key] (column_id id, const atomic_cell_or_collection& c) {
            auto&& column_definition = _schema.column_at(kind, id);
            if (!column_definition.is_atomic()) {
                _collections.push_back({&column_definition, c});
                return;
            }
            atomic_cell_view cell = c.as_atomic_cell(column_definition);
            write_cell(writer, clustering_key, cell, column_definition, properties);
        });

        for (const auto& col: _collections) {
            write_collection(writer, clustering_key, *col.cdef, col.collection.get().as_collection_mutation(), properties, has_complex_deletion);
        }
        _collections.clear();
    }

    void write_row_body(bytes_ostream& writer, const clustering_row& row, bool has_complex_deletion) {
        write_liveness_info(writer, row.marker());
        auto write_tombstone_and_update_stats = [this, &writer] (const tombstone& t) {
            _observer.on_tombstone(t);
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

    template <typename W>
    requires Writer<W>
    void write_static_row(W& out, const row& static_row, column_kind kind) {
        uint64_t current_pos = writer_offset(out);
        // Static row flag is stored in extended flags so extension_flag is always set for static rows
        row_flags flags = row_flags::extension_flag;
        if (static_row.size() == _sst_schema.static_columns.size()) {
            flags |= row_flags::has_all_columns;
        }
        bool has_complex_deletion = row_has_complex_deletion(_schema, static_row, kind);
        if (has_complex_deletion) {
            flags |= row_flags::has_complex_deletion;
        }
        write(_version, out, flags);
        write(_version, out, row_extended_flags::is_static);

        write_vint(_tmp_bufs, 0); // as the static row always comes first, the previous row size is always zero
        write_cells(_tmp_bufs, nullptr, column_kind::static_column, static_row, row_time_properties{}, has_complex_deletion);
        write_vint(out, _tmp_bufs.size());
        flush_tmp_bufs(out);

        _observer.on_static_row_written(writer_offset(out) - current_pos);
        _static_row_written = true;
    }

    // Clustered is a term used to denote an entity that has a clustering key prefix
    // and constitutes an entry of a partition.
    // Both clustered_rows and rt_markers are instances of Clustered
    template <typename W, typename T>
    requires Writer<W> && Clustered<T>
    void write_clustered(W& out, const T& clustered, tombstone preceding_range_tombstone) {
        uint64_t pos = writer_offset(out);
        do_write_clustered(out, clustered, pos - _prev_row_start);
        _prev_row_start = pos;
        _observer.on_clustered_written(clustered.key(), get_kind(clustered), preceding_range_tombstone, pos);
    }

    template <typename W>
    requires Writer<W>
    void do_write_clustered(W& out, const clustering_row& clustered_row, uint64_t prev_row_size) {
        uint64_t current_pos = writer_offset(out);
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
        write(_version, out, flags);
        if (ext_flags != row_extended_flags::none) {
            write(_version, out, ext_flags);
        }

        write_clustering_prefix(_version, out, _schema, clustered_row.key(), ephemerally_full_prefix{_schema.is_compact_table()});

        write_vint(_tmp_bufs, prev_row_size);
        write_row_body(_tmp_bufs, clustered_row, has_complex_deletion);

        uint64_t row_body_size = _tmp_bufs.size();
        write_vint(out, row_body_size);
        flush_tmp_bufs(out);

        _observer.on_row_written(clustered_row, writer_offset(out) - current_pos);
    }

    template <typename W>
    requires Writer<W>
    void do_write_clustered(W& out, const rt_marker& marker, uint64_t prev_row_size) {
        write(_version, out, row_flags::is_marker);
        write_clustering_prefix(_version, out, marker.kind, _schema, marker.clustering);
        auto write_marker_body = [this, &marker] (bytes_ostream& writer) {
            write_delta_deletion_time(writer, marker.tomb);
            _observer.on_tombstone(marker.tomb);
            if (marker.boundary_tomb) {
                do_write_delta_deletion_time(writer, *marker.boundary_tomb);
                _observer.on_tombstone(*marker.boundary_tomb);
            }
        };

        write_vint(_tmp_bufs, prev_row_size);
        write_marker_body(_tmp_bufs);
        write_vint(out, _tmp_bufs.size());
        flush_tmp_bufs(out);
        _observer.on_range_tombstone_written(marker);
    }
};

}
}
