/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "disk_types.hh"
#include <seastar/core/enum.hh>
#include <seastar/core/weak_ptr.hh>
#include "bytes.hh"
#include "gc_clock.hh"
#include "locator/host_id.hh"
#include "mutation/tombstone.hh"
#include "utils/streaming_histogram.hh"
#include "utils/estimated_histogram.hh"
#include "sstables/key.hh"
#include "sstables/file_writer.hh"
#include "db/commitlog/replay_position.hh"
#include "version.hh"
#include <vector>
#include <unordered_map>
#include <type_traits>
#include <concepts>
#include "version.hh"
#include "encoding_stats.hh"
#include "types_fwd.hh"

// While the sstable code works with char, bytes_view works with int8_t
// (signed char). Rather than change all the code, let's do a cast.
inline bytes_view to_bytes_view(const temporary_buffer<char>& b) {
    using byte = bytes_view::value_type;
    return bytes_view(reinterpret_cast<const byte*>(b.get()), b.size());
}

namespace sstables {

template<typename T>
concept Writer =
    requires(T& wr, const char* data, size_t size) {
        { wr.write(data, size) } -> std::same_as<void>;
    };

struct sample_describer_for_self_describing_concept {
    // A describer can return any type, but we can't check any type in a concept.
    // Pick "long" arbitrarily and check that describe_type returns long too in that case.
    long operator()(auto&&...) const;
};

template <typename T>
concept self_describing = requires (T& obj, sstable_version_types v, sample_describer_for_self_describing_concept d) {
    { obj.describe_type(v, d) } -> std::same_as<long>;
};

struct commitlog_interval {
    db::replay_position start;
    db::replay_position end;
};

struct deletion_time {
    int32_t local_deletion_time;
    int64_t marked_for_delete_at;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(local_deletion_time, marked_for_delete_at); }

    bool live() const {
        return (local_deletion_time == std::numeric_limits<int32_t>::max()) &&
               (marked_for_delete_at == std::numeric_limits<int64_t>::min());
    }

    bool operator==(const deletion_time& d) const = default;
    explicit operator tombstone() {
        return !live() ? tombstone(marked_for_delete_at, gc_clock::time_point(gc_clock::duration(local_deletion_time))) : tombstone();
    }
};

struct option {
    disk_string<uint16_t> key;
    disk_string<uint16_t> value;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(key, value); }
};

struct filter {
    uint32_t hashes;
    disk_array<uint32_t, uint64_t> buckets;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(hashes, buckets); }

    // Create an always positive filter if nothing else is specified.
    filter() : hashes(0), buckets({}) {}
    explicit filter(int hashes, utils::chunked_vector<uint64_t> buckets) : hashes(hashes), buckets({std::move(buckets)}) {}
};

// Do this so we don't have to copy on write time. We can just keep a reference.
struct filter_ref {
    uint32_t hashes;
    disk_array_ref<uint32_t, uint64_t> buckets;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(hashes, buckets); }
    explicit filter_ref(int hashes, const utils::chunked_vector<uint64_t>& buckets) : hashes(hashes), buckets(buckets) {}
};

enum class indexable_element {
    partition,
    cell
};

inline auto format_as(indexable_element e) {
    return fmt::underlying(e);
}

class summary_entry {
public:
    int64_t raw_token;
    bytes_view key;
    uint64_t position;

    explicit summary_entry(dht::token token, bytes_view key, uint64_t position)
            : raw_token(dht::token::to_int64(token))
            , key(key)
            , position(position) {
    }

    key_view get_key() const {
        return key_view{key};
    }

    dht::token get_token() const {
        return dht::token::from_int64(raw_token);
    }

    decorated_key_view get_decorated_key() const {
        return decorated_key_view(get_token(), get_key());
    }

    bool operator==(const summary_entry& x) const {
        return position ==  x.position && key == x.key;
    }
};

// Note: Sampling level is present in versions ka and higher. We ATM only support ka,
// so it's always there. But we need to make this conditional if we ever want to support
// other formats.
struct summary_ka {
    struct header {
        // The minimum possible amount of indexes per group (sampling level)
        uint32_t min_index_interval;
        // The number of entries in the Summary File
        uint32_t size;
        // The memory to be consumed to map the whole Summary into memory.
        uint64_t memory_size;
        // The actual sampling level.
        uint32_t sampling_level;
        // The number of entries the Summary *would* have if the sampling
        // level would be equal to min_index_interval.
        uint32_t size_at_full_sampling;
    } header;
    // The position in the Summary file for each of the indexes.
    // NOTE1 that its actual size is determined by the "size" parameter, not
    // by its preceding size_at_full_sampling
    // NOTE2: They are laid out in *MEMORY* order, not BE.
    // NOTE3: The sizes in this array represent positions in the memory stream,
    // not the file. The memory stream effectively begins after the header,
    // so every position here has to be added of sizeof(header).
    utils::chunked_vector<uint32_t> positions;   // can be large, so use a deque instead of a vector
    utils::chunked_vector<summary_entry> entries;

    disk_string<uint32_t> first_key;
    disk_string<uint32_t> last_key;

    // NOTE4: There is a structure written by Cassandra into the end of the Summary
    // file, after the field last_key, that we haven't understand yet, but we know
    // that its content isn't related to the summary itself.
    // The structure is basically as follow:
    // struct { disk_string<uint16_t>; uint32_t; uint64_t; disk_string<uint16_t>; }
    // Another interesting fact about this structure is that it is apparently always
    // filled with the same data. It's too early to judge that the data is useless.
    // However, it was tested that Cassandra loads successfully a Summary file with
    // this structure removed from it. Anyway, let's pay attention to it.

    /*
     * Returns total amount of memory used by the summary
     * Similar to origin off heap size
     */
    uint64_t memory_footprint() const {
        auto sz = sizeof(summary_entry) * entries.size() + sizeof(uint32_t) * positions.size() + sizeof(*this);
        sz += first_key.value.size() + last_key.value.size();
        for (auto& sd : _summary_data) {
            sz += sd.size();
        }
        return sz;
    }

    explicit operator bool() const {
        return entries.size();
    }

    bytes_view add_summary_data(bytes_view data) {
        if (_summary_data.empty() || (_summary_index_pos + data.size() > _buffer_size)) {
            _buffer_size = std::min(_buffer_size << 1, 128u << 10);
            // Keys are 64kB max, so it might be one key may not fit in a buffer
            _buffer_size = std::max(_buffer_size, unsigned(data.size()));
            _summary_data.emplace_back(_buffer_size);
            _summary_index_pos = 0;
        }

        auto ret = _summary_data.back().store_at(_summary_index_pos, data);
        _summary_index_pos += data.size();
        return ret;
    }
private:
    class summary_data_memory {
        unsigned _size;
        std::unique_ptr<bytes::value_type[]> _data;
    public:
        summary_data_memory(unsigned size) : _size(size), _data(std::make_unique<bytes::value_type[]>(size)) {}
        bytes_view store_at(unsigned pos, bytes_view src) {
            auto addr = _data.get() + pos;
            std::copy_n(src.data(), src.size(), addr);
            return bytes_view(addr, src.size());
        }
        unsigned size() const {
            return _size;
        }
    };
    unsigned _buffer_size = 1 << 10;
    std::vector<summary_data_memory> _summary_data = {};
    unsigned _summary_index_pos = 0;
};
using summary = summary_ka;

struct metadata {
    virtual ~metadata() {}
    virtual uint64_t serialized_size(sstable_version_types v) const = 0;
    virtual void write(sstable_version_types v, file_writer& write) const = 0;
};

template <typename T>
uint64_t serialized_size(sstable_version_types v, const T& object);

template <self_describing T, Writer W>
void
write(sstable_version_types v, W& out, const T& t);

// serialized_size() implementation for metadata class
template <typename Component>
class metadata_base : public metadata {
public:
    virtual uint64_t serialized_size(sstable_version_types v) const override {
        return sstables::serialized_size(v, static_cast<const Component&>(*this));
    }
    virtual void write(sstable_version_types v, file_writer& writer) const override {
        return sstables::write(v, writer, static_cast<const Component&>(*this));
    }
};

struct validation_metadata : public metadata_base<validation_metadata> {
    disk_string<uint16_t> partitioner;
    double filter_chance;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(partitioner, filter_chance); }
};

struct compaction_metadata : public metadata_base<compaction_metadata> {
    disk_array<uint32_t, uint32_t> ancestors; // DEPRECATED, not available in sstable format mc.
    disk_array<uint32_t, uint8_t> cardinality;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) {
        switch (v) {
        case sstable_version_types::mc:
        case sstable_version_types::md:
        case sstable_version_types::me:
            return f(
                cardinality
            );
        case sstable_version_types::ka:
        case sstable_version_types::la:
            return f(
                ancestors,
                cardinality
            );
        }
        // Should never reach here - compiler will complain if switch above does not cover all sstable versions
        abort();
    }
};

struct stats_metadata : public metadata_base<stats_metadata> {
    utils::estimated_histogram estimated_partition_size;
    utils::estimated_histogram estimated_cells_count;
    db::replay_position position;
    int64_t min_timestamp;
    int64_t max_timestamp;
    int32_t min_local_deletion_time; // 3_x only
    int32_t max_local_deletion_time;
    int32_t min_ttl; // 3_x only
    int32_t max_ttl; // 3_x only
    double compression_ratio;
    utils::streaming_histogram estimated_tombstone_drop_time;
    uint32_t sstable_level;
    // There is not meaningful value to put in this field, since we have no
    // incremental repair. Before we have it, let's set it to 0.
    uint64_t repaired_at = 0;
    disk_array<uint32_t, disk_string<uint16_t>> min_column_names;
    disk_array<uint32_t, disk_string<uint16_t>> max_column_names;
    bool has_legacy_counter_shards;
    int64_t columns_count; // 3_x only
    int64_t rows_count; // 3_x only
    db::replay_position commitlog_lower_bound; // 3_x only
    disk_array<uint32_t, commitlog_interval> commitlog_intervals; // 3_x only
    std::optional<locator::host_id> originating_host_id; // 3_11_11 and later (me format)

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) {
        switch (v) {
        case sstable_version_types::me:
            return f(
                estimated_partition_size,
                estimated_cells_count,
                position,
                min_timestamp,
                max_timestamp,
                min_local_deletion_time,
                max_local_deletion_time,
                min_ttl,
                max_ttl,
                compression_ratio,
                estimated_tombstone_drop_time,
                sstable_level,
                repaired_at,
                min_column_names,
                max_column_names,
                has_legacy_counter_shards,
                columns_count,
                rows_count,
                commitlog_lower_bound,
                commitlog_intervals,
                originating_host_id
            );
        case sstable_version_types::mc:
        case sstable_version_types::md:
            return f(
                estimated_partition_size,
                estimated_cells_count,
                position,
                min_timestamp,
                max_timestamp,
                min_local_deletion_time,
                max_local_deletion_time,
                min_ttl,
                max_ttl,
                compression_ratio,
                estimated_tombstone_drop_time,
                sstable_level,
                repaired_at,
                min_column_names,
                max_column_names,
                has_legacy_counter_shards,
                columns_count,
                rows_count,
                commitlog_lower_bound,
                commitlog_intervals
            );
        case sstable_version_types::ka:
        case sstable_version_types::la:
            return f(
                estimated_partition_size,
                estimated_cells_count,
                position,
                min_timestamp,
                max_timestamp,
                max_local_deletion_time,
                compression_ratio,
                estimated_tombstone_drop_time,
                sstable_level,
                repaired_at,
                min_column_names,
                max_column_names,
                has_legacy_counter_shards
            );
        }
        // Should never reach here - compiler will complain if switch above does not cover all sstable versions
        abort();
    }
};

using bytes_array_vint_size = disk_string_vint_size;

struct serialization_header : public metadata_base<serialization_header> {
    vint<uint64_t> min_timestamp_base;
    vint<uint64_t> min_local_deletion_time_base;
    vint<uint64_t> min_ttl_base;
    bytes_array_vint_size pk_type_name;
    disk_array_vint_size<bytes_array_vint_size> clustering_key_types_names;
    struct column_desc {
        bytes_array_vint_size name;
        bytes_array_vint_size type_name;
        template <typename Describer>
        auto describe_type(sstable_version_types v, Describer f) {
            return f(
                name,
                type_name
            );
        }
    };
    disk_array_vint_size<column_desc> static_columns;
    disk_array_vint_size<column_desc> regular_columns;
    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) {
        switch (v) {
        case sstable_version_types::mc:
        case sstable_version_types::md:
        case sstable_version_types::me:
            return f(
                min_timestamp_base,
                min_local_deletion_time_base,
                min_ttl_base,
                pk_type_name,
                clustering_key_types_names,
                static_columns,
                regular_columns
            );
        case sstable_version_types::ka:
        case sstable_version_types::la:
            throw std::runtime_error(
                "Statistics is malformed: SSTable is in 2.x format but contains serialization header.");
        }
        // Should never reach here - compiler will complain if switch above does not cover all sstable versions
        abort();
    }

    // mc serialization header minimum values are delta-encoded based on the default timestamp epoch times
    // Note: following conversions rely on min_*_base.value being unsigned to prevent signed integer overflow
    api::timestamp_type get_min_timestamp() const {
        return static_cast<api::timestamp_type>(min_timestamp_base.value + encoding_stats::timestamp_epoch);
    }

    int64_t get_min_ttl() const {
        return static_cast<int64_t>(min_ttl_base.value + encoding_stats::ttl_epoch);
    }

    int64_t get_min_local_deletion_time() const {
        return static_cast<int64_t>(min_local_deletion_time_base.value + encoding_stats::deletion_time_epoch);
    }
};

struct disk_token_bound {
    uint8_t exclusive; // really a boolean
    disk_string<uint16_t> token;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(exclusive, token); }
};

struct disk_token_range {
    disk_token_bound left;
    disk_token_bound right;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(left, right); }
};

// Scylla-specific sharding information.  This is a set of token
// ranges that are spanned by this sstable.  When loading the
// sstable, we can see which shards own data in the sstable by
// checking each such range.
struct sharding_metadata {
    disk_array<uint32_t, disk_token_range> token_ranges;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(token_ranges); }
};

// Scylla-specific list of features an sstable supports.
enum sstable_feature : uint8_t {
    NonCompoundPIEntries = 0,       // See #2993
    NonCompoundRangeTombstones = 1, // See #2986
    ShadowableTombstones = 2, // See #3885
    CorrectStaticCompact = 3, // See #4139
    CorrectEmptyCounters = 4, // See #4363
    CorrectUDTsInCollections = 5, // See #6130
    CorrectLastPiBlockWidth = 6,
    End = 7,
};

// Scylla-specific features enabled for a particular sstable.
struct sstable_enabled_features {
    uint64_t enabled_features;

    bool is_enabled(sstable_feature f) const {
        return enabled_features & (1 << f);
    }

    void disable(sstable_feature f) {
        enabled_features &= ~(1<< f);
    }

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(enabled_features); }

    static sstable_enabled_features all() {
        return sstable_enabled_features{(1 << sstable_feature::End) - 1};
    }
};

// Numbers are found on disk, so they do matter. Also, setting their sizes of
// that of an uint32_t is a bit wasteful, but it simplifies the code a lot
// since we can now still use a strongly typed enum without introducing a
// notion of "disk-size" vs "memory-size".
enum class metadata_type : uint32_t {
    Validation = 0,
    Compaction = 1,
    Stats = 2,
    Serialization = 3,
};

enum class scylla_metadata_type : uint32_t {
    Sharding = 1,
    Features = 2,
    ExtensionAttributes = 3,
    RunIdentifier = 4,
    LargeDataStats = 5,
    SSTableOrigin = 6,
    ScyllaBuildId = 7,
    ScyllaVersion = 8,
    ExtTimestampStats = 9,
    SSTableIdentifier = 10,
};

// UUID is used for uniqueness across nodes, such that an imported sstable
// will not have its run identifier conflicted with the one of a local sstable.
struct run_identifier {
    // UUID is used for uniqueness across nodes, such that an imported sstable
    // will not have its run identifier conflicted with the one of a local sstable.
    run_id id;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(id); }
};

using sstable_id = utils::tagged_uuid<struct sstable_id_tag>;

// UUID is used for uniqueness across nodes, such that an imported sstable
// will not have its identifier conflicted with the one of a local sstable.
// The identifier is initialized using the sstable UUID generation, if available,
// or a time-UUID otherwise.
struct sstable_identifier_type {
    sstable_id value;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(value); }
};

// Types of large data statistics.
//
// Note: For extensibility, never reuse an identifier,
// only add new ones, since these are stored on stable storage.
enum class large_data_type : uint32_t {
    partition_size = 1,     // partition size, in bytes
    row_size = 2,           // row size, in bytes
    cell_size = 3,          // cell size, in bytes
    rows_in_partition = 4,  // number of rows in a partition
    elements_in_collection = 5,// number of elements in a collection
};

struct large_data_stats_entry {
    uint64_t max_value;
    uint64_t threshold;
    uint32_t above_threshold;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(max_value, threshold, above_threshold); }
};

// Types of extended timestamp statistics.
//
// Note: For extensibility, never reuse an identifier,
// only add new ones, since these are stored on stable storage.
enum class ext_timestamp_stats_type : uint32_t {
    min_live_timestamp = 1,
    min_live_row_marker_timestamp = 2,
};

struct scylla_metadata {
    using extension_attributes = disk_hash<uint32_t, disk_string<uint32_t>, disk_string<uint32_t>>;
    using large_data_stats = disk_hash<uint32_t, large_data_type, large_data_stats_entry>;
    using sstable_origin = disk_string<uint32_t>;
    using scylla_build_id = disk_string<uint32_t>;
    using scylla_version = disk_string<uint32_t>;
    using ext_timestamp_stats = disk_hash<uint32_t, ext_timestamp_stats_type, int64_t>;
    using sstable_identifier = sstable_identifier_type;

    disk_set_of_tagged_union<scylla_metadata_type,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::Sharding, sharding_metadata>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::Features, sstable_enabled_features>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::ExtensionAttributes, extension_attributes>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::RunIdentifier, run_identifier>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::LargeDataStats, large_data_stats>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::SSTableOrigin, sstable_origin>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::ScyllaBuildId, scylla_build_id>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::ScyllaVersion, scylla_version>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::ExtTimestampStats, ext_timestamp_stats>,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::SSTableIdentifier, sstable_identifier>
            > data;

    sstable_enabled_features get_features() const {
        auto features = data.get<scylla_metadata_type::Features, sstable_enabled_features>();
        if (!features) {
            return sstable_enabled_features{};
        }
        return *features;
    }
    const extension_attributes* get_extension_attributes() const {
        return data.get<scylla_metadata_type::ExtensionAttributes, extension_attributes>();
    }
    void remove_extension_attributes() {
        data.data.erase(scylla_metadata_type::ExtensionAttributes);
    }
    extension_attributes& get_or_create_extension_attributes() {
        auto* ext = data.get<scylla_metadata_type::ExtensionAttributes, extension_attributes>();
        if (ext == nullptr) {
            data.set<scylla_metadata_type::ExtensionAttributes>(extension_attributes{});
            ext = data.get<scylla_metadata_type::ExtensionAttributes, extension_attributes>();
        }
        return *ext;
    }
    std::optional<run_id> get_optional_run_identifier() const {
        auto* m = data.get<scylla_metadata_type::RunIdentifier, run_identifier>();
        return m ? std::make_optional(m->id) : std::nullopt;
    }
    const ext_timestamp_stats* get_ext_timestamp_stats() const {
        return data.get<scylla_metadata_type::ExtTimestampStats, ext_timestamp_stats>();
    }
    sstable_id get_optional_sstable_identifier() const {
        auto* sid = data.get<scylla_metadata_type::SSTableIdentifier, scylla_metadata::sstable_identifier>();
        return sid ? sid->value : sstable_id::create_null_id();
    }

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(data); }
};

static constexpr int DEFAULT_CHUNK_SIZE = 65536;

// checksums are generated using adler32 algorithm.
struct checksum : public weakly_referencable<checksum>, enable_lw_shared_from_this<checksum> {
    uint32_t chunk_size;
    utils::chunked_vector<uint32_t> checksums;

    checksum()
            : chunk_size(0)
            , checksums()
    {}

    explicit checksum(uint32_t chunk_size, utils::chunked_vector<uint32_t> checksums)
            : chunk_size(chunk_size)
            , checksums(std::move(checksums))
    {}

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(chunk_size, checksums); }
};

}

namespace std {

template <>
struct hash<sstables::metadata_type> : enum_hash<sstables::metadata_type> {};

}

namespace sstables {

// Special value to represent expired (i.e., 'dead') liveness info
constexpr static int64_t expired_liveness_ttl = std::numeric_limits<int32_t>::max();

inline bool is_expired_liveness_ttl(int64_t ttl) {
    return ttl == expired_liveness_ttl;
}

inline bool is_expired_liveness_ttl(gc_clock::duration ttl) {
    return is_expired_liveness_ttl(ttl.count());
}

// Corresponding to Cassandra's NO_DELETION_TIME
constexpr static int64_t no_deletion_time = std::numeric_limits<int32_t>::max();

// Corresponding to Cassandra's MAX_DELETION_TIME
constexpr static int64_t max_deletion_time = std::numeric_limits<int32_t>::max() - 1;

inline int32_t adjusted_local_deletion_time(gc_clock::time_point local_deletion_time, bool& capped) {
    int64_t ldt = local_deletion_time.time_since_epoch().count();
    if (ldt <= max_deletion_time) {
        capped = false;
        return static_cast<int32_t>(ldt);
    }
    capped = true;
    return static_cast<int32_t>(max_deletion_time);
}

struct statistics {
    disk_array<uint32_t, std::pair<metadata_type, uint32_t>> offsets; // ordered by metadata_type
    std::unordered_map<metadata_type, std::unique_ptr<metadata>> contents;
};

enum class column_mask : uint8_t {
    none = 0x0,
    deletion = 0x01,
    expiration = 0x02,
    counter = 0x04,
    counter_update = 0x08,
    range_tombstone = 0x10,
    shadowable = 0x40
};

inline column_mask operator&(column_mask m1, column_mask m2) {
    return column_mask(static_cast<uint8_t>(m1) & static_cast<uint8_t>(m2));
}

inline column_mask operator|(column_mask m1, column_mask m2) {
    return column_mask(static_cast<uint8_t>(m1) | static_cast<uint8_t>(m2));
}

class unfiltered_flags_m final {
    static constexpr uint8_t END_OF_PARTITION = 0x01u;
    static constexpr uint8_t IS_MARKER = 0x02u;
    static constexpr uint8_t HAS_TIMESTAMP = 0x04u;
    static constexpr uint8_t HAS_TTL = 0x08u;
    static constexpr uint8_t HAS_DELETION = 0x10u;
    static constexpr uint8_t HAS_ALL_COLUMNS = 0x20u;
    static constexpr uint8_t HAS_COMPLEX_DELETION = 0x40u;
    static constexpr uint8_t HAS_EXTENDED_FLAGS = 0x80u;
    uint8_t _flags;
    bool check_flag(const uint8_t flag) const {
        return (_flags & flag) != 0u;
    }
public:
    explicit unfiltered_flags_m(uint8_t flags) : _flags(flags) { }
    bool is_end_of_partition() const {
        return check_flag(END_OF_PARTITION);
    }
    bool is_range_tombstone() const {
        return check_flag(IS_MARKER);
    }
    bool has_extended_flags() const {
        return check_flag(HAS_EXTENDED_FLAGS);
    }
    bool has_timestamp() const {
        return check_flag(HAS_TIMESTAMP);
    }
    bool has_ttl() const {
        return check_flag(HAS_TTL);
    }
    bool has_deletion() const {
        return check_flag(HAS_DELETION);
    }
    bool has_all_columns() const {
        return check_flag(HAS_ALL_COLUMNS);
    }
    bool has_complex_deletion() const {
        return check_flag(HAS_COMPLEX_DELETION);
    }
};

class unfiltered_extended_flags_m final {
    static const uint8_t IS_STATIC = 0x01u;
    // This flag is used by Cassandra but not supported by Scylla because
    // Scylla's representation of shadowable tombstones is different.
    // We only check it on reading and error out if set but never set ourselves.
    static const uint8_t HAS_CASSANDRA_SHADOWABLE_DELETION = 0x02u;
    // This flag is Scylla-specific and used for writing shadowable tombstones.
    static const uint8_t HAS_SCYLLA_SHADOWABLE_DELETION = 0x80u;
    uint8_t _flags;
    bool check_flag(const uint8_t flag) const {
        return (_flags & flag) != 0u;
    }
public:
    explicit unfiltered_extended_flags_m(uint8_t flags) : _flags(flags) { }
    bool is_static() const {
        return check_flag(IS_STATIC);
    }
    bool has_cassandra_shadowable_deletion() const {
        return check_flag(HAS_CASSANDRA_SHADOWABLE_DELETION);
    }
    bool has_scylla_shadowable_deletion() const {
        return check_flag(HAS_SCYLLA_SHADOWABLE_DELETION);
    }
};

class column_flags_m final {
    static const uint8_t IS_DELETED = 0x01u;
    static const uint8_t IS_EXPIRING = 0x02u;
    static const uint8_t HAS_EMPTY_VALUE = 0x04u;
    static const uint8_t USE_ROW_TIMESTAMP = 0x08u;
    static const uint8_t USE_ROW_TTL = 0x10u;
    uint8_t _flags;
    bool check_flag(const uint8_t flag) const {
        return (_flags & flag) != 0u;
    }
public:
    explicit column_flags_m(uint8_t flags) : _flags(flags) { }
    bool use_row_timestamp() const {
        return check_flag(USE_ROW_TIMESTAMP);
    }
    bool use_row_ttl() const {
        return check_flag(USE_ROW_TTL);
    }
    bool is_deleted() const {
        return check_flag(IS_DELETED);
    }
    bool is_expiring() const {
        return check_flag(IS_EXPIRING);
    }
    bool has_value() const {
        return !check_flag(HAS_EMPTY_VALUE);
    }
};
}

template <>
struct fmt::formatter<sstables::deletion_time> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::deletion_time& dt, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(),
                              "{{timestamp={}, deletion_time={}}}",
                              dt.marked_for_delete_at, dt.marked_for_delete_at);
    }
};
