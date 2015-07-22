/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "disk_types.hh"
#include "core/enum.hh"
#include "bytes.hh"
#include "gc_clock.hh"
#include "tombstone.hh"
#include "streaming_histogram.hh"
#include "estimated_histogram.hh"
#include "column_name_helper.hh"
#include "sstables/key.hh"
#include <vector>
#include <unordered_map>
#include <type_traits>

namespace sstables {

struct option {
    disk_string<uint16_t> key;
    disk_string<uint16_t> value;

    template <typename Describer>
    auto describe_type(Describer f) { return f(key, value); }
};

struct filter {
    uint32_t hashes;
    disk_array<uint32_t, uint64_t> buckets;

    template <typename Describer>
    auto describe_type(Describer f) { return f(hashes, buckets); }

    // Create an always positive filter if nothing else is specified.
    filter() : hashes(0), buckets({}) {}
    explicit filter(int hashes, std::vector<uint64_t> buckets) : hashes(hashes), buckets({std::move(buckets)}) {}
};

struct index_entry {
    disk_string<uint16_t> key;
    uint64_t position;
    disk_string<uint32_t> promoted_index;

    key_view get_key() const {
        return { bytes_view(key) };
    }
};

struct summary_entry {
    bytes key;
    uint64_t position;

    key_view get_key() const {
        return { key };
    }

    bool operator==(const summary_entry& x) const {
        return position ==  x.position && key == x.key;
    }
};

// Note: Sampling level is present in versions ka and higher. We ATM only support la,
// so it's always there. But we need to make this conditional if we ever want to support
// other formats (unlikely)
struct summary_la {
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
    std::vector<uint32_t> positions;
    std::vector<summary_entry> entries;

    disk_string<uint32_t> first_key;
    disk_string<uint32_t> last_key;

    // Used to determine when a summary entry should be added based on min_index_interval.
    // NOTE: keys_written isn't part of on-disk format of summary.
    size_t keys_written;

    // NOTE4: There is a structure written by Cassandra into the end of the Summary
    // file, after the field last_key, that we haven't understand yet, but we know
    // that its content isn't related to the summary itself.
    // The structure is basically as follow:
    // struct { disk_string<uint16_t>; uint32_t; uint64_t; disk_string<uint16_t>; }
    // Another interesting fact about this structure is that it is apparently always
    // filled with the same data. It's too early to judge that the data is useless.
    // However, it was tested that Cassandra loads successfully a Summary file with
    // this structure removed from it. Anyway, let's pay attention to it.
};
using summary = summary_la;

struct replay_position {
    uint64_t segment;
    uint32_t position;

    replay_position() {}

    replay_position(uint64_t seg, uint32_t pos) {
        segment = seg;
        position = pos;
    }

    template <typename Describer>
    auto describe_type(Describer f) { return f(segment, position); }
};

struct metadata {
    virtual ~metadata() {}
};

struct validation_metadata : public metadata {
    disk_string<uint16_t> partitioner;
    double filter_chance;

    size_t serialized_size() {
        return sizeof(uint16_t) + partitioner.value.size() + sizeof(filter_chance);
    }

    template <typename Describer>
    auto describe_type(Describer f) { return f(partitioner, filter_chance); }
};

struct compaction_metadata : public metadata {
    disk_array<uint32_t, uint32_t> ancestors;
    disk_array<uint32_t, uint8_t> cardinality;

    size_t serialized_size() {
        return sizeof(uint32_t) + (ancestors.elements.size() * sizeof(uint32_t)) +
            sizeof(uint32_t) + (cardinality.elements.size() * sizeof(uint8_t));
    }

    template <typename Describer>
    auto describe_type(Describer f) { return f(ancestors, cardinality); }
};

struct la_stats_metadata : public metadata {
    estimated_histogram estimated_row_size;
    estimated_histogram estimated_column_count;
    replay_position position;
    uint64_t min_timestamp;
    uint64_t max_timestamp;
    uint32_t max_local_deletion_time;
    double compression_ratio;
    streaming_histogram estimated_tombstone_drop_time;
    uint32_t sstable_level;
    uint64_t repaired_at;
    disk_array<uint32_t, disk_string<uint16_t>> min_column_names;
    disk_array<uint32_t, disk_string<uint16_t>> max_column_names;
    bool has_legacy_counter_shards;

    template <typename Describer>
    auto describe_type(Describer f) {
        return f(
            estimated_row_size,
            estimated_column_count,
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
};
using stats_metadata = la_stats_metadata;

// Numbers are found on disk, so they do matter. Also, setting their sizes of
// that of an uint32_t is a bit wasteful, but it simplifies the code a lot
// since we can now still use a strongly typed enum without introducing a
// notion of "disk-size" vs "memory-size".
enum class metadata_type : uint32_t {
    Validation = 0,
    Compaction = 1,
    Stats = 2,
};


static constexpr int DEFAULT_CHUNK_SIZE = 65536;

// checksums are generated using adler32 algorithm.
struct checksum {
    uint32_t chunk_size;
    std::vector<uint32_t> checksums;

    template <typename Describer>
    auto describe_type(Describer f) { return f(chunk_size, checksums); }
};

}

namespace std {

template <>
struct hash<sstables::metadata_type> : enum_hash<sstables::metadata_type> {};

}

namespace sstables {

struct statistics {
    disk_hash<uint32_t, metadata_type, uint32_t> hash;
    std::unordered_map<metadata_type, std::unique_ptr<metadata>> contents;
};

struct deletion_time {
    int32_t local_deletion_time;
    int64_t marked_for_delete_at;

    template <typename Describer>
    auto describe_type(Describer f) { return f(local_deletion_time, marked_for_delete_at); }

    bool live() const {
        return (local_deletion_time == std::numeric_limits<int32_t>::max()) &&
               (marked_for_delete_at == std::numeric_limits<int64_t>::min());
    }

    explicit operator tombstone() {
        return tombstone(marked_for_delete_at, gc_clock::time_point(gc_clock::duration(local_deletion_time)));
    }
};

enum class column_mask : uint8_t {
    none = 0x0,
    deletion = 0x01,
    expiration = 0x02,
    counter = 0x04,
    counter_update = 0x08,
    range_tombstone = 0x10,
};

inline column_mask operator&(column_mask m1, column_mask m2) {
    return column_mask(static_cast<uint8_t>(m1) & static_cast<uint8_t>(m2));
}

inline column_mask operator|(column_mask m1, column_mask m2) {
    return column_mask(static_cast<uint8_t>(m1) | static_cast<uint8_t>(m2));
}
}

