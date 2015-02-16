#pragma once

#include "core/enum.hh"

namespace sstables {

// Some in-disk structures have an associated integer (of varying sizes) that
// represents how large they are. They can be a byte-length, in the case of a
// string, number of elements, in the case of an array, etc.
//
// For those elements, we encapsulate the underlying type in an outter
// structure that embeds how large is the in-disk size. It is a lot more
// convenient to embed it in the size than explicitly writing it in the parser.
// This way, we don't need to encode this information in multiple places at
// once - it is already part of the type.
template <typename Size>
struct disk_string {
    sstring value;
};

template <typename Size, typename Members>
struct disk_array {
    static_assert(std::is_integral<Size>::value, "Length type must be convertible to integer");
    std::vector<Members> elements;
};

template <typename Size, typename Key, typename Value>
struct disk_hash {
    std::unordered_map<Key, Value, std::hash<Key>> map;
};

struct option {
    disk_string<uint16_t> key;
    disk_string<uint16_t> value;
};

struct compression {
    disk_string<uint16_t> name;
    disk_array<uint32_t, option> options;
    uint32_t chunk_len;
    uint64_t data_len;
    disk_array<uint32_t, uint64_t> offsets;
};

struct filter {
    uint32_t hashes;
    disk_array<uint32_t, uint64_t> buckets;
};

// FIXME: Not yet, can't know what an index entry is without a schema.
struct summary_entry {
    int notyet;
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
        // We will ignore this.
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
    // size given by the "size" parameter. Have to parse slightly different
    disk_array<uint32_t, summary_entry> entries;
};
using summary = summary_la;
}
