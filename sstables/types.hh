/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include "disk_types.hh"
#include "core/enum.hh"
#include "bytes.hh"
#include "gc_clock.hh"
#include "tombstone.hh"
#include "utils/streaming_histogram.hh"
#include "utils/estimated_histogram.hh"
#include "column_name_helper.hh"
#include "sstables/key.hh"
#include "db/commitlog/replay_position.hh"
#include <vector>
#include <unordered_map>
#include <type_traits>

// While the sstable code works with char, bytes_view works with int8_t
// (signed char). Rather than change all the code, let's do a cast.
static inline bytes_view to_bytes_view(const temporary_buffer<char>& b) {
    using byte = bytes_view::value_type;
    return bytes_view(reinterpret_cast<const byte*>(b.get()), b.size());
}

namespace sstables {

struct deletion_time {
    int32_t local_deletion_time;
    int64_t marked_for_delete_at;

    template <typename Describer>
    auto describe_type(Describer f) { return f(local_deletion_time, marked_for_delete_at); }

    bool live() const {
        return (local_deletion_time == std::numeric_limits<int32_t>::max()) &&
               (marked_for_delete_at == std::numeric_limits<int64_t>::min());
    }

    bool operator==(const deletion_time& d) {
        return local_deletion_time == d.local_deletion_time &&
               marked_for_delete_at == d.marked_for_delete_at;
    }
    bool operator!=(const deletion_time& d) {
        return !(*this == d);
    }
    explicit operator tombstone() {
        return !live() ? tombstone(marked_for_delete_at, gc_clock::time_point(gc_clock::duration(local_deletion_time))) : tombstone();
    }
};

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
    explicit filter(int hashes, utils::chunked_vector<uint64_t> buckets) : hashes(hashes), buckets({std::move(buckets)}) {}
};

enum class indexable_element {
    partition,
    cell
};

// Exploded view of promoted index.
// Contains pointers into external buffer, so that buffer must be kept alive
// as long as this is used.
struct promoted_index {
    struct entry {
        composite_view start;
        composite_view end;
        uint64_t offset;
        uint64_t width;
    };
    deletion_time del_time;
    std::deque<entry> entries;
};

class promoted_index_view {
    bytes_view _bytes;
public:
    explicit promoted_index_view(bytes_view v) : _bytes(v) {}
    sstables::deletion_time get_deletion_time() const;
    promoted_index parse(const schema&) const;
    explicit operator bool() const { return !_bytes.empty(); }
};

class index_entry {
    temporary_buffer<char> _key;
    mutable stdx::optional<dht::token> _token;
    uint64_t _position;
    temporary_buffer<char> _promoted_index_bytes;
    stdx::optional<promoted_index> _promoted_index;
public:

    bytes_view get_key_bytes() const {
        return to_bytes_view(_key);
    }

    key_view get_key() const {
        return key_view{get_key_bytes()};
    }

    decorated_key_view get_decorated_key() const {
        if (!_token) {
            _token.emplace(dht::global_partitioner().get_token(get_key()));
        }
        return decorated_key_view(*_token, get_key());
    }

    uint64_t position() const {
        return _position;
    }

    bytes_view get_promoted_index_bytes() const {
        return to_bytes_view(_promoted_index_bytes);
    }

    promoted_index_view get_promoted_index_view() const {
        return promoted_index_view(get_promoted_index_bytes());
    }

    index_entry(temporary_buffer<char>&& key, uint64_t position, temporary_buffer<char>&& promoted_index)
        : _key(std::move(key)), _position(position), _promoted_index_bytes(std::move(promoted_index)) {}

    index_entry(const index_entry& o)
        : _key(o._key.get(), o._key.size())
        , _position(o._position)
        , _promoted_index_bytes(o._promoted_index_bytes.get(), o._promoted_index_bytes.size())
    { }

    index_entry(index_entry&&) = default;

    index_entry& operator=(const index_entry& x) {
        return operator=(index_entry(x));
    }

    index_entry& operator=(index_entry&&) = default;

    promoted_index* get_promoted_index(const schema& s) {
        if (!_promoted_index) {
            auto v = get_promoted_index_view();
            if (v) {
                _promoted_index = v.parse(s);
            }
        }
        return _promoted_index ? &*_promoted_index : nullptr;
    }
};

struct summary_entry {
    dht::token token;
    bytes key;
    uint64_t position;

    key_view get_key() const {
        return key_view{key};
    }

    decorated_key_view get_decorated_key() const {
        return decorated_key_view(token, get_key());
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
        for (auto& e : entries) {
            sz += e.key.size();
        }
        return sz;
    }

    explicit operator bool() const {
        return entries.size();
    }
};
using summary = summary_ka;

class file_writer;

struct metadata {
    virtual ~metadata() {}
    virtual uint64_t serialized_size() const = 0;
    virtual void write(file_writer& write) const = 0;
};

template <typename T>
uint64_t serialized_size(const T& object);

template <class T>
typename std::enable_if_t<!std::is_integral<T>::value && !std::is_enum<T>::value, void>
write(file_writer& out, const T& t);

// serialized_size() implementation for metadata class
template <typename Component>
class metadata_base : public metadata {
public:
    virtual uint64_t serialized_size() const override {
        return sstables::serialized_size(static_cast<const Component&>(*this));
    }
    virtual void write(file_writer& writer) const override {
        return sstables::write(writer, static_cast<const Component&>(*this));
    }
};

struct validation_metadata : public metadata_base<validation_metadata> {
    disk_string<uint16_t> partitioner;
    double filter_chance;

    template <typename Describer>
    auto describe_type(Describer f) { return f(partitioner, filter_chance); }
};

struct compaction_metadata : public metadata_base<compaction_metadata> {
    disk_array<uint32_t, uint32_t> ancestors;
    disk_array<uint32_t, uint8_t> cardinality;

    template <typename Describer>
    auto describe_type(Describer f) { return f(ancestors, cardinality); }
};

struct ka_stats_metadata : public metadata_base<ka_stats_metadata> {
    utils::estimated_histogram estimated_row_size;
    utils::estimated_histogram estimated_column_count;
    db::replay_position position;
    int64_t min_timestamp;
    int64_t max_timestamp;
    int32_t max_local_deletion_time;
    double compression_ratio;
    utils::streaming_histogram estimated_tombstone_drop_time;
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
using stats_metadata = ka_stats_metadata;

struct disk_token_bound {
    uint8_t exclusive; // really a boolean
    disk_string<uint16_t> token;

    template <typename Describer>
    auto describe_type(Describer f) { return f(exclusive, token); }
};

struct disk_token_range {
    disk_token_bound left;
    disk_token_bound right;

    template <typename Describer>
    auto describe_type(Describer f) { return f(left, right); }
};

// Scylla-specific sharding information.  This is a set of token
// ranges that are spanned by this sstable.  When loading the
// sstable, we can see which shards own data in the sstable by
// checking each such range.
struct sharding_metadata {
    disk_array<uint32_t, disk_token_range> token_ranges;

    template <typename Describer>
    auto describe_type(Describer f) { return f(token_ranges); }
};


// Numbers are found on disk, so they do matter. Also, setting their sizes of
// that of an uint32_t is a bit wasteful, but it simplifies the code a lot
// since we can now still use a strongly typed enum without introducing a
// notion of "disk-size" vs "memory-size".
enum class metadata_type : uint32_t {
    Validation = 0,
    Compaction = 1,
    Stats = 2,
};


enum class scylla_metadata_type : uint32_t {
    Sharding = 1,
};

struct scylla_metadata {
    disk_set_of_tagged_union<scylla_metadata_type,
            disk_tagged_union_member<scylla_metadata_type, scylla_metadata_type::Sharding, sharding_metadata>
            > data;

    template <typename Describer>
    auto describe_type(Describer f) { return f(data); }
};

static constexpr int DEFAULT_CHUNK_SIZE = 65536;

// checksums are generated using adler32 algorithm.
struct checksum {
    uint32_t chunk_size;
    utils::chunked_vector<uint32_t> checksums;

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
}

