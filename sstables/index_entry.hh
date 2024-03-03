/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <functional>
#include <variant>
#include "mutation/position_in_partition.hh"
#include "utils/overloaded_functor.hh"
#include "utils/lsa/chunked_managed_vector.hh"
#include "reader_permit.hh"
#include "sstables/types.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/mx/parsers.hh"
#include "utils/allocation_strategy.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "utils/managed_vector.hh"
#include "dht/i_partitioner.hh"

#include <seastar/core/fstream.hh>

namespace sstables {

using use_caching = bool_class<struct use_caching_tag>;
using promoted_index_block_position_view = std::variant<composite_view, position_in_partition_view>;
using promoted_index_block_position = std::variant<composite, position_in_partition>;

inline
promoted_index_block_position_view to_view(const promoted_index_block_position& v) {
    return std::visit(overloaded_functor{
            [] (const composite& v) -> promoted_index_block_position_view {
                return composite_view(v);
            },
            [] (const position_in_partition& v) -> promoted_index_block_position_view {
                return position_in_partition_view(v);
            }
        }, v);
}

// Return the owning version of the position given a view.
inline
promoted_index_block_position materialize(const promoted_index_block_position_view& v) {
    return std::visit(overloaded_functor{
            [] (const composite_view& v) -> promoted_index_block_position {
                return composite(v);
            },
            [] (const position_in_partition_view& v) -> promoted_index_block_position {
                return position_in_partition(v);
            }
        }, v);
}

class promoted_index_block_compare {
    const position_in_partition::composite_less_compare _cmp;
public:
    explicit promoted_index_block_compare(const schema& s) : _cmp{s} {}

    bool operator()(const promoted_index_block_position_view& lhs, position_in_partition_view rhs) const {
        return std::visit([this, rhs] (const auto& pos) { return _cmp(pos, rhs); }, lhs);
    }

    bool operator()(position_in_partition_view lhs, const promoted_index_block_position_view& rhs) const {
        return std::visit([this, lhs] (const auto& pos) { return _cmp(lhs, pos); }, rhs);
    }

    bool operator()(const promoted_index_block_position_view& lhs, composite_view rhs) const {
        return std::visit([this, rhs] (const auto& pos) { return _cmp(pos, rhs); }, lhs);
    }

    bool operator()(composite_view lhs, const promoted_index_block_position_view& rhs) const {
        return std::visit([this, lhs] (const auto& pos) { return _cmp(lhs, pos); }, rhs);
    }

    bool operator()(const promoted_index_block_position_view& lhs, const promoted_index_block_position_view& rhs) const {
        return std::visit([this, &lhs] (const auto& pos) { return (*this)(lhs, pos); }, rhs);
    }
};

class promoted_index_block {
    /*
     * Block bounds are read and stored differently for ka/la and mc formats.
     * For ka/la formats, we just read and store the whole sequence of bytes representing a 'composite' key,
     * but for 'mc' we need to parse the clustering key prefix entirely along with its bound_kind.
     * So we store them as a discriminated union, aka std::variant.
     * As those representations are used differently for comparing positions in partition,
     * we expose it through a discriminated union of views.
     */
    using bound_storage = std::variant<temporary_buffer<char>, position_in_partition>;
    // The block includes positions in the [_start, _end] range (both bounds inclusive)
    bound_storage _start;
    bound_storage _end;
    uint64_t _offset;
    uint64_t _width;
    std::optional<deletion_time> _end_open_marker;

    inline static
    promoted_index_block_position_view get_position(const schema& s, const bound_storage& storage) {
        return std::visit(overloaded_functor{
            [&s] (const temporary_buffer<char>& buf) -> promoted_index_block_position_view {
                return composite_view{to_bytes_view(buf), s.is_compound()}; },
            [] (const position_in_partition& pos) -> promoted_index_block_position_view {
                return pos;
            }}, storage);
    }

public:
    // Constructor for ka/la format blocks
    promoted_index_block(temporary_buffer<char>&& start, temporary_buffer<char>&& end,
            uint64_t offset, uint64_t width)
        : _start(std::move(start)), _end(std::move(end))
        , _offset(offset), _width(width)
    {}
    // Constructor for mc format blocks
    promoted_index_block(position_in_partition&& start, position_in_partition&& end,
            uint64_t offset, uint64_t width, std::optional<deletion_time>&& end_open_marker)
        : _start{std::move(start)}, _end{std::move(end)}
        , _offset{offset}, _width{width}, _end_open_marker{end_open_marker}
    {}

    promoted_index_block(const promoted_index_block&) = delete;
    promoted_index_block(promoted_index_block&&) noexcept = default;

    promoted_index_block& operator=(const promoted_index_block&) = delete;
    promoted_index_block& operator=(promoted_index_block&&) noexcept = default;

    promoted_index_block_position_view start(const schema& s) const { return get_position(s, _start);}
    promoted_index_block_position_view end(const schema& s) const { return get_position(s, _end);}
    uint64_t offset() const { return _offset; }
    uint64_t width() const { return _width; }
    std::optional<deletion_time> end_open_marker() const { return _end_open_marker; }

};

// Cursor over the index for clustered elements of a single partition.
//
// The user is expected to call advance_to() for monotonically increasing positions
// in order to check if the index has information about more precise location
// of the fragments relevant for the range starting at given position.
//
// The user must serialize all async methods. The next call may start only when the future
// returned by the previous one has resolved.
//
// The user must call close() and wait for it to resolve before destroying.
//
class clustered_index_cursor {
public:
    // Position of indexed elements in the data file relative to the start of the partition.
    using offset_in_partition = uint64_t;

    struct skip_info {
        offset_in_partition offset;
        tombstone active_tombstone;
        position_in_partition active_tombstone_pos;
    };

    struct entry_info {
        promoted_index_block_position_view start;
        promoted_index_block_position_view end;
        offset_in_partition offset;
    };

    virtual ~clustered_index_cursor() {};
    // Note: Close must not fail
    virtual future<> close() noexcept = 0;

    // Advances the cursor to given position. When the cursor has more accurate information about
    // location of the fragments from the range [pos, +inf) in the data file (since it was last advanced)
    // it resolves with an engaged optional containing skip_info.
    //
    // The index may not be precise, so fragments from the range [pos, +inf) may be located after the
    // position indicated by skip_info. It is guaranteed that no such fragments are located before the returned position.
    //
    // Offsets returned in skip_info are monotonically increasing.
    //
    // Must be called for non-decreasing positions.
    // The caller must ensure that pos remains valid until the future resolves.
    virtual future<std::optional<skip_info>> advance_to(position_in_partition_view pos) = 0;

    // Determines the data file offset relative to the start of the partition such that fragments
    // from the range (-inf, pos] are located before that offset.
    //
    // If such offset cannot be determined in a cheap way, returns a disengaged optional.
    //
    // Does not advance the cursor.
    //
    // The caller must ensure that pos remains valid until the future resolves.
    virtual future<std::optional<offset_in_partition>> probe_upper_bound(position_in_partition_view pos) = 0;

    // Returns skip information about the next position after the cursor
    // or nullopt if there is no information about further positions.
    //
    // When entry_info is returned, the cursor was advanced to entry_info::start.
    //
    // The returned entry_info is only valid until the next invocation of any method on this instance.
    virtual future<std::optional<entry_info>> next_entry() = 0;
};

// Allocated inside LSA.
class promoted_index {
    deletion_time _del_time;
    uint64_t _promoted_index_start;
    uint32_t _promoted_index_size;
    uint32_t _num_blocks;
public:
    promoted_index(const schema& s,
        deletion_time del_time,
        uint64_t promoted_index_start,
        uint32_t promoted_index_size,
        uint32_t num_blocks)
            : _del_time{del_time}
            , _promoted_index_start(promoted_index_start)
            , _promoted_index_size(promoted_index_size)
            , _num_blocks(num_blocks)
    { }

    [[nodiscard]] deletion_time get_deletion_time() const { return _del_time; }
    [[nodiscard]] uint32_t get_promoted_index_size() const { return _promoted_index_size; }

    // Call under allocating_section.
    // For sstable versions >= mc the returned cursor will be of type `bsearch_clustered_cursor`.
    std::unique_ptr<clustered_index_cursor> make_cursor(shared_sstable,
        reader_permit,
        tracing::trace_state_ptr,
        file_input_stream_options,
        use_caching);
};

// A partition index element.
// Allocated inside LSA.
class index_entry {
private:
    managed_bytes _key;
    mutable std::optional<dht::token> _token;
    uint64_t _position;
    managed_ref<promoted_index> _index;

public:

    key_view get_key() const {
        return key_view{_key};
    }

    // May allocate so must be called under allocating_section.
    decorated_key_view get_decorated_key(const schema& s) const {
        if (!_token) {
            _token.emplace(s.get_partitioner().get_token(get_key()));
        }
        return decorated_key_view(*_token, get_key());
    }

    uint64_t position() const { return _position; };

    std::optional<deletion_time> get_deletion_time() const {
        if (_index) {
            return _index->get_deletion_time();
        }

        return {};
    }

    index_entry(managed_bytes&& key, uint64_t position, managed_ref<promoted_index>&& index)
        : _key(std::move(key))
        , _position(position)
        , _index(std::move(index))
    {}

    index_entry(index_entry&&) = default;
    index_entry& operator=(index_entry&&) = default;

    // Can be nullptr
    const managed_ref<promoted_index>& get_promoted_index() const { return _index; }
    managed_ref<promoted_index>& get_promoted_index() { return _index; }
    uint32_t get_promoted_index_size() const { return _index ? _index->get_promoted_index_size() : 0; }

    size_t external_memory_usage() const {
        return _key.external_memory_usage() + _index.external_memory_usage();
    }
};

// A partition index page.
//
// Allocated in the standard allocator space but with an LSA allocator as the current allocator.
// So the shallow part is in the standard allocator but all indirect objects are inside LSA.
class partition_index_page {
public:
    lsa::chunked_managed_vector<managed_ref<index_entry>> _entries;
public:
    partition_index_page() = default;
    partition_index_page(partition_index_page&&) noexcept = default;
    partition_index_page& operator=(partition_index_page&&) noexcept = default;

    bool empty() const { return _entries.empty(); }
    size_t size() const { return _entries.size(); }

    void clear_one_entry() {
        _entries.pop_back();
    }

    size_t external_memory_usage() const {
        size_t size = _entries.external_memory_usage();
        for (auto&& e : _entries) {
            size += sizeof(index_entry) + e->external_memory_usage();
        }
        return size;
    }
};

using index_list = partition_index_page;

}

inline std::ostream& operator<<(std::ostream& out, const sstables::promoted_index_block_position_view& pos) {
    std::visit([&out] (const auto& pos) mutable { fmt::print(out, "{}", pos); }, pos);
    return out;
}

inline std::ostream& operator<<(std::ostream& out, const sstables::promoted_index_block_position& pos) {
    std::visit([&out] (const auto& pos) mutable { fmt::print(out, "{}", pos); }, pos);
    return out;
}
