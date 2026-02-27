/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>
#include "common.hh"
#include "utils/assert.hh"

// Adds expensive extra checks against use-after-free on pointers obtained from the bump_allocator.
// Must be a macro so that it can affect whether some struct fields are defined or not.
#define TRIE_SANITIZE_BUMP_ALLOCATOR 0

#if TRIE_SANITIZE_BUMP_ALLOCATOR
static_assert(sstables::trie::developer_build, "TRIE_SANITIZE_BUMP_ALLOCATOR needs sstables::trie::developer_build == true");
#include <map>
#endif

namespace sstables::trie {

// Special-purpose stack-like allocator.
//
// It has two allowed operations:
// 1. Append a new buffer to the "stack".
// 2. Shrink the stack to the given address, invalidating everything allocated after it.
//    (In particular, it's legal to shrink the stack to the middle of some allocation,
//    invalidating the allocation's tail but preserving its head.)
//
// This exists because the trie writer's allocation pattern is quite stack-like,
// and it would be a shame not to try to take advantage of it.
//
// That said, this isn't really a critical optimization.
// Last time I checked, it only saves around ~25% of work.
// I'm not sure if it's worth the added complexity.
class bump_allocator {
public:
    // This is just a sanitizing wrapper around a pointer.
    // In release builds it should behave exactly like a regular pointer.
    template <typename T>
    struct ptr {
        using value_type = std::conditional_t<std::is_array_v<T>, std::remove_extent_t<T>, T>;
        friend bump_allocator;
        value_type* _ptr = nullptr;
    #if TRIE_SANITIZE_BUMP_ALLOCATOR
        // Owner.
        bump_allocator* _alctr = nullptr;
        // The index of last discard at the time this allocation was created.
        size_t _discard_counter = 0;
        // The position of this allocation in the allocator's stack.
        size_t _global_pos = 0;
        // The size of this allocation, in bytes.
        // Only a past-the-end pointer, obtained with x.offset(x._size), might have _size equal to zero.
        // Other pointers must have non-zero _size.
        size_t _size = 0;
    #endif
    public:
        ptr() {}
        [[nodiscard]] value_type* get() const noexcept {
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                if (_ptr) {
                    auto legal_timestamp_it = std::prev(this->_alctr->_max_legal_timestamp.lower_bound(this->_global_pos + this->_size - 1));
                    expensive_assert(this->_discard_counter >= legal_timestamp_it->second);
                    expensive_assert(this->_global_pos + (this->_size != 0) <= this->_alctr->global_pos());
                }
            #endif
            return _ptr;
        }
        [[nodiscard]] value_type& operator[] (size_t i) const noexcept {
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                expensive_assert(_size);
            #endif
            return get()[i];
        };
        [[nodiscard]] value_type& operator*() const noexcept {
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                expensive_assert(_size);
            #endif
            return get()[0];
        };
        [[nodiscard]] value_type* operator->() const noexcept {
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                expensive_assert(_size);
            #endif
            return get();
        };
        // Returns a pointer which represents the tail of this allocation,
        // but starting at `n`th element.
        [[nodiscard]] ptr offset(size_t n) const noexcept {
            auto ret = *this;
            ret._ptr += n;
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                size_t sz = n * sizeof(value_type);
                expensive_assert(ret._size >= sz);
                ret._global_pos += sz;
                ret._size -= sz;
            #endif
            return ret;
        };
        [[nodiscard]] ptr<value_type> element(size_t n) const noexcept
        requires(std::is_array_v<T>) {
            auto tmp = offset(n);
            ptr<value_type> ret;
            ret._ptr = tmp._ptr;
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                ret._alctr = tmp._alctr;
                ret._discard_counter = tmp._discard_counter;
                ret._global_pos = tmp._global_pos;
                ret._size = tmp._size;
            #endif
            return ret;
        };
        // Splits the allocation in two.
        [[nodiscard]] std::pair<ptr, ptr> split(size_t first_size) const noexcept
        requires(std::is_array_v<T>)
        {
            auto ret1 = *this;
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                size_t sz = first_size * sizeof(value_type);
                expensive_assert(_size > sz);
                ret1._size = sz;
            #endif
            auto ret2 = this->offset(first_size);
            return {ret1, ret2};
        };
    };

    constexpr static size_t segment_alignment = alignof(max_align_t);
    // As usual in Scylla, the allocation stack is fragmented to avoid large contiguous buffers.
    size_t _segment_size;

private:
    struct aligned_deleter {
        void operator()(std::byte ptr[]) { free(ptr); }
    };
    using buf = std::unique_ptr<std::byte[], aligned_deleter>;
    // We keep the most recently freed segment here.
    // If we need a new buffer and this is present,
    // we reuse it instead of allocating a new one.
    // This ensures we won't repeatedly free and allocate a segment from the main allocator.
    buf _spare = nullptr;
    // The segment currently at the top of the stack.
    buf _current = nullptr;
    // Remaining free space in _current.
    size_t _remaining = 0;
    // Previous segments in the stack.
    std::vector<buf> _old;

#if TRIE_SANITIZE_BUMP_ALLOCATOR
    // The number of discard() calls until now.
    uint64_t _discard_timestamp = 0;
    // Maps given stack positions to the index of the last discard() covering that position.
    // A valid pointer to some position will have a _discard_timestamp which matches this map.
    // An invalidated pointer will have a _discard_timestamp which is smaller than that.
    std::map<size_t, size_t> _max_legal_timestamp{{0, 0}};
#endif

private:
    // Returns the current total size of the "stack".
    size_t global_pos() const {
        return allocated_memory() - _remaining;
    }

    // Invalidate all bytes in the stack since the byte at given address (inclusive).
    void discard_impl(std::byte* addr) noexcept {
        expensive_assert(bool(_current));
        while (!(addr > _current.get() && addr <= _current.get() + _segment_size)) [[unlikely]] {
            #if TRIE_SANITIZE_BUMP_ALLOCATOR
                std::memset(_current.get(), 0xfe, _segment_size);
            #endif
            _spare = std::move(_current);
            expensive_assert(!_old.empty());
            _current = std::move(_old.back());
            _remaining = 0;
            _old.pop_back();
        }
        #if TRIE_SANITIZE_BUMP_ALLOCATOR
            auto end = _current.get() + _segment_size - _remaining;
            std::memset(addr, 0xfe, end - addr);
        #endif
        _remaining = _segment_size - (addr - _current.get());
    }

    void push_segment() {
        if (_current) {
            _old.push_back(std::move(_current));
        }
        if (_spare) {
            _current = std::move(_spare);
        } else {
            _current = std::unique_ptr<std::byte[], aligned_deleter>(
                static_cast<std::byte*>(aligned_alloc(_segment_size, _segment_size)));
        }
    }

    template <typename T>
    requires std::is_trivially_destructible_v<typename ptr<T>::value_type>
        && (alignof(typename ptr<T>::value_type) <= segment_alignment)
    [[nodiscard]] ptr<T> alloc_impl(size_t n) {
        using value_type = ptr<T>::value_type;
        expensive_assert(n < _segment_size / sizeof(value_type));
        SCYLLA_ASSERT(n > 0);
        auto sz = n * sizeof(value_type);
        _remaining -= _remaining % alignof(value_type);
        if (sz > _remaining) [[unlikely]] {
            push_segment();
            // We don't use the first byte of a segment because that would make discard() ambiguous --
            // a pointer to the first byte of a segment could point to an allocation starting at that byte,
            // or point past the end of an allocation from a neighbouring segment.
            _remaining = _segment_size - alignof(value_type);
        }
        auto result = &_current[_segment_size - _remaining];
        ptr<T> ret;
        #if TRIE_SANITIZE_BUMP_ALLOCATOR
            ret._global_pos = global_pos();
            ret._size = sz;
            ret._discard_counter = _discard_timestamp;
            ret._alctr = this;
        #endif
        ret._ptr = reinterpret_cast<value_type*>(result);
        _remaining -= sz;
        expensive_log("bump_allocator: alloc: this={} sz={} result={} _remaining={}",
            fmt::ptr(this), sz, fmt::ptr(result), _remaining);
        return ret;
    }

public:
    bump_allocator(size_t segment_size) : _segment_size(segment_size) {
        SCYLLA_ASSERT(_segment_size % alignof(max_align_t) == 0);
    }

    // Total memory usage by this allocator.
    size_t allocated_memory() const {
        return (_old.size() + bool(_current)) * _segment_size;
    }

    // Allocate a suitably-aligned (uninitialized) buffer for an array of N elements of type T.
    // The total size of this allocation must be smaller than segment_size.
    template <typename T>
    [[nodiscard]] ptr<T> alloc()
    requires (!std::is_array_v<T>) {
        return alloc_impl<T>(1);
    }

    template <typename T>
    [[nodiscard]] ptr<T> alloc(size_t n)
    requires (std::is_array_v<T>) {
        return alloc_impl<T>(n);
    }

    // Invalidate all bytes in the stack since the byte at given address (inclusive).
    template <typename T>
    void discard(ptr<T> p) noexcept {
        auto addr = reinterpret_cast<std::byte*>(p.get());
        expensive_log("bump_allocator: discard: this={} addr={} _global_pos={} _remaining={}",
            fmt::ptr(this), fmt::ptr(addr), global_pos(), _remaining);
        discard_impl(addr);
        #if TRIE_SANITIZE_BUMP_ALLOCATOR
            _discard_timestamp++;
            _max_legal_timestamp.erase(_max_legal_timestamp.upper_bound(p._global_pos), _max_legal_timestamp.end());
            _max_legal_timestamp.insert({p._global_pos, _discard_timestamp});
        #endif
    }
};

// The below few types are just strongly-typed wrappers around integers,
// just so they can have some extra correctness checks and
// so that we can write .valid() instead of "!= -1".
// I'm not sure if this is better or worse than just using regular integers.

// Represents a difference between positions in the output stream.
// Just a strongly-typed wrapper around int64_t.
struct sink_offset {
    int64_t value = -1;
    sink_offset() = default;
    explicit sink_offset(int64_t v) : value(v) {}
    std::strong_ordering operator<=>(const sink_offset& o) const {
        expensive_assert(valid());
        expensive_assert(o.valid());
        return value <=> o.value;
    };
    bool operator==(const sink_offset& o) const = default;
    bool valid() const {
        return value >= 0;
    }
    sink_offset operator+(sink_offset o) const {
        expensive_assert(valid());
        expensive_assert(o.valid());
        return sink_offset{value + o.value};
    }
};

// Represents a size of a serialized node.
// Just a strongly-typed wrapper around int16_t.
struct node_size {
    int16_t value = -1;
    node_size() = default;
    explicit node_size(int16_t v) : value(v) {}
    bool valid() const {
        return value >= 0;
    }
    // node_size is a "subtype" of sink_offset
    // in our typing exercise.
    operator sink_offset() const {
        return sink_offset(value);
    }
};

// Represents a position in the output stream.
// Just a strongly-typed wrapper around int64_t,
// which forbids some arithmetic operations.
//
// E.g. a sink_pos can be subtracted from sink_pos
// to get an offset,
// but a sink_pos can't be added to sink_pos, which
// would be nonsense.
struct sink_pos {
    int64_t value = -1;
    sink_pos() = default;
    explicit sink_pos(int64_t v) : value(v) {}
    bool valid() const {
        return value >= 0;
    }
    std::strong_ordering operator<=>(const sink_pos& o) const {
        expensive_assert(valid());
        expensive_assert(o.valid());
        return value <=> o.value;
    };
    sink_pos operator+(sink_offset o) const {
        expensive_assert(valid());
        expensive_assert(o.valid());
        return sink_pos{value + o.value};
    }
    sink_pos operator-(sink_offset o) const {
        expensive_assert(valid());
        expensive_assert(o.valid());
        return sink_pos{value - o.value};
    }
    sink_offset operator-(sink_pos pos) const {
        expensive_assert(valid());
        expensive_assert(pos.valid());
        return sink_offset{value - pos.value};
    }
};

// The user of the writer inserts (key, payload) pairs. This is the payload.
// A payload consists of a 4-bit header and several bytes of content.
struct trie_payload {
    // What the reader will see is a 4-bit "payload bits" header, and a pointer to the body ("bytes") of the payload.
    // The reader will use the header to determine the length and the meaning of the "bytes".
    //
    // The meaning of "bits" and "bytes" is generally opaque to the trie-writing layer,
    // except that the value `bits == 0` must represent "no payload", and "bytes" is assumed
    // empty in this case.
    uint8_t _payload_bits = 0;
    // We store the payload in a std::array + size pair,
    // since it makes managing allocations slightly easier.
    //
    // 20 bytes is the biggest maximum possible payload in the BTI index format,
    // so we don't need more.
    constexpr static int MAX_PAYLOAD_SIZE = 20;
    std::array<std::byte, MAX_PAYLOAD_SIZE> _payload_buf = {};
    // For the writer, the meaning of the "bits" is opaque,
    // we just store an explicit size of the "bytes".
    uint8_t _payload_size = 0;

    trie_payload() noexcept = default;
    trie_payload(uint8_t bits, const_bytes blob) noexcept {
        expensive_assert(blob.size() <= _payload_buf.size());
        expensive_assert(bits < 16);
        expensive_assert(bool(blob.size()) == bool(bits));
        _payload_bits = bits;
        _payload_size = blob.size();
        std::ranges::copy(blob, _payload_buf.data());
    }

    // For tests.
    bool operator==(const trie_payload& other) const {
        return other._payload_bits == _payload_bits
            && std::ranges::equal(other.blob(), blob());
    }

    const_bytes blob() const noexcept{
        return {_payload_buf.data(), _payload_size};
    }
};

class writer_node;

// trie_writer consumes a stream of keys and produces a stream of writer_node objects,
// which is fed into a trie_writer_sink.
//
// We pass the serialization logic to trie_writer via a template parameter for the sake
// of testability. This way the logic of trie_writer and the serialization format can be tested
// in isolation from each other.
template <typename T>
concept trie_writer_sink = requires(T& o, const T& co, const writer_node& x, sink_pos pos) {
    // Returns the serialized size of node x, if it was written starting at position `pos`.
    // This might only depend on the deltas between `pos` and the positions (known or expected)
    // of children.
    { o.serialized_size(x, pos) } -> std::same_as<node_size>;
    // Writes out the node and returns the position which the parent should point to.
    // (Which might be different than the start position. This is because the BTI format will
    // turns a node with a multi-character transition into many single-character nodes, and
    // the parent must point to the last one, not the first one).
    { o.write(x, pos) } -> std::same_as<sink_pos>;
    { o.pad_to_page_boundary() };
    { co.pos() } -> std::same_as<sink_pos>;
    { co.page_size() } -> std::same_as<uint64_t>;
    { co.bytes_left_in_page() } -> std::same_as<uint64_t>;
};

// The trie writer holds a trie of these nodes while it's building the index pages.
//
// Whenever a new key is fed to the writer:
// 1. The "latest node" pointer climbs up the rightmost path to the point of mismatch between
// the previous key and the new key, and "completes" nodes along the way.
// (A node is "completed" when we climb above it, which means that it won't receive any new children).
// 2. If a newly-completed subtree can't fit into a page of the file writer,
// branches of the subtree are flushed and freed from memory,
// and only the root of the subtree remains in memory, with metadata updated accordingly.
// 3. A new node, containing the chain of new characters after the mismatch point, is added as the latest node.
struct writer_node {
    template <typename T>
    using ptr = bump_allocator::ptr<T>;
    // The payload of this node. It might be absent -- this is represented by _payload._payload_bits == 0.
    //
    // A node has a payload iff it corresponds to some key.
    // All leaf nodes must contain a payload.
    trie_payload _payload;
    // True iff any of the direct children of this node lies on a different page
    // (i.e. has been written out beforehand, in a different lay_out_children() than this node).
    // It means that the size of this node might change from the initial estimate at completion
    // (because the offset to children might have grow, and the offset integers need more bits to fit),
    // so it has to be recalculated before the actual write.
    //
    // After the node itself is written out, this is reset to `false`.
    //
    // Invalid after _pos is set.
    bool _has_out_of_page_children = false;
    // True iff some yet-unwritten descendant has out-of-page children.
    // This means that the sizes of our children (and thus also our size) might change from the initial
    // estimate at completion time (because some offsets in their subtree might have grown, and the offset
    // integers now require more bits), so they have to be recalculated before the actual write.
    //
    // After the node's children are written out, this is always `false`.
    //
    // Invalid after _pos is set.
    bool _has_out_of_page_descendants = false;
    // This is a byte-based trie. A path from the root to a node corresponds
    // to some key, with each successive node appending several characters.
    // _transition points at the chain of characters (of length _transition_length)
    // corresponding to the edge between this node and its parent.
    //
    // In the root node, _transition is meaningless, though it's still set to something non-empty.
    //
    // In uncompleted nodes, must be allocated after the node itself.
    // (Our memory management assumes a certain order of things inside the bump_allocator).
    //
    // Invalid after _pos is set.
    ptr<std::byte[]> _transition;
    // Before _pos is set: _length represents the length of _transition.
    // After the node is fully constructed, _length will be greater than 0.
    //
    // After _pos is set: _first_byte is the first byte of this node's transition chain.
    union {
        uint32_t _transition_length = 0;
        std::byte _first_transition_byte;
    };
    // The expected serialized size of the node.
    // Depending on context (especially the value of _has_out_of_page_children),
    // this might contain an estimate (which will be re-checked later),
    // or an exact value (which will be later required to be correct).
    //
    // Only valid for completed nodes.
    // Invalidated after _pos is set.
    node_size _node_size;
    // A vector of children, ordered by the first byte of their `_transition`.
    //
    // In uncompleted nodes, must be allocated after the node itself.
    // (Our memory management assumes a certain order of things inside the bump_allocator).
    //
    // Invalid after _pos is set.
    uint16_t _children_capacity = 0;
    uint16_t _children_size = 0;
    ptr<ptr<writer_node>[]> _children;
    // The expected total serialized size of the node's descendants.
    // (In other words: if we wrote the node's entire subtree to a file, without any padding,
    // we would expect the file to grow by _branch_size + _node_size).
    //
    // Depending on context (especially the values
    // of _has_out_of_page_children and _has_out_of_page_descendants),
    // this might be an estimate or an exact value. (Just like _node_size).
    //
    // Only valid for completed nodes.
    // Invalidated after _pos is set.
    sink_offset _branch_size;
    // The identifier (in practice: position) of the node in the output file/stream.
    //
    // Only set after the node is flushed.
    // After _pos is set, most fields stop being meaningful.
    // Only _pos and _transition, remain meaningful, because they will be needed to
    // will need them to writer.
    sink_pos _pos;

    static ptr<writer_node> create(const_bytes b, bump_allocator&);
    // Emplaces a new child node into `_children`,
    // and returns a pointer to it.
    // Children must be added in order of rising transitions.
    ptr<writer_node> add_child(const_bytes b, bump_allocator&);
    // Accessors.
    std::span<const ptr<writer_node>> get_children() const;
    std::span<ptr<writer_node>> get_children();
    // Resets the _children vector.
    void reset_children();
    // Ensures children has size at least n, growing it if needed.
    void reserve_children(size_t n, bump_allocator& alctr);
    // Appends the new child to _children. Expects sufficient _children_capacity reserved in advance.
    void push_child(ptr<writer_node> x, bump_allocator& alctr);
    // Returns the first character of this node's transition chain.
    std::byte transition() const;
    // Sets the payload.
    //
    // Must only be used on nodes immediately after they are created,
    // i.e. before any other nodes are added.
    //
    // It's separate from `add_child` because the root might need a payload too,
    // but it isn't created by `add_child`.
    void set_payload(const trie_payload&) noexcept;
    // Re-calculates _node_size and/or _branch_size of this node and its descendants
    // (as needed by _has_out_of_page_children or _has_out_of_page_children) under
    // the assumption that we will write out the entire subtree without any padding,
    // with output initially at position start_pos. If the assumption is true,
    // then the values of _node_size and _branch_size are exact after this call.
    template <trie_writer_sink Output>
    static sink_offset recalc_sizes(ptr<writer_node> self, const Output&, sink_pos start_pos);
    // Writes out the entire subtree.
    //
    // When it's known in advance that the entire subtree will fit within the current page,
    // guaranteed_fit can be set to true. This is the typical case.
    //
    // Otherwise, guaranteed_fit must be set to false. In this case it's legal for the subtree
    // to not fit within current page (internal padding will be inserted accordingly so that
    // each node doesn't cross node boundaries), but it will re-calculate sizes again,
    // so it's more expensive.
    //
    // After this call, it's true that:
    // get_children().empty() && _pos.valid() && !_has_out_of_page_children && !_has_out_of_page_descendants
    template <trie_writer_sink Output>
    static void write(ptr<writer_node> self, Output&, bool guaranteed_fit);
};

} // namespace sstables::trie
