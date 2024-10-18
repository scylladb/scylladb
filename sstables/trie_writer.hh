/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// Overview:
//
// This file implements a page-aware trie writer.
//
// The main class of this file, trie_writer, turns a sorted stream of keys into a stream of
// trie nodes and padding which tries to balance the following goals:
//
// 1. Each node lies entirely within one page (guaranteed).
// 2. Parents are located in the same page as their children (best-effort).
// 3. Padding is minimized (best-effort).
//
// It does mostly what you'd expect a trie builder to do (whenever a new key is added,
// branch out from the rightmost path and add a chain of new nodes, perhaps write some finished nodes to disk).
// The complicated part is the size calculations needed to group parents and children together.
//
// For the most part we follow Cassandra's way of doing that.
// See also:
// https://github.com/apache/cassandra/blob/f9e2f1b219c0b570eb59d529da42722178608572/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
// https://github.com/apache/cassandra/blob/f9e2f1b219c0b570eb59d529da42722178608572/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md?plain=1#L347
// The main difference is that our writer operates in units of character chains,
// rather than on single characters.
// With a node per character, the performance of a writer based on single characters
// can get catastrophically bad for long keys.

#pragma once

#include <seastar/util/log.hh>
#include <map>
#include <set>
#include "utils/assert.hh"
#include "utils/small_vector.hh"

extern seastar::logger trie_logger;

namespace trie {

// Enables code which is useful for debugging during development,
// but too expensive to be compiled into release builds (even if dynamically disabled).
constexpr bool developer_build = false;
// Adds expensive extra checks against use-after-free on pointers obtained from the bump_allocator.
// Must be a macro so that it can affect whether some struct fields are defined or not.
#define TRIE_SANITIZE_BUMP_ALLOCATOR developer_build
// Many asserts and logs are only useful during development,
// where the cost of logging doesn't matter at all.
// And during development it might be useful to have A LOT of them,
// and make them expensive.
//
// But in production, if the code is hot enough is frequent enough, we might care even about the small
// cost of dynamically checking whether a logger is enabled, which discourages adding more trace logs,
// which is sad?
//
// Maybe the right thing is to do is to have a special most verbose log level disabled at compile time by default,
// and only enabled by developers who are actively working on the relevant feature?
// This way we free to add as many logs as we please, without worrying at all about the performance cost.
template <typename... Args>
void expensive_log(seastar::logger::format_info_t<Args...> fmt, Args&&... args) {
    if constexpr (developer_build) {
        trie_logger.trace(std::move(fmt), std::forward<Args>(args)...);
    }
}
inline void expensive_assert(bool expr, std::source_location srcloc = std::source_location::current()) {
    if constexpr (developer_build) {
        if (!expr) {
            __assert_fail("", srcloc.file_name(), srcloc.line(), srcloc.function_name());
        }
    }
}

// Yet another way to represent a buffer view.
// We aleady have bytes_view, and perhaps I should use it instead.
//
// But std::span<const std::byte> is, in some sense, the standard type for this purpose.
// std::as_bytes() exists, after all.
using const_bytes = std::span<const std::byte>;

inline constexpr uint64_t round_down(uint64_t a, uint64_t factor) {
    return a - a % factor;
}

inline constexpr uint64_t round_up(uint64_t a, uint64_t factor) {
    return round_down(a + factor - 1, factor);
}

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
// But this isn't really a critical optimization.
// Last time I checked, it only saves something on the order of ~25% of work.
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
    // We keep the last freed segment here. If we need a new buffer and this is present, we reuse it instead of allocating a new one.
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

template <typename T>
using ptr = bump_allocator::ptr<T>;

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
    std::array<std::byte, 20> _payload_buf = {};
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
    bool operator==(const trie_payload& other) const {
        return other._payload_bits == _payload_bits
            && std::ranges::equal(other.blob(), blob());
    }

    const_bytes blob() const noexcept{
        return {_payload_buf.data(), _payload_size};
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
    operator sink_offset() const {
        return sink_offset(value);
    }
};

// Represents a position in the output stream.
// Just a strongly-typed wrapper around int64_t.
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

struct writer_node;

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
    // _transition points at the chain of characters (of length _transition_meta)
    // corresponding to this node.
    //
    // In the root node, _transition is meaningless, though it's still set to something non-empty.
    //
    // In uncompleted nodes, must be allocated after the node itself.
    // (Our memory management assumes a certain order of things inside the bump_allocator).
    //
    // Invalid after _pos is set.
    ptr<std::byte[]> _transition;
    // Before _pos is set: represents the length of _transition.
    // After the node is fully constructed, must be greater than 0.
    //
    // After _pos is set: *contains* the first byte of this node's transition chain.
    uint32_t _transition_meta = 0;
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
    static sink_offset recalc_sizes(ptr<writer_node> self, const trie_writer_sink auto&, sink_pos start_pos);
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
    static void write(ptr<writer_node> self, trie_writer_sink auto&, bool guaranteed_fit);
};

inline ptr<writer_node> writer_node::create(const_bytes b, bump_allocator& alctr) {
    // We could do this in a single allocation, though we would have to change the interface
    // of bump_allocator somewhat to enable that.
    auto x = alctr.alloc<writer_node>();
    new (x.get()) writer_node;
    auto transition = alctr.alloc<std::byte[]>(b.size());
    std::memcpy(transition.get(), b.data(), b.size());
    x->_transition = transition;
    x->_transition_meta = b.size();
    return x;
}

inline ptr<writer_node> writer_node::add_child(const_bytes b, bump_allocator& alctr) {
    SCYLLA_ASSERT(get_children().empty() || b[0] > get_children().back()->_transition[0]);
    reserve_children(get_children().size() + 1, alctr);
    auto new_child = create(b, alctr);
    push_child(new_child, alctr);
    return get_children().back();
}

inline std::span<const ptr<writer_node>> writer_node::get_children() const {
    return {_children.get(), _children_size};
}

inline std::span<ptr<writer_node>> writer_node::get_children() {
    return {_children.get(), _children_size};
}

inline void writer_node::reset_children() {
    _children_size = 0;
    _children_capacity = 0;
    _children = {};
}

inline void writer_node::reserve_children(size_t n, bump_allocator& alctr) {
    if (n > _children_capacity) {
        auto new_capacity = std::bit_ceil(n);
        auto new_array = alctr.alloc<ptr<writer_node>[]>(new_capacity);
        std::ranges::copy(get_children(), new_array.get());
        _children_capacity = new_capacity;
        _children = new_array;
    }
}

inline void writer_node::push_child(ptr<writer_node> x, bump_allocator& alctr) {
    expensive_assert(_children_capacity > _children_size);
    _children[_children_size++] = x;
}

inline std::byte writer_node::transition() const {
    return _pos.valid() ? std::byte(_transition_meta) : _transition[0];
}

inline void writer_node::set_payload(const trie_payload& p) noexcept {
    expensive_assert(!_node_size.valid());
    expensive_assert(!_pos.valid());
    _payload = p;
}

inline void writer_node::write(ptr<writer_node> self, trie_writer_sink auto& out, bool guaranteed_fit) {
    expensive_log("writer_node::write: subtree={}", fmt::ptr(self.get()));
    expensive_assert(!self->_pos.valid());
    expensive_assert(self->_node_size.valid());
    if (guaranteed_fit) {
        auto page_of_first_byte = round_down(out.pos().value, out.page_size());
        auto page_of_last_byte = round_down((out.pos() + self->_branch_size + self->_node_size).value - 1, out.page_size());
        expensive_assert(page_of_first_byte == page_of_last_byte);
    }
    auto starting_pos = out.pos();
    // A trie can be arbitrarily deep, so we can't afford to use recursion.
    // We have to maintain a walk stack manually.
    struct local_state {
        ptr<writer_node> _node;
        // The index of the next child to be visited.
        // When equal to the number of children, it's time to walk out of the node.
        int _stage;
        // The start position of the node's entire subtree.
        sink_pos _startpos;
    };
    // Note: partition index should almost always have depth smaller than 6.
    // Row index can have depth of up to several thousand.
    utils::small_vector<local_state, 8> stack;
    // Depth-first walk.
    // We write out the subtree in postorder.
    stack.push_back({self, 0, starting_pos});
    while (!stack.empty()) {
        // Caution: note that `node`, `pos`, `stage` are references to contents of `stack`.
        // Any pushing or popping will invalidate them, so be sure not to use them afterwards!
        auto& [node, stage, startpos] = stack.back();
        if (stage < static_cast<int>(node->get_children().size())) {
            stage += 1;
            if (!node->get_children()[stage - 1]->_pos.valid()) {
                stack.push_back({node->get_children()[stage - 1], 0, out.pos()});
            }
            continue;
        }

        // Leaf nodes must have a payload.
        expensive_assert(node->get_children().size() || node->_payload._payload_bits);

        expensive_log("writer_node::write: node={} pos={} n_children={} size={} transition_size={}",
            fmt::ptr(node.get()), out.pos().value, node->get_children().size(), node->_node_size.value, node->_transition_meta);

        if (guaranteed_fit) {
            SCYLLA_ASSERT(out.pos() - startpos == node->_branch_size);
            node->_pos = sink_pos(out.write(*node, sink_pos(out.pos())));
            SCYLLA_ASSERT(out.pos() - startpos == node->_branch_size + node->_node_size);
        } else {
            if (uint64_t(out.serialized_size(*node, sink_pos(out.pos())).value) > out.bytes_left_in_page()) {
                out.pad_to_page_boundary();
            }
            node->_branch_size = out.pos() - startpos;
            auto before = out.pos();
            node->_pos = sink_pos(out.write(*node, sink_pos(out.pos())));
            node->_node_size = node_size((out.pos() - before).value);
            expensive_assert(uint64_t(node->_node_size.value) <= out.page_size());
        }

        node->_transition_meta = uint8_t(node->_transition[0]);
        stack.pop_back();
    }
    expensive_assert(out.pos() - starting_pos == self->_branch_size + self->_node_size);
    self->_branch_size = sink_offset(0);
    self->_node_size = node_size(0);
    self->_has_out_of_page_children = false;
    self->_has_out_of_page_descendants = false;
}

inline sink_offset writer_node::recalc_sizes(ptr<writer_node> self, const trie_writer_sink auto& out, sink_pos global_pos) {
    // This routine is very similar to `write` in its overall structure.

    // A trie can be arbitrarily deep, so we can't afford to use recursion.
    // We have to maintain a walk stack manually.
    struct local_state {
        ptr<writer_node> _node;
        // The index of the next child to be visited.
        // When equal to the number of children, it's time to walk out of the node.
        int _stage;
        // The start position of the node's entire subtree.
        sink_pos _startpos;
    };
    utils::small_vector<local_state, 8> stack;
    // Depth-first walk.
    // To calculate the sizes, we essentially simulate a write of the tree.
    stack.push_back({self, 0, global_pos});
    while (!stack.empty()) {
        // Caution: note that `node`, `pos`, `stage` are references to contents of `stack`.
        // Any pushing or popping will invalidate them, so be sure not to use them afterwards!
        auto& [node, stage, startpos] = stack.back();
        // If the node has out of page grandchildren, the sizes of children might have changed from the estimate,
        // so we have to recurse into the children and update them.
        if (stage < static_cast<int>(node->get_children().size()) && node->_has_out_of_page_descendants) {
            stage += 1;
            stack.push_back({node->get_children()[stage - 1], 0, global_pos});
            continue;
        }
        // If we got here, then either we have recursed into children
        // (and then global_pos was updated accordingly),
        // or we skipped that because there was no need. In the latter case,
        // we have to update global_pos manually here.
        if (!node->_has_out_of_page_descendants) {
            global_pos = global_pos + node->_branch_size;
        }
        node->_branch_size = global_pos - startpos;
        if (node->_has_out_of_page_children || node->_has_out_of_page_descendants) {
            // The size of children might have changed, which might have in turn changed
            // our offsets to children, which might have changed our size.
            // We have to recalculate.
            node->_node_size = out.serialized_size(*node, sink_pos(global_pos));
            expensive_assert(uint64_t(node->_node_size.value) <= out.page_size());
        }
        global_pos = global_pos + node->_node_size;
        stack.pop_back();
    }
    return self->_branch_size + self->_node_size;
}

// Turns a stream of keys into a stream of serializable trie nodes and appropriately
// sized padding, such that:
//
// 1. Each node lies entirely within one page (guaranteed).
// 2. Parents are located in the same page as their children (best-effort).
// 3. Padding is minimized (best-effort).
//
// The part of code responsible for serialization of nodes to bytes is
// separated from trie_writer for the sake of testability.
// It's passed to the writer via the Output parameter.
// It is a template parameter instead of a virtual object because we don't want to incur optimization barriers.
// The effect of this use of templates on the developer tooling (e.g. code completion, incremental compilation)
// is sad, though.
template <trie_writer_sink Output>
class trie_writer {
    // As usual.
    constexpr static size_t default_allocator_segment_size = 128 * 1024;
    // Something much smaller than a page, but big enough to make the batching efficient.
    constexpr static size_t default_max_chain_length = 300;
    Output& _out;
    bump_allocator _allocator;
    size_t _current_depth = 0;
    size_t _max_chain_length;
    // Holds pointers to all nodes in the rightmost path in the tree.
    // Never empty. _stack[0] is the root of the trie.
    utils::small_vector<ptr<writer_node>, 8> _stack;
private:
    // Initializes/resets _root.
    void reset_root();
    // Completes and pops top nodes in _stack until _stack[depth] (exclusive).
    void complete_until_depth(size_t depth);
    // Completes the given node.
    void complete(ptr<writer_node> x);
    // If it fits into the current page
    // (or if it doesn't fit, but there is no hope that it will fit later either),
    // writes out the given subtree and returns true.
    // Otherwise returns false.
    bool try_write(ptr<writer_node> x);
    // Writes out the (proper) subtrees of the given node, and frees them,
    // leaving only the positions and first transition characters of the immediate children.
    void lay_out_children(ptr<writer_node> x);
    // The last part of `lay_out_children`, the one which frees the subtrees.
    // In a separate method just because it's allocator-specific.
    void compact_after_writing_children(ptr<writer_node> x);
public:
    trie_writer(
        Output&,
        size_t max_chain_length = default_max_chain_length,
        size_t allocator_segment_size = default_allocator_segment_size);
    ~trie_writer() = default;
    // Adds a new key to the trie.
    //
    // `depth` describes the level of the trie where we have to fork off a new branch
    // (a chain with the transition `key_tail` and payload `p` at the end).
    //
    // The fact that our caller calculates the `depth`, not us, might seem awkward,
    // but since the callers usually know the mismatch point for other reasons,
    // I figured this API spares us from a double key comparison.
    void add(size_t depth, const_bytes key_tail, const trie_payload& p);
    // Flushes all remaining nodes and returns the position of the root node.
    // The position is valid iff at least one key was added.
    sink_pos finish();
};

template <trie_writer_sink Output>
inline void trie_writer<Output>::reset_root() {
    std::byte empty[] = {std::byte(0)};
    _stack.push_back(writer_node::create(empty, _allocator));
}

template <trie_writer_sink Output>
inline trie_writer<Output>::trie_writer(Output& out, size_t max_chain_length, size_t allocator_segment_size)
    : _out(out)
    , _allocator(allocator_segment_size)
    , _max_chain_length(max_chain_length)
{
    reset_root();
}

// Called when the writer walks out of the node because it's done receiving children.
// This will initialize the members involved in size calculations.
// If the size of the subtree grows big enough, the node's children will be written
// out.
template <trie_writer_sink Output>
inline void trie_writer<Output>::complete(ptr<writer_node> x) {
    expensive_log("trie_writer::complete: x={}", fmt::ptr(x.get()));
    expensive_assert(!x->_branch_size.valid());
    expensive_assert(!x->_node_size.valid());
    expensive_assert(x->_has_out_of_page_children == false);
    expensive_assert(x->_has_out_of_page_descendants == false);
    expensive_assert(!x->_pos.valid());
    bool has_out_of_page_children = false;
    bool has_out_of_page_descendants = false;
    auto branch_size = sink_offset{0};
    for (const auto& c : x->get_children()) {
        branch_size = branch_size + c->_branch_size + sink_offset(c->_node_size);
        has_out_of_page_children |= c->_pos.valid();
        has_out_of_page_descendants |= c->_has_out_of_page_descendants || c->_has_out_of_page_children;
    }
    auto node_size = _out.serialized_size(*x, sink_pos((_out.pos() + branch_size)));
    // We try to keep parents in the same page as their children as much as possible.
    //
    // If the completed subtree fits into a page, we leave it in one piece.
    //
    // If it doesn't fit, we have to split it. We choose to do that by writing out *all* children right here.
    // `x` will be written out later by its ancestor.
    //
    // The details of when and how to perform the splitting are fairly arbitrary. But we aren't trying to be optimal.
    // We assume that our greedy strategy should be good enough.
    //
    // See https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
    // for an alternative description of the process.
    if (branch_size + node_size <= sink_offset(_out.page_size())) {
        x->_branch_size = branch_size;
        x->_node_size = node_size;
        x->_has_out_of_page_children = has_out_of_page_children;
        x->_has_out_of_page_descendants = has_out_of_page_descendants;
    } else {
        lay_out_children(x);
        x->_branch_size = sink_offset(0);
        x->_node_size = _out.serialized_size(*x, sink_pos(_out.pos()));
        x->_has_out_of_page_children = true;
        x->_has_out_of_page_descendants = false;
    }
    expensive_assert(uint64_t(x->_node_size.value) <= _out.page_size());
}

template <trie_writer_sink Output>
inline bool trie_writer<Output>::try_write(ptr<writer_node> candidate) {
    // We picked the candidate based on size estimates which might not be exact.
    // In truth, the candidate might not fit into the current page.
    // If the estimates aren't known (via _has_out_of_page_*) to be exact,
    // we have to update them and check for fit.
    uint64_t true_size;
    if (candidate->_has_out_of_page_children || candidate->_has_out_of_page_descendants) {
        true_size = writer_node::recalc_sizes(candidate, _out, _out.pos()).value;
    } else {
        true_size = (candidate->_branch_size + candidate->_node_size).value;
    }
    bool guaranteed_fit = true;
    if (true_size > _out.bytes_left_in_page()) {
        if (true_size > _out.page_size()) {
            // We get here if, after updating the estimates, we see that the candidate branch
            // actually can't fit even in a full page.
            //
            // If we end up in this situation, we don't really have a sane choice other than just splitting it here.
            //
            // That's unfortunate, because it might mean an additional page hop for some range of keys.
            // But we assume that this inefficiency is rare enough to accept.
            //
            // Note: at this point Cassandra performs the split by recursively calling the equivalent
            // of lay_out_children on the candidate.
            // I'm not cool with that, because in the worst case it could lead to stack overflow.
            // I just write the candidate's children out in order, without applying the greedy packing heuristic.
            expensive_log("trie_writer::lay_out_children: size exceeded full page after recalc: {}", true_size);
            guaranteed_fit = false;
        } else {
            // The node doesn't actually fit in current page. The caller might want either to pad to next page,
            // or to pick a different node for writing.
            return false;
        }
    }
    // Write the candidate branch, in postorder, to the file.
    writer_node::write(candidate, _out, guaranteed_fit);
    return true;
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::lay_out_children(ptr<writer_node> x) {
    expensive_log("trie_writer::lay_out_children x={}", fmt::ptr(x.get()));
    expensive_assert(!x->_pos.valid());

    // In this routine, we write out all child branches.
    // We are free to do it in an arbitrary order.
    //
    // In an attempt to minimize padding, we always first try to write out
    // the largest child branch which still fits into the current page.
    // If none of them fit, we pad to the next page boundary and try again.
    //
    // In theory, this results in an index that is from 1x to 2x smaller than
    // and index where all child branches are just written in transition order.
    // I don't know what's the number in practice. Maybe it's not worth the CPU
    // spent on sorting by size? (It probably is worth it, though).

    struct cmp {
        using is_transparent = bool;
        bool operator()(ptr<writer_node> a, ptr<writer_node> b) const {
            return std::make_pair(a->_branch_size + a->_node_size, a->_transition[0])
                 < std::make_pair(b->_branch_size + b->_node_size, b->_transition[0]);
        }
        bool operator()(int64_t a, ptr<writer_node> b) const {
            return a < (b->_branch_size + b->_node_size).value;
        }
    };
    auto unwritten_children = std::set<ptr<writer_node>, cmp>(cmp());

    for (const auto& c : x->get_children()) {
        if (!c->_pos.valid()) {
            unwritten_children.insert(c);
        }
    }

    while (unwritten_children.size()) {
        // Find the smallest child which doesn't fit. (All might fit, then we will get the past-the-end iterator).
        // Its predecessor will be the biggest child which does fit.
        auto choice_it = unwritten_children.upper_bound(_out.bytes_left_in_page());
        if (choice_it == unwritten_children.begin()) {
            // None of the still-unwritten children fits into the current page,
            // so we pad to page boundary and "try again".
            //
            // We don't have to call upper_bound again, though.
            // All children should fit into a full page,
            // so we can just the biggest child.
            _out.pad_to_page_boundary();
            expensive_assert(_out.bytes_left_in_page() == _out.page_size());
            // Pick the biggest child branch.
            choice_it = std::end(unwritten_children);
        }
        // The predecessor of upper_bound is the biggest child which still fits.
        choice_it = std::prev(choice_it);
        ptr<writer_node> candidate = *choice_it;
        unwritten_children.erase(choice_it);

        if (!try_write(candidate)) {
            // After updating the estimates, we see that the node doesn't actually fit into the current page.
            // Following Cassandra, we return this candidate to the set and try again to find a different
            // candidate that fits.
            //
            // I'm not sure what's the upper bound of the number of retries we might have to do because of this.
            // It's probably not worth caring about, but if we wanted a hard limit, then we could just pad to the next
            // page at this point and just write this candidate out.
            unwritten_children.insert(candidate);
        }
    }
    // Removes all unneeded information from the node's children.
    // This is the place where we free memory which isn't needed anymore.
    compact_after_writing_children(x);
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::compact_after_writing_children(ptr<writer_node> x) {
    // Due to how the bump allocator works, we free the unneeded subtrees as follows:
    // 1. Copy the relevant metadata of children to a stack-allocated array.
    // 2. Discard everything that comes after x's transition. The allocation order
    //    guarantees that this will cover the written-out subtrees, but not anything
    //    yet-unwritten.
    // 3. Allocate a new array to hold the relevant child metadata,
    //    and copy the metadata saved on the stack into it.

    // Step 1.
    struct child_copy {
        uint8_t _transition_byte;
        int16_t _node_size;
        int64_t _branch_size;
        int64_t _pos;
    };
    std::array<child_copy, 256> copy;
    size_t n_children = x->get_children().size();
    for (size_t i = 0; i < n_children; ++i) {
        copy[i]._transition_byte = uint8_t(x->get_children()[i]->_transition[0]);
        copy[i]._pos = x->get_children()[i]->_pos.value;
        copy[i]._branch_size = x->get_children()[i]->_branch_size.value;
        copy[i]._node_size = x->get_children()[i]->_node_size.value;
    }
    // Step 2.
    x->reset_children();
    #if TRIE_SANITIZE_BUMP_ALLOCATOR
        // Check that we aren't freeing things still in use.
        expensive_assert(x._global_pos < x->_transition._global_pos);
    #endif
    _allocator.discard(x->_transition.offset(x->_transition_meta));
    // Step 3.
    x->reserve_children(n_children, _allocator);
    // Even though only the 4 copied child fields are needed after this point,
    // we use an array of full `writer_node` objects to store them.
    // This is somewhat wasteful. With some extra flags and reinterpret_casts,
    // we could be more efficient.
    auto new_nodes = _allocator.alloc<writer_node[]>(n_children);
    for (size_t i = 0; i < n_children; ++i) {
        new (&new_nodes[i]) writer_node;
        new_nodes[i]._transition_meta = copy[i]._transition_byte;
        new_nodes[i]._pos = sink_pos(sink_pos(copy[i]._pos));
        new_nodes[i]._branch_size = sink_offset(copy[i]._branch_size);
        new_nodes[i]._node_size = node_size(copy[i]._node_size);
        x->push_child(new_nodes.element(i), _allocator);
    }
    #if TRIE_SANITIZE_BUMP_ALLOCATOR
        // Sanity check.
        expensive_assert(x->_children->_global_pos > x->_transition._global_pos);
    #endif
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::complete_until_depth(size_t depth) {
    expensive_log("writer_node::complete_until_depth: start,_stack={}, depth={}, _current_depth={}", _stack.size(), depth, _current_depth);
    while (_current_depth > depth) {
        // Every node must be smaller than a page, and the transition chain
        // must be short enough to ensure that.
        //
        // But also smaller nodes are easier to pack (they result in less padding in the index),
        // so it's good for them to be smaller than some fraction of a page, e.g. 10%.
        //
        // So we limit the transition length for a single node to something around that size.
        size_t cut_off = std::min<size_t>(_current_depth - depth, _max_chain_length);

        auto back = _stack.back();
        auto transition = back->_transition;
        auto transition_size = back->_transition_meta;
        #if TRIE_SANITIZE_BUMP_ALLOCATOR
            expensive_assert(transition._size == transition_size);
        #endif
        if (transition_size > cut_off) {
            // Either we are completing a transition chain which is either longer than acceptable,
            // or we are branching off from a point in the middle of a chain.
            // We have to split the chain.

            // First, we complete the current node, but with transition limited to the tail part.
            auto [t1, t2] = transition.split(transition_size - cut_off);
            back->_transition = t2;
            back->_transition_meta = cut_off;
            complete(back);

            // Then, we insert a new node in between the completed one and its parent.
            // It points to the front of the transition string.
            //
            // To make compact_after_writing_children possible,
            // we must maintain the property that _transition comes later in the bump_allocator
            // than the node itself. So we do something tricky: we reuse the memory address
            // of the just-completed node, and move the just-completed node elsewhere.
            auto new_node = _allocator.alloc<writer_node>();
            new (new_node.get()) writer_node;
            std::swap(*new_node, *back);

            back->_transition = t1;
            back->_transition_meta = transition_size - cut_off;
            back->reserve_children(2, _allocator);
            back->push_child(new_node, _allocator);

            _current_depth -= cut_off;
        } else {
            complete(back);
            _stack.pop_back();
            _current_depth -= transition_size;
        }
        expensive_assert(back->_transition_meta > 0);
        expensive_assert(_stack.back()->_transition_meta > 0);
    }
    expensive_log("writer_node::complete_until_depth: end, stack={}, depth={}, _current_depth={}", _stack.size(), depth, _current_depth);
}

template <trie_writer_sink Output>
inline void trie_writer<Output>::add(size_t depth, const_bytes key_tail, const trie_payload& p) {
    expensive_assert(_stack.size() >= 1);
    SCYLLA_ASSERT(_current_depth >= depth);
    // There is only one case where a zero-length tail is legal:
    // when inserting the empty key.
    SCYLLA_ASSERT(!key_tail.empty() || depth == 0);
    SCYLLA_ASSERT(p._payload_bits);

    complete_until_depth(depth);
    if (key_tail.size()) {
        _stack.push_back(_stack.back()->add_child(key_tail, _allocator));
        _current_depth = depth + key_tail.size();
    }
    _stack.back()->set_payload(p);
}

template <trie_writer_sink Output>
inline sink_pos trie_writer<Output>::finish() {
    expensive_assert(_stack.size() >= 1);
    if (!(_stack.front()->get_children().size() || _stack.front()->_payload._payload_bits)) {
        return sink_pos();
    }

    // Flush all held nodes, except the root.
    complete_until_depth(0);
    expensive_assert(_stack.size() == 1);
    complete(_stack.front());

    // Flush the root.
    if (!try_write(_stack[0])) {
        _out.pad_to_page_boundary();
        bool ok = try_write(_stack[0]);
        SCYLLA_ASSERT(ok);
    }
    auto root_pos = _stack[0]->_pos;

    // Reset the writer.
    _allocator.discard(_stack[0]);
    _stack.clear();
    _current_depth = 0;
    reset_root();

    return root_pos;
}

#undef developer_build
#undef expensive_assert
#undef expensive_log
#undef TRIE_SANITIZE_BUMP_ALLOCATOR

} // namespace trie
