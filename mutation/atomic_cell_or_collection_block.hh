/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <bit>
#include <concepts>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>

#include <seastar/core/loop.hh>

#include "mutation/cell_hash.hh"
#include "seastarx.hh"
#include "utils/allocation_strategy.hh"
#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"
#include "utils/small_vector.hh"

// Packed, allocation-light storage for the cells of a row.
//
// Cells are grouped by column id into atomic_cell_or_collection_blocks, each holding
// up to atomic_cell_or_collection_block_nr_cells consecutive column slots in a single
// variable-size allocation. Small serialized cells are concatenated into an inline byte
// area; cells which do not fit the inline budget are stored in managed_bytes inside the
// block. Blocks are aggregated by atomic_cell_or_collection_block_vectors into a radix
// tree, atomic_cell_or_collection_block_tree, whose height depends only on the largest
// column id stored, i.e. effectively on the schema.
//
// All nodes are allocated with current_allocator() and are LSA-migratable. Each node
// carries a back-reference to the pointer that owns it.

using atomic_cell_or_collection_block_mask = uint8_t;
inline constexpr unsigned atomic_cell_or_collection_block_nr_cells = std::numeric_limits<atomic_cell_or_collection_block_mask>::digits;
inline constexpr unsigned atomic_cell_or_collection_block_slot_bits = std::countr_zero(atomic_cell_or_collection_block_nr_cells);

using atomic_cell_or_collection_block_vector_mask = uint64_t;
inline constexpr unsigned atomic_cell_or_collection_block_vector_fanout = std::numeric_limits<atomic_cell_or_collection_block_vector_mask>::digits;
inline constexpr unsigned atomic_cell_or_collection_block_vector_index_bits = std::countr_zero(atomic_cell_or_collection_block_vector_fanout);

// Helpers for the masks of present cells and children.
namespace bitmask {

template <std::unsigned_integral Mask>
constexpr Mask bit(unsigned position) noexcept {
    return Mask(Mask(1) << position);
}

// Bits strictly below `position`.
template <std::unsigned_integral Mask>
constexpr Mask bits_below(unsigned position) noexcept {
    return Mask(bit<Mask>(position) - 1);
}

// Number of set bits strictly below `position`; the index of `position`'s element
// in an array compressed by `mask`.
template <std::unsigned_integral Mask>
constexpr unsigned rank(Mask mask, unsigned position) noexcept {
    return std::popcount(Mask(mask & bits_below<Mask>(position)));
}

// Calls func(position, rank) for each set bit of `mask`, in increasing position
// order, where rank is the number of set bits below position. If func returns
// stop_iteration, iteration stops when it returns stop_iteration::yes.
// Returns stop_iteration::yes if iteration was stopped.
template <std::unsigned_integral Mask, std::invocable<unsigned, unsigned> Func>
constexpr stop_iteration for_each_set_with_rank(Mask mask, Func&& func) {
    for (unsigned rank = 0; mask; mask &= mask - 1, ++rank) {
        const unsigned position = std::countr_zero(mask);
        if constexpr (std::same_as<std::invoke_result_t<Func&, unsigned, unsigned>, stop_iteration>) {
            if (func(position, rank) == stop_iteration::yes) {
                return stop_iteration::yes;
            }
        } else {
            func(position, rank);
        }
    }
    return stop_iteration::no;
}

// Calls func(position) for each set bit of `mask`, in increasing position order.
// Stops early like for_each_set_with_rank().
template <std::unsigned_integral Mask, std::invocable<unsigned> Func>
constexpr stop_iteration for_each_set(Mask mask, Func&& func) {
    return for_each_set_with_rank(mask, [&func] (unsigned position, unsigned) {
        return func(position);
    });
}

}

class atomic_cell_or_collection_block;
class atomic_cell_or_collection_block_vector;

// Pointer to a node of an atomic_cell_or_collection_block_tree. Whether the node is
// an atomic_cell_or_collection_block (a leaf) or an atomic_cell_or_collection_block_vector
// follows from its height in the tree, which the tree keeps.
class atomic_cell_or_collection_block_tree_node_ptr {
    void* _node = nullptr;
public:
    atomic_cell_or_collection_block_tree_node_ptr() noexcept = default;
    explicit atomic_cell_or_collection_block_tree_node_ptr(void* node) noexcept : _node(node) {}

    explicit operator bool() const noexcept { return _node != nullptr; }
    void* get() const noexcept { return _node; }
    atomic_cell_or_collection_block* block() const noexcept {
        return static_cast<atomic_cell_or_collection_block*>(_node);
    }
    atomic_cell_or_collection_block_vector* vector() const noexcept {
        return static_cast<atomic_cell_or_collection_block_vector*>(_node);
    }

    // Tells the node pointed to (if any) that it is now owned by `owner`.
    inline void set_owner(atomic_cell_or_collection_block_tree_node_ptr* owner) const noexcept;
};

// Common header of tree nodes.
struct atomic_cell_or_collection_block_tree_node_header {
    // Pointer owning this node, updated when the node is relocated. May be null
    // while the node is not installed in a tree.
    atomic_cell_or_collection_block_tree_node_ptr* _backref = nullptr;

    void relocated_to(void* self) noexcept {
        if (_backref) {
            *_backref = atomic_cell_or_collection_block_tree_node_ptr(self);
        }
    }
};

inline void atomic_cell_or_collection_block_tree_node_ptr::set_owner(atomic_cell_or_collection_block_tree_node_ptr* owner) const noexcept {
    if (_node) {
        static_cast<atomic_cell_or_collection_block_tree_node_header*>(_node)->_backref = owner;
    }
}

// A leaf of the tree: up to atomic_cell_or_collection_block_nr_cells cells in one allocation.
//
// Layout (the trailing arrays are compressed: they only have elements for set mask bits,
// in slot order, so the element of slot s is at index bitmask::rank(mask, s)). The arrays are
// ordered by decreasing alignment, so each is aligned naturally after the previous one:
//
//   header                              (sizeof(atomic_cell_or_collection_block))
//   managed_bytes  external[popcount(_external)]
//   cell_hash_opt  hashes[popcount(_hashed)]
//   offset_type    offsets[popcount(_present & ~_external) + 1]   offsets[0] == 0; internal cell i
//                                                                  occupies [offsets[i], offsets[i + 1])
//   int8_t         data[_inline_size]
//
// The shape of a block (which cells it holds and where) never changes after it is built;
// changes are made by building a new block with atomic_cell_or_collection_block_builder.
// The cell contents (and hashes) can be modified in place, as long as sizes don't change.
//
// Only cells of at least hashed_cell_min_size bytes have a hash slot (_hashed): a small
// cell is cheap to rehash, while its 8-byte slot stays in memory for the cell's lifetime.
class atomic_cell_or_collection_block : public atomic_cell_or_collection_block_tree_node_header {
public:
    using mask_type = atomic_cell_or_collection_block_mask;
    using offset_type = uint16_t;
    using ptr = alloc_strategy_unique_ptr<atomic_cell_or_collection_block>;
    static constexpr unsigned nr_cells = atomic_cell_or_collection_block_nr_cells;
    // managed_bytes is byte-aligned, but its pointers are better accessed aligned.
    static constexpr size_t external_cells_alignment = alignof(void*);
    static constexpr size_t hashed_cell_min_size = 128;
private:
    mask_type _present = 0;
    mask_type _external = 0;
    offset_type _inline_size = 0;
    // Subset of _present.
    mask_type _hashed = 0;

    friend class atomic_cell_or_collection_block_builder;

    atomic_cell_or_collection_block(mask_type present, mask_type external, mask_type hashed, offset_type inline_size) noexcept
        : _present(present), _external(external), _inline_size(inline_size), _hashed(hashed) {
    }

    unsigned nr_hashed() const noexcept { return std::popcount(_hashed); }

    unsigned nr_internal() const noexcept { return std::popcount(mask_type(_present & ~_external)); }

    managed_bytes* external_cells() const noexcept {
        return std::assume_aligned<external_cells_alignment>(
                const_cast<managed_bytes*>(reinterpret_cast<const managed_bytes*>(this + 1)));
    }
    cell_hash_opt* hashes() const noexcept {
        return std::assume_aligned<alignof(cell_hash_opt)>(reinterpret_cast<cell_hash_opt*>(external_cells() + nr_external()));
    }
    offset_type* offsets() const noexcept {
        return std::assume_aligned<alignof(offset_type)>(reinterpret_cast<offset_type*>(hashes() + nr_hashed()));
    }
    bytes::value_type* data() const noexcept {
        return reinterpret_cast<bytes::value_type*>(offsets() + nr_internal() + 1);
    }

    managed_bytes_view internal_cell(unsigned internal_rank) const noexcept {
        auto offs = offsets();
        return managed_bytes_view(bytes_view(data() + offs[internal_rank], offs[internal_rank + 1] - offs[internal_rank]));
    }
public:
    static size_t storage_size_for(unsigned nr_present, unsigned nr_external, unsigned nr_hashed, size_t inline_size) noexcept {
        return sizeof(atomic_cell_or_collection_block)
                + nr_external * sizeof(managed_bytes)
                + nr_hashed * sizeof(cell_hash_opt)
                + (nr_present - nr_external + 1) * sizeof(offset_type)
                + inline_size;
    }

    atomic_cell_or_collection_block(atomic_cell_or_collection_block&& o) noexcept;
    ~atomic_cell_or_collection_block();

    // Returns a copy of this block, allocated with current_allocator(), with no owner.
    // The copy has the same layout.
    ptr clone() const;

    size_t storage_size() const noexcept {
        return storage_size_for(nr_present(), nr_external(), nr_hashed(), _inline_size);
    }

    // Memory used by this block and the external cells it owns.
    size_t memory_usage() const noexcept;
    // The total size of the serialized cells, independent of how they are stored.
    // Costs a lookup per external cell, rather than per cell.
    size_t representation_independent_data_size() const noexcept;

    mask_type present() const noexcept { return _present; }
    mask_type external() const noexcept { return _external; }
    mask_type hashed() const noexcept { return _hashed; }
    unsigned nr_present() const noexcept { return std::popcount(_present); }
    unsigned nr_external() const noexcept { return std::popcount(_external); }
    bool contains(unsigned slot) const noexcept { return _present & bitmask::bit<mask_type>(slot); }
    bool is_external(unsigned slot) const noexcept { return _external & bitmask::bit<mask_type>(slot); }

    // The serialized cell at `slot`. The slot must be present.
    managed_bytes_view cell(unsigned slot) const noexcept {
        unsigned rank = bitmask::rank(_present, slot);
        unsigned external_rank = bitmask::rank(_external, slot);
        if (is_external(slot)) {
            return managed_bytes_view(external_cells()[external_rank]);
        }
        return internal_cell(rank - external_rank);
    }

    // A mutable view of the serialized cell at `slot`, for in-place modifications
    // which don't change the size. The slot must be present.
    managed_bytes_mutable_view mutable_cell(unsigned slot) noexcept;

    // The hash slot of the cell at `slot`, or nullptr if the cell has none. Hashes are
    // a cache which may be updated in place even through a const block.
    cell_hash_opt* hash(unsigned slot) const noexcept {
        return _hashed & bitmask::bit<mask_type>(slot) ? &hashes()[bitmask::rank(_hashed, slot)] : nullptr;
    }
    cell_hash_opt hash_value(unsigned slot) const noexcept {
        auto h = hash(slot);
        return h ? *h : cell_hash_opt();
    }

    // The managed_bytes holding the cell at `slot` if it is external, nullptr otherwise.
    managed_bytes* external_storage(unsigned slot) const noexcept {
        return is_external(slot) ? &external_cells()[bitmask::rank(_external, slot)] : nullptr;
    }

    struct cell_entry {
        unsigned slot;
        managed_bytes_view cell;
        // Null iff the cell has no hash slot.
        cell_hash_opt* hash;
        // Non-null iff the cell is external.
        managed_bytes* external_storage;

        cell_hash_opt hash_value() const noexcept { return hash ? *hash : cell_hash_opt(); }
    };

    // Calls func(const cell_entry&) for each cell, in slot order. If func returns
    // stop_iteration, iteration stops early when it returns stop_iteration::yes.
    // Returns stop_iteration::yes if iteration was stopped.
    stop_iteration for_each_cell(std::invocable<const cell_entry&> auto&& func) const {
        unsigned hashed_rank = 0;
        unsigned external_rank = 0;
        return bitmask::for_each_set_with_rank(_present, [&] (unsigned slot, unsigned rank) {
            const bool is_ext = is_external(slot);
            const bool is_hashed = _hashed & bitmask::bit<mask_type>(slot);
            managed_bytes* ext = is_ext ? &external_cells()[external_rank] : nullptr;
            cell_entry e{slot, is_ext ? managed_bytes_view(*ext) : internal_cell(rank - external_rank),
                    is_hashed ? &hashes()[hashed_rank] : nullptr, ext};
            hashed_rank += is_hashed;
            external_rank += is_ext;
            return func(e);
        });
    }
};

// Collects up to atomic_cell_or_collection_block_nr_cells cells and builds an
// atomic_cell_or_collection_block from them.
//
// Cells may be added in any slot order, each slot at most once. Their serialized form
// may come from:
//  - add_copy(): bytes copied into the builder's scratch buffer right away;
//  - add_uninitialized(): bytes the caller writes into the scratch buffer;
//  - add_borrowed(): bytes which stay valid until build() returns, optionally with the
//    managed_bytes holding them, which build() may move into the block (and
//    roll_back() can return);
//  - add_owned(): a managed_bytes owned by the builder.
// The scratch buffer holds inline_budget bytes; cells which don't fit get a
// managed_bytes of their own instead.
//
// build() decides which cells are stored inline: it starts with all cells inline and,
// while the inline area exceeds inline_budget, moves the largest remaining inline cell
// (the lowest slot among equals) out to external storage. This decision depends only on
// the sizes of the cells, so equal sets of cells always produce equal layouts.
class atomic_cell_or_collection_block_builder {
public:
    static constexpr size_t inline_budget = 4096;
    using mask_type = atomic_cell_or_collection_block_mask;
    using block_ptr = atomic_cell_or_collection_block::ptr;
private:
    enum class source_kind : uint8_t { scratch, borrowed, owned };
    struct entry {
        managed_bytes owned;
        managed_bytes_view borrowed;
        managed_bytes* stealable = nullptr;
        size_t scratch_offset = 0;
        size_t size = 0;
        cell_hash_opt hash;
        source_kind kind = source_kind::borrowed;
    };
    // Indexed by slot; the entry of a slot is constructed iff the slot is in _slots.
    union entry_storage {
        entry e;
        entry_storage() noexcept {}
        ~entry_storage() {}
    };
    std::array<entry_storage, atomic_cell_or_collection_block_nr_cells> _entries;
    mask_type _slots = 0;
    size_t _scratch_size = 0;
    std::array<bytes::value_type, inline_budget> _scratch;

    entry& new_entry(unsigned slot, size_t size, cell_hash_opt hash);
    entry& entry_of(unsigned slot) noexcept { return _entries[slot].e; }
    managed_bytes_view entry_view(const entry& e) const noexcept;
    // Reserves `size` bytes of scratch space for the entry, or an owned managed_bytes
    // if the scratch buffer is full, and returns a view of it.
    managed_bytes_mutable_view reserve(entry& e, size_t size);
public:
    atomic_cell_or_collection_block_builder() noexcept {}
    ~atomic_cell_or_collection_block_builder() { clear(); }
    atomic_cell_or_collection_block_builder(const atomic_cell_or_collection_block_builder&) = delete;
    atomic_cell_or_collection_block_builder& operator=(const atomic_cell_or_collection_block_builder&) = delete;

    bool empty() const noexcept { return _slots == 0; }
    unsigned size() const noexcept { return std::popcount(_slots); }
    mask_type slots() const noexcept { return _slots; }

    // Empty cells represent missing cells and are ignored.
    void add_copy(unsigned slot, managed_bytes_view cell, cell_hash_opt hash);
    // Reserves `size` bytes for the cell at `slot`, which the caller must fill with its
    // serialized form. The returned view is valid until the builder is cleared or built.
    // `size` must not be zero.
    managed_bytes_mutable_view add_uninitialized(unsigned slot, size_t size, cell_hash_opt hash);
    void add_borrowed(unsigned slot, managed_bytes_view cell, cell_hash_opt hash, managed_bytes* stealable = nullptr) noexcept;
    void add_owned(unsigned slot, managed_bytes&& cell, cell_hash_opt hash) noexcept;

    // Builds the block with current_allocator(). Must not be called on an empty builder.
    //
    // If it throws, nothing was moved from stealable sources. If it succeeds, some
    // stealable sources may have been moved from; roll_back() moves them back.
    // The returned block has no owner.
    block_ptr build();

    // Undoes the effects of a successful build() on stealable sources and destroys the
    // block. `block` must be the result of the last build(), and the builder must not
    // have been cleared since (its entries tell where the external cells came from).
    // The sources must still be alive.
    void roll_back(block_ptr block) noexcept;

    void clear() noexcept;
};

// An inner node of the tree: up to atomic_cell_or_collection_block_vector_fanout children.
//
// Layout: header, then atomic_cell_or_collection_block_tree_node_ptr children[_capacity],
// of which the first popcount(_present) are used. Children are inserted into spare
// capacity, or by reallocating the vector with more capacity (see
// atomic_cell_or_collection_block_tree::vector_capacity()); they are removed in place,
// so removal never allocates.
class atomic_cell_or_collection_block_vector : public atomic_cell_or_collection_block_tree_node_header {
public:
    using mask_type = atomic_cell_or_collection_block_vector_mask;
    using node_ptr = atomic_cell_or_collection_block_tree_node_ptr;
    static constexpr unsigned fanout = atomic_cell_or_collection_block_vector_fanout;
private:
    mask_type _present = 0;
    uint8_t _capacity = 0;

    friend class atomic_cell_or_collection_block_tree;

    atomic_cell_or_collection_block_vector(mask_type present, unsigned capacity) noexcept
        : _present(present), _capacity(capacity) {
    }
public:
    static size_t storage_size_for(unsigned capacity) noexcept {
        return sizeof(atomic_cell_or_collection_block_vector) + capacity * sizeof(node_ptr);
    }

    atomic_cell_or_collection_block_vector(atomic_cell_or_collection_block_vector&& o) noexcept;

    size_t storage_size() const noexcept { return storage_size_for(_capacity); }
    mask_type present() const noexcept { return _present; }
    unsigned nr_children() const noexcept { return std::popcount(_present); }
    unsigned capacity() const noexcept { return _capacity; }
    node_ptr* children() const noexcept {
        return std::assume_aligned<alignof(node_ptr)>(const_cast<node_ptr*>(reinterpret_cast<const node_ptr*>(this + 1)));
    }
    // Returns nullptr if there is no child at `index`.
    node_ptr* child(unsigned index) const noexcept {
        if (!(_present & bitmask::bit<mask_type>(index))) {
            return nullptr;
        }
        return &children()[bitmask::rank(_present, index)];
    }
};

// A sparse array of atomic_cell_or_collection_blocks, indexed by block index
// (column_id >> atomic_cell_or_collection_block_slot_bits).
//
// All blocks are at the same depth, the tree's height. A tree of height 0 is a single
// block (with block index 0); a vector at height h has children of height h - 1. The
// height is the smallest which can hold the largest block index present.
class atomic_cell_or_collection_block_tree {
public:
    using node_ptr = atomic_cell_or_collection_block_tree_node_ptr;
    using block_index_type = uint32_t;
    using block_type = atomic_cell_or_collection_block;
    using block_ptr = block_type::ptr;
    // Enough for all column ids.
    static constexpr unsigned max_height = (std::numeric_limits<uint32_t>::digits - atomic_cell_or_collection_block_slot_bits
            + atomic_cell_or_collection_block_vector_index_bits - 1) / atomic_cell_or_collection_block_vector_index_bits;
private:
    node_ptr _root;
    // The number of cells in all blocks.
    uint32_t _size = 0;
    uint8_t _height = 0;

    // The smallest height which can hold block_index: the number of
    // atomic_cell_or_collection_block_vector_index_bits-bit digits in it.
    static unsigned height_for(block_index_type block_index) noexcept {
        return (std::bit_width(block_index) + atomic_cell_or_collection_block_vector_index_bits - 1)
                / atomic_cell_or_collection_block_vector_index_bits;
    }
    static unsigned child_index(block_index_type block_index, unsigned height) noexcept {
        return (uint64_t(block_index) >> ((height - 1) * atomic_cell_or_collection_block_vector_index_bits))
                & (atomic_cell_or_collection_block_vector_fanout - 1);
    }
    static block_index_type child_base(block_index_type base, unsigned index, unsigned height) noexcept {
        return base | (block_index_type(index) << ((height - 1) * atomic_cell_or_collection_block_vector_index_bits));
    }
    static void install(node_ptr& slot, node_ptr value) noexcept {
        slot = value;
        value.set_owner(&slot);
    }
    static void destroy_subtree(node_ptr p, unsigned height) noexcept;
    static size_t subtree_memory_usage(node_ptr p, unsigned height) noexcept;
    static node_ptr make_vector(atomic_cell_or_collection_block_vector::mask_type present, const node_ptr* children, unsigned capacity);
    // Below this, the saving in memory is not worth the extra reallocations.
    static constexpr unsigned min_vector_capacity = 4;
    // The capacity to allocate for a new or grown vector at `height` which is to hold
    // the children in `present`. If blocks_to_follow is known (see set_block()), a vector
    // of blocks gets exactly the capacity for them; otherwise vectors start at
    // min_vector_capacity and grow by doubling.
    static unsigned vector_capacity(unsigned height, atomic_cell_or_collection_block_vector::mask_type present,
            std::optional<unsigned> blocks_to_follow) noexcept;
    // Builds a chain of single-child vectors above `node` (which has height `node_height`),
    // up to `height`, along the path to `block_index`.
    static node_ptr make_chain(node_ptr node, unsigned node_height, unsigned height, block_index_type block_index,
            std::optional<unsigned> blocks_to_follow);
    static void destroy_chain(node_ptr chain, unsigned height, unsigned node_height) noexcept;
    // Removes the child at `index` from the vector in `slot`, in place; if the vector
    // becomes empty, it is destroyed and `slot` cleared.
    static void remove_child(node_ptr& slot, unsigned index) noexcept;
    void shrink_root() noexcept;
    node_ptr* find_slot(block_index_type block_index) noexcept;
    // Removes the children cleared by rebuild_blocks() from the vectors of the
    // subtree in slot, in place; vectors left empty are destroyed and their slot cleared.
    static void remove_cleared_children(node_ptr& slot, unsigned height) noexcept;

    static stop_iteration for_each_block_in(node_ptr p, unsigned height, block_index_type base,
            std::invocable<block_index_type, block_type&> auto& func);
    static void for_each_block_slot_in(node_ptr& slot, unsigned height, block_index_type base,
            std::invocable<block_index_type, node_ptr&> auto& func);
    static node_ptr clone_subtree(node_ptr p, unsigned height,
            std::invocable<const block_type&> auto& clone_block);
    // The first block in the subtree p (of height `height`, holding block indexes
    // from base), with block index at least block_index; stores its index in found_index.
    static const block_type* find_block_at_or_after_in(node_ptr p, unsigned height, block_index_type base,
            block_index_type block_index, block_index_type& found_index) noexcept;
    // The first block with block index at least block_index, or nullptr; stores its
    // index in found_index.
    const block_type* find_block_at_or_after(block_index_type block_index, block_index_type& found_index) const noexcept;
public:
    atomic_cell_or_collection_block_tree() noexcept = default;
    atomic_cell_or_collection_block_tree(atomic_cell_or_collection_block_tree&& o) noexcept
        : _root(std::exchange(o._root, {})), _size(std::exchange(o._size, 0)), _height(std::exchange(o._height, 0)) {
        _root.set_owner(&_root);
    }
    atomic_cell_or_collection_block_tree& operator=(atomic_cell_or_collection_block_tree&& o) noexcept {
        if (this != &o) {
            clear();
            _root = std::exchange(o._root, {});
            _size = std::exchange(o._size, 0);
            _height = std::exchange(o._height, 0);
            _root.set_owner(&_root);
        }
        return *this;
    }
    ~atomic_cell_or_collection_block_tree() { clear(); }

    void clear() noexcept;
    bool empty() const noexcept { return !_root; }
    // The number of cells in the tree.
    size_t size() const noexcept { return _size; }
    unsigned height() const noexcept { return _height; }

    const block_type* find_block(block_index_type block_index) const noexcept;
    block_type* find_block(block_index_type block_index) noexcept {
        auto* slot = find_slot(block_index);
        return slot ? slot->block() : nullptr;
    }

    // Installs `block` at block_index, destroying the block previously there, if any.
    // Strong exception guarantee: if it throws, the tree is unchanged and `block`
    // still owns the block.
    //
    // blocks_to_follow, if known, is the number of blocks expected to be added next,
    // at higher block indexes; it is used to size vectors exactly rather than by doubling.
    // It may be an estimate.
    void set_block(block_index_type block_index, block_ptr&& block, std::optional<unsigned> blocks_to_follow = std::nullopt);

    // The blocks present among the atomic_cell_or_collection_block_vector_fanout
    // consecutive block indexes which include block_index (those which a vector of
    // blocks holds), as a mask indexed by block_index % fanout.
    atomic_cell_or_collection_block_vector::mask_type group_present(block_index_type block_index) const noexcept;

    class loader;

    // Removes and destroys the block at block_index, if any.
    void remove_block(block_index_type block_index) noexcept;

    // Calls func(block_index, const block_type&) for each block in index order.
    // func may return stop_iteration.
    stop_iteration for_each_block(std::invocable<block_index_type, const block_type&> auto&& func) const {
        if (!_root) {
            return stop_iteration::no;
        }
        auto as_const = [&func] (block_index_type block_index, block_type& block) -> decltype(auto) {
            return func(block_index, std::as_const(block));
        };
        return for_each_block_in(_root, _height, 0, as_const);
    }

    // Calls func(block_index, block_type&) for each block in index order, which may
    // modify the cells (but not the shape) of the block; see block_type::mutable_cell().
    // func may return stop_iteration.
    stop_iteration for_each_block(std::invocable<block_index_type, block_type&> auto&& func) {
        if (!_root) {
            return stop_iteration::no;
        }
        return for_each_block_in(_root, _height, 0, func);
    }

    struct block_entry {
        block_index_type index;
        const block_type& block;
    };

    // A forward iterator over the blocks, in index order, yielding block_entry by value.
    // Invalidated by any modification of the tree.
    //
    // Each step descends from the root, so for performance prefer for_each_block().
    // The iterator is for consumers which need to walk two trees side by side or
    // want std::ranges algorithms.
    class const_iterator {
        const atomic_cell_or_collection_block_tree* _tree = nullptr;
        // Declared before _block, whose initialization sets it.
        block_index_type _index = 0;
        const block_type* _block = nullptr;
    public:
        using value_type = block_entry;
        using difference_type = std::ptrdiff_t;

        const_iterator() noexcept = default;
        explicit const_iterator(const atomic_cell_or_collection_block_tree& t) noexcept
            : _tree(&t), _block(t.find_block_at_or_after(0, _index)) {
        }
        block_entry operator*() const noexcept { return {_index, *_block}; }
        const_iterator& operator++() noexcept {
            _block = _tree->find_block_at_or_after(_index + 1, _index);
            return *this;
        }
        const_iterator operator++(int) noexcept {
            auto ret = *this;
            ++*this;
            return ret;
        }
        bool operator==(const const_iterator& o) const noexcept { return _block == o._block; }
        bool operator==(std::default_sentinel_t) const noexcept { return !_block; }
    };

    // The blocks, as a range of const_iterator. See const_iterator for when to prefer
    // for_each_block().
    std::ranges::subrange<const_iterator, std::default_sentinel_t> block_range() const noexcept {
        return {const_iterator(*this), std::default_sentinel};
    }

    // A block visited by rebuild_blocks(). The block stays in the tree unchanged
    // unless func calls replace(), remove() or release_to().
    class block_being_rebuilt {
        block_ptr _block;
        bool _changed = false;

        friend class atomic_cell_or_collection_block_tree;
        explicit block_being_rebuilt(block_type* block) noexcept : _block(block) {}
    public:
        // The block, or its replacement. Not valid after remove() or release_to().
        const block_type& operator*() const noexcept { return *_block; }
        const block_type* operator->() const noexcept { return _block.get(); }

        // Replaces the block with `replacement` (which has no owner), destroying it.
        void replace(block_ptr replacement) noexcept {
            _block = std::move(replacement);
            _changed = true;
        }
        // Removes the block from the tree, destroying it.
        void remove() noexcept {
            _block.reset();
            _changed = true;
        }
        // Removes the block from the tree, passing it to take(block_ptr&&), which
        // must take it over (like set_block()), or else throw and leave it in
        // place, in which case the block stays in the tree.
        void release_to(std::invocable<block_ptr&&> auto&& take) {
            take(std::move(_block));
            _changed = true;
        }
    };

    // Calls func(block_index, block_being_rebuilt&) for each block in index order.
    // Blocks removed by func are removed from the tree when the walk is over, even
    // if func throws.
    void rebuild_blocks(std::invocable<block_index_type, block_being_rebuilt&> auto&& func);

    // Returns a copy, whose blocks are produced by clone_block(const block_type&),
    // returning block_ptr. The structure is copied exactly, except that vectors
    // get no spare capacity.
    atomic_cell_or_collection_block_tree clone(std::invocable<const block_type&> auto&& clone_block) const {
        atomic_cell_or_collection_block_tree result;
        if (_root) {
            install(result._root, clone_subtree(_root, _height, clone_block));
            result._size = _size;
            result._height = _height;
        }
        return result;
    }

    // Memory used by all the nodes and external cells.
    size_t memory_usage() const noexcept;
};

// Builds a tree from blocks added in increasing block index order, bottom-up, so
// that every vector is allocated once, with exactly the capacity for its children
// (atomic_cell_or_collection_block_tree::set_block() grows vectors by doubling,
// as it can't know how many children they will end up with).
//
// Since blocks come in index order, every vector of the tree is complete once a
// block past the block indexes it covers is added. So the loader only keeps, for
// each height, the rightmost node: the one which may still get children. It holds
// that node's children, not yet in a vector. When a block past it is added, the
// node is sealed: its children are moved into a vector of exactly their number,
// which becomes a child of the rightmost node of the next height. load_into()
// seals the remaining rightmost nodes, from the bottom up to the root.
//
// So the loader holds at most fanout children per height of the tree being built;
// a height-2 tree (up to fanout^2 * atomic_cell_or_collection_block_nr_cells
// columns) needs no allocation beyond the tree's own nodes.
//
// The held nodes are LSA-migratable, so the loader must not be moved.
class atomic_cell_or_collection_block_tree::loader {
    using vector_mask_type = atomic_cell_or_collection_block_vector::mask_type;
    static constexpr unsigned fanout = atomic_cell_or_collection_block_vector::fanout;
    // The rightmost node at some height h (>= 1), still open to new children: the
    // children (of height h - 1) of the vector it will be sealed into.
    struct rightmost_open_node {
        // The index of the vector among the vectors of its height, i.e. the block
        // indexes it covers divided by fanout^h.
        block_index_type index;
        vector_mask_type present = 0;
        // Indexed by child index; each owns (is the back-reference of) its child.
        std::array<node_ptr, fanout> children;
    };
    // _rightmost_open_node_by_height[h - 1] is the rightmost node at height h. The
    // one at height h + 1, if any, is the parent of the one at height h, and the
    // last is the top of the tree built so far.
    utils::small_vector<rightmost_open_node, 2> _rightmost_open_node_by_height;
    // The number of cells in all blocks.
    uint32_t _size = 0;
    // Most rows have a single block, which needs no vectors. So the first block is
    // kept here (owning it), and only moved into a rightmost node of height 1 when
    // a block with another index is added. Building that node (with its fanout
    // child slots) for a row which turns out to have one block would be wasted work.
    node_ptr _only_block;
    block_index_type _only_block_index = 0;

    // Points the back-references of the children of all rightmost nodes at their slots.
    void set_owners() noexcept;
    // Moves _only_block into a new rightmost node of height 1.
    void move_only_block_to_rightmost_node();
    void load_nonempty_into(atomic_cell_or_collection_block_tree& tree);
    // Adds a rightmost node of the next height, the parent of the current top one.
    void add_level();
    // Seals the rightmost node at `height` into a vector, adding it to the rightmost
    // node above, and leaves an empty rightmost node with `next_index` in its place.
    void seal(unsigned height, block_index_type next_index);
public:
    loader() noexcept = default;
    loader(const loader&) = delete;
    loader& operator=(const loader&) = delete;
    ~loader() {
        if (!empty()) {
            clear();
        }
    }

    bool empty() const noexcept { return !_only_block && _rightmost_open_node_by_height.empty(); }
    // Whether set_block() can take block_index: whether it is not before the block
    // indexes covered by the rightmost node of height 1, since the vectors of blocks
    // before it are sealed.
    bool can_set_block(block_index_type block_index) const noexcept {
        if (_only_block) {
            return block_index / fanout >= _only_block_index / fanout;
        }
        return _rightmost_open_node_by_height.empty() || block_index / fanout >= _rightmost_open_node_by_height.front().index;
    }
    // Returns the block at block_index, if it is held by the loader (it was added,
    // and can_set_block(block_index)).
    block_type* find_block(block_index_type block_index) noexcept;
    // Adds block at block_index, replacing the block there, if any.
    // can_set_block(block_index) must be true. If it throws, block still owns the
    // block, and the loader holds the blocks added before.
    void set_block(block_index_type block_index, block_ptr&& block);
    // Moves all blocks into `tree`, which must be empty. If it throws, the loader
    // still holds the blocks and the tree is empty.
    void load_into(atomic_cell_or_collection_block_tree& tree) {
        if (!empty()) {
            load_nonempty_into(tree);
        }
    }
    // Destroys all blocks.
    void clear() noexcept;
};

stop_iteration atomic_cell_or_collection_block_tree::for_each_block_in(node_ptr p, unsigned height, block_index_type base,
        std::invocable<block_index_type, block_type&> auto& func) {
    if (height == 0) {
        if constexpr (std::same_as<std::invoke_result_t<decltype(func), block_index_type, block_type&>, stop_iteration>) {
            return func(base, *p.block());
        } else {
            func(base, *p.block());
            return stop_iteration::no;
        }
    }
    auto* v = p.vector();
    auto* children = v->children();
    return bitmask::for_each_set_with_rank(v->present(), [&] (unsigned position, unsigned rank) {
        return for_each_block_in(children[rank], height - 1, child_base(base, position, height), func);
    });
}

atomic_cell_or_collection_block_tree::node_ptr atomic_cell_or_collection_block_tree::clone_subtree(node_ptr p, unsigned height,
        std::invocable<const block_type&> auto& clone_block) {
    if (height == 0) {
        block_ptr block = clone_block(std::as_const(*p.block()));
        return node_ptr(block.release());
    }
    auto* v = p.vector();
    const auto nr = v->nr_children();
    std::array<node_ptr, atomic_cell_or_collection_block_vector_fanout> cloned;
    unsigned nr_cloned = 0;
    try {
        for (; nr_cloned < nr; ++nr_cloned) {
            cloned[nr_cloned] = clone_subtree(v->children()[nr_cloned], height - 1, clone_block);
        }
        return make_vector(v->present(), cloned.data(), nr);
    } catch (...) {
        for (unsigned i = 0; i < nr_cloned; ++i) {
            destroy_subtree(cloned[i], height - 1);
        }
        throw;
    }
}

void atomic_cell_or_collection_block_tree::for_each_block_slot_in(node_ptr& slot, unsigned height, block_index_type base,
        std::invocable<block_index_type, node_ptr&> auto& func) {
    if (height == 0) {
        func(base, slot);
        return;
    }
    auto* v = slot.vector();
    auto* children = v->children();
    bitmask::for_each_set_with_rank(v->present(), [&] (unsigned position, unsigned rank) {
        for_each_block_slot_in(children[rank], height - 1, child_base(base, position, height), func);
    });
}

void atomic_cell_or_collection_block_tree::rebuild_blocks(std::invocable<block_index_type, block_being_rebuilt&> auto&& func) {
    if (!_root) {
        return;
    }
    // Slots of blocks removed by func are cleared during the walk, and removed from
    // their vectors after it, since removal shifts the vectors' children.
    bool any_removed = false;
    auto visit = [&] (block_index_type block_index, node_ptr& slot) {
        auto* old = slot.block();
        const auto old_count = old->nr_present();
        block_being_rebuilt block(old);
        old->_backref = nullptr;
        auto put_back = [&] () noexcept {
            if (!block._changed) {
                old->_backref = &slot;
                (void)block._block.release();
                return;
            }
            // func replaced, removed or released the old block.
            _size -= old_count;
            if (block._block) {
                _size += block._block->nr_present();
                install(slot, node_ptr(block._block.release()));
            } else {
                slot = {};
                any_removed = true;
            }
        };
        try {
            func(block_index, block);
        } catch (...) {
            put_back();
            throw;
        }
        put_back();
    };
    auto remove_cleared = [&] () noexcept {
        if (any_removed) {
            remove_cleared_children(_root, _height);
            shrink_root();
        }
    };
    try {
        for_each_block_slot_in(_root, _height, 0, visit);
    } catch (...) {
        remove_cleared();
        throw;
    }
    remove_cleared();
}
