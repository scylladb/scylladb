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
#include <limits>
#include <memory>

#include <seastar/core/loop.hh>

#include "mutation/cell_hash.hh"
#include "seastarx.hh"
#include "utils/allocation_strategy.hh"
#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"

// Packed, allocation-light storage for the cells of a row.
//
// Cells are grouped by column id into atomic_cell_or_collection_blocks, each holding
// up to atomic_cell_or_collection_block_nr_cells consecutive column slots in a single
// variable-size allocation. Small serialized cells are concatenated into an inline byte
// area; cells which do not fit the inline budget are stored in managed_bytes inside the
// block.
//
// All nodes are allocated with current_allocator() and are LSA-migratable. Each node
// carries a back-reference to the pointer that owns it.

using atomic_cell_or_collection_block_mask = uint8_t;
inline constexpr unsigned atomic_cell_or_collection_block_nr_cells = std::numeric_limits<atomic_cell_or_collection_block_mask>::digits;
inline constexpr unsigned atomic_cell_or_collection_block_slot_bits = std::countr_zero(atomic_cell_or_collection_block_nr_cells);

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
//   cell_hash_opt  hashes[popcount(_present)]
//   offset_type    offsets[popcount(_present & ~_external) + 1]   offsets[0] == 0; internal cell i
//                                                                  occupies [offsets[i], offsets[i + 1])
//   int8_t         data[_inline_size]
//
// The shape of a block (which cells it holds and where) never changes after it is built;
// changes are made by building a new block with atomic_cell_or_collection_block_builder.
// The cell contents (and hashes) can be modified in place, as long as sizes don't change.
class atomic_cell_or_collection_block : public atomic_cell_or_collection_block_tree_node_header {
public:
    using mask_type = atomic_cell_or_collection_block_mask;
    using offset_type = uint16_t;
    using ptr = alloc_strategy_unique_ptr<atomic_cell_or_collection_block>;
    static constexpr unsigned nr_cells = atomic_cell_or_collection_block_nr_cells;
    // managed_bytes is byte-aligned, but its pointers are better accessed aligned.
    static constexpr size_t external_cells_alignment = alignof(void*);
private:
    mask_type _present = 0;
    mask_type _external = 0;
    offset_type _inline_size = 0;

    friend class atomic_cell_or_collection_block_builder;

    atomic_cell_or_collection_block(mask_type present, mask_type external, offset_type inline_size) noexcept
        : _present(present), _external(external), _inline_size(inline_size) {
    }

    unsigned nr_internal() const noexcept { return std::popcount(mask_type(_present & ~_external)); }

    managed_bytes* external_cells() const noexcept {
        return std::assume_aligned<external_cells_alignment>(
                const_cast<managed_bytes*>(reinterpret_cast<const managed_bytes*>(this + 1)));
    }
    cell_hash_opt* hashes() const noexcept {
        return std::assume_aligned<alignof(cell_hash_opt)>(reinterpret_cast<cell_hash_opt*>(external_cells() + nr_external()));
    }
    offset_type* offsets() const noexcept {
        return std::assume_aligned<alignof(offset_type)>(reinterpret_cast<offset_type*>(hashes() + nr_present()));
    }
    bytes::value_type* data() const noexcept {
        return reinterpret_cast<bytes::value_type*>(offsets() + nr_internal() + 1);
    }

    managed_bytes_view internal_cell(unsigned internal_rank) const noexcept {
        auto offs = offsets();
        return managed_bytes_view(bytes_view(data() + offs[internal_rank], offs[internal_rank + 1] - offs[internal_rank]));
    }
public:
    static size_t storage_size_for(unsigned nr_present, unsigned nr_external, size_t inline_size) noexcept {
        return sizeof(atomic_cell_or_collection_block)
                + nr_external * sizeof(managed_bytes)
                + nr_present * sizeof(cell_hash_opt)
                + (nr_present - nr_external + 1) * sizeof(offset_type)
                + inline_size;
    }

    atomic_cell_or_collection_block(atomic_cell_or_collection_block&& o) noexcept;
    ~atomic_cell_or_collection_block();

    // Returns a copy of this block, allocated with current_allocator(), with no owner.
    // The copy has the same layout.
    ptr clone() const;

    size_t storage_size() const noexcept {
        return storage_size_for(nr_present(), nr_external(), _inline_size);
    }

    // Memory used by this block and the external cells it owns.
    size_t memory_usage() const noexcept;
    // The total size of the serialized cells, independent of how they are stored.
    // Costs a lookup per external cell, rather than per cell.
    size_t representation_independent_data_size() const noexcept;

    mask_type present() const noexcept { return _present; }
    mask_type external() const noexcept { return _external; }
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

    // The hash of the cell at `slot`. Hashes are a cache which may be updated in
    // place even through a const block. The slot must be present.
    cell_hash_opt& hash(unsigned slot) const noexcept {
        return hashes()[bitmask::rank(_present, slot)];
    }

    // The managed_bytes holding the cell at `slot` if it is external, nullptr otherwise.
    managed_bytes* external_storage(unsigned slot) const noexcept {
        return is_external(slot) ? &external_cells()[bitmask::rank(_external, slot)] : nullptr;
    }

    struct cell_entry {
        unsigned slot;
        managed_bytes_view cell;
        cell_hash_opt& hash;
        // Non-null iff the cell is external.
        managed_bytes* external_storage;
    };

    // Calls func(const cell_entry&) for each cell, in slot order. If func returns
    // stop_iteration, iteration stops early when it returns stop_iteration::yes.
    // Returns stop_iteration::yes if iteration was stopped.
    stop_iteration for_each_cell(std::invocable<const cell_entry&> auto&& func) const {
        unsigned external_rank = 0;
        return bitmask::for_each_set_with_rank(_present, [&] (unsigned slot, unsigned rank) {
            const bool is_ext = is_external(slot);
            managed_bytes* ext = is_ext ? &external_cells()[external_rank] : nullptr;
            cell_entry e{slot, is_ext ? managed_bytes_view(*ext) : internal_cell(rank - external_rank), hashes()[rank], ext};
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
