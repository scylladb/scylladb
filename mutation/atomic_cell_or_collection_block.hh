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

static_assert(std::has_single_bit(atomic_cell_or_collection_block_nr_cells));
static_assert(std::has_single_bit(atomic_cell_or_collection_block_vector_fanout));

template <std::unsigned_integral Mask>
constexpr Mask mask_bit(unsigned position) noexcept {
    return Mask(Mask(1) << position);
}

// Bits strictly below `position`.
template <std::unsigned_integral Mask>
constexpr Mask mask_below(unsigned position) noexcept {
    return Mask(mask_bit<Mask>(position) - 1);
}

// Number of set bits strictly below `position`; the index of `position`'s element
// in an array compressed by `mask`.
template <std::unsigned_integral Mask>
constexpr unsigned mask_rank(Mask mask, unsigned position) noexcept {
    return std::popcount(Mask(mask & mask_below<Mask>(position)));
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
// in slot order, so the element of slot s is at index mask_rank(mask, s)). The arrays are
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

    mask_type present() const noexcept { return _present; }
    mask_type external() const noexcept { return _external; }
    unsigned nr_present() const noexcept { return std::popcount(_present); }
    unsigned nr_external() const noexcept { return std::popcount(_external); }
    bool contains(unsigned slot) const noexcept { return _present & mask_bit<mask_type>(slot); }
    bool is_external(unsigned slot) const noexcept { return _external & mask_bit<mask_type>(slot); }

    // The serialized cell at `slot`. The slot must be present.
    managed_bytes_view cell(unsigned slot) const noexcept {
        unsigned rank = mask_rank(_present, slot);
        unsigned external_rank = mask_rank(_external, slot);
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
        return hashes()[mask_rank(_present, slot)];
    }

    // The managed_bytes holding the cell at `slot` if it is external, nullptr otherwise.
    managed_bytes* external_storage(unsigned slot) const noexcept {
        return is_external(slot) ? &external_cells()[mask_rank(_external, slot)] : nullptr;
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
        unsigned rank = 0;
        unsigned external_rank = 0;
        unsigned internal_rank = 0;
        for (mask_type m = _present; m; m &= m - 1) {
            unsigned slot = std::countr_zero(m);
            bool is_ext = is_external(slot);
            managed_bytes* ext = is_ext ? &external_cells()[external_rank] : nullptr;
            cell_entry e{slot, is_ext ? managed_bytes_view(*ext) : internal_cell(internal_rank), hashes()[rank], ext};
            ++rank;
            external_rank += is_ext;
            internal_rank += !is_ext;
            if constexpr (std::same_as<std::invoke_result_t<decltype(func), const cell_entry&>, stop_iteration>) {
                if (func(e) == stop_iteration::yes) {
                    return stop_iteration::yes;
                }
            } else {
                func(e);
            }
        }
        return stop_iteration::no;
    }
};

static_assert(sizeof(atomic_cell_or_collection_block) == 16);
static_assert(sizeof(atomic_cell_or_collection_block) % atomic_cell_or_collection_block::external_cells_alignment == 0);
static_assert(alignof(atomic_cell_or_collection_block) >= atomic_cell_or_collection_block::external_cells_alignment);
static_assert(sizeof(managed_bytes) % alignof(cell_hash_opt) == 0);
static_assert(sizeof(cell_hash_opt) % alignof(atomic_cell_or_collection_block::offset_type) == 0);
static_assert(std::is_trivially_copyable_v<cell_hash_opt>);

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
    static_assert(inline_budget < std::numeric_limits<atomic_cell_or_collection_block::offset_type>::max());

    enum class source_kind : uint8_t { scratch, borrowed, owned };
    struct entry {
        managed_bytes owned;
        managed_bytes_view borrowed;
        managed_bytes* stealable = nullptr;
        size_t scratch_offset = 0;
        size_t size = 0;
        cell_hash_opt hash;
        source_kind kind = source_kind::borrowed;
        bool external = false;
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
    // After build(): for each external cell of the built block (in external rank order),
    // the managed_bytes it was moved from, if any.
    std::array<managed_bytes*, atomic_cell_or_collection_block_nr_cells> _stolen_from;
    unsigned _nr_built_external = 0;
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
    // block. The sources must still be alive.
    void roll_back(block_ptr block) noexcept;

    void clear() noexcept;
};

// An inner node of the tree: up to atomic_cell_or_collection_block_vector_fanout children.
//
// Layout: header, then atomic_cell_or_collection_block_tree_node_ptr children[_capacity],
// of which the first popcount(_present) are used. Children are inserted into spare
// capacity, or by reallocating the vector to the exact size; they are removed in place,
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
        if (!(_present & mask_bit<mask_type>(index))) {
            return nullptr;
        }
        return &children()[mask_rank(_present, index)];
    }
};

static_assert(sizeof(atomic_cell_or_collection_block_vector) == 24);
static_assert(sizeof(atomic_cell_or_collection_block_vector) % alignof(atomic_cell_or_collection_block_tree_node_ptr) == 0);

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

    static unsigned height_for(block_index_type block_index) noexcept;
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
    // Builds a chain of single-child vectors above `node` (which has height `node_height`),
    // up to `height`, along the path to `block_index`.
    static node_ptr make_chain(node_ptr node, unsigned node_height, unsigned height, block_index_type block_index);
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
            std::invocable<block_index_type, const block_type&> auto& func);
    static void for_each_block_slot_in(node_ptr& slot, unsigned height, block_index_type base,
            std::invocable<block_index_type, node_ptr&> auto& func);
    static node_ptr clone_subtree(node_ptr p, unsigned height,
            std::invocable<const block_type&> auto& clone_block);
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
    void set_block(block_index_type block_index, block_ptr&& block);

    // Removes and destroys the block at block_index, if any.
    void remove_block(block_index_type block_index) noexcept;

    // Calls func(block_index, const block_type&) for each block in index order.
    // func may return stop_iteration.
    stop_iteration for_each_block(std::invocable<block_index_type, const block_type&> auto&& func) const {
        if (!_root) {
            return stop_iteration::no;
        }
        return for_each_block_in(_root, _height, 0, func);
    }

    // Calls func(block_index, block_ptr& block) for each block in index order. func
    // may replace the block with another one (with no owner), or reset it to remove
    // it. Blocks removed by func are removed from the tree when the walk is over,
    // even if func throws.
    void rebuild_blocks(std::invocable<block_index_type, block_ptr&> auto&& func);

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

static_assert(sizeof(atomic_cell_or_collection_block_tree) == 16);

stop_iteration atomic_cell_or_collection_block_tree::for_each_block_in(node_ptr p, unsigned height, block_index_type base,
        std::invocable<block_index_type, const block_type&> auto& func) {
    if (height == 0) {
        if constexpr (std::same_as<std::invoke_result_t<decltype(func), block_index_type, const block_type&>, stop_iteration>) {
            return func(base, std::as_const(*p.block()));
        } else {
            func(base, std::as_const(*p.block()));
            return stop_iteration::no;
        }
    }
    auto* v = p.vector();
    auto* children = v->children();
    unsigned rank = 0;
    for (auto m = v->present(); m; m &= m - 1, ++rank) {
        if (for_each_block_in(children[rank], height - 1, child_base(base, std::countr_zero(m), height), func) == stop_iteration::yes) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
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
    unsigned rank = 0;
    for (auto m = v->present(); m; m &= m - 1, ++rank) {
        for_each_block_slot_in(children[rank], height - 1, child_base(base, std::countr_zero(m), height), func);
    }
}

void atomic_cell_or_collection_block_tree::rebuild_blocks(std::invocable<block_index_type, block_ptr&> auto&& func) {
    if (!_root) {
        return;
    }
    // Slots of blocks removed by func are cleared during the walk, and removed from
    // their vectors after it, since removal shifts the vectors' children.
    bool any_removed = false;
    auto visit = [&] (block_index_type block_index, node_ptr& slot) {
        auto* old = slot.block();
        const auto old_count = old->nr_present();
        block_ptr block(old);
        old->_backref = nullptr;
        auto put_back = [&] () noexcept {
            if (block.get() == old) {
                old->_backref = &slot;
                (void)block.release();
                return;
            }
            // func replaced or removed, and so destroyed, the old block.
            _size -= old_count;
            if (block) {
                _size += block->nr_present();
                install(slot, node_ptr(block.release()));
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
