/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/atomic_cell_or_collection_block.hh"

#include <cstring>
#include <limits>
#include <memory>

#include "utils/assert.hh"

static_assert(std::has_single_bit(atomic_cell_or_collection_block_nr_cells));
static_assert(sizeof(atomic_cell_or_collection_block) == 16);
static_assert(sizeof(atomic_cell_or_collection_block) % atomic_cell_or_collection_block::external_cells_alignment == 0);
static_assert(alignof(atomic_cell_or_collection_block) >= atomic_cell_or_collection_block::external_cells_alignment);
static_assert(sizeof(managed_bytes) % alignof(cell_hash_opt) == 0);
static_assert(sizeof(cell_hash_opt) % alignof(atomic_cell_or_collection_block::offset_type) == 0);
static_assert(std::is_trivially_copyable_v<cell_hash_opt>);

static void copy_view_to(bytes::value_type* dst, managed_bytes_view src) noexcept {
    while (!src.empty()) {
        auto frag = src.current_fragment();
        std::memcpy(dst, frag.data(), frag.size());
        dst += frag.size();
        src.remove_current();
    }
}

atomic_cell_or_collection_block::atomic_cell_or_collection_block(atomic_cell_or_collection_block&& o) noexcept
    : atomic_cell_or_collection_block_tree_node_header{o._backref}
    , _present(o._present)
    , _external(o._external)
    , _inline_size(o._inline_size)
    , _hashed(o._hashed)
{
    std::uninitialized_move_n(o.external_cells(), nr_external(), external_cells());
    // hashes, offsets and data are contiguous and trivially copyable
    std::memcpy(hashes(), o.hashes(), nr_hashed() * sizeof(cell_hash_opt) + (nr_internal() + 1) * sizeof(offset_type) + _inline_size);
    relocated_to(this);
}

atomic_cell_or_collection_block::~atomic_cell_or_collection_block() {
    std::destroy_n(external_cells(), nr_external());
}

atomic_cell_or_collection_block::ptr atomic_cell_or_collection_block::clone() const {
    // Copy the external cells before allocating the block, so that the block is
    // fully constructed as soon as it is allocated.
    std::array<managed_bytes, nr_cells> external_copies;
    auto ext = external_cells();
    for (unsigned i = 0; i < nr_external(); ++i) {
        external_copies[i] = managed_bytes(ext[i]);
    }
    auto size = storage_size();
    void* storage = current_allocator().alloc<atomic_cell_or_collection_block>(size);
    // Nothrow from here on.
    auto* block = new (storage) atomic_cell_or_collection_block(_present, _external, _hashed, _inline_size);
    std::uninitialized_move_n(external_copies.begin(), nr_external(), block->external_cells());
    std::memcpy(block->hashes(), hashes(), nr_hashed() * sizeof(cell_hash_opt) + (nr_internal() + 1) * sizeof(offset_type) + _inline_size);
    return ptr(block);
}

size_t atomic_cell_or_collection_block::representation_independent_data_size() const noexcept {
    // The inline cells are stored back to back.
    size_t size = _inline_size;
    auto ext = external_cells();
    for (unsigned i = 0; i < nr_external(); ++i) {
        size += ext[i].size();
    }
    return size;
}

size_t atomic_cell_or_collection_block::memory_usage() const noexcept {
    size_t usage = storage_size();
    auto ext = external_cells();
    for (unsigned i = 0; i < nr_external(); ++i) {
        usage += ext[i].external_memory_usage();
    }
    return usage;
}

managed_bytes_mutable_view atomic_cell_or_collection_block::mutable_cell(unsigned slot) noexcept {
    unsigned rank = bitmask::rank(_present, slot);
    unsigned external_rank = bitmask::rank(_external, slot);
    if (is_external(slot)) {
        return managed_bytes_mutable_view(external_cells()[external_rank]);
    }
    auto internal_rank = rank - external_rank;
    auto offs = offsets();
    return managed_bytes_mutable_view(bytes_mutable_view(data() + offs[internal_rank], offs[internal_rank + 1] - offs[internal_rank]));
}

static_assert(atomic_cell_or_collection_block_builder::inline_budget < std::numeric_limits<atomic_cell_or_collection_block::offset_type>::max());

atomic_cell_or_collection_block_builder::entry&
atomic_cell_or_collection_block_builder::new_entry(unsigned slot, size_t size, cell_hash_opt hash) {
    SCYLLA_ASSERT(slot < atomic_cell_or_collection_block_nr_cells);
    SCYLLA_ASSERT(!(_slots & bitmask::bit<mask_type>(slot)));
    auto& e = *new (&_entries[slot].e) entry();
    e.size = size;
    e.hash = hash;
    _slots |= bitmask::bit<mask_type>(slot);
    return e;
}

managed_bytes_mutable_view atomic_cell_or_collection_block_builder::reserve(entry& e, size_t size) {
    if (size <= _scratch.size() - _scratch_size) {
        e.kind = source_kind::scratch;
        e.scratch_offset = _scratch_size;
        _scratch_size += size;
        return managed_bytes_mutable_view(bytes_mutable_view(_scratch.data() + e.scratch_offset, size));
    }
    // A cell this large will likely be external anyway.
    e.owned = managed_bytes(managed_bytes::initialized_later(), size);
    e.kind = source_kind::owned;
    return managed_bytes_mutable_view(e.owned);
}

void atomic_cell_or_collection_block_builder::add_copy(unsigned slot, managed_bytes_view cell, cell_hash_opt hash) {
    if (cell.empty()) {
        return;
    }
    auto& e = new_entry(slot, cell.size(), hash);
    try {
        auto out = reserve(e, cell.size());
        copy_fragmented_view(out, cell);
    } catch (...) {
        std::destroy_at(&e);
        _slots &= ~bitmask::bit<mask_type>(slot);
        throw;
    }
}

managed_bytes_mutable_view atomic_cell_or_collection_block_builder::add_uninitialized(unsigned slot, size_t size, cell_hash_opt hash) {
    SCYLLA_ASSERT(size);
    auto& e = new_entry(slot, size, hash);
    try {
        return reserve(e, size);
    } catch (...) {
        std::destroy_at(&e);
        _slots &= ~bitmask::bit<mask_type>(slot);
        throw;
    }
}

void atomic_cell_or_collection_block_builder::add_borrowed(unsigned slot, managed_bytes_view cell, cell_hash_opt hash, managed_bytes* stealable) noexcept {
    if (cell.empty()) {
        return;
    }
    auto& e = new_entry(slot, cell.size(), hash);
    e.kind = source_kind::borrowed;
    e.borrowed = cell;
    e.stealable = stealable;
}

void atomic_cell_or_collection_block_builder::add_owned(unsigned slot, managed_bytes&& cell, cell_hash_opt hash) noexcept {
    if (cell.empty()) {
        return;
    }
    auto& e = new_entry(slot, cell.size(), hash);
    e.kind = source_kind::owned;
    e.owned = std::move(cell);
}

managed_bytes_view atomic_cell_or_collection_block_builder::entry_view(const entry& e) const noexcept {
    switch (e.kind) {
    case source_kind::scratch:
        return managed_bytes_view(bytes_view(_scratch.data() + e.scratch_offset, e.size));
    case source_kind::borrowed:
        return e.borrowed;
    case source_kind::owned:
        return managed_bytes_view(e.owned);
    }
    std::abort();
}

atomic_cell_or_collection_block_builder::block_ptr atomic_cell_or_collection_block_builder::build() {
    SCYLLA_ASSERT(_slots);

    // Decide which cells are external. Depends only on the cell sizes. Cells are
    // visited in slot order, so ties are broken by the lowest slot.
    size_t inline_size = 0;
    mask_type hashed = 0;
    bitmask::for_each_set(_slots, [&] (unsigned slot) {
        auto size = entry_of(slot).size;
        inline_size += size;
        if (size >= atomic_cell_or_collection_block::hashed_cell_min_size) {
            hashed |= bitmask::bit<mask_type>(slot);
        }
    });
    mask_type external = 0;
    while (inline_size > inline_budget) {
        unsigned largest = 0;
        size_t largest_size = 0;
        bitmask::for_each_set(mask_type(_slots & ~external), [&] (unsigned slot) {
            if (entry_of(slot).size > largest_size) {
                largest = slot;
                largest_size = entry_of(slot).size;
            }
        });
        inline_size -= largest_size;
        external |= bitmask::bit<mask_type>(largest);
    }

    // External cells which we can neither steal nor already own are copied into
    // managed_bytes of their own. This and the block allocation are the only
    // steps which may throw.
    bitmask::for_each_set(external, [&] (unsigned slot) {
        auto& e = entry_of(slot);
        if (e.kind != source_kind::owned && !e.stealable) {
            e.owned = managed_bytes(entry_view(e));
            e.kind = source_kind::owned;
        }
    });

    auto size = atomic_cell_or_collection_block::storage_size_for(std::popcount(_slots), std::popcount(external), std::popcount(hashed), inline_size);
    void* storage = current_allocator().alloc<atomic_cell_or_collection_block>(size);

    // Nothrow from here on.
    auto* block = new (storage) atomic_cell_or_collection_block(_slots, external, hashed, inline_size);
    auto ext = block->external_cells();
    auto hashes = block->hashes();
    auto offsets = block->offsets();
    auto data = block->data();
    unsigned hashed_rank = 0;
    unsigned external_rank = 0;
    unsigned internal_rank = 0;
    offsets[0] = 0;
    bitmask::for_each_set(_slots, [&] (unsigned slot) {
        auto& e = entry_of(slot);
        if (hashed & bitmask::bit<mask_type>(slot)) {
            new (&hashes[hashed_rank++]) cell_hash_opt(e.hash);
        }
        if (external & bitmask::bit<mask_type>(slot)) {
            if (e.kind == source_kind::owned) {
                new (&ext[external_rank]) managed_bytes(std::move(e.owned));
            } else {
                new (&ext[external_rank]) managed_bytes(std::move(*e.stealable));
            }
            ++external_rank;
        } else {
            auto start = offsets[internal_rank];
            copy_view_to(data + start, entry_view(e));
            offsets[++internal_rank] = start + e.size;
        }
    });
    return block_ptr(block);
}

void atomic_cell_or_collection_block_builder::roll_back(block_ptr block) noexcept {
    // build() moved the external cells from the stealable sources of the entries
    // it didn't own (copying the others into owned ones first).
    auto ext = block->external_cells();
    bitmask::for_each_set_with_rank(block->external(), [&] (unsigned slot, unsigned external_rank) {
        auto& e = entry_of(slot);
        if (e.kind != source_kind::owned) {
            *e.stealable = std::move(ext[external_rank]);
        }
    });
}

void atomic_cell_or_collection_block_builder::clear() noexcept {
    bitmask::for_each_set(_slots, [&] (unsigned slot) {
        std::destroy_at(&entry_of(slot));
    });
    _slots = 0;
    _scratch_size = 0;
}

static_assert(std::has_single_bit(atomic_cell_or_collection_block_vector_fanout));
static_assert(sizeof(atomic_cell_or_collection_block_vector) == 24);
static_assert(std::forward_iterator<atomic_cell_or_collection_block_tree::const_iterator>);
static_assert(std::sentinel_for<std::default_sentinel_t, atomic_cell_or_collection_block_tree::const_iterator>);
static_assert(sizeof(atomic_cell_or_collection_block_vector) % alignof(atomic_cell_or_collection_block_tree_node_ptr) == 0);
static_assert(sizeof(atomic_cell_or_collection_block_tree) == 16);

atomic_cell_or_collection_block_vector::atomic_cell_or_collection_block_vector(atomic_cell_or_collection_block_vector&& o) noexcept
    : atomic_cell_or_collection_block_tree_node_header{o._backref}
    , _present(o._present)
    , _capacity(o._capacity)
{
    auto* src = o.children();
    auto* dst = children();
    for (unsigned i = 0; i < nr_children(); ++i) {
        new (&dst[i]) node_ptr(src[i]);
        dst[i].set_owner(&dst[i]);
    }
    relocated_to(this);
}

void atomic_cell_or_collection_block_tree::destroy_subtree(node_ptr p, unsigned height) noexcept {
    if (height == 0) {
        current_allocator().destroy(p.block());
        return;
    }
    auto* v = p.vector();
    auto* children = v->children();
    for (unsigned i = 0; i < v->nr_children(); ++i) {
        destroy_subtree(children[i], height - 1);
    }
    current_allocator().destroy(v);
}

void atomic_cell_or_collection_block_tree::clear() noexcept {
    if (_root) {
        destroy_subtree(_root, _height);
    }
    _root = {};
    _size = 0;
    _height = 0;
}

size_t atomic_cell_or_collection_block_tree::subtree_memory_usage(node_ptr p, unsigned height) noexcept {
    if (height == 0) {
        return p.block()->memory_usage();
    }
    auto* v = p.vector();
    size_t usage = v->storage_size();
    auto* children = v->children();
    for (unsigned i = 0; i < v->nr_children(); ++i) {
        usage += subtree_memory_usage(children[i], height - 1);
    }
    return usage;
}

size_t atomic_cell_or_collection_block_tree::memory_usage() const noexcept {
    return _root ? subtree_memory_usage(_root, _height) : 0;
}

atomic_cell_or_collection_block_tree::node_ptr
atomic_cell_or_collection_block_tree::make_vector(atomic_cell_or_collection_block_vector::mask_type present, const node_ptr* children, unsigned capacity) {
    auto nr = std::popcount(present);
    SCYLLA_ASSERT(unsigned(nr) <= capacity);
    void* storage = current_allocator().alloc<atomic_cell_or_collection_block_vector>(atomic_cell_or_collection_block_vector::storage_size_for(capacity));
    auto* v = new (storage) atomic_cell_or_collection_block_vector(present, capacity);
    auto* dst = v->children();
    for (int i = 0; i < nr; ++i) {
        new (&dst[i]) node_ptr(children[i]);
        dst[i].set_owner(&dst[i]);
    }
    return node_ptr(v);
}

unsigned atomic_cell_or_collection_block_tree::vector_capacity(unsigned height, atomic_cell_or_collection_block_vector::mask_type present,
        std::optional<unsigned> blocks_to_follow) noexcept {
    constexpr unsigned fanout = atomic_cell_or_collection_block_vector::fanout;
    const unsigned nr = std::popcount(present);
    if (blocks_to_follow == 0u) {
        // No vector will get more children.
        return nr;
    }
    if (height == 1 && blocks_to_follow) {
        // Blocks are expected in increasing index order, so only the indexes above the
        // highest present one can still be filled.
        const unsigned highest = std::bit_width(present) - 1;
        return nr + std::min(*blocks_to_follow, fanout - 1 - highest);
    }
    return std::max(nr, std::min(fanout, std::max(min_vector_capacity, std::bit_ceil(nr))));
}

atomic_cell_or_collection_block_tree::node_ptr
atomic_cell_or_collection_block_tree::make_chain(node_ptr node, unsigned node_height, unsigned height, block_index_type block_index,
        std::optional<unsigned> blocks_to_follow) {
    node_ptr top = node;
    unsigned top_height = node_height;
    try {
        for (; top_height < height; ++top_height) {
            auto present = bitmask::bit<atomic_cell_or_collection_block_vector::mask_type>(child_index(block_index, top_height + 1));
            top = make_vector(present, &top, vector_capacity(top_height + 1, present, blocks_to_follow));
        }
    } catch (...) {
        destroy_chain(top, top_height, node_height);
        node.set_owner(nullptr);
        throw;
    }
    return top;
}

void atomic_cell_or_collection_block_tree::destroy_chain(node_ptr chain, unsigned height, unsigned node_height) noexcept {
    // Destroys the single-child vectors above node_height, but not the node below them.
    for (; height > node_height; --height) {
        auto* v = chain.vector();
        auto child = v->children()[0];
        current_allocator().destroy(v);
        chain = child;
    }
}

void atomic_cell_or_collection_block_tree::remove_child(node_ptr& slot, unsigned index) noexcept {
    auto* v = slot.vector();
    auto bit = bitmask::bit<atomic_cell_or_collection_block_vector::mask_type>(index);
    auto rank = bitmask::rank(v->_present, index);
    auto* children = v->children();
    auto nr = v->nr_children();
    for (unsigned i = rank; i + 1 < nr; ++i) {
        install(children[i], children[i + 1]);
    }
    v->_present &= ~bit;
    if (!v->_present) {
        current_allocator().destroy(v);
        slot = {};
    }
}

void atomic_cell_or_collection_block_tree::remove_cleared_children(node_ptr& slot, unsigned height) noexcept {
    if (height == 0 || !slot) {
        return;
    }
    auto* v = slot.vector();
    auto* children = v->children();
    auto present = v->_present;
    unsigned kept = 0;
    bitmask::for_each_set_with_rank(v->_present, [&] (unsigned position, unsigned rank) {
        remove_cleared_children(children[rank], height - 1);
        if (children[rank]) {
            if (kept != rank) {
                install(children[kept], children[rank]);
            }
            ++kept;
        } else {
            present &= ~bitmask::bit<atomic_cell_or_collection_block_vector::mask_type>(position);
        }
    });
    v->_present = present;
    if (!present) {
        current_allocator().destroy(v);
        slot = {};
    }
}

void atomic_cell_or_collection_block_tree::shrink_root() noexcept {
    if (!_root) {
        _height = 0;
        return;
    }
    while (_height > 0) {
        auto* v = _root.vector();
        if (v->present() != 1) {
            return;
        }
        // The only child holds blocks 0 to the capacity of its height.
        auto child = v->children()[0];
        current_allocator().destroy(v);
        install(_root, child);
        --_height;
    }
}

atomic_cell_or_collection_block_tree::node_ptr* atomic_cell_or_collection_block_tree::find_slot(block_index_type block_index) noexcept {
    if (!_root || height_for(block_index) > _height) {
        return nullptr;
    }
    node_ptr* slot = &_root;
    for (auto h = _height; h > 0; --h) {
        slot = slot->vector()->child(child_index(block_index, h));
        if (!slot) {
            return nullptr;
        }
    }
    return slot;
}

const atomic_cell_or_collection_block* atomic_cell_or_collection_block_tree::find_block(block_index_type block_index) const noexcept {
    auto p = _root;
    if (!p || height_for(block_index) > _height) {
        return nullptr;
    }
    for (auto h = _height; h > 0; --h) {
        auto* slot = p.vector()->child(child_index(block_index, h));
        if (!slot) {
            return nullptr;
        }
        p = *slot;
    }
    return p.block();
}

const atomic_cell_or_collection_block*
atomic_cell_or_collection_block_tree::find_block_at_or_after_in(node_ptr p, unsigned height, block_index_type base,
        block_index_type block_index, block_index_type& found_index) noexcept {
    if (height == 0) {
        found_index = base;
        return p.block();
    }
    auto* v = p.vector();
    auto candidates = v->present();
    if (block_index > base) {
        // Skip the children wholly below block_index.
        candidates &= ~atomic_cell_or_collection_block_vector::mask_type(0) << child_index(block_index, height);
    }
    for (; candidates; candidates &= candidates - 1) {
        unsigned index = std::countr_zero(candidates);
        auto child = v->children()[bitmask::rank(v->present(), index)];
        if (auto* b = find_block_at_or_after_in(child, height - 1, child_base(base, index, height), block_index, found_index)) {
            return b;
        }
    }
    return nullptr;
}

const atomic_cell_or_collection_block*
atomic_cell_or_collection_block_tree::find_block_at_or_after(block_index_type block_index, block_index_type& found_index) const noexcept {
    if (!_root || height_for(block_index) > _height) {
        return nullptr;
    }
    return find_block_at_or_after_in(_root, _height, 0, block_index, found_index);
}

void atomic_cell_or_collection_block_tree::set_block(block_index_type block_index, block_ptr&& block, std::optional<unsigned> blocks_to_follow) {
    auto block_node = node_ptr(block.get());
    const auto block_count = block->nr_present();
    const auto needed = height_for(block_index);
    if (!_root) {
        install(_root, make_chain(block_node, 0, needed, block_index, blocks_to_follow));
        _height = needed;
        _size = block_count;
        (void)block.release();
        return;
    }
    // Grow the root. Each step leaves a valid tree, so on failure only shrink back.
    try {
        while (_height < needed) {
            using vector_mask_type = atomic_cell_or_collection_block_vector::mask_type;
            const auto present = bitmask::bit<vector_mask_type>(0);
            // The new block (and those to follow it) go into this vector only if it
            // is the one which holds blocks; otherwise they go into a sibling subtree.
            const auto capacity = _height + 1 < needed
                    ? vector_capacity(_height + 1, present, blocks_to_follow ? std::optional<unsigned>(0) : std::nullopt)
                    : vector_capacity(_height + 1, present | bitmask::bit<vector_mask_type>(child_index(block_index, needed)), blocks_to_follow);
            install(_root, make_vector(present, &_root, capacity));
            ++_height;
        }
    } catch (...) {
        shrink_root();
        throw;
    }
    node_ptr* slot = &_root;
    for (auto h = _height; h > 0; --h) {
        auto* v = slot->vector();
        auto idx = child_index(block_index, h);
        if (auto* child = v->child(idx)) {
            slot = child;
            continue;
        }
        // Insert a new child into v, through a chain of new vectors down to the block.
        node_ptr chain;
        try {
            chain = make_chain(block_node, 0, h - 1, block_index, blocks_to_follow);
            auto present = v->_present | bitmask::bit<atomic_cell_or_collection_block_vector::mask_type>(idx);
            auto rank = bitmask::rank(present, idx);
            auto nr = v->nr_children();
            if (nr < v->_capacity) {
                // Fits in place.
                auto* children = v->children();
                for (unsigned i = nr; i > rank; --i) {
                    install(children[i], children[i - 1]);
                }
                install(children[rank], chain);
                v->_present = present;
            } else {
                std::array<node_ptr, atomic_cell_or_collection_block_vector_fanout> children;
                std::copy_n(v->children(), rank, children.begin());
                children[rank] = chain;
                std::copy_n(v->children() + rank, nr - rank, children.begin() + rank + 1);
                auto nv = make_vector(present, children.data(), vector_capacity(h, present, blocks_to_follow));
                current_allocator().destroy(v);
                install(*slot, nv);
            }
        } catch (...) {
            if (chain) {
                destroy_chain(chain, h - 1, 0);
            }
            block_node.set_owner(nullptr);
            shrink_root();
            throw;
        }
        _size += block_count;
        (void)block.release();
        return;
    }
    // Replace the existing block.
    auto* old = slot->block();
    _size = _size - old->nr_present() + block_count;
    current_allocator().destroy(old);
    install(*slot, block_node);
    (void)block.release();
}

atomic_cell_or_collection_block_vector::mask_type
atomic_cell_or_collection_block_tree::group_present(block_index_type block_index) const noexcept {
    if (!_root || height_for(block_index) > _height) {
        return 0;
    }
    if (_height == 0) {
        // The only block is block 0.
        return block_index < atomic_cell_or_collection_block_vector_fanout ? 1 : 0;
    }
    auto p = _root;
    for (auto h = _height; h > 1; --h) {
        auto* slot = p.vector()->child(child_index(block_index, h));
        if (!slot) {
            return 0;
        }
        p = *slot;
    }
    return p.vector()->present();
}

void atomic_cell_or_collection_block_tree::loader::set_owners() noexcept {
    for (auto& n : _rightmost_open_node_by_height) {
        bitmask::for_each_set(n.present, [&] (unsigned position) {
            auto& slot = n.children[position];
            slot.set_owner(&slot);
        });
    }
}

void atomic_cell_or_collection_block_tree::loader::move_only_block_to_rightmost_node() {
    _rightmost_open_node_by_height.push_back(rightmost_open_node{.index = _only_block_index / fanout});
    auto& n = _rightmost_open_node_by_height.front();
    auto position = _only_block_index % fanout;
    install(n.children[position], std::exchange(_only_block, {}));
    n.present = bitmask::bit<vector_mask_type>(position);
}

void atomic_cell_or_collection_block_tree::loader::add_level() {
    auto* old_data = _rightmost_open_node_by_height.data();
    _rightmost_open_node_by_height.push_back(rightmost_open_node{.index = _rightmost_open_node_by_height.back().index / fanout});
    if (_rightmost_open_node_by_height.data() != old_data) {
        set_owners();
    }
}

void atomic_cell_or_collection_block_tree::loader::seal(unsigned height, block_index_type next_index) {
    if (height == _rightmost_open_node_by_height.size()) {
        add_level();
    }
    auto& n = _rightmost_open_node_by_height[height - 1];
    auto& parent = _rightmost_open_node_by_height[height];
    std::array<node_ptr, fanout> children;
    unsigned nr = 0;
    bitmask::for_each_set(n.present, [&] (unsigned position) {
        children[nr++] = n.children[position];
    });
    // Takes over the back-references of the children.
    auto v = make_vector(n.present, children.data(), nr);
    install(parent.children[n.index % fanout], v);
    parent.present |= bitmask::bit<vector_mask_type>(n.index % fanout);
    n.present = 0;
    n.index = next_index;
}

atomic_cell_or_collection_block*
atomic_cell_or_collection_block_tree::loader::find_block(block_index_type block_index) noexcept {
    if (_only_block) {
        return block_index == _only_block_index ? _only_block.block() : nullptr;
    }
    if (_rightmost_open_node_by_height.empty() || _rightmost_open_node_by_height.front().index != block_index / fanout) {
        return nullptr;
    }
    auto& n = _rightmost_open_node_by_height.front();
    auto position = block_index % fanout;
    return n.present & bitmask::bit<vector_mask_type>(position) ? n.children[position].block() : nullptr;
}

void atomic_cell_or_collection_block_tree::loader::set_block(block_index_type block_index, block_ptr&& block) {
    SCYLLA_ASSERT(can_set_block(block_index));
    if (empty() || (_only_block && block_index == _only_block_index)) {
        if (_only_block) {
            _size -= _only_block.block()->nr_present();
            current_allocator().destroy(_only_block.block());
        }
        _size += block->nr_present();
        install(_only_block, node_ptr(block.release()));
        _only_block_index = block_index;
        return;
    }
    if (_only_block) {
        move_only_block_to_rightmost_node();
    }
    // Seal the rightmost nodes which block_index is past, bottom-up: a sealed node
    // goes into the rightmost node above it, which may then be sealed too.
    block_index_type index = block_index / fanout;
    for (unsigned height = 1; height <= _rightmost_open_node_by_height.size() && _rightmost_open_node_by_height[height - 1].index != index; ++height, index /= fanout) {
        if (_rightmost_open_node_by_height[height - 1].present) {
            seal(height, index);
        } else {
            // Emptied by sealing earlier; nothing to seal.
            _rightmost_open_node_by_height[height - 1].index = index;
        }
    }
    auto& n = _rightmost_open_node_by_height.front();
    auto position = block_index % fanout;
    auto& slot = n.children[position];
    const auto block_count = block->nr_present();
    if (n.present & bitmask::bit<vector_mask_type>(position)) {
        _size -= slot.block()->nr_present();
        current_allocator().destroy(slot.block());
    }
    install(slot, node_ptr(block.release()));
    n.present |= bitmask::bit<vector_mask_type>(position);
    _size += block_count;
}

void atomic_cell_or_collection_block_tree::loader::load_nonempty_into(atomic_cell_or_collection_block_tree& tree) {
    SCYLLA_ASSERT(tree.empty());
    if (_only_block) {
        // No more blocks follow, so any vectors on its path are exactly sized.
        block_ptr block(_only_block.block());
        try {
            tree.set_block(_only_block_index, std::move(block), 0);
        } catch (...) {
            install(_only_block, node_ptr(block.release()));
            throw;
        }
        _only_block = {};
        _size = 0;
        return;
    }
    // Seal the rightmost nodes bottom-up, until the top one covers block 0, and so
    // is the root. A top node whose only child is its first, at index 0, is not
    // needed: the child is the root.
    unsigned height = 1;
    for (;; ++height) {
        auto& n = _rightmost_open_node_by_height[height - 1];
        const bool top = height == _rightmost_open_node_by_height.size();
        if (top && n.index == 0) {
            if (n.present == 1) {
                install(tree._root, n.children[0]);
                tree._height = height - 1;
            } else {
                std::array<node_ptr, fanout> children;
                unsigned nr = 0;
                bitmask::for_each_set(n.present, [&] (unsigned position) {
                    children[nr++] = n.children[position];
                });
                install(tree._root, make_vector(n.present, children.data(), nr));
                tree._height = height;
            }
            break;
        }
        seal(height, n.index);
    }
    tree._size = _size;
    _rightmost_open_node_by_height.clear();
    _size = 0;
}

void atomic_cell_or_collection_block_tree::loader::clear() noexcept {
    if (_only_block) {
        current_allocator().destroy(_only_block.block());
        _only_block = {};
    }
    for (unsigned height = 1; height <= _rightmost_open_node_by_height.size(); ++height) {
        auto& n = _rightmost_open_node_by_height[height - 1];
        bitmask::for_each_set(n.present, [&] (unsigned position) {
            destroy_subtree(n.children[position], height - 1);
        });
    }
    _rightmost_open_node_by_height.clear();
    _size = 0;
}

void atomic_cell_or_collection_block_tree::remove_block(block_index_type block_index) noexcept {
    if (!_root || height_for(block_index) > _height) {
        return;
    }
    // Walk down, remembering the path to remove the block and any vectors left empty.
    std::array<node_ptr*, max_height + 1> path;
    path[0] = &_root;
    for (unsigned h = _height, depth = 0; h > 0; --h, ++depth) {
        auto* child = path[depth]->vector()->child(child_index(block_index, h));
        if (!child) {
            return;
        }
        path[depth + 1] = child;
    }
    auto* block = path[_height]->block();
    _size -= block->nr_present();
    current_allocator().destroy(block);
    if (_height == 0) {
        _root = {};
    } else {
        // Remove the child from its vector; if that empties the vector, remove the
        // vector from its parent, and so on.
        for (unsigned depth = _height; depth > 0; --depth) {
            auto& parent = *path[depth - 1];
            remove_child(parent, child_index(block_index, _height - depth + 1));
            if (parent) {
                break;
            }
        }
    }
    shrink_root();
}
