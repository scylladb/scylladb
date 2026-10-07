/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/atomic_cell_or_collection_block.hh"

#include <cstring>
#include <memory>

#include "utils/assert.hh"

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
{
    std::uninitialized_move_n(o.external_cells(), nr_external(), external_cells());
    // hashes, offsets and data are contiguous and trivially copyable
    std::memcpy(hashes(), o.hashes(), nr_present() * sizeof(cell_hash_opt) + (nr_internal() + 1) * sizeof(offset_type) + _inline_size);
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
    auto* block = new (storage) atomic_cell_or_collection_block(_present, _external, _inline_size);
    std::uninitialized_move_n(external_copies.begin(), nr_external(), block->external_cells());
    std::memcpy(block->hashes(), hashes(), nr_present() * sizeof(cell_hash_opt) + (nr_internal() + 1) * sizeof(offset_type) + _inline_size);
    return ptr(block);
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
    unsigned rank = mask_rank(_present, slot);
    unsigned external_rank = mask_rank(_external, slot);
    if (is_external(slot)) {
        return managed_bytes_mutable_view(external_cells()[external_rank]);
    }
    auto internal_rank = rank - external_rank;
    auto offs = offsets();
    return managed_bytes_mutable_view(bytes_mutable_view(data() + offs[internal_rank], offs[internal_rank + 1] - offs[internal_rank]));
}

atomic_cell_or_collection_block_builder::entry&
atomic_cell_or_collection_block_builder::new_entry(unsigned slot, size_t size, cell_hash_opt hash) {
    SCYLLA_ASSERT(slot < atomic_cell_or_collection_block_nr_cells);
    SCYLLA_ASSERT(!(_slots & mask_bit<mask_type>(slot)));
    auto& e = *new (&_entries[slot].e) entry();
    e.size = size;
    e.hash = hash;
    _slots |= mask_bit<mask_type>(slot);
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
        _slots &= ~mask_bit<mask_type>(slot);
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
        _slots &= ~mask_bit<mask_type>(slot);
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
    for (mask_type m = _slots; m; m &= m - 1) {
        auto& e = entry_of(std::countr_zero(m));
        e.external = false;
        inline_size += e.size;
    }
    mask_type external = 0;
    while (inline_size > inline_budget) {
        unsigned largest = 0;
        size_t largest_size = 0;
        for (mask_type m = _slots & ~external; m; m &= m - 1) {
            auto slot = std::countr_zero(m);
            if (entry_of(slot).size > largest_size) {
                largest = slot;
                largest_size = entry_of(slot).size;
            }
        }
        entry_of(largest).external = true;
        inline_size -= largest_size;
        external |= mask_bit<mask_type>(largest);
    }

    // External cells which we can neither steal nor already own are copied into
    // managed_bytes of their own. This and the block allocation are the only
    // steps which may throw.
    for (mask_type m = external; m; m &= m - 1) {
        auto& e = entry_of(std::countr_zero(m));
        if (e.kind != source_kind::owned && !e.stealable) {
            e.owned = managed_bytes(entry_view(e));
            e.kind = source_kind::owned;
        }
    }

    auto size = atomic_cell_or_collection_block::storage_size_for(std::popcount(_slots), std::popcount(external), inline_size);
    void* storage = current_allocator().alloc<atomic_cell_or_collection_block>(size);

    // Nothrow from here on.
    auto* block = new (storage) atomic_cell_or_collection_block(_slots, external, inline_size);
    auto ext = block->external_cells();
    auto hashes = block->hashes();
    auto offsets = block->offsets();
    auto data = block->data();
    unsigned rank = 0;
    unsigned external_rank = 0;
    unsigned internal_rank = 0;
    offsets[0] = 0;
    for (mask_type m = _slots; m; m &= m - 1, ++rank) {
        auto& e = entry_of(std::countr_zero(m));
        new (&hashes[rank]) cell_hash_opt(e.hash);
        if (e.external) {
            if (e.kind == source_kind::owned) {
                new (&ext[external_rank]) managed_bytes(std::move(e.owned));
                _stolen_from[external_rank] = nullptr;
            } else {
                new (&ext[external_rank]) managed_bytes(std::move(*e.stealable));
                _stolen_from[external_rank] = e.stealable;
            }
            ++external_rank;
        } else {
            auto start = offsets[internal_rank];
            copy_view_to(data + start, entry_view(e));
            offsets[++internal_rank] = start + e.size;
        }
    }
    _nr_built_external = external_rank;
    return block_ptr(block);
}

void atomic_cell_or_collection_block_builder::roll_back(block_ptr block) noexcept {
    auto ext = block->external_cells();
    for (unsigned i = 0; i < _nr_built_external; ++i) {
        if (_stolen_from[i]) {
            *_stolen_from[i] = std::move(ext[i]);
        }
    }
    _nr_built_external = 0;
}

void atomic_cell_or_collection_block_builder::clear() noexcept {
    for (mask_type m = _slots; m; m &= m - 1) {
        std::destroy_at(&entry_of(std::countr_zero(m)));
    }
    _slots = 0;
    _scratch_size = 0;
    _nr_built_external = 0;
}

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

unsigned atomic_cell_or_collection_block_tree::height_for(block_index_type block_index) noexcept {
    unsigned height = 0;
    uint64_t capacity = 1;
    while (block_index >= capacity) {
        capacity <<= atomic_cell_or_collection_block_vector_index_bits;
        ++height;
    }
    return height;
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

atomic_cell_or_collection_block_tree::node_ptr
atomic_cell_or_collection_block_tree::make_chain(node_ptr node, unsigned node_height, unsigned height, block_index_type block_index) {
    node_ptr top = node;
    unsigned top_height = node_height;
    try {
        for (; top_height < height; ++top_height) {
            auto idx = child_index(block_index, top_height + 1);
            top = make_vector(mask_bit<atomic_cell_or_collection_block_vector::mask_type>(idx), &top, 1);
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
    auto bit = mask_bit<atomic_cell_or_collection_block_vector::mask_type>(index);
    auto rank = mask_rank(v->_present, index);
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
    unsigned rank = 0;
    for (auto m = present; m; m &= m - 1, ++rank) {
        remove_cleared_children(children[rank], height - 1);
        if (children[rank]) {
            if (kept != rank) {
                install(children[kept], children[rank]);
            }
            ++kept;
        } else {
            present &= ~mask_bit<atomic_cell_or_collection_block_vector::mask_type>(std::countr_zero(m));
        }
    }
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

void atomic_cell_or_collection_block_tree::set_block(block_index_type block_index, block_ptr&& block) {
    auto block_node = node_ptr(block.get());
    const auto block_count = block->nr_present();
    const auto needed = height_for(block_index);
    if (!_root) {
        install(_root, make_chain(block_node, 0, needed, block_index));
        _height = needed;
        _size = block_count;
        (void)block.release();
        return;
    }
    // Grow the root. Each step leaves a valid tree, so on failure only shrink back.
    try {
        while (_height < needed) {
            install(_root, make_vector(mask_bit<atomic_cell_or_collection_block_vector::mask_type>(0), &_root, 1));
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
            chain = make_chain(block_node, 0, h - 1, block_index);
            auto present = v->_present | mask_bit<atomic_cell_or_collection_block_vector::mask_type>(idx);
            auto rank = mask_rank(present, idx);
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
                auto nv = make_vector(present, children.data(), nr + 1);
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
