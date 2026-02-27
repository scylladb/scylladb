/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/util/log.hh>
#include "writer_node.hh"
#include "common.hh"

seastar::logger trie_logger("trie");

namespace sstables::trie {

auto writer_node::create(const_bytes b, bump_allocator& alctr) -> ptr<writer_node> {
    // We could do this in a single allocation, though we would have to change the interface
    // of bump_allocator somewhat to enable that.
    auto x = alctr.alloc<writer_node>();
    new (x.get()) writer_node;
    auto transition = alctr.alloc<std::byte[]>(b.size());
    std::memcpy(transition.get(), b.data(), b.size());
    x->_transition = transition;
    x->_transition_length = b.size();
    return x;
}

auto writer_node::add_child(const_bytes b, bump_allocator& alctr) -> ptr<writer_node> {
    SCYLLA_ASSERT(get_children().empty() || b[0] > get_children().back()->_transition[0]);
    reserve_children(get_children().size() + 1, alctr);
    auto new_child = create(b, alctr);
    push_child(new_child, alctr);
    return get_children().back();
}

auto writer_node::get_children() const -> std::span<const ptr<writer_node>> {
    return {_children.get(), _children_size};
}

auto writer_node::get_children() -> std::span<ptr<writer_node>> {
    return {_children.get(), _children_size};
}

void writer_node::reset_children() {
    _children_size = 0;
    _children_capacity = 0;
    _children = {};
}

void writer_node::reserve_children(size_t n, bump_allocator& alctr) {
    if (n > _children_capacity) {
        auto new_capacity = std::bit_ceil(n);
        auto new_array = alctr.alloc<ptr<writer_node>[]>(new_capacity);
        std::ranges::copy(get_children(), new_array.get());
        _children_capacity = new_capacity;
        _children = new_array;
    }
}

void writer_node::push_child(ptr<writer_node> x, bump_allocator& alctr) {
    expensive_assert(_children_capacity > _children_size);
    _children[_children_size++] = x;
}

std::byte writer_node::transition() const {
    return _pos.valid() ? std::byte(_first_transition_byte) : _transition[0];
}

void writer_node::set_payload(const trie_payload& p) noexcept {
    expensive_assert(!_node_size.valid());
    expensive_assert(!_pos.valid());
    _payload = p;
}

} // namespace sstables::trie
