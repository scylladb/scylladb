/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/allocation_strategy.hh"
#include "utils/assert.hh"
#include <algorithm>
#include <memory>
#include <utility>
#include <array>
#include <limits>

namespace lsa::leveled_managed_vector_internal {

struct node_base;

struct node_ptr {
    node_base* _ptr = nullptr;
    node_ptr() = default;
    node_ptr(const node_ptr& other) = delete;
    node_ptr(node_ptr&& other) noexcept;
    node_ptr& operator=(node_ptr&& other) noexcept;
};

struct node_base {
    node_ptr* _backref = nullptr;
    // _capacity (number of T slots in _data) and _size (number of live T
    // elements) live in the node itself, not in the owning node_ptr, so that
    // storage_size(), the LSA migrator's size(), and element destruction stay
    // valid regardless of back-reference state -- in particular for a node
    // that has just been moved-from (its _backref is null).
    size_t _size = 0;
    size_t _capacity = 0;
    bool _is_leaf;
    node_base(node_ptr* backref, size_t capacity, bool is_leaf) noexcept
        : _backref(backref)
        , _capacity(capacity)
        , _is_leaf(is_leaf) {
        SCYLLA_ASSERT(!_backref->_ptr);
        _backref->_ptr = this;
    }
    node_base(node_base&& other) noexcept
        : _backref(std::exchange(other._backref, nullptr))
        , _size(std::exchange(other._size, 0))
        // Copy capacity, don't clear it: a moved-from node is freed using its own
        // _capacity, and its elements were already destroyed by this move, so
        // both fields must survive (the _backref==null guard prevents reuse).
        , _capacity(other._capacity)
        , _is_leaf(other._is_leaf)
        {
        if (_backref) {
            _backref->_ptr = this;
        }
    }
    ~node_base() {
        if (_backref) {
            _backref->_ptr = nullptr;
        }
    }
};

inline node_ptr::node_ptr(node_ptr&& other) noexcept {
    _ptr = std::exchange(other._ptr, nullptr);
    if (_ptr) {
        _ptr->_backref = this;
    }
}

inline node_ptr& node_ptr::operator=(node_ptr&& other) noexcept {
    SCYLLA_ASSERT(!_ptr);
    if (this != &other) {
        this->~node_ptr();
        new (this) node_ptr(std::move(other));
    }
    return *this;
}

template <std::unsigned_integral T>
T constexpr saturating_sub(T a, T b) {
    return (a > b) ? (a - b) : 0;
}

template <typename T>
struct node : node_base {
    T _data[];

    node(node_ptr* backref, size_t capacity) noexcept : node_base(backref, capacity, !std::is_same_v<T, node_ptr>) {}
    node(node&& other) noexcept
        : node_base(std::move(other))
    {
        if (_backref) {
            std::uninitialized_move(other._data, other._data + _size, _data);
            std::destroy(other._data, other._data + _size);
        }
    }
    ~node() {
        if (_backref) {
            std::destroy(_data, _data + _size);
        }
    }
    size_t storage_size() const noexcept {
        return storage_size_for(_capacity);
    }
    constexpr static size_t storage_size_for(size_t n_elements) noexcept {
        return sizeof(node) + sizeof(T) * n_elements;
    }

    static constexpr size_t max_capacity(size_t max_alloc_size) {
        return saturating_sub(max_alloc_size, sizeof(node)) / sizeof(T);
    }
};

constexpr static size_t compute_max_path_length(
    size_t inner_node_capacity,
    size_t leaf_node_capacity
) {
    size_t path_length = 1;
    size_t i = std::numeric_limits<size_t>::max();
    i /= leaf_node_capacity;
    while (i) {
        path_length += 1;
        i /= inner_node_capacity;
    }
    return path_length;
}

// A fragmented buffer of arbitrary size, indexed by integers, with fast appends and random access.
// For use in LSA.
// Implemented as a tree of nodes smaller than max_alloc_size.
//
// Invariants:
// - _size <= _capacity, both for the tree and for each node
// - items between _size and _capacity are uninitialized, both for the tree and for each node
// - all nodes outside the right spine (i.e. path from root down along rightmost edges)
//   have maximum possible capacity for their type (i.e. leaf or inner)
// - all inner nodes outside the right spine have _size == _capacity
// - _height is the length of the left spine
template <typename T, size_t max_alloc_size>
class leveled_managed_vector {
    static_assert(std::is_nothrow_move_constructible_v<T>);
    static_assert(std::is_nothrow_destructible_v<T>);
    static_assert(!std::is_same_v<T, node_ptr>);
    // Note: this represents "confirmed" capacity, not the sum of all leaf capacities.
    // The latter might be greater than the former after an exception in reserve().
    size_t _capacity = 0;
    size_t _size = 0;
    size_t _height = 0;
    node_ptr _root{};

    using inner_node = node<node_ptr>;
    using leaf_node = node<T>;
    constexpr static size_t inner_node_capacity = inner_node::max_capacity(max_alloc_size);
    constexpr static size_t leaf_node_capacity = leaf_node::max_capacity(max_alloc_size);
    static_assert(inner_node_capacity >= 2);
    static_assert(leaf_node_capacity >= 1);
    static_assert(inner_node::storage_size_for(inner_node_capacity) <= max_alloc_size);
    static_assert(leaf_node::storage_size_for(leaf_node_capacity) <= max_alloc_size);

    static inner_node* as_inner_node(node_ptr& x) {
        return const_cast<inner_node*>(as_inner_node(std::as_const(x)));
    }
    static const inner_node* as_inner_node(const node_ptr& x) {
        SCYLLA_ASSERT(x._ptr);
        SCYLLA_ASSERT(!x._ptr->_is_leaf);
        return static_cast<inner_node*>(x._ptr);
    }
    static leaf_node* as_leaf_node(node_ptr& x) {
        return const_cast<leaf_node*>(as_leaf_node(std::as_const(x)));
    }
    static const leaf_node* as_leaf_node(const node_ptr& x) {
        SCYLLA_ASSERT(x._ptr);
        SCYLLA_ASSERT(x._ptr->_is_leaf);
        return static_cast<leaf_node*>(x._ptr);
    }
    static constexpr size_t max_path_length = compute_max_path_length(inner_node_capacity, leaf_node_capacity);

    struct path {
        std::array<size_t, max_path_length> indices;
        size_t length = 0;
    };
    static path compute_ideal_path(size_t i) {
        path result;

        result.indices[result.length++] = i % leaf_node_capacity;
        i /= leaf_node_capacity;
        
        while (i) {
            result.indices[result.length++] = i % inner_node_capacity;
            i /= inner_node_capacity;
        }
        return result;
    }
    static path compute_path(size_t i, size_t height) {
        path result = compute_ideal_path(i);
        while (result.length < height) {
            result.indices[result.length++] = 0;
        }
        return result;
    }
    struct lookup_result {
        const leaf_node* node;
        size_t offset;
    };
    lookup_result lookup(size_t i) const {
        auto p = compute_path(i, _height);

        const node_ptr* curr = &_root;
        while (p.length > 1) {
            --p.length;
            SCYLLA_ASSERT(p.indices[p.length] < curr->_ptr->_size);
            curr = &as_inner_node(*curr)->_data[p.indices[p.length]];
        }

        return lookup_result{as_leaf_node(*curr), p.indices[0]};
    }
    static void do_free(node_ptr& p) {
        if (!p._ptr) {
            return;
        }
        if (p._ptr->_is_leaf) {
            auto* x = as_leaf_node(p);
            current_allocator().destroy(x);
        } else {
            auto* x = as_inner_node(p);
            for (size_t i = 0; i < x->_size; ++i) {
                do_free(x->_data[i]);
            }
            current_allocator().destroy(x);
        }
    }
    void reserve_for_push_back() {
        SCYLLA_ASSERT(_size <= _capacity);
        if (_size == _capacity) {
            size_t new_capacity;
            if (_capacity == 0) {
                new_capacity = std::clamp(512 / sizeof(T), size_t(1), leaf_node_capacity);
            } else if (_capacity < leaf_node_capacity / 2) {
                new_capacity = _capacity * 2;
            } else {
                new_capacity = (_capacity / leaf_node_capacity + 1) * leaf_node_capacity;
            }
            reserve(new_capacity);
        }
    }
    static node_ptr make_inner_node(size_t capacity) {
        node_ptr result;
        auto new_node = static_cast<inner_node*>(current_allocator().alloc<inner_node>(inner_node::storage_size_for(capacity)));
        new (new_node) inner_node(&result, capacity);
        return result;
    }
    [[nodiscard]]
    static inner_node* resize_inner_node(inner_node* old_node, size_t capacity) {
        auto new_node = static_cast<inner_node*>(current_allocator().alloc<inner_node>(inner_node::storage_size_for(capacity)));
        new (new_node) inner_node(std::move(*old_node));
        new_node->_capacity = capacity;
        current_allocator().destroy(old_node);
        return new_node;
    }
    [[nodiscard]]
    static leaf_node* resize_leaf_node(leaf_node* old_node, size_t capacity) {
        auto new_node = static_cast<leaf_node*>(current_allocator().alloc<leaf_node>(leaf_node::storage_size_for(capacity)));
        new (new_node) leaf_node(std::move(*old_node));
        new_node->_capacity = capacity;
        current_allocator().destroy(old_node);
        return new_node;
    }
    static node_ptr make_leaf_node(size_t capacity) {
        node_ptr result;
        auto new_node = static_cast<leaf_node*>(current_allocator().alloc<leaf_node>(leaf_node::storage_size_for(capacity)));
        new (new_node) leaf_node(&result, capacity);
        return result;
    }
    void prepend_root(size_t root_capacity) {
        if (_root._ptr) {
            auto new_root = make_inner_node(root_capacity);
            inner_node* new_root_typed = as_inner_node(new_root);
            new (&new_root_typed->_data[new_root_typed->_size++]) node_ptr(std::move(_root));
            _root = std::move(new_root);
        } else {
            _root = make_leaf_node(root_capacity);
        }
        _height += 1;
    }
    // Reserve capacity under the given node (which has the given height),
    // growing nodes to a capacity just big enough to support a path described by `spine`.
    // (While maintaining the invariant that only right spine is allowed contain nodes
    // of non-maximal capacity). 
    // If spine_array is null, the entire subtree is grow to its maximal capacity.
    static void reserve_with_spine(node_ptr& p, const size_t* spine, size_t height) {
        if (height > 1) {
            auto desired_capacity = spine ? spine[height - 1] + 1 : inner_node_capacity;
            if (!p._ptr) {
                p = make_inner_node(desired_capacity);
            }
            inner_node* inner = as_inner_node(p);
            if (inner->_capacity < desired_capacity) {
                inner = resize_inner_node(inner, desired_capacity);
            }
            size_t old_size = inner->_size;
            size_t new_size = desired_capacity;
            size_t start_index = saturating_sub<size_t>(old_size, 1);
            for (size_t i = start_index; i < new_size; ++i) {
                if (i + 1 != old_size) {
                    new (&inner->_data[i]) node_ptr();
                    inner->_size++;
                }
                SCYLLA_ASSERT(i + 1 == inner->_size);
                if (i + 1 == new_size) {
                    reserve_with_spine(inner->_data[i], spine, height - 1);
                } else {
                    reserve_with_spine(inner->_data[i], nullptr, height - 1);
                }
            }
        } else {
            auto desired_capacity = spine ? spine[height - 1] + 1 : leaf_node_capacity;
            if (!p._ptr) {
                p = make_leaf_node(desired_capacity);
            }
            leaf_node* leaf = as_leaf_node(p);
            if (leaf->_capacity < desired_capacity) {
                leaf = resize_leaf_node(leaf, desired_capacity);
            }
        }
    }
    static size_t compute_memory_usage(const node_ptr& p, size_t height) {
        if (!p._ptr) {
            return 0;
        }
        if (height > 1) {
            const inner_node* inner = as_inner_node(p);
            size_t total = inner->storage_size();
            for (size_t i = 0; i < inner->_size; ++i) {
                total += compute_memory_usage(inner->_data[i], height - 1);
            }
            return total;
        } else {
            const leaf_node* leaf = as_leaf_node(p);
            return leaf->storage_size();
        }
    }
public:
    leveled_managed_vector() noexcept = default;
    leveled_managed_vector(leveled_managed_vector&& other) noexcept
        : _capacity(std::exchange(other._capacity, 0))
        , _size(std::exchange(other._size, 0))
        , _height(std::exchange(other._height, 0))
        , _root(std::move(other._root))
    {}
    leveled_managed_vector& operator=(leveled_managed_vector&& other) noexcept {
        if (this != &other) {
            this->~leveled_managed_vector();
            new (this) leveled_managed_vector(std::move(other));
        }
        return *this;
    }
    ~leveled_managed_vector() {
        do_free(_root);
    }
    void reserve(size_t n) {
        if (_capacity >= n) {
            return;
        }
        auto new_spine = compute_ideal_path(n - 1);
        while (_height < new_spine.length) {
            prepend_root(new_spine.indices[_height] + 1);
        }
        // The tree may be taller than n's ideal path, due to leftover nodes from an
        // earlier reserve that grew the height via prepend_root but then threw
        // before updating _capacity.
        while (new_spine.length < _height) {
            new_spine.indices[new_spine.length++] = 0;
        }
        reserve_with_spine(_root, new_spine.indices.data(), _height);
        _capacity = n;
    }
    const T& operator[](size_t i) const {
        SCYLLA_ASSERT(i < _size);
        auto coords = lookup(i);
        return coords.node->_data[coords.offset];
    }
    T& operator[](size_t i) {
        return const_cast<T&>(const_cast<const leveled_managed_vector&>(*this).operator[](i));
    }
    void grow_to(size_t n) {
        reserve(n);
        while (_size < n) {
            emplace_back();
        }
    }
    template <typename... Args>
    T& emplace_back(Args&&... args) {
        reserve_for_push_back();
        auto coords = lookup(_size);
        auto leaf = const_cast<leaf_node*>(coords.node);
        SCYLLA_ASSERT(leaf->_size == coords.offset);
        SCYLLA_ASSERT(leaf->_capacity > leaf->_size);
        auto p = new (&leaf->_data[coords.offset]) T(std::forward<Args>(args)...);
        ++leaf->_size;
        ++_size;
        return *p;
    }
    size_t size() const noexcept {
        return _size;
    }
    bool empty() const noexcept {
        return size() == 0;
    }
    size_t external_memory_usage() const {
        return compute_memory_usage(_root, _height);
    }
};

} // namespace lsa::leveled_managed_vector_internal

namespace lsa {
    using leveled_managed_vector_internal::leveled_managed_vector;
}