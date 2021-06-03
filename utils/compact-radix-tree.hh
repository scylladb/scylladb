/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <cassert>
#include <algorithm>
#include <bitset>
#include <fmt/core.h>
#include "utils/allocation_strategy.hh"
#include "utils/array-search.hh"
#include <boost/intrusive/parent_from_member.hpp>

class size_calculator;

namespace compact_radix_tree {

template <typename T, typename Idx> class printer;

template <unsigned Size>
inline unsigned find_in_array(uint8_t val, const uint8_t* arr);

template <>
inline unsigned find_in_array<4>(uint8_t val, const uint8_t* arr) {
    return utils::array_search_4_eq(val, arr);
}

template <>
inline unsigned find_in_array<8>(uint8_t val, const uint8_t* arr) {
    return utils::array_search_8_eq(val, arr);
}

template <>
inline unsigned find_in_array<16>(uint8_t val, const uint8_t* arr) {
    return utils::array_search_16_eq(val, arr);
}

template <>
inline unsigned find_in_array<32>(uint8_t val, const uint8_t* arr) {
    return utils::array_search_32_eq(val, arr);
}

template <>
inline unsigned find_in_array<64>(uint8_t val, const uint8_t* arr) {
    return utils::array_search_x32_eq(val, arr, 2);
}

// A union of any number of types.

template <typename... Ts>
struct variadic_union;

template <typename Tx>
struct variadic_union<Tx> {
    union {
        Tx _this;
    };

    variadic_union() noexcept {}
    ~variadic_union() {}
};

template <typename Tx, typename Ty, typename... Ts>
struct variadic_union<Tx, Ty, Ts...> {
    union {
        Tx _this;
        variadic_union<Ty, Ts...> _other;
    };

    variadic_union() noexcept {}
    ~variadic_union() {}
};

/*
 * Radix tree implementation for the key being an integer type.
 * The search key is split into equal-size pieces to find the
 * next node in each level. The pieces are defined compile-time
 * so the tree is compile-time limited in ints depth.
 *
 * Uses 3 memory optimizations:
 * - a node dynamically grows in size depending on the range of
 *   keys it carries
 * - additionally, if the set of keys on a node is very sparse the
 *   node may become "indirect" thus keeping only the actual set
 *   of keys
 * - if a node has 1 child it's removed from the tree and this
 *   loneley kid is attached directly to its (former) grandfather
 */

template <typename T, typename Index = unsigned int>
requires std::is_nothrow_move_constructible_v<T> && std::is_integral_v<Index>
class tree {
    friend class ::size_calculator;
    template <typename A, typename I> friend class printer;

    class leaf_node;
    class inner_node;
    struct node_head;
    class node_head_ptr;

public:
    /*
     * The search key in the tree is an integer, the whole
     * logic below is optimized for that.
     */
    using key_t = std::make_unsigned_t<Index>;

    /*
     * The lookup uses 7-bit pieces from the key to search on
     * each level. Thus all levels but the last one keep pointers
     * on lower levels, the last one is the leaf node that keeps
     * values on board.
     *
     * The 8th bit in the node index byte is used to denote an
     * unused index which is quite helpful.
     */
    using node_index_t = uint8_t;
    static constexpr unsigned radix_bits = 7;
    static constexpr key_t  radix_mask = (1 << radix_bits) - 1;
    static constexpr unsigned leaf_depth = (8 * sizeof(key_t) + radix_bits - 1) / radix_bits - 1;
    static constexpr unsigned node_index_limit = 1 << radix_bits;
    static_assert(node_index_limit != 0);
    static constexpr node_index_t unused_node_index = node_index_limit;

private:
    /*
     * Nodes can be of 2 kinds -- direct and indirect.
     *
     * Direct nodes are arrays of elements. Getting a value from
     * this node is simple indexing. There are 2 of them -- static
     * and dynamic. Static nodes have fixed size capable to keep all
     * the possible indices, dynamic work like a vector growing in
     * size. Former occupy more space, but work a bit faster because
     * of * missing boundary checks.
     *
     * Indirect nodes keep map of indices on board and perform lookup
     * rather than direct indexing to get a value. They also grow in
     * size, but unlike dynamic direct nodes by converting between each
     * other.
     *
     * When a node is tried to push a new index over its current
     * capacity it grows into some other node, that can fit all its
     * keys plus at least one.
     *
     * When a key is removed from an indirect node and it becomes
     * less that some threshold, it's shrunk into smaller node.
     *
     * The nil is a placeholder for non-existing empty node.
     */
    enum class layout : uint8_t { nil,
        indirect_tiny, indirect_small, indirect_medium, indirect_large,
        direct_dynamic, direct_static, };

    /*
     * When a node has only one child, the former is removed from
     * the tree and its parent is set up to directly point to this
     * only kid. The kid, in turn, carries a "prefix" on board
     * denoting the index that might have been skipped by this cut.
     *
     * The lower 7 bits are the prefix length, the rest is the
     * prefix itself.
     */
    static constexpr key_t prefix_len_mask = radix_mask;
    static constexpr key_t prefix_mask = ~prefix_len_mask;

    static key_t make_prefix(key_t key, unsigned len) noexcept {
        return (key & prefix_mask) + len;
    }

    /*
     * Mask to check node's prefix (mis-)match
     */
    static key_t prefix_mask_at(unsigned depth) noexcept {
        return prefix_mask << (radix_bits * (leaf_depth - depth));
    }

    /*
     * Finds the number of leading elements that coinside for two
     * indices. Needed on insertion, when a short-cut node gets
     * expanded back.
     */
    static unsigned common_prefix_len(key_t k1, key_t k2) noexcept {
        static constexpr unsigned trailing_bits = (8 * sizeof(key_t)) % radix_bits;
        static constexpr unsigned round_up_delta = trailing_bits == 0 ? 0 : radix_bits - trailing_bits;
        /*
         * This won't work if k1 == k2 (clz is undefined for full
         * zeroes value), but we don't get here in this case
         */
        return (__builtin_clz(k1 ^ k2) + round_up_delta) / radix_bits;
    }

    /*
     * Gets the depth's radix_bits-len index from the whole key, that's
     * used in intra-node search.
     */
    static node_index_t node_index(key_t key, unsigned depth) noexcept {
        return (key >> (radix_bits * (leaf_depth - depth))) & radix_mask;
    }

    enum class erase_mode { real, cleanup, };

    /*
     * When removing an index from a node it may end-up in one of 4
     * states:
     *
     * - empty  -- the last index was removed, the parent node is
     *             welcome to drop the slot and mark it as unused
     *             (and maybe get shrunk/squashed after that)
     * - squash -- only one index left, the parent node is welcome
     *             to remove this node and replace it with its only
     *             child (tuning it's prefix respectively)
     * - shrink -- current layout contains few indices, so parent
     *             node should shrink the slot into smaller node
     * - nothing - just nothing
     */
    enum class erase_result { nothing, empty, shrink, squash, };

    template <unsigned Threshold>
    static erase_result after_drop(unsigned count) noexcept {
        if (count == 0) {
            return erase_result::empty;
        }
        if (count == 1) {
            return erase_result::squash;
        }

        if constexpr (Threshold != 0) {
            if (count <= Threshold) {
                return erase_result::shrink;
            }
        }

        return erase_result::nothing;
    }

    /*
     * Lower-bound calls return back pointer on the value and the leaf
     * node_head on which the value was found. The latter is needed
     * for iterator's ++ optimization.
     */
    struct lower_bound_res {
        const T* elem;
        const node_head* leaf;
        key_t key;

        lower_bound_res(const T* e, const node_head& l, key_t k) noexcept : elem(e), leaf(&l), key(k) {}
        lower_bound_res() noexcept : elem(nullptr), leaf(nullptr), key(0) {}
    };

    /*
     * Allocation returns a slot pointer and a boolean denoting
     * if the allocation really took place (false if the slot
     * is aleady occupied)
     */
    using allocate_res = std::pair<T*, bool>;

    using clone_res = std::pair<node_head*, std::exception_ptr>;

    /*
     * A header all nodes start with. The type of a node (inner/leaf)
     * is evaluated (fingers-crossed) from the depth argument, so the
     * header doesn't have this bit.
     */
    struct node_head {
        node_head_ptr* _backref;
        // Prefix for squashed nodes
        key_t _prefix;
        const layout _base_layout;
        // Number of keys on the node
        uint8_t _size;
        // How many slots are there. Used only by direct dynamic nodes
        const uint8_t _capacity;

        node_head() noexcept : _backref(nullptr), _prefix(0), _base_layout(layout::nil), _size(0), _capacity(0) {}

        node_head(key_t prefix, layout lt, uint8_t capacity) noexcept
                : _backref(nullptr)
                , _prefix(prefix)
                , _base_layout(lt)
                , _size(0)
                , _capacity(capacity) {}

        node_head(node_head&& o) noexcept
                : _backref(std::exchange(o._backref, nullptr))
                , _prefix(o._prefix)
                , _base_layout(o._base_layout)
                , _size(std::exchange(o._size, 0))
                , _capacity(o._capacity) {
            if (_backref != nullptr) {
                *_backref = this;
            }
        }

        node_head(const node_head&) = delete;
        ~node_head() { assert(_size == 0); }

        /*
         * Helpers to cast header to the actual node class or to the
         * node's base class (see below).
         */

        template <typename NBT>
        NBT& as_base() noexcept {
            return *boost::intrusive::get_parent_from_member(this, &NBT::_head);
        }

        template <typename NBT>
        const NBT& as_base() const noexcept {
            return *boost::intrusive::get_parent_from_member(this, &NBT::_head);
        }

        template <typename NT>
        typename NT::node_type& as_base_of() noexcept {
            return as_base<typename NT::node_type>();
        }

        template <typename NT>
        const typename NT::node_type& as_base_of() const noexcept {
            return as_base<typename NT::node_type>();
        }

        template <typename NT>
        NT& as_node() noexcept {
            return *boost::intrusive::get_parent_from_member(&as_base_of<NT>(), &NT::_base);
        }

        template <typename NT>
        const NT& as_node() const noexcept {
            return *boost::intrusive::get_parent_from_member(&as_base_of<NT>(), &NT::_base);
        }

        // Construct a key from leaf node prefix and index
        key_t key_of(node_index_t ni) const noexcept {
            return (_prefix & prefix_mask) + ni;
        }

        // Prefix manipulations
        unsigned prefix_len() const noexcept { return _prefix & prefix_len_mask; }
        void trim_prefix(unsigned v) noexcept { _prefix -= v; }
        void bump_prefix(unsigned v) noexcept { _prefix += v; }

        bool check_prefix(key_t key, unsigned& depth) const noexcept {
            unsigned real_depth = depth + prefix_len();
            key_t mask = prefix_mask_at(real_depth);
            if ((key & mask) != (_prefix & mask)) {
                return false;
            }

            depth = real_depth;
            return true;
        }

        /*
         * A bunch of "polymorphic" API wrappers that selects leaf/inner
         * node to call the method on.
         *
         * The node_base below provides the same set, but ploymorphs
         * the calls into the actual node layout.
         */

        /*
         * Finds the element by the given key
         */

        const T* get(key_t key, unsigned depth) const noexcept {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().get(key, depth);
            } else {
                return as_base_of<inner_node>().get(key, depth);
            }
        }

        /*
         * Finds the element whose key is not greater than the given one
         */

        lower_bound_res lower_bound(key_t key, unsigned depth) const noexcept {
            unsigned real_depth = depth + prefix_len();
            key_t mask = prefix_mask_at(real_depth);
            if ((key & mask) > (_prefix & mask)) {
                return lower_bound_res();
            }

            depth = real_depth;
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().lower_bound(key, depth);
            } else {
                return as_base_of<inner_node>().lower_bound(key, depth);
            }
        }

        /*
         * Allocates a new slot for the value. The caller is given the
         * pointer to the slot and the sign if it's now busy or not,
         * so that it can destruct it and construct a new element.
         */

        allocate_res alloc(key_t key, unsigned depth) {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().alloc(key, depth);
            } else {
                return as_base_of<inner_node>().alloc(key, depth);
            }
        }

        /*
         * Erase the element with the given key, if present.
         */

        erase_result erase(key_t key, unsigned depth, erase_mode erm) noexcept {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().erase(key, depth, erm);
            } else {
                return as_base_of<inner_node>().erase(key, depth, erm);
            }
        }

        /*
         * Weed walks the tree and removes the elements for which
         * the filter() returns true.
         */

        template <typename Fn>
        erase_result weed(Fn&& filter, unsigned depth) {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().weed(filter, depth);
            } else {
                return as_base_of<inner_node>().weed(filter, depth);
            }
        }

        /*
         * Grow the current node and return the new one
         */

        node_head* grow(key_t key, unsigned depth) {
            node_index_t ni = node_index(key, depth);
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().template grow<leaf_node>(ni);
            } else {
                return as_base_of<inner_node>().template grow<inner_node>(ni);
            }
        }

        /*
         * Shrink the current node and return the new one
         */

        node_head* shrink(unsigned depth) {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().template shrink<leaf_node>();
            } else {
                return as_base_of<inner_node>().template shrink<inner_node>();
            }
        }

        /*
         * Walk the tree without modifying it (however, the elements
         * themselves can be modified)
         */

        template <typename Visitor>
        bool visit(Visitor&& v, unsigned depth) const {
            bool ret = true;
            depth += prefix_len();
            if (v(*this, depth, true)) {
                if (depth == leaf_depth) {
                    ret = as_base_of<leaf_node>().visit(v, depth);
                } else {
                    ret = as_base_of<inner_node>().visit(v, depth);
                }
                v(*this, depth, false);
            }
            return ret;
        }

        template <typename Fn>
        clone_res clone(Fn&& cloner, unsigned depth) const noexcept {
            depth += prefix_len();
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().template clone<leaf_node, Fn>(cloner, depth);
            } else {
                return as_base_of<inner_node>().template clone<inner_node, Fn>(cloner, depth);
            }
        }

        void free(unsigned depth) noexcept {
            if (depth == leaf_depth) {
                leaf_node::free(as_node<leaf_node>());
            } else {
                inner_node::free(as_node<inner_node>());
            }
        }

        size_t node_size(unsigned depth) const noexcept {
            if (depth == leaf_depth) {
                return as_base_of<leaf_node>().node_size();
            } else {
                return as_base_of<inner_node>().node_size();
            }
        }

        /*
         * A leaf-node specific helper for iterator
         */
        lower_bound_res lower_bound(key_t key) const noexcept {
            return as_base_of<leaf_node>().lower_bound(key, leaf_depth);
        }

        /*
         * And two inner-node specific calls for nodes
         * squashing/expanding
         */

        void set_lower(node_index_t ni, node_head* n) noexcept {
            as_node<inner_node>().set_lower(ni, n);
        }

        node_head_ptr pop_lower() noexcept {
            return as_node<inner_node>().pop_lower();
        }
    };

    /*
     * Pointer to node head. Inner nodes keep these, tree root pointer
     * is the one as well.
     */
    class node_head_ptr {
        node_head* _v;

    public:
        node_head_ptr(node_head* v) noexcept : _v(v) {}
        node_head_ptr(const node_head_ptr&) = delete;
        node_head_ptr(node_head_ptr&& o) noexcept : _v(std::exchange(o._v, nullptr)) {
            if (_v != nullptr) {
                _v->_backref = this;
            }
        }

        node_head& operator*() const noexcept { return *_v; }
        node_head* operator->() const noexcept { return _v; }
        node_head* raw() const noexcept { return _v; }

        operator bool() const noexcept { return _v != nullptr; }
        bool is(const node_head& n) const noexcept { return _v == &n; }

        node_head_ptr& operator=(node_head* v) noexcept {
            _v = v;
            if (_v != nullptr) {
                _v->_backref = this;
            }
            return *this;
        }
    };

    /*
     * This helper wraps several layouts into one and preceeds them with
     * the header. It does nothing but provides a polymorphic calls to the
     * lower/inner layouts depending on the head.base_layout value.
     */
    template <typename Slot, typename... Layouts>
    struct node_base {
        node_head _head;
        variadic_union<Layouts...> _layouts;

        template <typename Tx>
        static size_t node_size(layout lt, uint8_t capacity) noexcept {
            return sizeof(node_head) + Tx::layout_size(capacity);
        }

        template <typename Tx, typename Ty, typename... Ts>
        static size_t node_size(layout lt, uint8_t capacity) noexcept {
            return lt == Tx::layout ? sizeof(node_head) + Tx::layout_size(capacity) : node_size<Ty, Ts...>(lt, capacity);
        }

        static size_t node_size(layout lt, uint8_t capacity) noexcept {
            return node_size<Layouts...>(lt, capacity);
        }

        size_t node_size() const noexcept {
            return node_size(_head._base_layout, _head._capacity);
        }

        // construct

        template <typename Tx>
        void construct(variadic_union<Tx>& cur) noexcept {
            new (&cur._this) Tx(_head);
        }

        template <typename Tx, typename Ty, typename... Ts>
        void construct(variadic_union<Tx, Ty, Ts...>& cur) noexcept {
            if (_head._base_layout == Tx::layout) {
                new (&cur._this) Tx(_head);
                return;
            }

            construct<Ty, Ts...>(cur._other);
        }

        node_base(key_t prefix, layout lt, uint8_t capacity) noexcept
                : _head(prefix, lt, capacity) {
            construct<Layouts...>(_layouts);
        }

        node_base(const node_base&) = delete;

        template <typename Tx>
        void move_construct(variadic_union<Tx>& cur, variadic_union<Tx>&& o) noexcept {
            new (&cur._this) Tx(std::move(o._this), _head);
        }

        template <typename Tx, typename Ty, typename... Ts>
        void move_construct(variadic_union<Tx, Ty, Ts...>& cur, variadic_union<Tx, Ty, Ts...>&& o) noexcept {
            if (_head._base_layout == Tx::layout) {
                new (&cur._this) Tx(std::move(o._this), _head);
                return;
            }

            move_construct<Ty, Ts...>(cur._other, std::move(o._other));
        }

        node_base(node_base&& o) noexcept
                : _head(std::move(o._head)) {
            move_construct<Layouts...>(_layouts, std::move(o._layouts));
        }

        ~node_base() { }

        // get value by key

        template <typename Tx>
        const T* get(const variadic_union<Tx>& cur, key_t key, unsigned depth) const noexcept {
            if (_head._base_layout == Tx::layout) {
                return cur._this.get(_head, key, depth);
            }

            return (const T*)nullptr;
        }

        template <typename Tx, typename Ty, typename... Ts>
        const T* get(const variadic_union<Tx, Ty, Ts...>& cur, key_t key, unsigned depth) const noexcept {
            if (_head._base_layout == Tx::layout) {
                return cur._this.get(_head, key, depth);
            }

            return get<Ty, Ts...>(cur._other, key, depth);
        }

        const T* get(key_t key, unsigned depth) const noexcept {
            return get<Layouts...>(_layouts, key, depth);
        }

        // finds a lowed-bound element

        template <typename Tx>
        lower_bound_res lower_bound(const variadic_union<Tx>& cur, key_t key, unsigned depth) const noexcept {
            if (_head._base_layout == Tx::layout) {
                return cur._this.lower_bound(_head, key, depth);
            }

            return lower_bound_res();
        }

        template <typename Tx, typename Ty, typename... Ts>
        lower_bound_res lower_bound(const variadic_union<Tx, Ty, Ts...>& cur, key_t key, unsigned depth) const noexcept {
            if (_head._base_layout == Tx::layout) {
                return cur._this.lower_bound(_head, key, depth);
            }

            return lower_bound<Ty, Ts...>(cur._other, key, depth);
        }

        lower_bound_res lower_bound(key_t key, unsigned depth) const noexcept {
            return lower_bound<Layouts...>(_layouts, key, depth);
        }

        // erase by key

        template <typename Tx>
        erase_result erase(variadic_union<Tx>& cur, key_t key, unsigned depth, erase_mode erm) noexcept {
            return cur._this.erase(_head, key, depth, erm);
        }

        template <typename Tx, typename Ty, typename... Ts>
        erase_result erase(variadic_union<Tx, Ty, Ts...>& cur, key_t key, unsigned depth, erase_mode erm) noexcept {
            if (_head._base_layout == Tx::layout) {
                return cur._this.erase(_head, key, depth, erm);
            }

            return erase<Ty, Ts...>(cur._other, key, depth, erm);
        }

        erase_result erase(key_t key, unsigned depth, erase_mode erm) noexcept {
            return erase<Layouts...>(_layouts, key, depth, erm);
        }

        // weed values with filter

        template <typename Fn, typename Tx>
        erase_result weed(variadic_union<Tx>& cur, Fn&& filter, unsigned depth) {
            return cur._this.weed(_head, filter, _head._prefix, depth);
        }

        template <typename Fn, typename Tx, typename Ty, typename... Ts>
        erase_result weed(variadic_union<Tx, Ty, Ts...>& cur, Fn&& filter, unsigned depth) {
            if (_head._base_layout == Tx::layout) {
                return cur._this.weed(_head, filter, _head._prefix, depth);
            }

            return weed<Fn, Ty, Ts...>(cur._other, filter, depth);
        }

        template <typename Fn>
        erase_result weed(Fn&& filter, unsigned depth) {
            return weed<Fn, Layouts...>(_layouts, filter, depth);
        }

        // allocate new slot

        template <typename Tx>
        allocate_res alloc(variadic_union<Tx>& cur, key_t key, unsigned depth) {
            return cur._this.alloc(_head, key, depth);
        }

        template <typename Tx, typename Ty, typename... Ts>
        allocate_res alloc(variadic_union<Tx, Ty, Ts...>& cur, key_t key, unsigned depth) {
            if (_head._base_layout == Tx::layout) {
                return cur._this.alloc(_head, key, depth);
            }

            return alloc<Ty, Ts...>(cur._other, key, depth);
        }

        allocate_res alloc(key_t key, unsigned depth) {
            return alloc<Layouts...>(_layouts, key, depth);
        }

        // append slot to node

        template <typename Tx>
        void append(variadic_union<Tx>& cur, node_index_t ni, Slot&& val) noexcept {
            cur._this.append(_head, ni, std::move(val));
        }

        template <typename Tx, typename Ty, typename... Ts>
        void append(variadic_union<Tx, Ty, Ts...>& cur, node_index_t ni, Slot&& val) noexcept {
            if (_head._base_layout == Tx::layout) {
                cur._this.append(_head, ni, std::move(val));
                return;
            }

            append<Ty, Ts...>(cur._other, ni, std::move(val));
        }

        void append(node_index_t ni, Slot&& val) noexcept {
            return append<Layouts...>(_layouts, ni, std::move(val));
        }

        // find and remove some element (usually the last one)

        template <typename Tx>
        Slot pop(variadic_union<Tx>& cur) noexcept {
            return cur._this.pop(_head);
        }

        template <typename Tx, typename Ty, typename... Ts>
        Slot pop(variadic_union<Tx, Ty, Ts...>& cur) noexcept {
            if (_head._base_layout == Tx::layout) {
                return cur._this.pop(_head);
            }

            return pop<Ty, Ts...>(cur._other);
        }

        Slot pop() noexcept {
            return pop<Layouts...>(_layouts);
        }

        // visiting

        template <typename Visitor, typename Tx>
        bool visit(const variadic_union<Tx>& cur, Visitor&& v, unsigned depth) const {
            return cur._this.visit(_head, v, depth);
        }

        template <typename Visitor, typename Tx, typename Ty, typename... Ts>
        bool visit(const variadic_union<Tx, Ty, Ts...>& cur, Visitor&& v, unsigned depth) const {
            if (_head._base_layout == Tx::layout) {
                return cur._this.visit(_head, v, depth);
            }

            return visit<Visitor, Ty, Ts...>(cur._other, v, depth);
        }

        template <typename Visitor>
        bool visit(Visitor&& v, unsigned depth) const {
            return visit<Visitor, Layouts...>(_layouts, v, depth);
        }

        // cloning

        template <typename NT, typename Fn, typename Tx>
        clone_res clone(const variadic_union<Tx>& cur, Fn&& cloner, unsigned depth) const noexcept {
            return cur._this.template clone<NT, Fn>(_head, cloner, depth);
        }

        template <typename NT, typename Fn, typename Tx, typename Ty, typename... Ts>
        clone_res clone(const variadic_union<Tx, Ty, Ts...>& cur, Fn&& cloner, unsigned depth) const noexcept {
            if (_head._base_layout == Tx::layout) {
                return cur._this.template clone<NT, Fn>(_head, cloner, depth);
            }

            return clone<NT, Fn, Ty, Ts...>(cur._other, cloner, depth);
        }

        template <typename NT, typename Fn>
        clone_res clone(Fn&& cloner, unsigned depth) const noexcept {
            return clone<NT, Fn, Layouts...>(_layouts, cloner, depth);
        }

        // growing into larger layout

        template <typename NT, typename Tx>
        node_head* grow(variadic_union<Tx>& cur, node_index_t want_ni) {
            if constexpr (Tx::growable) {
                return cur._this.template grow<NT>(_head, want_ni);
            }

            std::abort();
        }

        template <typename NT, typename Tx, typename Ty, typename... Ts>
        node_head* grow(variadic_union<Tx, Ty, Ts...>& cur, node_index_t want_ni) {
            if constexpr (Tx::growable) {
                if (_head._base_layout == Tx::layout) {
                    return cur._this.template grow<NT>(_head, want_ni);
                }
            }

            return grow<NT, Ty, Ts...>(cur._other, want_ni);
        }

        template <typename NT>
        node_head* grow(node_index_t want_ni) {
            return grow<NT, Layouts...>(_layouts, want_ni);
        }

        // shrinking into smaller layout

        template <typename NT, typename Tx>
        node_head* shrink(variadic_union<Tx>& cur) {
            if constexpr (Tx::shrinkable) {
                return cur._this.template shrink<NT>(_head);
            }

            std::abort();
        }

        template <typename NT, typename Tx, typename Ty, typename... Ts>
        node_head* shrink(variadic_union<Tx, Ty, Ts...>& cur) {
            if constexpr (Tx::shrinkable) {
                if (_head._base_layout == Tx::layout) {
                    return cur._this.template shrink<NT>(_head);
                }
            }

            return shrink<NT, Ty, Ts...>(cur._other);
        }

        template <typename NT>
        node_head* shrink() {
            return shrink<NT, Layouts...>(_layouts);
        }
    };

    /*
     * Node layouts. Define the way indices and payloads are stored on the node
     */

    /*
     * Direct layout is just an array of data.
     *
     * It makes a difference between inner slots, that are pointers to other nodes,
     * and leaf slots, which are of user type. The former can be nullptr denoting
     * the missing slot, while the latter may not have this sign, so the layout
     * uses a bitmask to check if a slot is occupiued or not.
     */
    template <typename Slot, layout Layout, layout GrowInto, unsigned GrowThreshold, layout ShrinkInto, unsigned ShrinkThreshold>
    struct direct_layout {
        static constexpr bool shrinkable = ShrinkInto != layout::nil;
        static constexpr bool growable = GrowInto != layout::nil;
        static constexpr layout layout = Layout;

        static bool check_capacity(const node_head& head, node_index_t ni) noexcept {
            if constexpr (layout == layout::direct_static) {
                return true;
            } else {
                return ni < head._capacity;
            }
        }

        static unsigned capacity(const node_head& head) noexcept {
            if constexpr (layout == layout::direct_static) {
                return node_index_limit;
            } else {
                return head._capacity;
            }
        }

        template <typename>
        struct array_of {
            /*
             * This bismask is the maximum possible, while the array of slots
             * is dynamic. This is to make sure all direct layouts have the
             * slots at the same offset, so we may not introduce new layouts
             * for it, and to avoid some capacity if-s in the code below
             */
            std::bitset<node_index_limit> _present;
            Slot _slots[0];

            array_of(const node_head& head) noexcept {
                _present.reset();
            }

            array_of(array_of&& o, const node_head& head) noexcept
                    : _present(std::move(o._present)) {
                for (unsigned i = 0; i < capacity(head); i++) {
                    if (o.has(i)) {
                        new (&_slots[i]) Slot(std::move(o._slots[i]));
                        o._slots[i].~Slot();
                    }
                }
            }

            array_of(const array_of&) = delete;

            bool has(unsigned i) const noexcept { return _present.test(i); }
            bool has(const node_head& h, unsigned i) const noexcept { return has(i); }
            void add(node_head& head, unsigned i) noexcept { _present.set(i); }
            void del(node_head& head, unsigned i) noexcept { _present.set(i, false); }
            unsigned count(const node_head& head) const noexcept { return _present.count(); }
        };

        template <>
        struct array_of<node_head_ptr> {
            Slot _slots[0];

            array_of(const node_head& head) noexcept {
                for (unsigned i = 0; i < capacity(head); i++) {
                    new (&_slots[i]) node_head_ptr(nullptr);
                }
            }

            array_of(array_of&& o, const node_head& head) noexcept {
                for (unsigned i = 0; i < capacity(head); i++) {
                    new (&_slots[i]) Slot(std::move(o._slots[i]));
                    o._slots[i].~Slot();
                }
            }

            array_of(const array_of&) = delete;

            bool has(unsigned i) const noexcept { return _slots[i]; }
            bool has(const node_head& h, unsigned i) const noexcept { return check_capacity(h, i) && _slots[i]; }
            void add(node_head& head, unsigned i) noexcept { head._size++; }
            void del(node_head& head, unsigned i) noexcept { head._size--; }
            unsigned count(const node_head& head) const noexcept { return head._size; }
        };

        array_of<Slot> _data;

        direct_layout(const node_head& head) noexcept : _data(head) {}
        direct_layout(direct_layout&& o, const node_head& head) noexcept : _data(std::move(o._data), head) {}
        direct_layout(const direct_layout&) = delete;

        const T* get(const node_head& head, key_t key, unsigned depth) const noexcept {
            node_index_t ni = node_index(key, depth);
            if (!_data.has(head, ni)) {
                return nullptr;
            }
            return get_at(_data._slots[ni], key, depth + 1);
        }

        Slot pop(node_head& head) noexcept {
            for (unsigned i = 0; i < capacity(head); i++) {
                if (_data.has(i)) {
                    Slot ret = std::move(_data._slots[i]);
                    _data.del(head, i);
                    _data._slots[i].~Slot();
                    return ret;
                }
            }

            return nullptr;
        }

        allocate_res alloc(node_head& head, key_t key, unsigned depth) {
            node_index_t ni = node_index(key, depth);

            if (!check_capacity(head, ni)) {
                return allocate_res(nullptr, false);
            }

            bool exists = _data.has(ni);

            if (!exists) {
                populate_slot(_data._slots[ni], key, depth + 1);
                _data.add(head, ni);
            }

            return allocate_on(_data._slots[ni], key, depth + 1, !exists);
        }

        void append(node_head& head, node_index_t ni, Slot&& val) noexcept {
            assert(check_capacity(head, ni));
            assert(!_data.has(ni));
            _data.add(head, ni);
            new (&_data._slots[ni]) Slot(std::move(val));
        }

        erase_result erase(node_head& head, key_t key, unsigned depth, erase_mode erm) noexcept {
            node_index_t ni = node_index(key, depth);

            if (_data.has(head, ni)) {
                if (erase_from_slot(&_data._slots[ni], key, depth + 1, erm)) {
                    _data.del(head, ni);
                    return after_drop<ShrinkThreshold>(_data.count(head));
                }
            }

            return erase_result::nothing;
        }

        template <typename Fn>
        erase_result weed(node_head& head, Fn&& filter, key_t pfx, unsigned depth) {
            bool removed_something = false;

            for (unsigned i = 0; i < capacity(head); i++) {
                if (_data.has(i)) {
                    if (weed_from_slot(head, i, &_data._slots[i], filter, depth + 1)) {
                        _data.del(head, i);
                        removed_something = true;
                    }
                }
            }

            return removed_something ? after_drop<ShrinkThreshold>(_data.count(head)) : erase_result::nothing;
        }

        template <typename NT, typename Cloner>
        clone_res clone(const node_head& head, Cloner&& clone, unsigned depth) const noexcept {
            NT* nn;
            try {
                nn = NT::allocate(head._prefix, head._base_layout, head._capacity);
            } catch (...) {
                return clone_res(nullptr, std::current_exception());
            }

            auto ex = copy_slots(head, _data._slots, capacity(head), depth, nn->_base,
                        [this] (unsigned i) noexcept { return _data.has(i) ? i : unused_node_index; }, clone);
            return std::make_pair(&nn->_base._head, std::move(ex));
        }

        template <typename NT>
        node_head* grow(node_head& head, node_index_t want_ni) {
            static_assert(GrowInto == layout::direct_dynamic && GrowThreshold == 0);

            uint8_t next_cap = head._capacity << 1;
            while (want_ni >= next_cap) {
                next_cap <<= 1;
            }
            assert(next_cap > head._capacity);

            NT* nn = NT::allocate(head._prefix, layout::direct_dynamic, next_cap);
            move_slots(_data._slots, head._capacity, head._capacity + 1, nn->_base,
                    [this] (unsigned i) noexcept { return _data.has(i) ? i : unused_node_index; });
            head._size = 0;
            return &nn->_base._head;
        }

        template <typename NT>
        node_head* shrink(node_head& head) {
            static_assert(shrinkable && ShrinkThreshold != 0);

            NT* nn = NT::allocate(head._prefix, ShrinkInto);
            move_slots(_data._slots, node_index_limit, ShrinkThreshold, nn->_base,
                    [this] (unsigned i) noexcept { return _data.has(i) ? i : unused_node_index; });
            head._size = 0;
            return &nn->_base._head;
        }

        lower_bound_res lower_bound(const node_head& head, key_t key, unsigned depth) const noexcept {
            node_index_t ni = node_index(key, depth);

            if (_data.has(head, ni)) {
                lower_bound_res ret = lower_bound_at(&_data._slots[ni], head, ni, key, depth);
                if (ret.elem != nullptr) {
                    return ret;
                }
            }

            for (unsigned i = ni + 1; i < capacity(head); i++) {
                if (_data.has(i)) {
                    /*
                     * Nothing was found on the slot, that matches the
                     * given index. We need to move to the next one, but
                     * zero-out all key's bits related to lower levels.
                     *
                     * Fortunately, leaf nodes will rewrite the whole
                     * thing on match, so put 0 into the whole key.
                     *
                     * Also note, that short-cut iterator++ assumes that
                     * index is NOT 0-ed in case of mismatch!
                     */
                    return lower_bound_at(&_data._slots[i], head, i, 0, depth);
                }
            }

            return lower_bound_res();
        }

        template <typename Visitor>
        bool visit(const node_head& head, Visitor&& v, unsigned depth) const {
            for (unsigned i = 0; i < capacity(head); i++) {
                if (_data.has(i)) {
                    if (!visit_slot(v, head, i, &_data._slots[i], depth)) {
                        return false;
                    }
                }
            }
            return true;
        }

        static size_t layout_size(uint8_t capacity) noexcept {
            if constexpr (layout == layout::direct_static) {
                return sizeof(direct_layout) + node_index_limit * sizeof(Slot);
            } else {
                assert(capacity != 0);
                return sizeof(direct_layout) + capacity * sizeof(Slot);
            }
        }
    };

    /*
     * The indirect layout is used to keep small number of sparse keys on
     * small node. To do that it keeps an array of indices and when is
     * asked to get an element searches in this array. This map additionally
     * works as a presense bitmask from direct layout.
     *
     * Since indirect layouts of different sizes have slots starting at
     * different addresses in memory, they cannot grow dynamically, but are
     * converted (by moving data) into each other.
     */
    template <typename Slot, layout Layout, unsigned Size, layout GrowInto, unsigned GrowThreshold, layout ShrinkInto, unsigned ShrinkThreshold>
    struct indirect_layout {
        static constexpr bool shrinkable = ShrinkInto != layout::nil;
        static constexpr bool growable = GrowInto != layout::nil;
        static constexpr unsigned size = Size;
        static constexpr layout layout = Layout;

        node_index_t _idx[Size];
        Slot _slots[0];

        bool has(unsigned i) const noexcept { return _idx[i] != unused_node_index; }
        void unset(unsigned i) noexcept { _idx[i] = unused_node_index; }

        indirect_layout(const node_head& head) noexcept {
            for (unsigned i = 0; i < Size; i++) {
                _idx[i] = unused_node_index;
            }
        }

        indirect_layout(indirect_layout&& o, const node_head& head) noexcept {
            for (unsigned i = 0; i < Size; i++) {
                _idx[i] = o._idx[i];
                if (o.has(i)) {
                    new (&_slots[i]) Slot(std::move(o._slots[i]));
                    o._slots[i].~Slot();
                }
            }
        }

        indirect_layout(const indirect_layout&) = delete;

        const T* get(const node_head& head, key_t key, unsigned depth) const noexcept {
            node_index_t ni = node_index(key, depth);
            unsigned i = find_in_array<Size>(ni, _idx);
            if (i >= Size) {
                return nullptr;
            }
            return get_at(_slots[i], key, depth + 1);
        }

        Slot pop(node_head& head) noexcept {
            for (unsigned i = 0; i < Size; i++) {
                if (has(i)) {
                    Slot ret = std::move(_slots[i]);
                    head._size--;
                    _slots[i].~Slot();
                    return ret;
                }
            }

            return nullptr;
        }

        allocate_res alloc(node_head& head, key_t key, unsigned depth) {
            node_index_t ni = node_index(key, depth);
            bool new_slot = false;
            unsigned i = find_in_array<Size>(ni, _idx);
            if (i >= Size) {
                i = find_in_array<Size>(unused_node_index, _idx);
                if (i >= Size) {
                    return allocate_res(nullptr, false);
                }

                populate_slot(_slots[i], key, depth + 1);
                _idx[i] = ni;
                head._size++;
                new_slot = true;
            }

            return allocate_on(_slots[i], key, depth + 1, new_slot);
        }

        void append(node_head& head, node_index_t ni, Slot&& val) noexcept {
            unsigned i = head._size++;
            assert(i < Size);
            assert(_idx[i] == unused_node_index);
            _idx[i] = ni;
            new (&_slots[i]) Slot(std::move(val));
        }

        erase_result erase(node_head& head, key_t key, unsigned depth, erase_mode erm) noexcept {
            node_index_t ni = node_index(key, depth);
            unsigned i = find_in_array<Size>(ni, _idx);
            if (i < Size) {
                if (erase_from_slot(&_slots[i], key, depth + 1, erm)) {
                    unset(i);
                    head._size--;
                    return after_drop<ShrinkThreshold>(head._size);
                }
            }

            return erase_result::nothing;
        }

        template <typename Fn>
        erase_result weed(node_head& head, Fn&& filter, key_t pfx, unsigned depth) {
            bool removed_something = false;

            for (unsigned i = 0; i < Size; i++) {
                if (has(i)) {
                    if (weed_from_slot(head, _idx[i], &_slots[i], filter, depth + 1)) {
                        unset(i);
                        head._size--;
                        removed_something = true;
                    }
                }
            }

            return removed_something ? after_drop<ShrinkThreshold>(head._size) : erase_result::nothing;
        }

        template <typename NT, typename Cloner>
        clone_res clone(const node_head& head, Cloner&& clone, unsigned depth) const noexcept {
            NT* nn;
            try {
                nn = NT::allocate(head._prefix, head._base_layout, head._capacity);
            } catch (...) {
                return clone_res(nullptr, std::current_exception());
            }

            auto ex = copy_slots(head, _slots, Size, depth, nn->_base, [this] (unsigned i) noexcept { return _idx[i]; }, clone);
            return std::make_pair(&nn->_base._head, std::move(ex));
        }

        template <typename NT>
        node_head* grow(node_head& head, node_index_t want_ni) {
            static_assert(growable && GrowThreshold == 0);

            NT* nn = NT::allocate(head._prefix, GrowInto);
            move_slots(_slots, Size, Size + 1, nn->_base, [this] (unsigned i) noexcept { return _idx[i]; });
            head._size = 0;
            return &nn->_base._head;
        }

        template <typename NT>
        node_head* shrink(node_head& head) {
            static_assert(shrinkable && ShrinkThreshold != 0);

            NT* nn = NT::allocate(head._prefix, ShrinkInto);
            move_slots(_slots, Size, ShrinkThreshold, nn->_base, [this] (unsigned i) noexcept { return _idx[i]; });
            head._size = 0;
            return &nn->_base._head;
        }

        lower_bound_res lower_bound(const node_head& head, key_t key, unsigned depth) const noexcept {
            node_index_t ni = node_index(key, depth);
            unsigned i = find_in_array<Size>(ni, _idx);
            if (i < Size) {
                lower_bound_res ret = lower_bound_at(&_slots[i], head, _idx[i], key, depth);
                if (ret.elem != nullptr) {
                    return ret;
                }
            }

            unsigned ui = Size;
            for (unsigned i = 0; i < Size; i++) {
                if (has(i) && _idx[i] > ni && (ui == Size || _idx[i] < _idx[ui])) {
                    ui = i;
                }
            }

            if (ui == Size) {
                return lower_bound_res();
            }

            // See comment in direct_layout about the zero key argument
            return lower_bound_at(&_slots[ui], head, _idx[ui], 0, depth);
        }

        template <typename Visitor>
        bool visit(const node_head& head, Visitor&& v, unsigned depth) const {
            /*
             * Two common-case fast paths that save notable amount
             * of instructions from below.
             */
            if (head._size == 0) {
                return true;
            }

            if (head._size == 1 && has(0)) {
                return visit_slot(v, head, _idx[0], &_slots[0], depth);
            }

            unsigned indices[Size];
            unsigned sz = 0;

            for (unsigned i = 0; i < Size; i++) {
                if (has(i)) {
                    indices[sz++] = i;
                }
            }

            if (v.sorted) {
                std::sort(indices, indices + sz, [this] (int a, int b) {
                    return _idx[a] < _idx[b];
                });
            }

            for (unsigned i = 0; i < sz; i++) {
                unsigned pos = indices[i];
                if (!visit_slot(v, head, _idx[pos], &_slots[pos], depth)) {
                    return false;
                }
            }
            return true;
        }

        static size_t layout_size(uint8_t capacity) noexcept { return sizeof(indirect_layout) + size * sizeof(Slot); }
    };


    template <typename SlotType, typename FN>
    static void move_slots(SlotType* slots, unsigned nr, unsigned thresh, auto& into, FN&& node_index_of) noexcept {
        unsigned count = 0;
        for (unsigned i = 0; i < nr; i++) {
            node_index_t ni = node_index_of(i);
            if (ni != unused_node_index) {
                into.append(ni, std::move(slots[i]));
                slots[i].~SlotType();
                if (++count >= thresh) {
                    break;
                }
            }
        }
    }

    template <typename FN, typename Cloner>
    static std::exception_ptr copy_slots(const node_head& h, const T* slots, unsigned nr, unsigned depth, auto& into, FN&& node_index_of, Cloner&& cloner) noexcept {
        unsigned count = 0;
        for (unsigned i = 0; i < nr; i++) {
            node_index_t ni = node_index_of(i);
            if (ni != unused_node_index) {
                try {
                    into.append(ni, cloner(h.key_of(ni), slots[i]));
                } catch (...) {
                    return std::current_exception();
                }
            }
        }
        return nullptr;
    }

    template <typename FN, typename Cloner>
    static std::exception_ptr copy_slots(const node_head& h, const node_head_ptr* slots, unsigned nr, unsigned depth, auto& into, FN&& node_index_of, Cloner&& cloner) noexcept {
        unsigned count = 0;
        for (unsigned i = 0; i < nr; i++) {
            node_index_t ni = node_index_of(i);
            if (ni != unused_node_index) {
                clone_res res = slots[i]->clone(cloner, depth + 1);
                if (res.first != nullptr) {
                    /*
                     * Append the slot anyway. It may happen that this inner node
                     * has some entries on board, so rather than clearing them
                     * here -- propagate the exception back and let the top-level
                     * code call clear() on everything, including this half-cloned
                     * node.
                     */
                    into.append(ni, std::move(res.first));
                }
                if (res.second) {
                    return res.second;
                }
            }
        }
        return nullptr;
    }

    /*
     * Expand a node that failed prefix check.
     * Turns a node with non-zero prefix on which parent tries to allocate
     * an index beyond its limits. For this:
     * - the inner node is allocated on the level, that's enough to fit
     *   both -- current node and the desired index
     * - the given node is placed into this new inner one at the index it's
     *   expected to be found there (the prefix value)
     * - the allocation continues on this new inner (with fixed depth)
     */
    static node_head* expand(node_head& n, key_t key, unsigned& depth) {
        key_t n_prefix = n._prefix;

        /*
         * The plen is the level at which current node and desired
         * index still coinside
         */
        unsigned plen = common_prefix_len(key, n_prefix);
        assert(plen >= depth);
        plen -= depth;
        depth += plen;
        assert(n.prefix_len() > plen);

        node_index_t ni = node_index(n_prefix, depth);
        node_head* nn = inner_node::allocate_initial(make_prefix(key, plen), ni);
        // Trim all common nodes + nn one from n
        n.trim_prefix(plen + 1);
        nn->set_lower(ni, &n);

        return nn;
    }

    /*
     * Pop one the single lower node and prepare it to replace the
     * current one. This preparation is purely increasing its prefix
     * len, as the prefix value itself is already correct
     */
    static node_head* squash(node_head* n, unsigned depth) noexcept {
        const node_head_ptr np = n->pop_lower();
        node_head* kid = np.raw();
        assert(kid != nullptr);
        // Kid has n and it's prefix squashed
        kid->bump_prefix(n->prefix_len() + 1);
        return kid;
    }

    static bool maybe_drop_from(node_head_ptr* np, erase_result res, unsigned depth) noexcept {
        node_head* n = np->raw();

        switch (res) {
        case erase_result::empty:
            n->free(depth);
            *np = nullptr;
            return true;

        case erase_result::squash:
            if (depth != leaf_depth) {
                *np = squash(n, depth);
                n->free(depth);
            }
            break;
        case erase_result::shrink:
            try {
                *np = n->shrink(depth);
                n->free(depth);
            } catch(...) {
                /*
                 * The node tried to shrink but failed to
                 * allocate memory for the new layout. This
                 * is not that bad, it can survive in current
                 * layout and be shrunk (or squashed or even
                 * dropped) later.
                 */
            }
            break;
        case erase_result::nothing: ; // make compiler happy
        }

        return false;
    }

    static const T* get_at(const T& val, key_t key, unsigned depth) noexcept { return &val; }

    static const T* get_at(const node_head_ptr& np, key_t key, unsigned depth = 0) noexcept {
        if (!np->check_prefix(key, depth)) {
            return nullptr;
        }

        return np->get(key, depth);
    }

    static allocate_res allocate_on(T& val, key_t key, unsigned depth, bool allocated) noexcept {
        return allocate_res(&val, allocated);
    }

    static allocate_res allocate_on(node_head_ptr& n, key_t key, unsigned depth = 0, bool _ = false) {
        if (!n->check_prefix(key, depth)) {
            n = expand(*n, key, depth);
        }

        allocate_res ret = n->alloc(key, depth);
        if (ret.first == nullptr) {
            /*
             * The nullptr ret means the n has run out of
             * free slots. Grow one into bigger layout and
             * try again
             */
            node_head* nn = n->grow(key, depth);
            n->free(depth);
            n = nn;
            ret = nn->alloc(key, depth);
            assert(ret.first != nullptr);
        }
        return ret;
    }

    // Populating value slot happens in tree::emplace
    static void populate_slot(T& val, key_t key, unsigned depth) noexcept { }

    static void populate_slot(node_head_ptr& np, key_t key, unsigned depth) {
        /*
         * Allocate leaf immediatelly with the prefix
         * len big enough to cover all skipped node
         * up to the current depth
         */
        assert(leaf_depth >= depth);
        np = leaf_node::allocate_initial(make_prefix(key, leaf_depth - depth));
    }

    template <typename Visitor>
    static bool visit_slot(Visitor&& v, const node_head& n, node_index_t ni, const T* val, unsigned depth) {
        return v(n.key_of(ni), *val);
    }

    template <typename Visitor>
    static bool visit_slot(Visitor&& v, const node_head& n, node_index_t, const node_head_ptr* ptr, unsigned depth) {
        return (*ptr)->visit(v, depth + 1);
    }

    static lower_bound_res lower_bound_at(const T* val, const node_head& n, node_index_t ni, key_t, unsigned) noexcept {
        return lower_bound_res(val, n, n.key_of(ni));
    }

    static lower_bound_res lower_bound_at(const node_head_ptr* ptr, const node_head&, node_index_t, key_t key, unsigned depth) noexcept {
        return (*ptr)->lower_bound(key, depth + 1);
    }

    template <typename Fn>
    static bool weed_from_slot(node_head& n, node_index_t ni, T* val, Fn&& filter, unsigned depth) {
        if (!filter(n.key_of(ni), *val)) {
            return false;
        }

        val->~T();
        return true;
    }

    template <typename Fn>
    static bool weed_from_slot(node_head&, node_index_t, node_head_ptr* np, Fn&& filter, unsigned depth) {
        return weed_from_slot(np, filter, depth);
    }

    template <typename Fn>
    static bool weed_from_slot(node_head_ptr* np, Fn&& filter, unsigned depth) {
        node_head* n = np->raw();
        depth += n->prefix_len();

        erase_result er = n->weed(filter, depth);

        // FIXME -- after weed the node might want to shrink into
        // even smaller, than just previous, layout
        return maybe_drop_from(np, er, depth);
    }

    static bool erase_from_slot(T* val, key_t key, unsigned depth, erase_mode erm) noexcept {
        if (erm == erase_mode::real) {
            val->~T();
        }

        return true;
    }

    static bool erase_from_slot(node_head_ptr* np, key_t key, unsigned depth, erase_mode erm) noexcept {
        node_head* n = np->raw();
        assert(n->check_prefix(key, depth));

        erase_result er = n->erase(key, depth, erm);
        if (erm == erase_mode::cleanup) {
            return false;
        }

        return maybe_drop_from(np, er, depth);
    }

    template <typename Visitor>
    void visit(Visitor&& v) const {
        if (!_root.is(nil_root)) {
            _root->visit(std::move(v), 0);
        }
    }

    template <typename Visitor>
    void visit(Visitor&& v) {
        struct adaptor {
            Visitor&& v;
            bool sorted;

            bool operator()(key_t key, const T& val) {
                return v(key, const_cast<T&>(val));
            }

            bool operator()(const node_head& n, unsigned depth, bool enter) {
                return v(const_cast<node_head&>(n), depth, enter);
            }
        };

        const_cast<const tree*>(this)->visit(adaptor{std::move(v), v.sorted});
    }

    /*
     * Actual types that define inner and leaf nodes.
     *
     * Leaf nodes are the most numerous inhabitant of the tree and
     * they must be as small as possible for the tree to be memory
     * efficient. Opposite to this, inner nodes are ~1% of the tree,
     * so it's not that important to keep them small.
     *
     * At the same time ...
     *
     * When looking up a value leaf node is the last in a row and
     * makes a single lookup, while there can be several inner ones,
     * on which several lookups will be done.
     *
     * Said that ...
     *
     * Leaf nodes are optimized for size, but not for speed, so they
     * have several layouts to grow (and shrink) strictly on demand.
     *
     * Inner nodes are optimized for speed, and just a little-but for
     * size, so they are of direct dynamic size only.
     */

    class leaf_node {
        template <typename A, typename B> friend class printer;
        friend class tree;
        friend class node_head;
        template <typename A, layout L, unsigned S, layout GL, unsigned GT, layout SL, unsigned ST> friend class indirect_layout;
        template <typename A, layout L, layout GL, unsigned GT, layout SL, unsigned ST> friend class direct_layout;

        using tiny_node = indirect_layout<T, layout::indirect_tiny, 4, layout::indirect_small, 0, layout::nil, 0>;
        using small_node = indirect_layout<T, layout::indirect_small, 8, layout::indirect_medium, 0, layout::indirect_tiny, 4>;
        using medium_node = indirect_layout<T, layout::indirect_medium, 16, layout::indirect_large, 0, layout::indirect_small, 8>;
        using large_node = indirect_layout<T, layout::indirect_large, 32, layout::direct_static, 0, layout::indirect_medium, 16>;
        using direct_node = direct_layout<T, layout::direct_static, layout::nil, 0, layout::indirect_large, 32>;

    public:
        using node_type = node_base<T, tiny_node, small_node, medium_node, large_node, direct_node>;

        leaf_node(leaf_node&& other) noexcept : _base(std::move(other._base)) {}
        ~leaf_node() { }

        size_t storage_size() const noexcept {
            return _base.node_size();
        }

    private:
        node_type _base;

        leaf_node(key_t prefix, layout lt, uint8_t capacity) noexcept : _base(prefix, lt, capacity) { }
        leaf_node(const leaf_node&) = delete;

        static node_head* allocate_initial(key_t prefix) {
            return &allocate(prefix, layout::indirect_tiny)->_base._head;
        }

        static leaf_node* allocate(key_t prefix, layout lt, uint8_t capacity = 0) {
            void* mem = current_allocator().alloc<leaf_node>(node_type::node_size(lt, capacity));
            return new (mem) leaf_node(prefix, lt, capacity);
        }

        static void free(leaf_node& node) noexcept {
            node.~leaf_node();
            current_allocator().free(&node, node._base.node_size());
        }
    };

    class inner_node {
        template <typename A, typename B> friend class printer;
        friend class tree;
        friend class node_head;
        template <typename A, layout L, unsigned S, layout GL, unsigned GT, layout SL, unsigned ST> friend class indirect_layout;
        template <typename A, layout L, layout GL, unsigned GT, layout SL, unsigned ST> friend class direct_layout;

        static constexpr uint8_t initial_capacity = 4;

        using dynamic_node = direct_layout<node_head_ptr, layout::direct_dynamic, layout::direct_dynamic, 0, layout::nil, 0>;

    public:
        using node_type = node_base<node_head_ptr, dynamic_node>;

        inner_node(inner_node&& other) noexcept : _base(std::move(other._base)) {}
        ~inner_node() {}

        size_t storage_size() const noexcept {
            return _base.node_size();
        }

    private:
        node_type _base;

        inner_node(key_t prefix, layout lt, uint8_t capacity) noexcept : _base(prefix, lt, capacity) {}
        inner_node(const inner_node&) = delete;

        static node_head* allocate_initial(key_t prefix, node_index_t want_ni) {
            uint8_t capacity = initial_capacity;
            while (want_ni >= capacity) {
                capacity <<= 1;
            }
            return &allocate(prefix, layout::direct_dynamic, capacity)->_base._head;
        }

        static inner_node* allocate(key_t prefix, layout lt, uint8_t capacity = 0) {
            void* mem = current_allocator().alloc<inner_node>(node_type::node_size(lt, capacity));
            return new (mem) inner_node(prefix, lt, capacity);
        }

        static void free(inner_node& node) noexcept {
            node.~inner_node();
            current_allocator().free(&node, node._base.node_size());
        }

        node_head_ptr pop_lower() noexcept {
            return _base.pop();
        }

        void set_lower(node_index_t ni, node_head* n) noexcept {
            _base.append(ni, node_head_ptr(n));
        }
    };

    node_head_ptr _root;
    static inline node_head nil_root;

public:
    tree() noexcept : _root(&nil_root) {}
    ~tree() {
        clear();
    }

    tree(const tree&) = delete;
    tree(tree&& o) noexcept : _root(std::exchange(o._root, &nil_root)) {}

    const T* get(key_t key) const noexcept {
        return get_at(_root, key);
    }

    T* get(key_t key) noexcept {
        return const_cast<T*>(get_at(_root, key));
    }

    const T* lower_bound(key_t key) const noexcept {
        return _root->lower_bound(key, 0).elem;
    }

    T* lower_bound(key_t key) noexcept {
        return const_cast<T*>(const_cast<const tree*>(this)->lower_bound(key));
    }

    template <typename... Args>
    void emplace(key_t key, Args&&... args) {
        if (_root.is(nil_root)) {
            populate_slot(_root, key, 0);
        }

        allocate_res v = allocate_on(_root, key);
        if (!v.second) {
            v.first->~T();
        }

        try {
            new (v.first) T(std::forward<Args>(args)...);
        } catch (...) {
            erase_from_slot(&_root, key, 0, erase_mode::cleanup);
            throw;
        }
    }

    void erase(key_t key) noexcept {
        if (!_root.is(nil_root)) {
            erase_from_slot(&_root, key, 0, erase_mode::real);
            if (!_root) {
                _root = &nil_root;
            }
        }
    }

    void clear() noexcept {
        struct clearing_visitor {
            bool sorted = false;

            bool operator()(key_t key, T& val) noexcept {
                val.~T();
                return true;
            }
            bool operator()(node_head& n, unsigned depth, bool enter) noexcept {
                if (!enter) {
                    n._size = 0;
                    n.free(depth);
                }
                return true;
            }
        };

        visit(clearing_visitor{});
        _root = &nil_root;
    }

    template <typename Cloner>
    requires std::is_invocable_r<T, Cloner, key_t, const T&>::value
    void clone_from(const tree& tree, Cloner&& cloner) {
        assert(_root.is(nil_root));
        if (!tree._root.is(nil_root)) {
            clone_res cres = tree._root->clone(cloner, 0);
            if (cres.first != nullptr) {
                _root = cres.first;
            }
            if (cres.second) {
                clear();
                std::rethrow_exception(cres.second);
            }
        }
    }

    /*
     * Weed walks the tree and removes the elements for which the
     * fn returns true.
     */

    template <typename Fn>
    requires std::is_invocable_r<bool, Fn, key_t, T&>::value
    void weed(Fn&& filter) {
        if (!_root.is(nil_root)) {
            weed_from_slot(&_root, filter, 0);
            if (!_root) {
                _root = &nil_root;
            }
        }
    }

private:
    template <typename Fn, bool Const>
    struct walking_visitor {
            Fn&& fn;
            bool sorted;

            using value_t = std::conditional_t<Const, const T, T>;
            using node_t = std::conditional_t<Const, const node_head, node_head>;

            bool operator()(key_t key, value_t& val) {
                return fn(key, val);
            }
            bool operator()(node_t& n, unsigned depth, bool enter) noexcept {
                return true;
            }
    };

public:

    /*
     * Walking the tree element-by-element. The called function Fn
     * may return false to stop the walking and return.
     *
     * The \sorted value specifies if the walk should call Fn on
     * keys in ascending order. If it's false keys will be called
     * randomly, because indirect nodes store the slots without
     * sorting
     */
    template <typename Fn>
    requires std::is_invocable_r<bool, Fn, key_t, const T&>::value
    void walk(Fn&& fn, bool sorted = true) const {
        visit(walking_visitor<Fn, true>{std::move(fn), sorted});
    }

    template <typename Fn>
    requires std::is_invocable_r<bool, Fn, key_t, T&>::value
    void walk(Fn&& fn, bool sorted = true) {
        visit(walking_visitor<Fn, false>{std::move(fn), sorted});
    }

    template <bool Const>
    class iterator_base {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::conditional_t<Const, const T, T>;
        using difference_type = ssize_t;
        using pointer = value_type*;
        using reference = value_type&;

    private:
        key_t _key = 0;
        pointer _value = nullptr;
        const tree* _tree = nullptr;
        const node_head* _leaf = nullptr;

    public:
        key_t key() const noexcept { return _key; }

        iterator_base() noexcept = default;
        iterator_base(const tree* t) noexcept : _tree(t) {
            lower_bound_res res = _tree->_root->lower_bound(_key, 0);
            _leaf = res.leaf;
            _value = const_cast<pointer>(res.elem);
            _key = res.key;
        }

        iterator_base& operator++() noexcept {
            if (_value == nullptr) {
                _value = nullptr;
                return *this;
            }

            _key++;
            if (node_index(_key, leaf_depth) != 0) {
                /*
                 * Short-cut. If we're still inside the leaf,
                 * then it's worth trying to shift forward on
                 * it without messing with upper levels
                 */
                lower_bound_res res = _leaf->lower_bound(_key);
                _value = const_cast<pointer>(res.elem);
                if (_value != nullptr) {
                    _key = res.key;
                    return *this;
                }

                /*
                 * No luck. Go ahead and scan the tree from top
                 * again. It's only leaf_depth levels though. Also
                 * not to make the call below visit this leaf one
                 * more time, bump up the index to move out of the
                 * current leaf and keep the leaf's part zero.
                 */

                 _key += node_index_limit;
                 _key &= ~radix_mask;
            }

            lower_bound_res res = _tree->_root->lower_bound(_key, 0);
            _leaf = res.leaf;
            _value = const_cast<pointer>(res.elem);
            _key = res.key;

            return *this;
        }

        iterator_base operator++(int) noexcept {
            iterator_base cur = *this;
            operator++();
            return cur;
        }

        pointer operator->() const noexcept { return _value; }
        reference operator*() const noexcept { return *_value; }

        bool operator==(const iterator_base& o) const noexcept { return _value == o._value; }
        bool operator!=(const iterator_base& o) const noexcept { return !(*this == o); }
    };

    using iterator = iterator_base<false>;
    using const_iterator = iterator_base<true>;

    iterator begin() noexcept { return iterator(this); }
    iterator end() noexcept { return iterator(); }
    const_iterator cbegin() const noexcept { return const_iterator(this); }
    const_iterator cend() const noexcept { return const_iterator(); }
    const_iterator begin() const noexcept { return cbegin(); }
    const_iterator end() const noexcept { return cend(); }

    bool empty() const noexcept { return _root.is(nil_root); }

    template <typename Fn>
    requires std::is_nothrow_invocable_r<size_t, Fn, key_t, const T&>::value
    size_t memory_usage(Fn&& entry_mem_usage) const noexcept {
        struct counting_visitor {
                Fn&& entry_mem_usage;
                bool sorted = false;
                size_t mem = 0;

                bool operator()(key_t key, const T& val) {
                    mem += entry_mem_usage(key, val);
                    return true;
                }
                bool operator()(const node_head& n, unsigned depth, bool enter) noexcept {
                    if (enter) {
                        mem += n.node_size(depth);
                    }
                    return true;
                }
        };

        counting_visitor v{std::move(entry_mem_usage)};
        visit(v);

        return v.mem;
    }

    struct stats {
        struct node_stats {
            unsigned long indirect_tiny = 0;
            unsigned long indirect_small = 0;
            unsigned long indirect_medium = 0;
            unsigned long indirect_large = 0;
            unsigned long direct_static = 0;
            unsigned long direct_dynamic = 0;
        };

        node_stats inners;
        node_stats leaves;
    };

    stats get_stats() const noexcept {
        struct counting_visitor {
            bool sorted = false;
            stats st;

            bool operator()(key_t key, const T& val) noexcept { std::abort(); }

            void update(typename stats::node_stats& ns, const node_head& n) const noexcept {
                switch (n._base_layout) {
                case layout::indirect_tiny: ns.indirect_tiny++; break;
                case layout::indirect_small: ns.indirect_small++; break;
                case layout::indirect_medium: ns.indirect_medium++; break;
                case layout::indirect_large: ns.indirect_large++; break;
                case layout::direct_static: ns.direct_static++; break;
                case layout::direct_dynamic: ns.direct_dynamic++; break;
                default: break;
                }
            }

            bool operator()(const node_head& n, unsigned depth, bool enter) noexcept {
                if (!enter) {
                    return true;
                }

                if (depth == leaf_depth) {
                    update(st.leaves, n);
                    return false; // don't visit elements
                } else {
                    update(st.inners, n);
                    return true;
                }
            }
        };

        counting_visitor v;
        visit(v);
        return v.st;
    }
};

} // namespace
