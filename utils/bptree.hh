/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <boost/intrusive/parent_from_member.hpp>
#include <seastar/util/defer.hh>
#include <cassert>
#include "utils/logalloc.hh"
#include "utils/collection-concepts.hh"
#include "utils/neat-object-id.hh"
#include "utils/array-search.hh"

namespace bplus {

enum class with_debug { no, yes };

/*
 * Linear search in a sorted array of keys slightly beats the
 * binary one on small sizes. For debugging purposes both methods
 * should be used (and the result must coincide).
 */
enum class key_search { linear, binary, both };

/*
 * The less-comparator can be any, but in trivial case when it is
 * literally 'a < b' it may define the conversion of a lookup Key
 * into a 64-bit integer type. Then the intra-node keys scan will
 * use simd instructions.
 */

template <typename Key, typename Less>
concept SimpleLessCompare = requires (Less l, Key k) {
    { l.simplify_key(k) } noexcept -> std::same_as<int64_t>;
};

/*
 * This wrapper prevents the value from being default-constructed
 * when its container is created. The intended usage is to wrap
 * elements of static arrays or containers with .emplace() methods
 * that can live some time without the value in it.
 *
 * Similarly, the value is _not_ automatically destructed when this
 * thing is, so ~Value() must be called by hand. For this there is the
 * .remove() method and two helpers for common cases -- std::move-ing
 * the value into another maybe-location (.emplace(maybe&&)) and
 * constructing the new in place of the existing one (.replace(args...))
 */
template <typename Value, typename Less>
union maybe_key {
    Value v;

    /*
     * When using simple lesser the avx searcher needs the unused keys
     * to be set to minimal value (see comment in array_search_gt() why),
     * so the default constructor and reset() need special implementation
     * for this case
     */

    template <typename L = Less>
    requires (!SimpleLessCompare<Value, L>)
    maybe_key() noexcept {}

    template <typename L = Less>
    requires (!SimpleLessCompare<Value, L>)
    void reset() noexcept { v.~Value(); }

    template <typename L = Less>
    requires (SimpleLessCompare<Value, L>)
    maybe_key() noexcept : v(utils::simple_key_unused_value) {}

    template <typename L = Less>
    requires (SimpleLessCompare<Value, L>)
    void reset() noexcept { v = utils::simple_key_unused_value; }

    ~maybe_key() {}
    maybe_key(const maybe_key&) = delete;
    maybe_key(maybe_key&&) = delete;

    /*
     * Constructs the value inside the empty maybe wrapper.
     */
    template <typename... Args>
    void emplace(Args&&... args) noexcept {
        new (&v) Value (std::forward<Args>(args)...);
    }

    /*
     * The special-case handling of moving some other alive maybe-value.
     * Calls the source destructor after the move.
     */
    void emplace(maybe_key&& other) noexcept {
        new (&v) Value(std::move(other.v));
        other.reset();
    }

    /*
     * Similar to emplace, but to be used on the alive maybe.
     * Calls the destructor on it before constructing the new value.
     */
    template <typename... Args>
    void replace(Args&&... args) noexcept {
        reset();
        emplace(std::forward<Args>(args)...);
    }

    void replace(maybe_key&& other) = delete; // not to be called by chance
};

// For .{do_something_with_data}_and_dispose methods below
template <typename T>
void default_dispose(T* value) noexcept { }

/*
 * Helper to explicitly capture all keys copying.
 * Check test_key for more information.
 */
template <typename Key>
requires std::is_nothrow_copy_constructible_v<Key>
Key copy_key(const Key& other) noexcept {
    return Key(other);
}

/*
 * Consider a small 2-level tree like this
 *
 *        [ . 5 . ]
 *          |   |
 *   +------+   +-----+
 *   |                |
 *   [ 1 . 2 . 3 . ]  [ 5 . 6 . 7 . ]
 *
 * And we remove key 5 from it. First -- the key is removed
 * from the leaf entry
 *
 *        [ . 5 . ]
 *          |   |
 *   +------+   +-----+
 *   |                |
 *   [ 1 . 2 . 3 . ]  [ 6 . 7. ]
 *
 * At this point we have a choice -- whether or not to update
 * the separation key on the parent (root). Strictly speaking,
 * the whole tree is correct now -- all the keys on the right
 * are greater-or-equal than their separation key, though the
 * "equal" never happens.
 *
 * This can be problematic if the keys are stored on data nodes
 * and are referenced from the (non-)leaf nodes. In this case
 * the separation key must be updated to point to some real key
 * in its sub-tree.
 *
 *        [ . 6 . ]  <--- this key updated
 *          |   |
 *   +------+   +-----+
 *   |                |
 *   [ 1 . 2 . 3 . ]  [ 6 . 7. ]
 *
 * As this update takes some time, this behaviour is tunable.
 *
 */
constexpr bool strict_separation_key = true;

/*
 * This is for testing, validator will be everybody's friend
 * to have rights to check if the tree is internally correct.
 */
template <typename Key, typename T, typename Less, size_t NodeSize> class validator;
template <with_debug Debug> class statistics;

template <typename Key, typename T, typename Less, size_t NodeSize, key_search Search, with_debug Debug> class node;
template <typename Key, typename T, typename Less, size_t NodeSize, key_search Search, with_debug Debug> class data;

/*
 * The tree itself.
 * Equipped with O(1) (with little constant) begin() and end()
 * and the iterator, that scans through sorted keys and is not
 * invalidated on insert/remove.
 *
 * The NodeSize parameter describes the amount of keys to be
 * held on each node. Inner nodes will thus have N+1 sub-trees,
 * leaf nodes will have N data pointers.
 */

template <typename T, typename Key>
concept CanGetKeyFromValue = requires (T val) {
    { val.key() } -> std::same_as<Key>;
};

struct stats {
    unsigned long nodes;
    std::vector<unsigned long> nodes_filled;
    unsigned long leaves;
    std::vector<unsigned long> leaves_filled;
    unsigned long datas;
};

template <typename Key, typename T, typename Less, size_t NodeSize,
            key_search Search = key_search::binary, with_debug Debug = with_debug::no>
requires LessNothrowComparable<Key, Key, Less> &&
        std::is_nothrow_move_constructible_v<Key> &&
        std::is_nothrow_move_constructible_v<T>
class tree {
public:
    class iterator;
    class const_iterator;

    friend class validator<Key, T, Less, NodeSize>;
    friend class node<Key, T, Less, NodeSize, Search, Debug>;

    // Sanity not to allow slow key-search in non-debug mode
    static_assert(Debug == with_debug::yes || Search != key_search::both);

    using node = class node<Key, T, Less, NodeSize, Search, Debug>;
    using data = class data<Key, T, Less, NodeSize, Search, Debug>;
    using kid_index = typename node::kid_index;

private:

    node* _root = nullptr;
    node* _left = nullptr;
    node* _right = nullptr;
    [[no_unique_address]] Less _less;

    template <typename K>
    node& find_leaf_for(const K& k) const noexcept {
        node* cur = _root;

        while (!cur->is_leaf()) {
            kid_index i = cur->index_for(k, _less);
            cur = cur->_kids[i].n;
        }

        return *cur;
    }

    void maybe_init_empty_tree() {
        if (_root != nullptr) {
            return;
        }

        node* n = node::create();
        n->_flags |= node::NODE_LEAF | node::NODE_ROOT | node::NODE_RIGHTMOST | node::NODE_LEFTMOST;
        do_set_root(n);
        do_set_left(n);
        do_set_right(n);
    }

    node* left_leaf_slow() const noexcept {
        node* cur = _root;
        while (!cur->is_leaf()) {
            cur = cur->_kids[0].n;
        }
        return cur;
    }

    node* right_leaf_slow() const noexcept {
        node* cur = _root;
        while (!cur->is_leaf()) {
            cur = cur->_kids[cur->_num_keys].n;
        }
        return cur;
    }

    template <typename K>
    requires LessNothrowComparable<K, Key, Less>
    const_iterator get_bound(const K& k, bool upper, bool& match) const noexcept {
        match = false;
        if (empty()) {
            return end();
        }

        node& n = find_leaf_for(k);
        kid_index i = n.index_for(k, _less);

        /*
         * Element at i (key at i - 1) is less or equal to the k,
         * the next element is greater. Mind corner cases.
         */

        if (i == 0) {
            assert(n.is_leftmost());
            return begin();
        } else if (i <= n._num_keys) {
            const_iterator cur = const_iterator(n._kids[i].d, i);
            if (upper || _less(n._keys[i - 1].v, k)) {
                cur++;
            } else {
                match = true;
            }

            return cur;
        } else {
            assert(n.is_rightmost());
            return end();
        }
    }

    template <typename K>
    iterator get_bound(const K& k, bool upper, bool& match) noexcept {
        return iterator(const_cast<const tree*>(this)->get_bound(k, upper, match));
    }

public:

    tree(const tree& other) = delete;
    const tree& operator=(const tree& other) = delete;
    tree& operator=(tree&& other) = delete;

    explicit tree(Less less) noexcept : _less(less) { }
    ~tree() { clear(); }

    Less less() const noexcept { return _less; }

    tree(tree&& other) noexcept : _less(std::move(other._less)) {
        if (other._root) {
            do_set_root(other._root);
            do_set_left(other._left);
            do_set_right(other._right);

            other._root = nullptr;
            other._left = nullptr;
            other._right = nullptr;
        }
    }

    // XXX -- this uses linear scan over the leaf nodes
    size_t size_slow() const noexcept {
        if (_root == nullptr) {
            return 0;
        }

        size_t ret = 0;
        const node* leaf = _left;
        while (1) {
            assert(leaf->is_leaf());
            ret += leaf->_num_keys;
            if (leaf == _right) {
                break;
            }
            leaf = leaf->get_next();
        }

        return ret;
    }

    // Returns result that is equal (both not less than each other)
    template <typename K = Key>
    requires LessNothrowComparable<K, Key, Less>
    const_iterator find(const K& k) const noexcept {
        if (empty()) {
            return end();
        }

        node& n = find_leaf_for(k);
        kid_index i = n.index_for(k, _less);

        if (i >= 1 && !_less(n._keys[i - 1].v, k)) {
            return const_iterator(n._kids[i].d, i);
        } else {
            return end();
        }
    }

    template <typename K = Key>
    requires LessNothrowComparable<K, Key, Less>
    iterator find(const K& k) noexcept {
        return iterator(const_cast<const tree*>(this)->find(k));
    }

    // Returns the least x out of those !less(x, k)
    template <typename K = Key>
    iterator lower_bound(const K& k) noexcept {
        bool match;
        return get_bound(k, false, match);
    }

    template <typename K = Key>
    const_iterator lower_bound(const K& k) const noexcept {
        bool match;
        return get_bound(k, false, match);
    }

    template <typename K = Key>
    iterator lower_bound(const K& k, bool& match) noexcept {
        return get_bound(k, false, match);
    }

    template <typename K = Key>
    const_iterator lower_bound(const K& k, bool& match) const noexcept {
        return get_bound(k, false, match);
    }

    // Returns the least x out of those less(k, x)
    template <typename K = Key>
    iterator upper_bound(const K& k) noexcept {
        bool match;
        return get_bound(k, true, match);
    }

    template <typename K = Key>
    const_iterator upper_bound(const K& k) const noexcept {
        bool match;
        return get_bound(k, true, match);
    }

    /*
     * Constructs the element with key k inside the tree and returns
     * iterator on it. If the key already exists -- just returns the
     * iterator on it and sets the .second to false.
     */
    template <typename... Args>
    std::pair<iterator, bool> emplace(Key k, Args&&... args) {
        maybe_init_empty_tree();

        node& n = find_leaf_for(k);
        kid_index i = n.index_for(k, _less);

        if (i >= 1 && !_less(n._keys[i - 1].v, k)) {
            // Direct hit
            return std::pair(iterator(n._kids[i].d, i), false);
        }

        data* d = data::create(std::forward<Args>(args)...);
        auto x = seastar::defer([&d] { data::destroy(*d, default_dispose<T>); });
        n.insert(i, std::move(k), d, _less);
        assert(d->attached());
        x.cancel();
        return std::pair(iterator(d, i + 1), true);
    }

    template <typename Func>
    requires Disposer<Func, T>
    iterator erase_and_dispose(const Key& k, Func&& disp) noexcept {
        maybe_init_empty_tree();

        node& n = find_leaf_for(k);

        data* d;
        kid_index i = n.index_for(k, _less);

        if (i == 0) {
            return end();
        }

        assert(n._num_keys > 0);

        if (_less(n._keys[i - 1].v, k)) {
            return end();
        }

        d = n._kids[i].d;
        iterator it(d, i);
        it++;

        n.remove(i, _less);

        data::destroy(*d, disp);
        return it;
    }

    template <typename Func>
    requires Disposer<Func, T>
    iterator erase_and_dispose(iterator from, iterator to, Func&& disp) noexcept {
        /*
         * FIXME this is dog slow k*logN algo, need k+logN one
         */
        while (from != to) {
            from = from.erase_and_dispose(disp, _less);
        }

        return to;
    }

    template <typename... Args>
    iterator erase(Args&&... args) noexcept { return erase_and_dispose(std::forward<Args>(args)..., default_dispose<T>); }

    template <typename Func>
    requires Disposer<Func, T>
    void clear_and_dispose(Func&& disp) noexcept {
        if (_root != nullptr) {
            _root->clear(
                [this, &disp] (data* d) noexcept { data::destroy(*d, disp); },
                [this] (node* n) noexcept { node::destroy(*n); }
            );

            node::destroy(*_root);
            _root = nullptr;
            _left = nullptr;
            _right = nullptr;
        }
    }

    void clear() noexcept { clear_and_dispose(default_dispose<T>); }

private:
    void do_set_left(node *n) noexcept {
        assert(n->is_leftmost());
        _left = n;
        n->_kids[0]._leftmost_tree = this;
    }

    void do_set_right(node *n) noexcept {
        assert(n->is_rightmost());
        _right = n;
        n->_rightmost_tree = this;
    }

    void do_set_root(node *n) noexcept {
        assert(n->is_root());
        n->_root_tree = this;
        _root = n;
    }

public:
    /*
     * Iterator. Scans the datas in the sorted-by-key order.
     * Is not invalidated by emplace/erase-s of other elements.
     * Move constructors may turn the _idx invalid, but the
     * .revalidate() method makes it good again.
     */
    template <bool Const>
    class iterator_base {
    protected:
        using tree_ptr = std::conditional_t<Const, const tree*, tree*>;
        using data_ptr = std::conditional_t<Const, const data*, data*>;
        using node_ptr = std::conditional_t<Const, const node*, node*>;

        /*
         * When the iterator gets to the end the _data is
         * replaced with the _tree obtained from the right
         * leaf, and the _idx is set to npos
         */
        union {
            tree_ptr    _tree;
            data_ptr    _data;
        };
        kid_index _idx; // Index in leaf's _kids array pointing to _data

        /*
         * Leaf nodes cannot have kids (data nodes) at 0 position, so
         * 0 is good for unsigned undefined position.
         */
        static constexpr kid_index npos = 0;

        bool is_end() const noexcept { return _idx == npos; }

        explicit iterator_base(tree_ptr t) noexcept : _tree(t), _idx(npos) { }
        iterator_base(data_ptr d, kid_index idx) noexcept : _data(d), _idx(idx) {
            assert(!is_end());
        }
        iterator_base() noexcept : iterator_base(static_cast<tree_ptr>(nullptr)) {}

        /*
         * The routine makes sure the iterator's index is valid
         * and returns back the leaf that points to it.
         */
        node_ptr revalidate() noexcept {
            assert(!is_end());

            node_ptr leaf = _data->_leaf;

            /*
             * The data._leaf pointer is always valid (it's updated
             * on insert/remove operations), the datas do not move
             * as well, so if the leaf still points at us, it is valid.
             */
            if (_idx > leaf->_num_keys || leaf->_kids[_idx].d != _data) {
                _idx = leaf->index_for(_data);
            }

            return leaf;
        }

    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = std::conditional_t<Const, const T, T>;
        using difference_type = ssize_t;
        using pointer = value_type*;
        using reference = value_type&;

        reference operator*() const noexcept { return _data->value; }
        pointer operator->() const noexcept { return &_data->value; }

        iterator_base& operator++() noexcept {
            node_ptr leaf = revalidate();
            if (_idx < leaf->_num_keys) {
                _idx++;
            } else {
                if (leaf->is_rightmost()) {
                    _idx = npos;
                    _tree = leaf->_rightmost_tree;
                    return *this;
                }

                leaf = leaf->get_next();
                _idx = 1;
            }
            _data = leaf->_kids[_idx].d;
            return *this;
        }

        iterator_base& operator--() noexcept {
            if (is_end()) {
                node* n = _tree->_right;
                assert(n->_num_keys > 0);
                _data = n->_kids[n->_num_keys].d;
                _idx = n->_num_keys;
                return *this;
            }

            node_ptr leaf = revalidate();
            if (_idx > 1) {
                _idx--;
            } else {
                leaf = leaf->get_prev();
                _idx = leaf->_num_keys;
            }
            _data = leaf->_kids[_idx].d;
            return *this;
        }

        iterator_base operator++(int) noexcept {
            iterator_base cur = *this;
            operator++();
            return cur;
        }

        iterator_base operator--(int) noexcept {
            iterator_base cur = *this;
            operator--();
            return cur;
        }

        bool operator==(const iterator_base& o) const noexcept { return is_end() ? o.is_end() : _data == o._data; }
        bool operator!=(const iterator_base& o) const noexcept { return !(*this == o); }
    };

    using iterator_base_const = iterator_base<true>;
    using iterator_base_nonconst = iterator_base<false>;

    class const_iterator final : public iterator_base_const {
        friend class tree;
        using super = iterator_base_const;

        explicit const_iterator(const tree* t) noexcept : super(t) {}
        const_iterator(const data* d, kid_index idx) noexcept : super(d, idx) {}

    public:
        const_iterator() noexcept : super() {}
    };

    class iterator final : public iterator_base_nonconst {
        friend class tree;
        using super = iterator_base_nonconst;

        explicit iterator(tree* t) noexcept : super(t) {}
        iterator(data* d, kid_index idx) noexcept : super(d, idx) {}

    public:
        iterator(const const_iterator&& other) noexcept {
            if (other.is_end()) {
                super::_idx = super::npos;
                super::_tree = const_cast<tree *>(other._tree);
            } else {
                super::_idx = other._idx;
                super::_data = const_cast<data *>(other._data);
            }
        }

        iterator() noexcept : super() {}

        /*
         * Special constructor for the case when there's the need for an
         * iterator to the given value poiter. In this case we need to
         * get three things:
         *  - pointer on class data: we assume that the value pointer
         *    is indeed embedded into the data and do the "container_of"
         *    maneuver
         *  - index at which the data is seen on the leaf: use the
         *    standard revalidation. Note, that we start with index 1
         *    which gives us 1/NodeSize chance of hitting the right index
         *    right at once :)
         *  - the tree itself: the worst thing here, creating an iterator
         *    like this is logN operation
         */
        explicit iterator(T* value) noexcept
                : super(boost::intrusive::get_parent_from_member(value, &data::value), 1) {
            super::revalidate();
        }

        /*
         * The key _MUST_ be in order and not exist,
         * neither of those is checked
         */
        template <typename KeyFn, typename... Args>
        iterator emplace_before(KeyFn key, Less less, Args&&... args) {
            node* leaf;
            kid_index i;

            if (!super::is_end()) {
                leaf = super::revalidate();
                i = super::_idx - 1;

                if (i == 0 && !leaf->is_leftmost()) {
                    /*
                     * If we're about to insert a key before the 0th one, then
                     * we must make sure the separation keys from upper layers
                     * will separate the new key as well. If they won't then we
                     * should select the left sibling for insertion.
                     *
                     * For !strict_separation_key the solution is simple -- the
                     * upper level separation keys match the current 0th one, so
                     * we always switch to the left sibling.
                     *
                     * If we're already on the left-most leaf -- just insert, as
                     * there's no separatio key above it.
                     */
                    if (!strict_separation_key) {
                        assert(false && "Not implemented");
                    }
                    leaf = leaf->get_prev();
                    i = leaf->_num_keys;
                }
            } else {
                super::_tree->maybe_init_empty_tree();
                leaf = super::_tree->_right;
                i = leaf->_num_keys;
            }

            assert(i >= 0);

            data* d = data::create(std::forward<Args>(args)...);
            auto x = seastar::defer([&d] { data::destroy(*d, default_dispose<T>); });
            leaf->insert(i, std::move(key(d)), d, less);
            assert(d->attached());
            x.cancel();
            /*
             * XXX -- if the node was not split we can ++ it index
             * and keep iterator valid :)
             */
            return iterator(d, i + 1);
        }

        template <typename... Args>
        iterator emplace_before(Key k, Less less, Args&&... args) {
            return emplace_before([&k] (data*) -> Key { return std::move(k); },
                    less, std::forward<Args>(args)...);
        }

        template <typename... Args>
        requires CanGetKeyFromValue<T, Key>
        iterator emplace_before(Less less, Args&&... args) {
            return emplace_before([] (data* d) -> Key { return d->value.key(); },
                    less, std::forward<Args>(args)...);
        }

    private:
        /*
         * Prepare a likely valid iterator for the next element.
         * Likely means, that unless removal starts rebalancing
         * datas the _idx will be for the correct pointer.
         *
         * This is just like the operator++, with the exception
         * that staying on the leaf doesn't increase the _idx, as
         * in this case the next element will be shifted left to
         * the current position.
         */
        iterator next_after_erase(node* leaf) const noexcept {
            if (super::_idx < leaf->_num_keys) {
                return iterator(leaf->_kids[super::_idx + 1].d, super::_idx);
            }

            if (leaf->is_rightmost()) {
                return iterator(leaf->_rightmost_tree);
            }

            leaf = leaf->get_next();
            return iterator(leaf->_kids[1].d, 1);
        }

    public:
        template <typename Func>
        requires Disposer<Func, T>
        iterator erase_and_dispose(Func&& disp, Less less) noexcept {
            node* leaf = super::revalidate();
            iterator cur = next_after_erase(leaf);

            leaf->remove(super::_idx, less);
            data::destroy(*super::_data, disp);

            return cur;
        }

        iterator erase(Less less) { return erase_and_dispose(default_dispose<T>, less); }

        template <typename... Args>
        requires DynamicObject<T>
        void reconstruct(size_t new_payload_size, Args&&... args) {
            size_t new_size = super::_data->storage_size(new_payload_size);

            node* leaf = super::revalidate();
            auto ptr = current_allocator().alloc<data>(new_size);
            data *dat, *cur = super::_data;

            try {
                dat = new (ptr) data(std::forward<Args>(args)...);
            } catch(...) {
                current_allocator().free(ptr, new_size);
                throw;
            }

            dat->_leaf = leaf;
            cur->_leaf = nullptr;

            super::_data = dat;
            leaf->_kids[super::_idx].d = dat;

            current_allocator().destroy(cur);
        }
    };

    const_iterator begin() const noexcept {
        if (empty()) {
            return end();
        }

        assert(_left->_num_keys > 0);
        // Leaf nodes have data pointers starting from index 1
        return const_iterator(_left->_kids[1].d, 1);
    }
    const_iterator end() const noexcept { return const_iterator(this); }

    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    const_reverse_iterator rbegin() const noexcept { return std::make_reverse_iterator(end()); }
    const_reverse_iterator rend() const noexcept { return std::make_reverse_iterator(begin()); }

    iterator begin() noexcept { return iterator(const_cast<const tree*>(this)->begin()); }
    iterator end() noexcept { return iterator(this); }

    using reverse_iterator = std::reverse_iterator<iterator>;
    reverse_iterator rbegin() noexcept { return std::make_reverse_iterator(end()); }
    reverse_iterator rend() noexcept { return std::make_reverse_iterator(begin()); }

    bool empty() const noexcept { return _root == nullptr || _root->_num_keys == 0; }

    struct stats get_stats() const noexcept {
        struct stats st;

        st.nodes = 0;
        st.leaves = 0;
        st.datas = 0;

        if (_root != nullptr) {
            st.nodes_filled.resize(NodeSize + 1);
            st.leaves_filled.resize(NodeSize + 1);
            _root->fill_stats(st);
        }

        return st;
    }
};

/*
 * Algorithms for searching a key in array.
 *
 * The gt() method accepts sorted array of keys and searches the index of the
 * upper-bound element of the given key.
 */

template <typename K, typename Key, typename Less, size_t Size, key_search Search>
struct searcher { };

template <typename K, typename Key, typename Less, size_t Size>
struct searcher<K, Key, Less, Size, key_search::linear> {
    static size_t gt(const K& k, const maybe_key<Key, Less>* keys, size_t nr, Less less) noexcept {
        size_t i;

        for (i = 0; i < nr; i++) {
            if (less(k, keys[i].v)) {
                break;
            }
        }

        return i;
    };
};

template <typename K, typename Less, size_t Size>
requires SimpleLessCompare<K, Less>
struct searcher<K, int64_t, Less, Size, key_search::linear> {
    static_assert(sizeof(maybe_key<int64_t, Less>) == sizeof(int64_t));
    static size_t gt(const K& k, const maybe_key<int64_t, Less>* keys, size_t nr, Less less) noexcept {
        return utils::array_search_gt(less.simplify_key(k), reinterpret_cast<const int64_t*>(keys), Size, nr);
    }
};

template <typename K, typename Key, typename Less, size_t Size>
struct searcher<K, Key, Less, Size, key_search::binary> {
    static size_t gt(const K& k, const maybe_key<Key, Less>* keys, size_t nr, Less less) noexcept {
        ssize_t s = 0, e = nr - 1; // signed for below s <= e corner cases

        while (s <= e) {
            size_t i = (s + e) / 2;
            if (less(k, keys[i].v)) {
                e = i - 1;
            } else {
                s = i + 1;
            }
        }

        return s;
    }
};

template <typename K, typename Key, typename Less, size_t Size>
struct searcher<K, Key, Less, Size, key_search::both> {
    static size_t gt(const K& k, const maybe_key<Key, Less>* keys, size_t nr, Less less) noexcept {
        size_t rl = searcher<K, Key, Less, Size, key_search::linear>::gt(k, keys, nr, less);
        size_t rb = searcher<K, Key, Less, Size, key_search::binary>::gt(k, keys, nr, less);
        assert(rl == rb);
        assert(rl <= nr);
        return rl;
    }
};

/*
 * A node describes both, inner and leaf nodes.
 */
template <typename Key, typename T, typename Less, size_t NodeSize, key_search Search, with_debug Debug>
class node final {
    friend class validator<Key, T, Less, NodeSize>;
    friend class tree<Key, T, Less, NodeSize, Search, Debug>;
    friend class data<Key, T, Less, NodeSize, Search, Debug>;

    using tree = class tree<Key, T, Less, NodeSize, Search, Debug>;
    using data = class data<Key, T, Less, NodeSize, Search, Debug>;

    class prealloc;

    /*
     * The NodeHalf is the level at which the node is considered
     * to be underflown and should be re-filled. This slightly
     * differs for even and odd sizes.
     *
     * For odd sizes the node will stand until it contains literally
     * more than 1/2 of it's size (e.g. for size 5 keeping 3 keys
     * is OK). For even cases this barrier is less than the actual
     * half (e.g. for size 4 keeping 2 is still OK).
     */
    static constexpr size_t NodeHalf = ((NodeSize - 1) / 2);
    static_assert(NodeHalf >= 1);

    union node_or_data_or_tree {
        node* n;
        data* d;

        tree* _leftmost_tree; // See comment near node::__next about this
    };

    using node_or_data = node_or_data_or_tree;

    [[no_unique_address]] utils::neat_id<Debug == with_debug::yes> id;

    unsigned short _num_keys;
    unsigned short _flags;

    static const unsigned short NODE_ROOT       = 0x1;
    static const unsigned short NODE_LEAF       = 0x2;
    static const unsigned short NODE_LEFTMOST   = 0x4; // leaf with smallest keys in the tree
    static const unsigned short NODE_RIGHTMOST  = 0x8; // leaf with greatest keys in the tree

    bool is_leaf() const noexcept { return _flags & NODE_LEAF; }
    bool is_root() const noexcept { return _flags & NODE_ROOT; }
    bool is_rightmost() const noexcept { return _flags & NODE_RIGHTMOST; }
    bool is_leftmost() const noexcept { return _flags & NODE_LEFTMOST; }

    /*
     * separation keys
     *   non-leaf nodes:
     *     keys in kids[i] < keys[i] <= keys in kids[i+1], i in [0, NodeSize)
     *   leaf nodes:
     *     kids[i + 1] is the data for keys[i]
     *     kids[0] is unused
     *
     * In the examples below the leaf nodes will be shown like
     *
     *  keys: [012]
     * datas: [-012]
     *
     * and the non-leaf ones like
     *
     *  keys: [012]
     *  kids: [A012]
     *
     * to have digits correspond to different elements and staying
     * in its correct positions. And the A kid is this left-most one
     * at index 0 for the non-leaf node.
     */

    maybe_key<Key, Less> _keys[NodeSize];
    node_or_data _kids[NodeSize + 1];

    // Type-aliases for code-reading convenience
    using key_index = size_t;
    using kid_index = size_t;

    /*
     * The root node uses this to point to the tree object. This is
     * needed to update tree->_root on node move.
     */
    union {
        node* _parent;
        tree* _root_tree;
    };

    /*
     * Leaf nodes are linked in a list, since leaf nodes do
     * not use the _kids[0] pointer we re-use it. Respectively,
     * non-leaf nodes don't use the __next one.
     *
     * Also, leftmost and rightmost respectively have prev and
     * next pointing to the tree object itsef. This is done for
     * _left/_right update on node move.
     */
    union {
        node* __next;
        tree* _rightmost_tree;
    };

    node* get_next() const noexcept {
        assert(is_leaf());
        return __next;
    }

    void set_next(node *n) noexcept {
        assert(is_leaf());
        __next = n;
    }

    node* get_prev() const noexcept {
        assert(is_leaf());
        return _kids[0].n;
    }

    void set_prev(node* n) noexcept {
        assert(is_leaf());
        _kids[0].n = n;
    }

    // Links the new node n right after the current one
    void link(node& n) noexcept {
        if (is_rightmost()) {
            _flags &= ~NODE_RIGHTMOST;
            n._flags |= node::NODE_RIGHTMOST;
            tree* t = _rightmost_tree;
            assert(t->_right == this);
            t->do_set_right(&n);
        } else {
            n.set_next(get_next());
            get_next()->set_prev(&n);
        }

        n.set_prev(this);
        set_next(&n);
    }

    void unlink() noexcept {
        node* x;
        tree* t;

        switch (_flags & (node::NODE_LEFTMOST | node::NODE_RIGHTMOST)) {
        case node::NODE_LEFTMOST:
            x = get_next();
            _flags &= ~node::NODE_LEFTMOST;
            x->_flags |= node::NODE_LEFTMOST;
            t = _kids[0]._leftmost_tree;
            assert(t->_left == this);
            t->do_set_left(x);
            break;
        case node::NODE_RIGHTMOST:
            x = get_prev();
            _flags &= ~node::NODE_RIGHTMOST;
            x->_flags |= node::NODE_RIGHTMOST;
            t = _rightmost_tree;
            assert(t->_right == this);
            t->do_set_right(x);
            break;
        case 0:
            get_prev()->set_next(get_next());
            get_next()->set_prev(get_prev());
            break;
        default:
            /*
             * Right- and left-most at the same time can only be root,
             * otherwise this would mean we have root with 0 keys.
             */
            assert(false);
        }

        set_next(this);
        set_prev(this);
    }

    node(const node& other) = delete;
    const node& operator=(const node& other) = delete;
    node& operator=(node&& other) = delete;

    /*
     * There's no pointer/reference from nodes to the tree, neither
     * there is such from data, because otherwise we'd have to update
     * all of them inside tree move constructor, which in turn would
     * make it toooo slow linear operation. Thus we walk up the nodes
     * ._parent chain up to the root node which has the _root_tree.
     */
    tree* tree_slow() const noexcept {
        const node* cur = this;

        while (!cur->is_root()) {
            cur = cur->_parent;
        }

        return cur->_root_tree;
    }

    /*
     * For inner node finds the subtree to which k belongs.
     * For leaf node finds the data that should correspond to the key,
     * in this case index is not 0 for sure.
     *
     * In both cases keys[index - 1] <= k < keys[index].
     */
    template <typename K>
    kid_index index_for(const K& k, Less less) const noexcept {
        return searcher<K, Key, Less, NodeSize, Search>::gt(k, _keys, _num_keys, less);
    }

    kid_index index_for(node *n) const noexcept {
        // Keep index on kid (FIXME?)

        kid_index i;

        for (i = 0; i <= _num_keys; i++) {
            if (_kids[i].n == n) {
                break;
            }
        }
        assert(i <= _num_keys);
        return i;
    }

    bool need_refill() const noexcept {
        return _num_keys <= NodeHalf;
    }

    bool can_grab_from() const noexcept {
        return _num_keys > NodeHalf + 1;
    }

    bool can_push_to() const noexcept {
        return _num_keys < NodeSize;
    }

    bool can_merge_with(const node& n) const noexcept {
        return _num_keys + n._num_keys + (is_leaf() ? 0u : 1u) <= NodeSize;
    }

    void shift_right(size_t s) noexcept {
        for (size_t i = _num_keys; i > s; i--) {
            _keys[i].emplace(std::move(_keys[i - 1]));
            _kids[i + 1] = _kids[i];
        }
        _num_keys++;
    }

    void shift_left(size_t s) noexcept {
        // The key at s is expected to be .remove()-d !
        for (size_t i = s + 1; i < _num_keys; i++) {
            _keys[i - 1].emplace(std::move(_keys[i]));
            _kids[i] = _kids[i + 1];
        }
        _num_keys--;
    }

    void move_keys_and_kids(size_t foff, node& to, size_t count) noexcept {
        size_t toff = to._num_keys;

        for (size_t i = 0; i < count; i++) {
            to._keys[toff + i].emplace(std::move(_keys[foff + i]));
            to._kids[toff + i + 1] = _kids[foff + i + 1];
        }
        _num_keys = foff;

        if (is_leaf()) {
            for (size_t i = toff; i < toff + count; i++) {
                to._kids[i + 1].d->reattach(&to);
            }
        } else {
            for (size_t i = toff; i < toff + count; i++) {
                to._kids[i + 1].n->_parent = &to;
            }
        }
        to._num_keys += count;
    }

    void move_to(node& to, size_t off) noexcept {
        assert(off <= _num_keys);
        to._num_keys = 0;
        move_keys_and_kids(off, to, _num_keys - off);
    }

    void grab_from_left(node& from, maybe_key<Key, Less>& sep) noexcept {
        /*
         * Grab one element from the left sibling and return
         * the new separation key for them.
         *
         * Leaf: just move the last key (and the last kid) and report
         * it as new separation key
         *
         *  keys: [012]  -> [56]  = [01]  [256]  2 is new separation
         * datas: [-012] -> [-56] = [-01] [-256]
         *
         * Non-leaf is trickier. We need the current separation key
         * as we're grabbing the last element which has no the right
         * boundary on the node. So the parent node tells us one.
         *
         *  keys: [012]  -> s [56]  = [01]  2 [s56] 2 is new separation
         *  kids: [A012] ->   [B56] = [A01]   [2B56]
         */

        assert(from._num_keys > 0);
        key_index i = from._num_keys - 1;

        shift_right(0);
        from._num_keys--;

        if (is_leaf()) {
            _keys[0].emplace(std::move(from._keys[i]));
            _kids[1] = from._kids[i + 1];
            _kids[1].d->reattach(this);
            sep.replace(copy_key(_keys[0].v));
        } else {
            _keys[0].emplace(std::move(sep));
            _kids[1] = _kids[0];
            _kids[0] = from._kids[i + 1];
            _kids[0].n->_parent = this;
            sep.emplace(std::move(from._keys[i]));
        }
    }

    void merge_into(node& t, Key key) noexcept {
        /*
         * Merge current node into t preparing it for being
         * killed. This merge is slightly different for leaves
         * and for non-leaves wrt the 0th element.
         *
         * Non-leaves. For those we need the separation key, whic
         * is passed to us. The caller "knows" that this and t are
         * two siblings and thus the separation key is the one from
         * the parent node. For this reason merging two non-leaf
         * nodes needs one more slot in the target as compared to
         * the leaf-nodes case.
         *
         *   keys: [012]  + K + [456]  = [012K456]
         *   kids: [A012] +     [B456] = [A012B456]
         *
         * Leaves. This is simple -- just go ahead and merge.
         *
         *   keys: [012]  + [456]  = [012456]
         *  datas: [-012] + [-456] = [-012456]
         */

        if (!t.is_leaf()) {
            key_index i = t._num_keys;
            t._keys[i].emplace(std::move(key));
            t._kids[i + 1] = _kids[0];
            t._kids[i + 1].n->_parent = &t;
            t._num_keys++;
        }

        move_keys_and_kids(0, t, _num_keys);
    }

    void grab_from_right(node& from, maybe_key<Key, Less>& sep) noexcept {
        /*
         * Grab one element from the right sibling and return
         * the new separation key for them.
         *
         * Leaf: just move the 0th key (and 1st kid) and the
         * new separation key is what becomes 0 in the source.
         *
         *  keys: [01]  <- [456]  = [014]  [56]  5 is new separation
         * datas: [-01] <- [-456] = [-014] [-56]
         *
         * Non-leaf is trickier. We need the current separation
         * key as we're grabbing the kids[0] element which has no
         * corresponding keys[-1]. So the parent node tells us one.
         *
         *  keys: [01]  <- s [456]  = [01s]  4 [56] 4 is new separation
         *  kids: [A01] <-   [B456] = [A01B]   [456]
         */

        key_index i = _num_keys;

        if (is_leaf()) {
            _keys[i].emplace(std::move(from._keys[0]));
            _kids[i + 1] = from._kids[1];
            _kids[i + 1].d->reattach(this);
            sep.replace(copy_key(from._keys[1].v));
        } else {
            _kids[i + 1] = from._kids[0];
            _kids[i + 1].n->_parent = this;
            _keys[i].emplace(std::move(sep));
            from._kids[0] = from._kids[1];
            sep.emplace(std::move(from._keys[0]));
        }

        _num_keys++;
        from.shift_left(0);
    }

    /*
     * When splitting, the result should be almost equal. The
     * "almost" depends on the node-size being odd or even and
     * on the node itself being leaf or inner.
     */
    bool equally_split(const node& n2) const noexcept {
        if (Debug == with_debug::yes) {
            return (_num_keys == n2._num_keys) ||
                    (_num_keys == n2._num_keys + 1) ||
                    (_num_keys + 1 == n2._num_keys);
        }
        return true;
    }

    // Helper for assert(). See comment for do_insert for details.
    bool left_kid_sorted(const Key& k, Less less) const noexcept {
        if (Debug == with_debug::yes && !is_leaf() && _num_keys > 0) {
            node* x = _kids[0].n;
            if (x != nullptr && less(k, x->_keys[x->_num_keys - 1].v)) {
                return false;
            }
        }

        return true;
    }

    template <typename DFunc, typename NFunc>
    requires Disposer<DFunc, data> && Disposer<NFunc, node>
    void clear(DFunc&& ddisp, NFunc&& ndisp) noexcept {
        if (is_leaf()) {
            _flags &= ~(node::NODE_LEFTMOST | node::NODE_RIGHTMOST);
            set_next(this);
            set_prev(this);
        } else {
            node* n = _kids[0].n;
            n->clear(ddisp, ndisp);
            ndisp(n);
        }

        for (key_index i = 0; i < _num_keys; i++) {
            _keys[i].reset();
            if (is_leaf()) {
                ddisp(_kids[i + 1].d);
            } else {
                node* n = _kids[i + 1].n;
                n->clear(ddisp, ndisp);
                ndisp(n);
            }
        }

        _num_keys = 0;
    }

    static node* create() {
        return current_allocator().construct<node>();
    }

    static void destroy(node& n) noexcept {
        current_allocator().destroy(&n);
    }

    void drop() noexcept {
        assert(!is_root());
        if (is_leaf()) {
            unlink();
        }
        destroy(*this);
    }

    void insert_into_full(kid_index idx, Key k, node_or_data nd, Less less, prealloc& nodes) noexcept {
        if (!is_root()) {
            node& p = *_parent;
            kid_index i = p.index_for(_keys[0].v, less);

            /*
             * Try to push left or right existing keys to the respective
             * siblings. Keep in mind two corner cases:
             *
             * 1. Push to left. In this case the new key should not go
             * to the [0] element, otherwise we'd have to update the p's
             * separation key one more time.
             *
             * 2. Push to right. In this case we must make sure the new
             * key is not the rightmost itself, otherwise it's _him_ who
             * must be pushed there.
             *
             * Both corner cases are possible to implement though.
             */
            if (idx > 1 && i > 0) {
                node* left = p._kids[i - 1].n;
                if (left->can_push_to()) {
                    /*
                     * We've moved the 0th elemet from this, so the index
                     * for the new key shifts too
                     */
                    idx--;
                    left->grab_from_right(*this, p._keys[i - 1]);
                }
            }

            if (idx < _num_keys && i < p._num_keys) {
                node* right = p._kids[i + 1].n;
                if (right->can_push_to()) {
                    right->grab_from_left(*this, p._keys[i]);
                }
            }

            if (_num_keys < NodeSize) {
                do_insert(idx, std::move(k), nd, less);
                nodes.drain();
                return;
            }

            /*
             * We can only get here if both ->can_push_to() checks above
             * had failed. In this case -- go ahead and split this.
             */
        }

        split_and_insert(idx, std::move(k), nd, less, nodes);
    }

    void split_and_insert(kid_index idx, Key k, node_or_data nd, Less less, prealloc& nodes) noexcept {
        assert(_num_keys == NodeSize);

        node* nn = nodes.pop();
        maybe_key<Key, Less> sep;

        /*
         * Insertion with split.
         * 1. Existing node (this) is split into two. We try a bit harder
         *    than we might to to make the split equal.
         * 2. The new element is added to either of the resulting nodes.
         * 3. The new node nn is inserted into parent one with the help
         *    of a separation key sep
         *
         * First -- find the position in the current node at which the
         * new element should have appeared.
         */

        size_t off = NodeHalf + (idx > NodeHalf ? 1 : 0);

        if (is_leaf()) {
            nn->_flags |= NODE_LEAF;
            link(*nn);

            /*
             * Split of leaves. This is simple -- just copy the needed
             * amount of keys and kids from this to nn, then insert the
             * new pair into the proper place. When inserting the new
             * node into parent the separation key is the one latter
             * starts with.
             *
             *  keys: [01234]
             * datas: [-01234]
             *
             * if the new key is below 2, then
             *  keys: -> [01]  [234]   -> [0n1]  [234]   -> sep is 2
             * datas: -> [-01] [-234]  -> [-0n1] [-234]
             *
             * if the new key is above 2, then
             *  keys: -> [012]  [34]   -> [012]  [3n4]   -> sep is 3 (or n)
             * datas: -> [-012] [-34]  -> [-012] [-3n4]
             */
            move_to(*nn, off);

            if (idx <= NodeHalf) {
                do_insert(idx, std::move(k), nd, less);
            } else {
                nn->do_insert(idx - off, std::move(k), nd, less);
            }
            sep.emplace(std::move(copy_key(nn->_keys[0].v)));
        } else {
            /*
             * Node insertion has one special case -- when the new key
             * gets directly into the middle.
             */
            if (idx == NodeHalf + 1) {
                /*
                 * Split of nodes and the new key is in the middle. In this
                 * we need to split the node into two, but take the k as the
                 * separation kep. The corresponding data becomes new node's
                 * 0 kid.
                 *
                 *  keys: [012345]  -> [012] k [345]   (and the k goes up)
                 *  kids: [A012345] -> [A012]  [n345]
                 */
                move_to(*nn, off);
                sep.emplace(std::move(k));
                nn->_kids[0] = nd;
                nn->_kids[0].n->_parent = nn;
            } else {
                /*
                 * Split of nodes and the new key gets into either of the
                 * halves. This is like leaves split, but we need to carefully
                 * handle the kids[0] for both. The correspoding key is not
                 * on the node and "has" an index of -1 and thus becomes the
                 * separation one for the upper layer.
                 *
                 *  keys: [012345]
                 * datas: [A012345]
                 *
                 * if the new key goes left then
                 *  keys: -> [01] 2 [345]   -> [0n1]  2 [345]
                 * datas: -> [A01]  [2345]  -> [A0n1]   [2345]
                 *
                 * if the new key goes right then
                 *  keys: -> [012]  3 [45]   -> [012]  3 [4n5]
                 * datas: -> [A012]   [345]  -> [-123]   [34n5]
                 */
                move_to(*nn, off + 1);
                sep.emplace(std::move(_keys[off]));
                nn->_kids[0] = _kids[off + 1];
                nn->_kids[0].n->_parent = nn;
                _num_keys--;

                if (idx <= NodeHalf) {
                    do_insert(idx, std::move(k), nd, less);
                } else {
                    nd.n->_parent = nn;
                    nn->do_insert(idx - off - 1, std::move(k), nd, less);
                }
            }
        }

        assert(equally_split(*nn));

        if (is_root()) {
            insert_into_root(*nn, std::move(sep.v), nodes);
        } else {
            insert_into_parent(*nn, std::move(sep.v), less, nodes);
        }
        sep.reset();
    }

    void do_insert(kid_index i, Key k, node_or_data nd, Less less) noexcept {
        assert(_num_keys < NodeSize);

        /*
         * The new k:nd pair should be put into the given index and
         * shift offenders to the right. However, if it should be
         * put left to the non-leaf's left-most node -- it's a BUG,
         * as there's no corresponding key here.
         *
         * Non-leaf nodes get here when their kids are split, and
         * when they do, if the kid gets into the left-most sub-tree,
         * it's directly put there, and this helper is not called.
         * Said that, if we're inserting a new pair, the newbie can
         * only get to the right of the left-most kid.
         */
        assert(i != 0 || left_kid_sorted(k, less));

        shift_right(i);

        /*
         * The k:nd pair belongs to keys[i-1]:kids[i] subtree, and since
         * what's already there is less than this newcomer, the latter goes
         * one step right.
         */
        _keys[i].emplace(std::move(k));
        _kids[i + 1] = nd;
        if (is_leaf()) {
            nd.d->attach(*this);
        }
    }

    void insert_into_parent(node& nn, Key sep, Less less, prealloc& nodes) noexcept {
        nn._parent = _parent;
        _parent->insert_key(std::move(sep), node_or_data{.n = &nn}, less, nodes);
    }

    void insert_into_root(node& nn, Key sep, prealloc& nodes) noexcept {
        tree* t = _root_tree;

        node* nr = nodes.pop();

        nr->_num_keys = 1;
        nr->_keys[0].emplace(std::move(sep));
        nr->_kids[0].n = this;
        nr->_kids[1].n = &nn;
        _flags &= ~node::NODE_ROOT;
        _parent = nr;
        nn._parent = nr;

        nr->_flags |= node::NODE_ROOT;
        t->do_set_root(nr);
    }

    void insert_key(Key k, node_or_data nd, Less less, prealloc& nodes) noexcept {
        kid_index i = index_for(k, less);
        insert(i, std::move(k), nd, less, nodes);
    }

    void insert(kid_index i, Key k, node_or_data nd, Less less, prealloc& nodes) noexcept {
        if (_num_keys == NodeSize) {
            insert_into_full(i, std::move(k), nd, less, nodes);
        } else {
            do_insert(i, std::move(k), nd, less);
        }
    }

    void insert(kid_index i, Key k, data* d, Less less) {
        prealloc nodes;

        /*
         * Prepare the nodes for split in advaice, if the node::create will
         * start throwing while splitting we'll have troubles "unsplitting"
         * the nodes back.
         */
        node* cur = this;
        while (cur->_num_keys == NodeSize) {
            nodes.push();
            if (cur->is_root()) {
                nodes.push();
                break;
            }
            cur = cur->_parent;
        }

        insert(i, std::move(k), node_or_data{.d = d}, less, nodes);
        assert(nodes.empty());
    }

    void remove_from(key_index i, Less less) noexcept {
        _keys[i].reset();
        shift_left(i);

        if (!is_root()) {
            if (need_refill()) {
                refill(less);
            }
        } else if (_num_keys == 0 && !is_leaf()) {
            node* nr;
            nr = _kids[0].n;
            nr->_flags |= node::NODE_ROOT;
            _root_tree->do_set_root(nr);

            _flags &= ~node::NODE_ROOT;
            _parent = nullptr;
            drop();
        }
    }

    void merge_kids(node& t, node& n, key_index sep_idx, Less less) noexcept {
        n.merge_into(t, std::move(_keys[sep_idx].v));
        n.drop();
        remove_from(sep_idx, less);
    }

    void refill(Less less) noexcept {
        node& p = *_parent, *left, *right;

        /*
         * We need to locate this node's index at parent array by using
         * the 0th key, so make sure it exists. We can go even without
         * it, but since we don't let's be on the safe side.
         */
        assert(_num_keys > 0);
        kid_index i = p.index_for(_keys[0].v, less);
        assert(p._kids[i].n == this);

        /*
         * The node is "underflown" (see comment near NodeHalf
         * about what this means), so we try to refill it at the
         * siblings' expense. Many cases possible, but we go with
         * only four:
         *
         * 1. Left sibling exists and it has at least 1 item
         *    above being the half-full. -> we grab one element
         *    from it.
         *
         * 2. Left sibling exists and we can merge current with
         * it. "Can" means the resulting node will not overflow
         * which, in turn, differs by one for leaf and non-leaf
         * nodes. For leaves the merge is possible is the total
         * number of the elements fits the maximum. For non-leaf
         * we'll need room for one more element, here's why:
         *
         *  [012]  + [456]   ->  [012X456]
         *  [A012] + [B456]  ->  [A012B456]
         *
         * The key X in the middle separates B from everything on
         * the left and this key was not sitting on either of the
         * wannabe merging nodes. This X is the current separation
         * of these two nodes taken from their parent.
         *
         * And two same cases for the right sibling.
         */

        left = i > 0 ? p._kids[i - 1].n : nullptr;
        right = i < p._num_keys ? p._kids[i + 1].n : nullptr;

        if (left != nullptr && left->can_grab_from()) {
            grab_from_left(*left, p._keys[i - 1]);
            return;
        }

        if (right != nullptr && right->can_grab_from()) {
            grab_from_right(*right, p._keys[i]);
            return;
        }

        if (left != nullptr && can_merge_with(*left)) {
            p.merge_kids(*left, *this, i - 1, less);
            return;
        }

        if (right != nullptr && can_merge_with(*right)) {
            p.merge_kids(*this, *right, i, less);
            return;
        }

        /*
         * Susprisingly, the node in the B+ tree can violate the
         * "minimally filled" rule for non roots. It _can_ stay with
         * less than half elements on board. The next remove from
         * it or either of its siblings will probably refill it.
         *
         * Keeping 1 key on the non-root node is possible, but needs
         * some special care -- if we will remove this last key from
         * this node, the code will try to refill one and will not
         * be able to find this node's index at parent (the call for
         * index_for() above).
         */
        assert(_num_keys > 1);
    }

    void remove(kid_index ki, Less less) noexcept {
        key_index i = ki - 1;

        /*
         * Update the matching separation key from above. It
         * exists only if we're removing the 0th key, but for
         * the left-most child it doesn't exist.
         *
         * Note, that the latter check is crucial for clear()
         * performance, as it's always removes the left-most
         * key, without this check each remove() would walk the
         * tree upwards in vain.
         */
        if (strict_separation_key && i == 0 && !is_leftmost()) {
            const Key& k = _keys[i].v;
            node* p = this;

            while (!p->is_root()) {
                p = p->_parent;
                kid_index j = p->index_for(k, less);
                if (j > 0) {
                    p->_keys[j - 1].replace(copy_key(_keys[1].v));
                    break;
                }
            }
        }

        remove_from(i, less);
    }

public:
    explicit node() noexcept : _num_keys(0) , _flags(0) , _parent(nullptr) { }

    ~node() {
        assert(_num_keys == 0);
        assert(is_root() || !is_leaf() || (get_prev() == this && get_next() == this));
    }

    node(node&& other) noexcept : _flags(other._flags) {
        if (is_leaf()) {
            if (!is_rightmost()) {
                set_next(other.get_next());
                get_next()->set_prev(this);
            } else {
                other._rightmost_tree->do_set_right(this);
            }

            if (!is_leftmost()) {
                set_prev(other.get_prev());
                get_prev()->set_next(this);
            } else {
                other._kids[0]._leftmost_tree->do_set_left(this);
            }

            other._flags &= ~(NODE_LEFTMOST | NODE_RIGHTMOST);
            other.set_next(&other);
            other.set_prev(&other);
        } else {
            _kids[0].n = other._kids[0].n;
            _kids[0].n->_parent = this;
        }

        other.move_to(*this, 0);

        if (!is_root()) {
            _parent = other._parent;
            kid_index i = _parent->index_for(&other);
            assert(_parent->_kids[i].n == &other);
            _parent->_kids[i].n = this;
        } else {
            other._root_tree->do_set_root(this);
        }
    }

    kid_index index_for(const data *d) const noexcept {
        /*
         * We'd could look up the data's new idex with binary search,
         * but we don't have the key at hands
         */

        kid_index i;

        for (i = 1; i <= _num_keys; i++) {
            if (_kids[i].d == d) {
                break;
            }
        }
        assert(i <= _num_keys);
        return i;
    }

private:
    class prealloc {
        std::vector<node*> _nodes;
    public:
        bool empty() noexcept { return _nodes.empty(); }

        void push() {
            _nodes.push_back(node::create());
        }

        node* pop() noexcept {
            assert(!_nodes.empty());
            node* ret = _nodes.back();
            _nodes.pop_back();
            return ret;
        }

        void drain() noexcept {
            while (!empty()) {
                node::destroy(*pop());
            }
        }

        ~prealloc() {
            drain();
        }
    };

    void fill_stats(struct stats& st) const noexcept {
        if (is_leaf()) {
            st.leaves_filled[_num_keys]++;
            st.leaves++;
            st.datas += _num_keys;
        } else {
            st.nodes_filled[_num_keys]++;
            st.nodes++;
            for (kid_index i = 0; i <= _num_keys; i++) {
                _kids[i].n->fill_stats(st);
            }
        }
    }
};

/*
 * The data represents data node (the actual data is stored "outside"
 * of the tree). The tree::emplace() constructs the payload inside the
 * data before inserting it into the tree.
 */
template <typename K, typename T, typename Less, size_t NS, key_search S, with_debug D>
class data final {
    friend class validator<K, T, Less, NS>;
    template <typename c1, typename c2, typename c3, size_t s1, key_search p1, with_debug p2>
            friend class tree<c1, c2, c3, s1, p1, p2>::iterator;
    template <typename c1, typename c2, typename c3, size_t s1, key_search p1, with_debug p2>
            friend class tree<c1, c2, c3, s1, p1, p2>::iterator_base_const;
    template <typename c1, typename c2, typename c3, size_t s1, key_search p1, with_debug p2>
            friend class tree<c1, c2, c3, s1, p1, p2>::iterator_base_nonconst;

    using node = class node<K, T, Less, NS, S, D>;

    node* _leaf;
    T value;

public:
    template <typename... Args>
    static data* create(Args&&... args) {
        return current_allocator().construct<data>(std::forward<Args>(args)...);
    }

    template <typename Func>
    requires Disposer<Func, T>
    static void destroy(data& d, Func&& disp) noexcept {
        disp(&d.value);
        d._leaf = nullptr;
        current_allocator().destroy(&d);
    }

    template <typename... Args>
    data(Args&& ... args) : _leaf(nullptr), value(std::forward<Args>(args)...) {}

    data(data&& other) noexcept : _leaf(other._leaf), value(std::move(other.value)) {
        if (attached()) {
            auto i = _leaf->index_for(&other);
            _leaf->_kids[i].d = this;
            other._leaf = nullptr;
        }
    }

    ~data() { assert(!attached()); }

    bool attached() const noexcept { return _leaf != nullptr; }

    void attach(node& to) noexcept {
        assert(!attached());
        _leaf = &to;
    }

    void reattach(node* to) noexcept {
        assert(attached());
        _leaf = to;
    }

private:
    // Data node may describe a T without fixed size, e.g. an array that grows on
    // demand. So this helper returns the size of the memory chunk that's required
    // to carry the node with T of the payload size on board.
    //
    // The tree::iterator::reconstruct does this growing (or shrinking).
    size_t storage_size(size_t payload) const noexcept {
        return sizeof(data) - sizeof(T) + payload;
    }

public:
    size_t storage_size() const noexcept {
        return storage_size(size_for_allocation_strategy(value));
    }
};

} // namespace bplus
