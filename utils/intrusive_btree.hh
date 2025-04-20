/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/intrusive/parent_from_member.hpp>
#include <seastar/util/alloc_failure_injector.hh>
#include <cassert>
#include <fmt/core.h>
#include "utils/assert.hh"
#include "utils/collection-concepts.hh"
#include "utils/neat-object-id.hh"
#include "utils/allocation_strategy.hh"

namespace intrusive_b {

template <typename Func, typename T>
concept KeyCloner = requires (Func f, T* val) {
    { f(val) } -> std::same_as<T*>;
};

/*
 * The KeyPointer is any wrapper that carries a "real" key one board and that
 * can release it, thus giving its ownership to the tree. It's used in insert()
 * methods where either key conflict or an exception may occur. In either case
 * the key will not be released and freeing it is up to the caller.
 */
template <typename Pointer, typename T>
concept KeyPointer = std::is_nothrow_move_constructible_v<Pointer> &&
    requires (Pointer p) { { *p } -> std::same_as<T&>; } &&
    requires (Pointer p) { { p.release() } noexcept -> std::same_as<T*>; };

enum class with_debug { no, yes };
enum class key_search { linear, binary, both };

class member_hook;

// The LinearThreshold is explained below, see NODE_LINEAR flag
template <typename Key, member_hook Key::* Hook, typename Compare, size_t NodeSize, size_t LinearThreshold, key_search Search, with_debug Debug> class node;
template <typename Key, member_hook Key::*, typename Compare, size_t NodeSize, size_t LinearThreshold> class validator;

// For .{do_something_with_data}_and_dispose methods below
template <typename T>
void default_dispose(T* value) noexcept { }

using key_index = size_t;
using kid_index = size_t;

/*
 * The key's member_hook must point to something that's independent from
 * the tree's template parameters, so here's this base. It carries the
 * bare minimum of information needed for member_hook to operate (see
 * the iterator::erase()).
 */
class node_base {
    template <typename K, member_hook K::* H, typename C, size_t NS, size_t LT, key_search KS, with_debug D> friend class node;
    node_base(unsigned short n, unsigned short cap, unsigned short f) noexcept : num_keys(n), flags(f), capacity(cap) {}

public:
    unsigned short num_keys;
    unsigned short flags;
    unsigned short capacity; // used by linear node only

    /*
     * Each node keeps pointers on keys, not their values. This allows keeping
     * iterators valid after insert/remove.
     *
     * The size of this array is zero, because we don't know it. The real memory
     * for it is reserved in class node.
     */
    member_hook* keys[0];

    static constexpr unsigned short NODE_ROOT = 0x1;
    static constexpr unsigned short NODE_LEAF = 0x2;
    static constexpr unsigned short NODE_LEFTMOST = 0x4; // leaf with smallest keys in the tree
    static constexpr unsigned short NODE_RIGHTMOST = 0x8; // leaf with greatest keys in the tree
    /*
     * Linear node is the root leaf that grows above the NodeSize
     * limit up to resching the LinearThreshold number of keys.
     * After this the root leaf is shattered into a small tree,
     * then B-tree works as usual.
     *
     * The backward (small tree -> linear node) transition is not
     * performed, so the root leaf can be either linear, or regular,
     * thus the explicit flag.
     */
    static constexpr unsigned short NODE_LINEAR = 0x10;
    /*
     * Inline node is embedded into tree itself and is capabale
     * of carrying a single key.
     */
    static constexpr unsigned short NODE_INLINE = 0x20;

    struct inline_tag{};
    node_base(inline_tag) noexcept : num_keys(0), flags(NODE_ROOT | NODE_LEAF | NODE_INLINE), capacity(1) {}

    bool is_root() const noexcept { return flags & NODE_ROOT; }
    bool is_leaf() const noexcept { return flags & NODE_LEAF; }
    bool is_leftmost() const noexcept { return flags & NODE_LEFTMOST; }
    bool is_rightmost() const noexcept { return flags & NODE_RIGHTMOST; }
    bool is_linear() const noexcept { return flags & NODE_LINEAR; }
    bool is_inline() const noexcept { return flags & NODE_INLINE; }

    node_base(const node_base&) = delete;
    node_base(node_base&&) = delete;

    key_index index_for(const member_hook* hook) const noexcept {
        for (key_index i = 0; i < num_keys; i++) {
            if (keys[i] == hook) {
                return i;
            }
        }

        std::abort();
    }

    bool empty() const noexcept { return num_keys == 0; }

private:
    friend class member_hook;

    void reattach(member_hook* to, member_hook* from) noexcept {
        key_index idx = index_for(from);
        keys[idx] = to;
    }
};

/*
 * Struct that's to be embedded into the key. Should be kept as small as possible.
 */
class member_hook {
    template <typename K, member_hook K::* H, typename C, size_t NS, size_t LT> friend class validator;
    template <typename K, member_hook K::* H, typename C, size_t NS, size_t LT, key_search KS, with_debug D> friend class node;

private:
    node_base* _node = nullptr;

public:
    bool attached() const noexcept { return _node != nullptr; }
    node_base* node() const noexcept { return _node; }

    void attach_first(node_base& to) noexcept {
        SCYLLA_ASSERT(to.num_keys == 0);
        to.num_keys = 1;
        to.keys[0] = this;
        _node = &to;
    }

    member_hook() noexcept = default;
    member_hook(const member_hook&) = delete;
    ~member_hook() {
        SCYLLA_ASSERT(!attached());
    }

    member_hook(member_hook&& other) noexcept : _node(other._node) {
        if (attached()) {
            _node->reattach(this, &other);
            other._node = nullptr;
        }
    }

    template <typename K, member_hook K::* Hook>
    const K* to_key() const noexcept {
        return boost::intrusive::get_parent_from_member(this, Hook);
    }

    template <typename K, member_hook K::* Hook>
    K* to_key() noexcept {
        return boost::intrusive::get_parent_from_member(this, Hook);
    }
};

struct stats {
    unsigned long nodes;
    std::vector<unsigned long> nodes_filled;
    unsigned long leaves;
    std::vector<unsigned long> leaves_filled;
    unsigned long linear_keys;
};

/*
 * The tree itself.
 * Equipped with constant time begin() and end() and the iterator, that
 * scans through sorted keys and is not invalidated on insert/remove.
 *
 * The NodeSize parameter describes the amount of keys to be held on each
 * node. Inner nodes will thus have N+1 pointers on sub-trees.
 */

template <typename Key, member_hook Key::* Hook, typename Compare, size_t NodeSize, size_t LinearThreshold, key_search Search, with_debug Debug = with_debug::no>
requires Comparable<Key, Key, Compare>
class tree {
    // Sanity not to allow slow key-search in non-debug mode
    static_assert(Debug == with_debug::yes || Search != key_search::both);

public:
    friend class node<Key, Hook, Compare, NodeSize, LinearThreshold, Search, Debug>;
    friend class validator<Key, Hook, Compare, NodeSize, LinearThreshold>;

    using node = class node<Key, Hook, Compare, NodeSize, LinearThreshold, Search, Debug>;

    class iterator;
    class const_iterator;

private:

    node* _root = nullptr;

    struct corners {
        node* left;
        node* right;
        corners() noexcept : left(nullptr), right(nullptr) {}
    };

    union {
        corners _corners;
        node_base _inline;
        static_assert(sizeof(corners) >= sizeof(node_base) + sizeof(member_hook*));
    };

    static const tree* from_inline(const node_base* n) noexcept {
        SCYLLA_ASSERT(n->is_inline());
        return boost::intrusive::get_parent_from_member(n, &tree::_inline);
    }

    static tree* from_inline(node_base* n) noexcept {
        SCYLLA_ASSERT(n->is_inline());
        return boost::intrusive::get_parent_from_member(n, &tree::_inline);
    }

    /*
     * Helper structure describing a position in a tree. Filled
     * by key_lower_bound() method and is used by tree's API calls.
     */
    struct cursor {
        node* n;
        kid_index idx;

        void descend() noexcept {
            n = n->_kids[idx];
            __builtin_prefetch(n);
        }

        template <typename Pointer>
        iterator insert(Pointer kptr) {
            if (n->is_linear()) {
                n = n->check_linear_capacity(idx);
            }

            Key& k = *kptr;
            n->insert(idx, std::move(kptr));
            /*
             * We cannot trust cur.idx as insert might have moved
             * it anywhere across the tree.
             */
            return iterator(k.*Hook, 0);
        }
    };

    /*
     * Find the key in the tree or the position before which it should be
     * and targets the cursor into this place. Returns true if the key
     * itself was found, false otherwise.
     */
    template <typename K>
    bool key_lower_bound(const K& key, const Compare& cmp, cursor& cur) const {
        cur.n = _root;

        while (true) {
            bool match;

            cur.idx = cur.n->index_for(key, cmp, match);
            SCYLLA_ASSERT(cur.idx <= cur.n->_base.num_keys);
            if (match || cur.n->is_leaf()) {
                return match;
            }

            cur.descend();
        }
    }

    void do_set_root(node& n) noexcept {
        SCYLLA_ASSERT(n.is_root());
        n._parent.t = this;
        _root = &n;
    }

    void do_set_left(node& n) noexcept {
        SCYLLA_ASSERT(n.is_leftmost());
        if (!n.is_linear()) {
            n._leaf_tree = this;
        }
        _corners.left = &n;
    }

    void do_set_right(node& n) noexcept {
        SCYLLA_ASSERT(n.is_rightmost());
        if (!n.is_linear()) {
            n._leaf_tree = this;
        }
        _corners.right = &n;
    }

    template <typename Pointer>
    iterator insert_into_inline(Pointer kptr) noexcept {
        member_hook* hook = &(kptr.release()->*Hook);
        hook->attach_first(_inline);
        return iterator(*hook, 0);
    }

    template <typename K>
    std::strong_ordering find_in_inline(const K& k, const Compare& cmp) const {
        return _inline.empty() ? std::strong_ordering::greater : cmp(k, *(_inline.keys[0]->to_key<Key, Hook>()));
    }

    void break_inline() {
        node* n = node::create_empty_root();
        _inline.keys[0]->attach_first(n->_base);
        do_set_root(*n);
        do_set_left(*n);
        do_set_right(*n);
    }

    const node_base* rightmost_node() const noexcept {
        return _root == nullptr ? &_inline : &_corners.right->_base;
    }

    node_base* rightmost_node() noexcept {
        return _root == nullptr ? &_inline : &_corners.right->_base;
    }

    const node_base* leftmost_node() const noexcept {
        return _root == nullptr ? &_inline : &_corners.left->_base;
    }

    node_base* leftmost_node() noexcept {
        return _root == nullptr ? &_inline : &_corners.left->_base;
    }

    bool inline_root() const noexcept { return _root == nullptr; }

public:
    tree() noexcept : _root(nullptr), _inline(node_base::inline_tag{}) {}

    tree(tree&& other) noexcept : tree() {
        if (!other.inline_root()) {
            do_set_root(*other._root);
            do_set_left(*other._corners.left);
            do_set_right(*other._corners.right);

            other._root = nullptr;
            other._corners.left = nullptr;
            other._corners.right = nullptr;
        } else if (!other._inline.empty()) {
            other._inline.keys[0]->attach_first(_inline);
            other._inline.num_keys = 0;
        }
    }

    tree(const tree& other) = delete;
    ~tree() noexcept {
        if (!inline_root()) {
            SCYLLA_ASSERT(_root->is_leaf());
            node::destroy(*_root);
        } else {
            SCYLLA_ASSERT(_inline.empty());
        }
    }

    template <typename Pointer>
    requires KeyPointer<Pointer, Key>
    std::pair<iterator, bool> insert(Pointer kptr, Compare cmp) {
        seastar::memory::on_alloc_point();
        cursor cur;

        if (inline_root()) {
            if (_inline.empty()) {
                return std::pair(insert_into_inline(std::move(kptr)), true);
            }
            break_inline();
        }

        if (key_lower_bound(*kptr, cmp, cur)) {
            return std::pair(iterator(cur), false);
        }

        return std::pair(cur.insert(std::move(kptr)), true);
    }

    /*
     * Inserts the key into the tree using hint as an attempt not to lookup
     * its position with logN algo. If the new key is hint - 1 <= key <= hint
     * then the insertion goes in O(1) (amortizing rebalancing).
     */
    template <typename Pointer>
    requires KeyPointer<Pointer, Key>
    std::pair<iterator, bool> insert_before_hint(iterator hint, Pointer kptr, Compare cmp) {
        seastar::memory::on_alloc_point();
        auto x = std::strong_ordering::less;

        if (hint != end()) {
            x = cmp(*kptr, *hint);
            if (x == 0) {
                return std::pair(iterator(hint), false);
            }
        }

        if (x < 0) {
            x = std::strong_ordering::greater;

            if (hint != begin()) {
                auto prev = std::prev(hint);
                x = cmp(*kptr, *prev);
                if (x == 0) {
                    return std::pair(iterator(prev), false);
                }
            }

            if (x > 0) {
                return std::pair(hint.insert_before(std::move(kptr)), true);
            }
        }

        return insert(std::move(kptr), std::move(cmp));
    }

    /*
     * Constant-time insertion right before the given position. No sorting
     * is checked, the tree will be broken if the key/it are not in order.
     */

    template <typename Pointer>
    requires KeyPointer<Pointer, Key>
    iterator insert_before(iterator it, Pointer kptr) {
        seastar::memory::on_alloc_point();
        return it.insert_before(std::move(kptr));
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    const_iterator find(const K& k, Compare cmp) const {
        cursor cur;

        if (inline_root()) {
            if (find_in_inline(k, cmp) == 0) {
                return const_iterator(*_inline.keys[0], 0);
            }
            return cend();
        }
        if (!key_lower_bound(k, cmp, cur)) {
            return cend();
        }

        return const_iterator(cur);
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    iterator find(const K& k, Compare cmp) {
        return iterator(const_cast<const tree*>(this)->find(k, cmp));
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    const_iterator lower_bound(const K& k, bool& match, Compare cmp) const {
        if (inline_root()) {
            auto x = find_in_inline(k, cmp);
            if (x <= 0) {
                match = x == 0;
                return const_iterator(*_inline.keys[0], 0);
            }

            match = false;
            return cend();
        }

        if (_root->_base.num_keys == 0) {
            match = false;
            return cend();
        }

        cursor cur;
        match = key_lower_bound(k, cmp, cur);
        if (!match && cur.idx == cur.n->_base.num_keys) {
            SCYLLA_ASSERT(cur.idx > 0);
            cur.idx--;
            return ++const_iterator(cur);
        }

        return const_iterator(cur);
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    iterator lower_bound(const K& k, bool& match, Compare cmp) {
        return iterator(const_cast<const tree*>(this)->lower_bound(k, match, cmp));
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    const_iterator lower_bound(const K& k, Compare cmp) const {
        bool match;
        return lower_bound(k, match, cmp);
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    iterator lower_bound(const K& k, Compare cmp) {
        bool match;
        return lower_bound(k, match, cmp);
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    const_iterator upper_bound(const K& k, Compare cmp) const {
        bool match;

        const_iterator ret = lower_bound(k, match, cmp);
        if (match) {
            ret++;
        }

        return ret;
    }

    template <typename K>
    requires Comparable<K, Key, Compare>
    iterator upper_bound(const K& k, Compare cmp) {
        return iterator(const_cast<const tree*>(this)->upper_bound(k, cmp));
    }

    template <typename K, typename Disp>
    requires Comparable<K, Key, Compare> && Disposer<Disp, Key>
    iterator erase_and_dispose(const K& k, Compare cmp, Disp&& disp) {
        cursor cur;

        if (inline_root()) {
            if (find_in_inline(k, cmp) == 0) {
                node::dispose_key(_inline.keys[0], disp);
                _inline.num_keys = 0;
            }
            return cend();
        }

        if (!key_lower_bound(k, cmp, cur)) {
            return end();
        }

        iterator it(cur);
        member_hook* hook = it._hook;
        it++;
        cur.n->remove(cur.idx);
        node::dispose_key(hook, disp);

        return it;
    }

    /*
     * This range-erase is trivial and not optimal, each key erasure may
     * end up rebalancing the upper nodes in vain.
     */
    template <typename Disp>
    requires Disposer<Disp, Key>
    iterator erase_and_dispose(iterator from, iterator to, Disp&& disp) noexcept {
        while (from != to) {
            from = from.erase_and_dispose(disp);
        }
        return to;
    }

    template <typename Disp>
    requires Disposer<Disp, Key>
    iterator erase_and_dispose(const_iterator from, const_iterator to, Disp&& disp) noexcept {
        return erase_and_dispose(iterator(from), iterator(to), std::forward<Disp>(disp));
    }

    template <typename Disp>
    requires Disposer<Disp, Key>
    iterator erase_and_dispose(iterator it, Disp&& disp) noexcept {
        return it.erase_and_dispose(disp);
    }

    Key* unlink_leftmost_without_rebalance() noexcept {
        node_base* nb = leftmost_node();
        if (nb->num_keys == 0) {
            return nullptr;
        }

        member_hook* hook = nb->keys[0];
        node::dispose_key(hook, default_dispose<Key>);

        if (nb->is_inline()) {
            nb->num_keys = 0;
        } else {
            node* n = node::from_base(nb);
            SCYLLA_ASSERT(n->is_leaf());
            n->remove_leftmost_light_rebalance();
        }
        return hook->to_key<Key, Hook>();
    }

    template <typename... Args>
    iterator erase(Args&&... args) { return erase_and_dispose(std::forward<Args>(args)..., default_dispose<Key>); }

    template <typename Func>
    requires Disposer<Func, Key>
    void clear_and_dispose(Func&& disp) noexcept {
        if (!inline_root()) {
            _root->clear([&disp] (member_hook* h) { node::dispose_key(h, disp); });
            node::destroy(*_root);
            _root = nullptr;
            // Both left and right leaves pointers are not touched as this
            // initialization of inline node overwrites them anyway
            new (&_inline) node_base(node_base::inline_tag{});
        } else if (!_inline.empty()) {
            node::dispose_key(_inline.keys[0], disp);
            _inline.num_keys = 0;
        }
    }

    void clear() noexcept { clear_and_dispose(default_dispose<Key>); }

    /*
     * Clone the tree using given Cloner (and Deleter for roll-back).
     */
    template <typename Cloner, typename Deleter>
    requires KeyCloner<Cloner, Key> && Disposer<Deleter, Key>
    void clone_from(const tree& t, Cloner&& cloner, Deleter&& deleter) {
        clear_and_dispose(deleter);
        if (!t.inline_root()) {
            node* left = nullptr;
            node* right = nullptr;

            _root = t._root->clone(left, right, cloner, deleter);

            left->_base.flags |= node_base::NODE_LEFTMOST;
            do_set_left(*left);
            right->_base.flags |= node_base::NODE_RIGHTMOST;
            do_set_right(*right);
            _root->_base.flags |= node_base::NODE_ROOT;
            do_set_root(*_root);
        } else if (!t._inline.empty()) {
            Key* key = cloner(t._inline.keys[0]->template to_key<Key, Hook>());
            (key->*Hook).attach_first(_inline);
        }
    }

    template <bool Const, typename Iterator> // Iterator will be derived from iterator_base
    class iterator_base {
    protected:
        using tree_ptr = std::conditional_t<Const, const tree*, tree*>;
        using key_hook_ptr = std::conditional_t<Const, const member_hook*, member_hook*>;
        using node_base_ptr = std::conditional_t<Const, const node_base*, node_base*>;
        using node_ptr = std::conditional_t<Const, const node*, node*>;

        // The end() iterator uses _tree pointer, all the others use _hook.
        union {
            tree_ptr _tree;
            key_hook_ptr _hook;
        };
        key_index _idx;

        // No keys can be at this index, so it's used as the "end" mark.
        static constexpr key_index npos = LinearThreshold;

        explicit iterator_base(tree_ptr t) noexcept : _tree(t), _idx(npos) {}
        iterator_base(key_hook_ptr h, key_index idx) noexcept : _hook(h), _idx(idx) {
            SCYLLA_ASSERT(!is_end());
            SCYLLA_ASSERT(h->attached());
        }
        explicit iterator_base(const cursor& cur) noexcept : _idx(cur.idx) {
            SCYLLA_ASSERT(_idx < cur.n->_base.num_keys);
            _hook = cur.n->_base.keys[_idx];
            SCYLLA_ASSERT(_hook->attached());
        }
        iterator_base() noexcept : _tree(static_cast<tree_ptr>(nullptr)), _idx(npos) {}

        bool is_end() const noexcept { return _idx == npos; }

        /*
         * The routine makes sure the iterator's index is valid
         * and returns back the node that points to it.
         */
        node_base_ptr revalidate() noexcept {
            SCYLLA_ASSERT(!is_end());
            node_base_ptr n = _hook->node();

            /*
             * The hook pointer is always valid (it's updated on insert/remove
             * operations), the keys are not moved, so if the node still points
             * at us, it is valid.
             */
            if (_idx >= n->num_keys || n->keys[_idx] != _hook) {
                _idx = n->index_for(_hook);
            }

            return n;
        }

    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = std::conditional_t<Const, const Key, Key>;
        using difference_type = ssize_t;
        using pointer = value_type*;
        using reference = value_type&;

        iterator_base(const iterator_base& other) noexcept {
            if (other.is_end()) {
                _idx = npos;
                _tree = other._tree;
            } else {
                _idx = other._idx;
                _hook = other._hook;
            }
        }

        reference operator*() const noexcept { return *_hook->template to_key<Key, Hook>(); }
        pointer operator->() const noexcept { return _hook->template to_key<Key, Hook>(); }

        Iterator& operator++() noexcept {
            node_base_ptr n = revalidate();

            if (n->is_leaf()) [[likely]] {
                if (_idx < n->num_keys - 1u) [[likely]] {
                    _idx++;
                    _hook = n->keys[_idx];
                } else if (n->is_inline()) {
                    _idx = npos;
                    _tree = tree::from_inline(n);
                } else if (n->is_rightmost()) {
                    _idx = npos;
                    _tree = node::from_base(n)->corner_tree();
                } else {
                    node_ptr nd = node::from_base(n);
                    do {
                        node_ptr p = nd->_parent.n;
                        _idx = p->index_for(nd);
                        nd = p;
                    } while (_idx == nd->_base.num_keys);
                    _hook = nd->_base.keys[_idx];
                }
            } else {
                node_ptr nd = node::from_base(n);
                nd = nd->_kids[_idx + 1];
                while (!nd->is_leaf()) {
                    nd = nd->_kids[0];
                }
                _idx = 0;
                _hook = nd->_base.keys[_idx];
            }

            static_assert(std::is_base_of_v<iterator_base, Iterator>);
            return static_cast<Iterator&>(*this);
        }

        Iterator& operator--() noexcept {
            if (is_end()) {
                node_base_ptr n = _tree->rightmost_node();
                SCYLLA_ASSERT(n->num_keys > 0);
                _idx = n->num_keys - 1u;
                _hook = n->keys[_idx];
                static_assert(std::is_base_of_v<iterator_base, Iterator>);
                return static_cast<Iterator&>(*this);
            }

            node_ptr n = node::from_base(revalidate());

            if (n->is_leaf()) {
                while (_idx == 0) {
                    node_ptr p = n->_parent.n;
                    _idx = p->index_for(n);
                    n = p;
                }
                _idx--;
            } else {
                n = n->_kids[_idx];
                while (!n->is_leaf()) {
                    n = n->_kids[n->_base.num_keys];
                }
                _idx = n->_base.num_keys - 1;
            }

            _hook = n->_base.keys[_idx];
            static_assert(std::is_base_of_v<iterator_base, Iterator>);
            return static_cast<Iterator&>(*this);
        }

        Iterator operator++(int) noexcept {
            auto cur = Iterator(*this);
            operator++();
            return cur;
        }

        Iterator operator--(int) noexcept {
            auto cur = Iterator(*this);
            operator--();
            return cur;
        }

        bool operator==(const iterator_base& o) const noexcept { return is_end() ? o.is_end() : _hook == o._hook; }
        operator bool() const noexcept { return !is_end(); }

        /*
         * Special constructor for the case when there's the need for an
         * iterator to the given value pointer. We can get all we need
         * through the hook -> node_base -> node chain.
         */
        iterator_base(pointer key) noexcept : iterator_base(&(key->*Hook), 0) {
            revalidate();
        }

        /*
         * Returns pointer on the owning tree if the element is the
         * last one left in it.
         */
        tree_ptr tree_if_singular() noexcept {
            node_base* n = revalidate();

            if (n->is_root() && n->is_leaf() && n->num_keys == 1) {
                return n->is_inline() ? tree::from_inline(n) : node::from_base(n)->_parent.t;
            } else {
                return nullptr;
            }
        }
    };

    using iterator_base_const = iterator_base<true, const_iterator>;
    using iterator_base_nonconst = iterator_base<false, iterator>;

    class const_iterator final : public iterator_base_const {
        friend class tree;
        using super = iterator_base_const;

        explicit const_iterator(const tree* t) noexcept : super(t) {}
        explicit const_iterator(const cursor& cur) noexcept : super(cur) {}
        const_iterator(const member_hook& h, key_index idx) noexcept : super(&h, idx) {}

    public:
        const_iterator() noexcept : super() {}
        const_iterator(const iterator_base_const& other) noexcept : super(other) {}
        const_iterator(const iterator& other) noexcept {
            if (other.is_end()) {
                super::_idx = super::npos;
                super::_tree = const_cast<const tree*>(other._tree);
            } else {
                super::_idx = other._idx;
                super::_hook = const_cast<const member_hook*>(other._hook);
            }
        }
    };

    class iterator final : public iterator_base_nonconst {
        friend class tree;
        friend class key_grabber;
        using super = iterator_base_nonconst;

        explicit iterator(const tree* t) noexcept : super(t) {}
        explicit iterator(const cursor& cur) noexcept : super(cur) {}
        iterator(member_hook& h, key_index idx) noexcept : super(&h, idx) {}

    public:
        iterator() noexcept : super() {}
        iterator(const iterator_base_nonconst& other) noexcept : super(other) {}
        iterator(const const_iterator& other) noexcept {
            if (other.is_end()) {
                super::_idx = super::npos;
                super::_tree = const_cast<tree*>(other._tree);
            } else {
                super::_idx = other._idx;
                super::_hook = const_cast<member_hook*>(other._hook);
            }
        }

    private:
        template <typename Disp>
        requires Disposer<Disp, Key>
        iterator erase_and_dispose(Disp&& disp) noexcept {
            node_base* nb = super::revalidate();
            iterator cur;

            if (nb->is_inline()) {
                cur._idx = super::npos;
                cur._tree = tree::from_inline(nb);
                nb->num_keys = 0;
            } else {
                cur = *this;
                cur++;

                node::from_base(nb)->remove(super::_idx);
                if (cur._hook->node() == nb && cur._idx > 0) {
                    cur._idx--;
                }
            }

            node::dispose_key(super::_hook, disp);
            return cur;
        }

        template <typename Pointer>
        iterator insert_before(Pointer kptr) {
            cursor cur;

            if (super::is_end()) {
                tree* t = super::_tree;
                if (t->inline_root()) {
                    if (t->_inline.empty()) {
                        return t->insert_into_inline(std::move(kptr));
                    }
                    t->break_inline();
                }
                cur.n = t->_corners.right;
                cur.idx = cur.n->_base.num_keys;
            } else {
                node_base* n = super::revalidate();
                if (n->is_inline()) {
                    tree* t = tree::from_inline(n);
                    t->break_inline();
                    cur.n = t->_root;
                    cur.idx = 0;
                } else {
                    cur.n = node::from_base(n);
                    cur.idx = super::_idx;

                    while (!cur.n->is_leaf()) {
                        cur.descend();
                        cur.idx = cur.n->_base.num_keys;
                    }
                }
            }

            return cur.insert(std::move(kptr));
        }
    };

    bool empty() const noexcept { return inline_root() ? _inline.empty() : _root->_base.empty(); }

    const_iterator cbegin() const noexcept {
        const node_base* n = leftmost_node();
        return n->num_keys == 0 ? cend() : const_iterator(*n->keys[0], 0);
    }

    const_iterator cend() const noexcept {
        return const_iterator(this);
    }

    const_iterator begin() const noexcept { return cbegin(); }
    const_iterator end() const noexcept { return cend(); }

    iterator begin() noexcept {
        return iterator(const_cast<const tree*>(this)->cbegin());
    }

    iterator end() noexcept {
        return iterator(const_cast<const tree*>(this)->cend());
    }

    using reverse_iterator = std::reverse_iterator<iterator>;
    reverse_iterator rbegin() noexcept { return std::make_reverse_iterator(end()); }
    reverse_iterator rend() noexcept { return std::make_reverse_iterator(begin()); }

    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    const_reverse_iterator crbegin() const noexcept { return std::make_reverse_iterator(cend()); }
    const_reverse_iterator crend() const noexcept { return std::make_reverse_iterator(cbegin()); }
    const_reverse_iterator rbegin() const noexcept { return crbegin(); }
    const_reverse_iterator rend() const noexcept { return crend(); }

    size_t calculate_size() const noexcept {
        return inline_root() ? _inline.num_keys : _root->size_slow();
    }

    size_t external_memory_usage() const noexcept {
        return inline_root() ? 0 : _root->external_memory_usage();
    }

    /*
     * Helper to remove keys from trees using only the key iterator.
     *
     * Conforms to KeyPointer and can be used to move keys between trees.
     * Create it with an iterator to a key in one tree and feed to some
     * .insert method into the other. If the key will be taken by the
     * target, it will be instantly removed from the source, and the
     * original iterator will be updated as if it did i = src.erase(i).
     */
    class key_grabber {
        iterator& _it;

    public:
        explicit key_grabber(iterator& it) : _it(it) {
            SCYLLA_ASSERT(!_it.is_end());
        }

        key_grabber(const key_grabber&) = delete;
        key_grabber(key_grabber&&) noexcept = default;

        Key& operator*() const noexcept { return *_it; }

        template <typename Disp>
        requires Disposer<Disp, Key>
        void release(Disp&& disp) {
            _it = _it.erase_and_dispose(std::move(disp));
        }

        Key* release() noexcept {
            Key& key = *_it;
            release(default_dispose<Key>);
            return &key;
        }
    };

    struct stats get_stats() const noexcept {
        struct stats st;

        st.nodes = 0;
        st.leaves = 0;
        st.linear_keys = 0;

        if (!inline_root()) {
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
 * The ge() method accepts sorted array of keys and searches the index of the
 * lower-bound element of the given key. The bool match is set to true if the
 * key matched, to false otherwise.
 */

template <typename K, typename Key, member_hook Key::* Hook, typename Compare, key_search Search>
struct searcher { };

template <typename K, typename Key, member_hook Key::* Hook, typename Compare>
struct searcher<K, Key, Hook, Compare, key_search::linear> {
    static key_index ge(const K& k, const node_base& node, const Compare& cmp, bool& match) {
        key_index i;

        match = false;
        for (i = 0; i < node.num_keys; i++) {
            if (i + 1 < node.num_keys) {
                __builtin_prefetch(node.keys[i + 1]->to_key<Key, Hook>());
            }
            auto x = cmp(k, *node.keys[i]->to_key<Key, Hook>());
            if (x <= 0) {
                match = x == 0;
                break;
            }
        }

        return i;
    };
};

template <typename K, typename Key, member_hook Key::* Hook, typename Compare>
struct searcher<K, Key, Hook, Compare, key_search::binary> {
    static key_index ge(const K& k, const node_base& node, const Compare& cmp, bool& match) {
        ssize_t s = 0, e = node.num_keys - 1; // signed for below s <= e corner cases

        while (s <= e) {
            key_index i = (s + e) / 2;
            auto x = cmp(k, *node.keys[i]->to_key<Key, Hook>());
            if (x < 0) {
                e = i - 1;
            } else if (x > 0) {
                s = i + 1;
            } else {
                match = true;
                return i;
            }
        }

        match = false;
        return s;
    }
};

template <typename K, typename Key, member_hook Key::* Hook, typename Compare>
struct searcher<K, Key, Hook, Compare, key_search::both> {
    static key_index ge(const K& k, const node_base& node, const Compare& cmp, bool& match) {
        bool ml, mr;
        key_index rl = searcher<K, Key, Hook, Compare, key_search::linear>::ge(k, node, cmp, ml);
        key_index rb = searcher<K, Key, Hook, Compare, key_search::binary>::ge(k, node, cmp, mr);
        SCYLLA_ASSERT(rl == rb);
        SCYLLA_ASSERT(ml == mr);
        match = ml;
        return rl;
    }
};

/*
 * A node describes all kinds of nodes -- inner, leaf and linear ones
 */
template <typename Key, member_hook Key::* Hook, typename Compare, size_t NodeSize, size_t LinearThreshold, key_search Search, with_debug Debug>
class node {
    friend class tree<Key, Hook, Compare, NodeSize, LinearThreshold, Search, Debug>;
    friend class validator<Key, Hook, Compare, NodeSize, LinearThreshold>;

    using tree = class tree<Key, Hook, Compare, NodeSize, LinearThreshold, Search, Debug>;

    class prealloc;
    [[no_unique_address]] utils::neat_id<Debug == with_debug::yes> id;

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

    /*
     * The LinearThreshold defines the maximum size of the linear growth,
     * though limiting it with values less than NodeSize itself makes
     * little sense.
     */
    static_assert(LinearThreshold >= NodeSize);

    /*
     * When shattering the number of resulting nodes can be any, but the
     * current implementation only makes one-level tree.
     */
    static_assert(LinearThreshold <= NodeSize * (NodeSize + 1) + NodeSize);

    // Hint for the compiler when not to mess with linear stuff at all
    static constexpr bool make_linear_root = (LinearThreshold > NodeSize);

    union node_or_tree {
        node* n;
        tree* t;
    };

    // root node keeps .t pointer on tree, all others -- .n on parents
    node_or_tree _parent;

    node_base _base;

    /*
     * The node_base has keys[] field of zero size at the end, because it should
     * be NodeSize-agnostic. Thus the real memory for key's pointers is reserved
     * here.
     */
    char __room_for_keys[NodeSize * sizeof(member_hook*)];
    static_assert(offsetof(node_base, keys[NodeSize]) == sizeof(node_base) + NodeSize * sizeof(member_hook*));

    /*
     * Leaf nodes don't have kids, so this array is empty for them, but
     * left- and rightmost leaves need pointers on the tree itself.
     *
     * // Unlike B+ trees they are not linked into a list, but still
     * // need the tree pointer to update its _corners.left/right on move.
     *
     * Inner nodes do have kids and since this field goes last allocating
     * the enoug big chunk of memory for a node gives room for it.
     *
     * Linear node doesn't use this union at all.
     */
    union {
        node* _kids[0];
        tree* _leaf_tree;
    };

    tree* corner_tree() const noexcept {
        SCYLLA_ASSERT(is_leaf());
        if (!is_linear()) {
            return _leaf_tree;
        }

        SCYLLA_ASSERT(is_root());
        return _parent.t;
    }

public:
    /*
     * Leaf node layout
     *
     *  _parent        (pointer)
     *  _base.num_keys (short)
     *  _base.flags    (short)
     *  ...            (int compiler's alignment gap)
     *  _base.keys     (N pointers, thanks to __room_for_keys)
     *  _leaf_tree     (pointer)
     */
    static constexpr size_t leaf_node_size = sizeof(node);

    /*
     * Inner node layout
     *
     *  _parent        (pointer)
     *  _base.num_keys (short)
     *  _base.flags    (short)
     *  ...            (int compiler's alignment gap)
     *  _base.keys     (N pointers)
     *  _kids          (N + 1 pointers)
     */
    static constexpr size_t inner_node_size = sizeof(node) - sizeof(tree*) + (NodeSize + 1) * sizeof(node*);

    /*
     * Linear node layout (dynamic)
     *
     *  _parent        (pointer)
     *  _base.num_keys (short)
     *  _base.flags    (short)
     *  _base.capacity (short)
     *  ...            (short compiler's alignment gap)
     *  _base.keys     (.capacity pointers)
     */
    static size_t linear_node_size(size_t cap) {
        return sizeof(node) - sizeof(tree*) - NodeSize * sizeof(member_hook*) + cap * sizeof(member_hook*);
    }

private:
    bool is_root() const noexcept { return _base.is_root(); }
    bool is_leaf() const noexcept { return _base.is_leaf(); }
    bool is_leftmost() const noexcept { return _base.is_leftmost(); }
    bool is_rightmost() const noexcept { return _base.is_rightmost(); }
    bool is_linear() const noexcept { return make_linear_root && _base.is_linear(); }

    // Helpers to move keys/kids around

    // ... locally
    void move_key(key_index f, key_index t) noexcept {
        _base.keys[t] = _base.keys[f];
    }
    void move_kid(kid_index f, kid_index t) noexcept {
        _kids[t] = _kids[f];
    }

    void set_key(key_index idx, member_hook* hook) noexcept {
        _base.keys[idx] = hook;
        hook->_node = &_base;
    }
    void set_kid(kid_index idx, node* n) noexcept {
        _kids[idx] = n;
        n->_parent.n = this;
    }

    // ... to other nodes
    void move_key(key_index f, node& n, key_index t) noexcept {
        n.set_key(t, _base.keys[f]);
    }
    void move_kid(kid_index f, node& n, kid_index t) noexcept {
        n.set_kid(t, _kids[f]);
    }

    void unlink_corner_leaf() noexcept {
        SCYLLA_ASSERT(!is_root());
        node* p = _parent.n, *x;

        switch (_base.flags & (node_base::NODE_LEFTMOST | node_base::NODE_RIGHTMOST)) {
            case 0:
                break;
            case node_base::NODE_LEFTMOST:
                SCYLLA_ASSERT(p->_base.num_keys > 0 && p->_kids[0] == this);
                x = p->_kids[1];
                _base.flags &= ~node_base::NODE_LEFTMOST;
                x->_base.flags |= node_base::NODE_LEFTMOST;
                _leaf_tree->do_set_left(*x);
                break;
            case node_base::NODE_RIGHTMOST:
                SCYLLA_ASSERT(p->_base.num_keys > 0 && p->_kids[p->_base.num_keys] == this);
                x = p->_kids[p->_base.num_keys - 1];
                _base.flags &= ~node_base::NODE_RIGHTMOST;
                x->_base.flags |= node_base::NODE_RIGHTMOST;
                _leaf_tree->do_set_right(*x);
                break;
            default:
                /*
                 * Right- and left-most at the same time can only be root,
                 * otherwise this would mean we have root with 0 keys.
                 */
                SCYLLA_ASSERT(false);
        }
    }

    static const node* from_base(const node_base* nb) noexcept {
        SCYLLA_ASSERT(!nb->is_inline());
        return boost::intrusive::get_parent_from_member(nb, &node::_base);
    }

    static node* from_base(node_base* nb) noexcept {
        SCYLLA_ASSERT(!nb->is_inline());
        return boost::intrusive::get_parent_from_member(nb, &node::_base);
    }

    template <typename Disp>
    static void dispose_key(member_hook* hook, Disp&& disp) noexcept {
        hook->_node = nullptr;
        disp(hook->to_key<Key, Hook>());
    }

public:
    node(size_t cap, unsigned short flags) noexcept : _base(0, cap, flags) { }

    node(node&& other) noexcept : node(other._base.capacity, std::move(other)) {}
    node(size_t cap, node&& other) noexcept : _base(0, cap, other._base.flags) {
        if (is_leaf()) {
            if (is_leftmost()) {
                other.corner_tree()->do_set_left(*this);
            }

            if (is_rightmost()) {
                other.corner_tree()->do_set_right(*this);
            }

            other._base.flags &= ~(node_base::NODE_LEFTMOST | node_base::NODE_RIGHTMOST);
        } else {
            other.move_kid(0, *this, 0);
        }

        other.move_to(*this, 0, other._base.num_keys);

        if (!is_root()) {
            _parent.n = other._parent.n;
            kid_index i = _parent.n->index_for(&other);
            _parent.n->_kids[i] = this;
        } else {
            other._parent.t->do_set_root(*this);
        }
    }

    node(const node& other) = delete;
    ~node() {
        SCYLLA_ASSERT(_base.num_keys == 0);
    }

    size_t storage_size() const noexcept {
        return is_linear() ? linear_node_size(_base.capacity) :
            is_leaf() ? leaf_node_size : inner_node_size;
    }

private:
    template <typename... Args>
    static node* construct(size_t size, Args&&... args) {
        void* mem = current_allocator().alloc<node>(size);
        return new (mem) node(std::forward<Args>(args)...);
    }

    static node* create_leaf() { return construct(leaf_node_size, NodeSize, node_base::NODE_LEAF); }
    static node* create_inner() { return construct(inner_node_size, NodeSize, 0); }

    static node* create_empty_root() {
        if (make_linear_root) {
            return construct(node::linear_node_size(1), 1,
                    node_base::NODE_LINEAR | node_base::NODE_ROOT | node_base::NODE_LEAF |
                    node_base::NODE_LEFTMOST | node_base::NODE_RIGHTMOST);
        } else {
            node* n = node::create_leaf();
            n->_base.flags |= node_base::NODE_ROOT | node_base::NODE_LEFTMOST | node_base::NODE_RIGHTMOST;
            return n;
        }
    }

    static void destroy(node& n) noexcept {
        current_allocator().destroy(&n);
    }

    void drop() noexcept {
        SCYLLA_ASSERT(!(is_leftmost() || is_rightmost()));
        if (Debug == with_debug::yes && !is_root()) {
            node* p = _parent.n;
            if (p->_base.num_keys != 0) {
                for (kid_index i = 0; i <= p->_base.num_keys; i++) {
                    SCYLLA_ASSERT(p->_kids[i] != this);
                }
            }
        }
        destroy(*this);
    }

    /*
     * Finds the key in the node or the subtree in which to continue
     * the search.
     */
    template <typename K>
    key_index index_for(const K& k, const Compare& cmp, bool& match) const {
        return searcher<K, Key, Hook, Compare, Search>::ge(k, _base, cmp, match);
    }

    // Two helpers for raw pointers lookup.
    kid_index index_for(const node* kid) const noexcept {
        SCYLLA_ASSERT(!is_leaf());

        for (kid_index i = 0; i <= _base.num_keys; i++) {
            if (_kids[i] == kid) {
                return i;
            }
        }

        std::abort();
    }

    bool need_refill() const noexcept {
        return _base.num_keys <= NodeHalf;
    }

    bool need_collapse_root() const noexcept {
        return !is_leaf() && (_base.num_keys == 0);
    }

    bool can_grab_from() const noexcept {
        return _base.num_keys > NodeHalf + 1u;
    }

    bool can_push_to() const noexcept {
        return _base.num_keys < NodeSize;
    }

    bool can_merge_with(const node& n) const noexcept {
        return _base.num_keys + n._base.num_keys + 1u <= NodeSize;
    }

    // Make a room for a new key (and kid) at \at position
    void shift_right(size_t at) noexcept {
        for (size_t i = _base.num_keys; i > at; i--) {
            move_key(i - 1, i);
            if (!is_leaf()) {
                move_kid(i, i + 1);
            }
        }
        _base.num_keys++;
    }

    // Occupy the hole at \at after key (and kid) removal
    void shift_left(size_t at) noexcept {
        _base.num_keys--;
        for (size_t i = at; i < _base.num_keys; i++) {
            move_key(i + 1, i);
            if (!is_leaf()) {
                move_kid(i + 2, i + 1);
            }
        }
    }

    // Move keys (and kids) to other node
    void move_to(node& to, size_t off, size_t nr) noexcept {
        for (size_t i = 0; i < nr; i++) {
            move_key(i + off, to, to._base.num_keys + i);
            if (!is_leaf()) {
                move_kid(i + off + 1, to, to._base.num_keys + i + 1);
            }
        }
        _base.num_keys -= nr;
        to._base.num_keys += nr;
    }

    void maybe_allocate_nodes(prealloc& nodes) const {
        // this is full leaf

        nodes.push(node::create_leaf());
        if (is_root()) {
            nodes.push(node::create_inner());
            return;
        }

        const node* cur = _parent.n;
        while (cur->_base.num_keys == NodeSize) {
            nodes.push(node::create_inner());
            if (cur->is_root()) {
                nodes.push(node::create_inner());
                break;
            }
            cur = cur->_parent.n;
        }
    }

    // Constants for linear node shattering into a tree

    // Nr of leaves to keep LinearThreshold keys (inc. keys in the root)
    static constexpr size_t ShatterLeaves = (LinearThreshold + NodeSize + 1) / (NodeSize + 1);
    // This many keys will be put into leaves themselves
    static constexpr size_t ShatterKeysInLeaves = LinearThreshold - (ShatterLeaves - 1);
    // Each leaf gets this amount of keys ...
    static constexpr size_t ShatterKeysPerLeaf = ShatterKeysInLeaves / ShatterLeaves;
    // ... plus 0 or 1 from the remainder
    static constexpr size_t ShatterKeysRemain = ShatterKeysInLeaves % ShatterLeaves;

    /*
     * Break the linear node into a small tree. The result is 1-level tree
     * with leaves evenly filled with the keys.
     *
     * Since this method is called on insertion, it also returns back the
     * new node and updates the insertion index.
     */
    node* shatter(prealloc& nodes, kid_index& idx) noexcept {
        node* new_insertion = nullptr;

        node* root = nodes.pop(false);
        root->_base.flags |= node_base::NODE_ROOT;
        _parent.t->do_set_root(*root);

        node* leaf = nodes.pop(true);
        root->set_kid(root->_base.num_keys, leaf);
        leaf->_base.flags |= node_base::NODE_LEFTMOST;
        _parent.t->do_set_left(*leaf);

        key_index src = 0;
        ssize_t rem = ShatterKeysRemain;

        auto adjust_idx = [&] () noexcept {
            if (new_insertion == nullptr && src == idx) {
                new_insertion = leaf;
                idx = leaf->_base.num_keys;
            }
        };

        while (true) {
            adjust_idx();
            move_key(src++, *leaf, leaf->_base.num_keys++);

            if (src == _base.num_keys) {
                leaf->_base.flags |= node_base::NODE_RIGHTMOST;
                _parent.t->do_set_right(*leaf);
                break;
            }

            if (leaf->_base.num_keys == ShatterKeysPerLeaf + (rem > 0 ? 1 : 0)) {
                rem--;
                adjust_idx();
                move_key(src++, *root, root->_base.num_keys++);
                leaf = nodes.pop(true);
                root->set_kid(root->_base.num_keys, leaf);
                SCYLLA_ASSERT(src != _base.num_keys); // need more keys for the next leaf
            }
        }
        adjust_idx();

        _base.num_keys = 0;
        _base.flags &= ~(node_base::NODE_LEFTMOST | node_base::NODE_RIGHTMOST);
        drop();

        SCYLLA_ASSERT(new_insertion != nullptr);
        return new_insertion;
    }

    node* check_linear_capacity(kid_index& idx) {
        SCYLLA_ASSERT(make_linear_root && is_root() && is_leaf());

        if (_base.num_keys < _base.capacity) {
            return this;
        }

        if (_base.capacity < LinearThreshold) {
            size_t ncap = std::min<size_t>(LinearThreshold, _base.capacity * 2);
            node* n = node::construct(linear_node_size(ncap), ncap, std::move(*this));
            drop();
            return n;
        }

        /*
         * Here we have the linear node fully packed with
         * LinearThreshold keys, thus they need ShatterLeaves
         * leaves and one inner root.
         */

        prealloc nodes;
        nodes.push(node::create_inner());
        for (size_t i = 0; i < ShatterLeaves; i++) {
            nodes.push(node::create_leaf());
        }

        return shatter(nodes, idx);
    }

    /*
     * This is the only throwing part of the insertion. It
     * pre-allocates the nodes (if needed), then grabs the
     * key from the pointer and dives into the non-failing
     * continuation
     */
    template <typename KeyPointer>
    void insert(kid_index idx, KeyPointer kptr) {
        /*
         * Although keys may live at any level, insertion always
         * starts with the leaf. Upper levels get their new keys
         * only if these come up from the deep.
         */
        SCYLLA_ASSERT(is_leaf());

        if (_base.num_keys < _base.capacity) {
            /*
             * Most expected case -- just put the key into leaf.
             * Linear node also goes through it.
             */
            do_insert(idx, kptr.release()->*Hook, nullptr);
            return;
        }

        prealloc nodes;
        maybe_allocate_nodes(nodes);
        insert_into_full(idx, kptr.release()->*Hook, nullptr, nodes);
    }

    void insert(kid_index idx, member_hook& key, node* kid, prealloc& nodes) noexcept {
        if (_base.num_keys < NodeSize) {
            do_insert(idx, key, kid);
        } else {
            insert_into_full(idx, key, kid, nodes);
        }
    }

    void insert_into_full(kid_index idx, member_hook& key, node* kid, prealloc& nodes) noexcept {
        if (!is_root()) {
            /*
             * An exception from classical B-tree split-balancing -- an
             * attempt to move the keys between siblings if they allow
             * for it.
             *
             * There are 2 pairs of symmetrical options for this -- when
             * either left or right siblings can accept more keys we put
             * there the parent's key that sits between us and that sibling
             * (sort of separation key), then put our's corner key into
             * the parent, then put the newcomer into the freed slot.
             *
             * A corner case in each pair -- when the newcomer goes at the
             * left- or rightmost slot on this node. In this case parent
             * immediately gets the new key, current node is not updated.
             *
             * Like this (push-right case, 4 keys per node)
             *
             * this --> ACDE   G   HIJ <--- right sibling
             *                 |
             *                 parent key in between
             *
             * if we insert B, then first shift C ... G right
             *          A_CD   E   GHIJ
             * then put B into the free slot
             *          ABCD   E   GHIJ
             *
             * if we insert F, then first shift G right
             *          ACDE   _   GHIJ
             * then put F into the parent's free slot
             *          ACDE   F   GHIJ
             */
            node* p = _parent.n;
            kid_index i = p->index_for(this);

            if (i > 0) {
                node* left = p->_kids[i - 1];
                if (left->can_push_to()) {
                    if (idx > 0) {
                        left->grab_from_right(this, i - 1);
                        /*
                         * We've moved the 0th element from this, so the index
                         * for the new key shifts too
                         */
                        idx--;
                    } else if (is_leaf()) {
                        SCYLLA_ASSERT(kid == nullptr);
                        p->move_key(i - 1, *left, left->_base.num_keys);
                        left->_base.num_keys++;
                        p->set_key(i - 1, &key);
                        return;
                    }
                }
            }

            if (i < p->_base.num_keys) {
                node* right = p->_kids[i + 1];
                if (right->can_push_to()) {
                    if (idx < _base.num_keys) {
                        right->grab_from_left(this, i + 1);
                    } else if (is_leaf()) {
                        SCYLLA_ASSERT(kid == nullptr);
                        right->shift_right(0);
                        p->move_key(i, *right, 0);
                        p->set_key(i, &key);
                        return;
                    }
                }
            }

            if (_base.num_keys < NodeSize) {
                do_insert(idx, key, kid);
                return;
            }
        }

        split_and_insert(idx, key, kid, nodes);
    }

    void split_and_insert(kid_index idx, member_hook& key, node* kid, prealloc& nodes) noexcept {
        node* n = nodes.pop(is_leaf());
        size_t off = NodeHalf + 1;

        if (is_leaf() && is_rightmost()) {
            /*
             * Link the right-most leaf. Leftmost cannot be updated here, the
             * new node is always to the right.
             */
            _base.flags &= ~node_base::NODE_RIGHTMOST;
            n->_base.flags |= node_base::NODE_RIGHTMOST;
            corner_tree()->do_set_right(*n);
        }

        /*
         * Insertion with split.
         *
         * The existing node is split into two halves (for odd case -- almost
         * halves), then the new key goes into either part and parent gets
         * a new key.
         *
         * One corner case here is when the new key is in the middle and it's
         * _it_ who gets into the parent.
         *
         * The algo is the same for both -- leaves and inner nodes.
         */

        if (idx == off) {
            /*
             * Here's what we have here:
             *
             *   parent->  A . H
             *               |
             *       this->  BDFG
             *
             * and want to insert E here. The new key is in the middle, so
             * it goes to parent node and the result would look like this
             *
             *   parent->  A . E . H
             *               |   |
             *      this->  BD   FG <- new node
             */
            move_to(*n, off, NodeSize - off);
            if (!is_leaf()) {
                n->_kids[0] = kid;
                kid->_parent.n = n;
            }
            insert_into_parent(key, n, nodes);
        } else {
            /*
             * That's another case, e.g. like this:
             *
             *   parent->  A . H
             *               |
             *       this->  BDFG
             *
             * and want to insert C here. The new key is left from the middle,
             * so push the left half's right key up and put C into it:
             *
             *   parent->  A . D . G
             *               |   |
             *      this->  BC   EF <- new node
             */
            if (idx < off) {
                move_to(*n, off, NodeSize - off);
                do_insert(idx, key, kid);
            } else {
                off++;
                move_to(*n, off, NodeSize - off);
                n->do_insert(idx - off, key, kid);
            }

            if (!is_leaf()) {
                move_kid(_base.num_keys, *n, 0);
            }
            _base.num_keys--;
            insert_into_parent(*_base.keys[_base.num_keys], n, nodes);
        }
    }

    void do_insert(kid_index idx, member_hook& key, node* kid) noexcept {
        /*
         * The key:kid pair belongs to keys[idx-1]:kids[idx] subtree, and since
         * what's already there is less than this newcomer, the latter goes
         * one step right.
         */
        shift_right(idx);
        set_key(idx, &key);
        if (kid != nullptr) {
            _kids[idx + 1] = kid;
            kid->_parent.n = this;
        }
    }

    void insert_into_parent(member_hook& key, node* kid, prealloc& nodes) noexcept {
        if (is_root()) {
            insert_into_root(key, kid, nodes);
        } else {
            kid_index idx = _parent.n->index_for(this);
            _parent.n->insert(idx, key, kid, nodes);
        }
    }

    void insert_into_root(member_hook& key, node* kid, prealloc& nodes) noexcept {
        tree* t = _parent.t;

        node* nr = nodes.pop(false);

        nr->_base.num_keys = 1;
        nr->set_key(0, &key);

        nr->_kids[0] = this;
        this->_parent.n = nr;
        nr->_kids[1] = kid;
        kid->_parent.n = nr;

        _base.flags &= ~node_base::NODE_ROOT;
        nr->_base.flags |= node_base::NODE_ROOT;
        t->do_set_root(*nr);
    }

    void remove(kid_index idx) noexcept {
        if (is_leaf()) { // ... or linear
            remove_key(idx);
        } else {
            remove_from_inner(idx);
        }
    }

    void remove_key(kid_index idx) noexcept {
        shift_left(idx);
        check_refill();
    }

    void remove_leftmost_light_rebalance() noexcept {
        shift_left(0);
        check_light_refill();
    }

    void check_refill() noexcept {
        if (!is_root()) {
            if (need_refill()) {
                refill();
            }
        } else if (need_collapse_root()) {
            collapse_root();
        }
    }

    void check_light_refill() noexcept {
        if (_base.num_keys == 0) {
            if (!is_root()) {
                light_refill();
            } else if (!is_leaf()) {
                collapse_root();
            }
        }
    }

    void collapse_root() noexcept {
        node& nr = *_kids[0];
        nr._base.flags |= node_base::NODE_ROOT;
        _parent.t->do_set_root(nr);
        drop();
    }

    void grab_from_left(node* left, key_index idx) noexcept {
        /*
         * Shift keys right -- left sibling's right key goes to parent,
         * parent's goes to us. Like this
         *
         * left --> ABC  D   EF  <-- this
         *               |
         *               parent key in between
         *
         * gets transformed into
         *
         * left -->  AB  C   DEF  <-- this
         */
        shift_right(0);
        _parent.n->move_key(idx - 1, *this, 0);
        left->move_key(left->_base.num_keys - 1, *_parent.n, idx - 1);
        if (!is_leaf()) {
            move_kid(0, 1);
            left->move_kid(left->_base.num_keys, *this, 0);
        }

        left->_base.num_keys--;
    }

    void grab_from_right(node* right, key_index idx) noexcept {
        /*
         * Shift keys left -- rights sibling's zeroth key goes to parent,
         * parent's goes to us. Like this
         *
         * this -->  AB  C   DEF <-- right
         *               |
         *               parent key in between
         *
         * gets transformed into
         *
         * this --> ABC  D   EF  <-- right
         */
        _parent.n->move_key(idx, *this, _base.num_keys);
        right->move_key(0, *_parent.n, idx);
        if (!is_leaf()) {
            right->move_kid(0, *this, _base.num_keys + 1);
            right->move_kid(1, 0);
        }
        right->shift_left(0);

        _base.num_keys++;
    }

    void merge_kids(node& t, node& n, key_index idx) noexcept {
        /*
         * Merge two kids together (this points to their parent node)
         * and put the key that was between them into the new node as
         * well. Respectively, the current filling of nodes should be
         *      a.num_keys + b.num_keys + 1 <= NodeSize
         * but that's checked by the caller. The process looks like
         *
         * t -->  A  B  C <-- n
         *           |
         *           parent key in between (at \idx position)
         *
         *  goes into
         *
         * t -->  ABC  _  X <-- n gets removed
         *             | parent loses one key
         */
        move_key(idx, t, t._base.num_keys);
        if (!t.is_leaf()) {
            n.move_kid(0, t, t._base.num_keys + 1);
        }
        t._base.num_keys++;
        n.move_to(t, 0, n._base.num_keys);
        n._base.num_keys = 0;

        /*
         * First unlink the node from tree/parent, then drop it, so that
         * the drop's and destructor's asserts do not find this node in
         * unexpected state.
         */
        if (n.is_leaf()) {
            n.unlink_corner_leaf();
        }
        shift_left(idx);
        n.drop();
    }

    void merge_kids_and_refill(node& t, node& n, key_index idx) noexcept {
        merge_kids(t, n, idx);
        check_refill();
    }

    void refill() noexcept {
        kid_index idx = _parent.n->index_for(this);
        node* right = idx < _parent.n->_base.num_keys ? _parent.n->_kids[idx + 1] : nullptr;
        node* left = idx > 0 ? _parent.n->_kids[idx - 1] : nullptr;

        /*
         * The node is "underflown" (see comment near NodeHalf
         * about what this means), so we try to refill it at the
         * siblings' expense. Many cases possible, but we go with
         * two pairs -- either of the siblings has large enough
         * keys to give us one or it has small enough heys to be
         * merged with us (and one more key from the parent).
         */

        if (left != nullptr && left->can_grab_from()) {
            grab_from_left(left, idx);
            return;
        }

        if (right != nullptr && right->can_grab_from()) {
            grab_from_right(right, idx);
            return;
        }

        if (left != nullptr && can_merge_with(*left)) {
            _parent.n->merge_kids_and_refill(*left, *this, idx - 1);
            return;
        }

        if (right != nullptr && can_merge_with(*right)) {
            _parent.n->merge_kids_and_refill(*this, *right, idx);
            return;
        }

        /*
         * Susprisingly, the node in the B-tree can violate the
         * "minimally filled" rule for non roots. It _can_ stay with
         * less than half elements on board. The next remove from
         * it or either of its siblings will probably refill it.
         */
    }

    void light_refill() noexcept {
        SCYLLA_ASSERT(_parent.n->_base.num_keys > 0);
        node* right = _parent.n->_kids[1];

        /*
         * The current node is empty and needs to either go away
         * from the tree or get refilled.
         *
         * In case our right sibling can carry one more key (which
         * is the same as it can be merged with current empty node)
         * then we just perform regular merge, which will move all
         * the keys from right node on the current one (plus the
         * parent's separation key). Note that this is NOT worse
         * than just updating the parent's 0th key to point to the
         * right kid -- in the latter case we'd still have to shift
         * the whole right kid right to make room for the parent
         * separation key at its 0 position, so it's moving the
         * whole node anyway.
         *
         * In case our right sibling is full there's no choice but
         * to grab a key from it and continue. Next time we get
         * here the right node will be int mergeable state.
         */

        if (can_merge_with(*right)) {
            _parent.n->merge_kids(*this, *right, 0);
            _parent.n->check_light_refill();
        } else {
            grab_from_right(right, 0);
        }
    }


    void remove_from_inner(kid_index idx) noexcept {
        /*
         * Removing from inner node is only possible if the
         * respecrtive kids get squashed together, but the
         * latter is (almost) impossible, as nodes are kept
         * at least half-filled. Thus the only way here is to
         * go down to the previous key and replace the key
         * being removed from this[idx] with that one. The
         * previous key sits ... on the leaf, so go dive as
         * deep as we can and move the key from there.
         */
        node* rightmost = _kids[idx + 1];

        while (!rightmost->is_leaf()) {
            rightmost = rightmost->_kids[0];
        }

        rightmost->move_key(0, *this, idx);

        /*
         * Whee, we've just removed one key from the leaf. Time
         * to go up again and rebalance the tree.
         */
        rightmost->remove_key(0);
    }

    template <typename KFunc>
    void clear(KFunc&& k_clear) noexcept {
        size_t nk = _base.num_keys;
        _base.num_keys = 0;

        if (!is_leaf()) {
            for (kid_index i = 0; i <= nk; i++) {
                _kids[i]->clear(k_clear);
                destroy(*_kids[i]);
            }
        }

        for (key_index i = 0; i < nk; i++) {
            k_clear(_base.keys[i]);
        }
    }

    template <typename Cloner, typename Deleter>
    node* clone(node*& left_leaf, node*& right_leaf, Cloner&& cloner, Deleter&& deleter) const {
        node* n;

        if (is_linear()) {
            n = construct(linear_node_size(_base.capacity), _base.capacity, _base.flags);
        } else if (is_leaf()) {
            n = create_leaf();
        } else {
            n = create_inner();
        }

        key_index ki = 0;
        kid_index ni = 0;

        try {
            for (ki = 0; ki < _base.num_keys; ki++) {
                Key* key = cloner(_base.keys[ki]->to_key<Key, Hook>());
                n->set_key(ki, &(key->*Hook));
            }

            if (is_leaf()) {
                if (left_leaf == nullptr) {
                    left_leaf = n;
                }
                right_leaf = n;
            } else {
                for (ni = 0; ni <= _base.num_keys; ni++) {
                    n->_kids[ni] = _kids[ni]->clone(left_leaf, right_leaf, cloner, deleter);
                    n->_kids[ni]->_parent.n = n;
                }
            }

            n->_base.num_keys = _base.num_keys;

        } catch(...) {
            while (ki != 0) {
                node::dispose_key(n->_base.keys[--ki], deleter);
            }
            while (ni != 0) {
                n->_kids[ni - 1]->clear([&deleter] (member_hook* h) { node::dispose_key(h, deleter); });
                destroy(*n->_kids[--ni]);
            }
            destroy(*n);

            // No need to "reset" left_leaf/right_leaf on exception as these
            // pointers are only valid iff clone() returns successfully
            throw;
        }

        return n;
    }

    size_t size_slow() const noexcept {
        size_t ret = _base.num_keys;
        if (!is_leaf()) {
            for (kid_index i = 0; i <= _base.num_keys; i++) {
                ret += _kids[i]->size_slow();
            }
        }
        return ret;
    }

    size_t external_memory_usage() const noexcept {
        if (is_linear()) {
            SCYLLA_ASSERT(is_leaf());
            return linear_node_size(_base.capacity);
        }

        if (is_leaf()) {
            return leaf_node_size;
        }

        size_t size = inner_node_size;
        SCYLLA_ASSERT(_base.num_keys != 0);
        for (kid_index i = 0; i <= _base.num_keys; i++) {
            size += _kids[i]->external_memory_usage();
        }
        return size;
    }

    void fill_stats(struct stats& st) const noexcept {
        if (is_linear()) {
            st.linear_keys = _base.num_keys;
        } else if (is_leaf()) {
            st.leaves_filled[_base.num_keys]++;
            st.leaves++;
        } else {
            st.nodes_filled[_base.num_keys]++;
            st.nodes++;
            SCYLLA_ASSERT(_base.num_keys != 0);
            for (kid_index i = 0; i <= _base.num_keys; i++) {
                _kids[i]->fill_stats(st);
            }
        }
    }

    class prealloc {
        node* _nodes;
        node** _tail = &_nodes;

        node* pop() noexcept {
            SCYLLA_ASSERT(!empty());
            node* ret = _nodes;
            _nodes = ret->_parent.n;
            if (_tail == &ret->_parent.n) {
                _tail = &_nodes;
            }
            return ret;
        }

        bool empty() const noexcept { return _tail == &_nodes; }

        void drain() noexcept {
            while (!empty()) {
                node* n = pop();
                node::destroy(*n);
            }
        }

    public:
        void push(node* n) noexcept {
            *_tail = n;
            _tail = &n->_parent.n;
        }

        node* pop(bool leaf) noexcept {
            node* ret = pop();
            SCYLLA_ASSERT(leaf == ret->is_leaf());
            return ret;
        }

        ~prealloc() {
            drain();
        }
    };
};

} // namespace
