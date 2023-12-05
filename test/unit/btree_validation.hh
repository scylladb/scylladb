/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <stddef.h>
#include "utils/intrusive_btree.hh"

namespace intrusive_b {

class member_hook;

template <typename Key, member_hook Key::* Hook, typename Compare, size_t NodeSize, size_t LinearThreshold>
class validator {
    using key = Key;
    using tree = class tree<Key, Hook, Compare, NodeSize, LinearThreshold, key_search::both, with_debug::yes>;
    using node = class node<Key, Hook, Compare, NodeSize, LinearThreshold, key_search::both, with_debug::yes>;

    bool validate(const tree& t, const node* n, unsigned long& prev_key) {
        if (n->is_root() && n->_parent.t != &t) {
            fmt::print("invalid root pointer @{}\n", n->id());
            return false;
        }

        if (!n->is_leaf()) {
            for (size_t i = 0; i <= n->_base.num_keys; i++) {
                node* k = n->_kids[i];
                if (k == nullptr) {
                    fmt::print("node {} kid {} is null\n", n->id(), i);
                    return false;
                }
                if (k->_parent.n != n) {
                    fmt::print("node {} parent != {}\n", k->id(), n->id());
                    return false;
                }

                if (!validate(t, k, prev_key)) {
                    return false;
                }

                if (i != n->_base.num_keys) {
                    member_hook* h = n->_base.keys[i];
                    if (h == nullptr) {
                        fmt::print("node {} key {} is null\n", n->id(), i);
                        return false;
                    }
                    if (h->_node != &n->_base) {
                        fmt::print("node {} key {} is misowned\n", n->id(), i);
                        return false;
                    }

                    Key& k = *h->to_key<Key, Hook>();
                    if ((unsigned long)k < prev_key) {
                        fmt::print("key misorder on node {}, idx {}\n", n->id(), i);
                        return false;
                    }

                    prev_key = (unsigned long)k;
                }
            }
        } else {
            if (n->is_leftmost()) {
                if (t._corners.left != n) {
                    fmt::print("leftmost tree {}, expected {}\n", t._corners.left->id(), n->id());
                    return false;
                }
                if (!n->is_root() && n->_leaf_tree != &t) {
                    fmt::print("leftmost tree broken for {}\n", n->id());
                    return false;
                }
            }
            if (n->is_rightmost()) {
                if (t._corners.right != n) {
                    fmt::print("rightmost tree {}, expected {}\n", t._corners.right->id(), n->id());
                    return false;
                }
                if (!n->is_root() && n->_leaf_tree != &t) {
                    fmt::print("rightmost tree broken for {}\n", n->id());
                    return false;
                }
            }
            for (size_t i = 0; i < n->_base.num_keys; i++) {
                member_hook* h = n->_base.keys[i];
                Key& k = *h->to_key<Key, Hook>();

                if ((unsigned long)k < prev_key) {
                    fmt::print("key misorder on leaf {}\n", n->id());
                    return false;
                }

                prev_key = (unsigned long)k;
            }
        }

        return true;
    }

    void print_node(const node& n, char pfx, int indent) {
        if (n.is_leaf()) {
            fmt::print("{:<{}} {} ({}/{} keys) (l{}{}{}):", pfx, indent, n.id(), n._base.num_keys, n._base.capacity,
                    n.is_root() ? "r" : "-", n.is_leftmost() ? "<" : "-", n.is_rightmost() ? ">" : "-");
            int i;
            for (i = 0; i < n._base.num_keys; i++) {
                Key& k = *n._base.keys[i]->template to_key<Key, Hook>();
                fmt::print(" {}", (unsigned long)k);
            }
            fmt::print("\n");
        } else {
            fmt::print("{:<{}} {} ({} keys) (i{})\n", pfx, indent, n.id(), n._base.num_keys,
                    n.is_root() ? "r" : "-");
            int i;
            for (i = 0; i < n._base.num_keys; i++) {
                print_node(*n._kids[i], pfx, indent + 2);
                Key& k = *n._base.keys[i]->template to_key<Key, Hook>();
                fmt::print("{:<{}} - {} -\n", pfx, indent, (unsigned long)k);
            }
            print_node(*n._kids[i], pfx, indent + 2);
        }
    }

public:
    bool validate(const tree& t) {
        if (t._root == nullptr) {
            return true;
        }

        unsigned long pk = 0;
        return validate(t, t._root, pk);
    }

    void print_tree(const tree& t, char pfx) {
        fmt::print("/--- TREE ---\n");
        if (t._root != nullptr) {
            fmt::print("{} <- {} -> {}\n", t._corners.left->id(), t._root->id(), t._corners.right->id());
            print_node(*t._root, pfx, 0);
        } else {
            fmt::print("inline {} keys: ", t._inline.num_keys);
            for (int i = 0; i < t._inline.num_keys; i++) {
                const Key& k = *t._inline.keys[i]->template to_key<Key, Hook>();
                fmt::print(" {}", (unsigned long)k);
            }
            fmt::print("\n");
        }
        fmt::print("\\------------\n");
    }
};

template <typename Key, member_hook Key::* Hook, typename Compare, size_t NodeSize, size_t LinearThreshold>
class iterator_checker {
    using tree = class tree<Key, Hook, Compare, NodeSize, LinearThreshold, key_search::both, with_debug::yes>;
    using validator = class validator<Key, Hook, Compare, NodeSize, LinearThreshold>;

    validator _tv;
    tree& _tree;
    typename tree::iterator _pos;
    unsigned long _pk = 0;

    bool step(tree& t, bool& valid) {
        if (_pk == 0) {
            return false;
        }

        if (_pos == t.end()) {
            return false;
        }

        _pos++;
        if (_pos == t.end()) {
            return false;
        }

        unsigned long k = (unsigned long)(*_pos);
        if (_pk >= k) {
            fmt::print("iterator misorder {} -> {}\n", _pk, (unsigned long)k);
            valid = false;
            return true;
        }

        _pos++;
        _pos--;

        if (k != (unsigned long)(*_pos)) {
            fmt::print("iterator backward miss {} <- {}\n", (unsigned long)k, (unsigned long)(*_pos));
            valid = false;
            return true;
        }

        _pk = (unsigned long)k;
        valid = true;
        return true;
    }

public:
    iterator_checker(validator& tv, tree& t) : _tv(tv), _tree(t) {}

    bool step() {
        bool valid;

        if (step(_tree, valid)) {
            return valid;
        }

        _pos = _tree.begin();
        if (_pos == _tree.end()) {
            return true;
        }

        _pk = (unsigned long)(*_pos);
        return true;
    }

    bool here(const Key& k) {
        return _pk != 0 && *_pos == k;
    }
};

} // namespace
