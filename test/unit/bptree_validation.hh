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

#include <stddef.h>
#include "utils/bptree.hh"

namespace bplus {

template <typename K, typename T, typename Less, size_t NodeSize>
class validator {
    using tree = class tree<K, T, Less, NodeSize, key_search::both, with_debug::yes>;
    using node = typename tree::node;

    void validate_node(const tree& t, const node& n, int& prev, int& min, bool is_root);
    void validate_list(const tree& t);

public:
    void print_tree(const tree& t, char pfx) const {
        fmt::print("/ {} <- | {} | -> {}\n", t._left->id(), t._root->id(), t._right->id());
        print_node(*t._root, pfx, 2);
        fmt::print("\\\n");
    }

    void print_node(const node& n, char pfx, int indent) const {
        int i;

        fmt::print("{:<{}c}{:s} {:d} ({:d} keys, {:x} flags):", pfx, indent,
                n.is_leaf() ? "leaf" : "node", n.id(), n._num_keys, n._flags);
        if (n.is_leaf()) {
            for (i = 0; i < n._num_keys; i++) {
                fmt::print(" {}", (int)n._keys[i].v);
            }
            fmt::print("\n");

            return;
        }
        fmt::print("\n");

        if (n._kids[0].n != nullptr) {
            print_node(*n._kids[0].n, pfx, indent + 2);
        }
        for (i = 0; i < n._num_keys; i++) {
            fmt::print("{:<{}c}---{}---\n", pfx, indent, (int)n._keys[i].v);
            print_node(*n._kids[i + 1].n, pfx, indent + 2);
        }
    }

    void validate(const tree& t);
};


template <typename K, typename T, typename L, size_t NS>
void validator<K, T, L, NS>::validate_node(const tree& t, const node& n, int& prev_key, int& min_key, bool is_root) {
    int i;

    if (n.is_root() != is_root) {
        fmt::print("node {} needs to {} root, but {}\n", n.id(), is_root ? "be" : "be not", n._flags);
        throw "root broken";
    }

    for (i = 0; i < n._num_keys; i++) {
        if (!n._keys[i].v.is_alive()) {
            fmt::print("node {} key {} is not alive\n", n.id(), i);
            throw "key dead";
        }
    }

    if (n.is_leaf()) {
        for (i = 0; i < n._num_keys; i++) {
            if (t._less(n._keys[i].v, K(prev_key))) {
                fmt::print("node misordered @{} (prev {})\n", (int)n._keys[i].v, prev_key);
                throw "misorder";
            }
            if (n._kids[i + 1].d->_leaf != &n) {
                fmt::print("data mispoint\n");
                throw "data backlink";
            }

            prev_key = n._keys[i].v;
            if (!n._kids[i + 1].d->value.match_key(n._keys[i].v)) {
                fmt::print("node value corrupted @{:d}.{:d}\n", n.id(), i);
                throw "data corruption";
            }
        }

        if (n._num_keys > 0) {
            min_key = (int)n._keys[0].v;
        }
    } else if (n._num_keys > 0) {
        node* k = n._kids[0].n;

        if (k->_parent != &n) {
            fmt::print("node {:d} -parent-> {:d}, expect {:d}\n", k->id(), k->_parent->id(), n.id());
            throw "mis-parented node";
        }
        validate_node(t, *k, prev_key, min_key, false);
        for (i = 0; i < n._num_keys; i++) {
            k = n._kids[i + 1].n;
            if (k->_parent != &n) {
                fmt::print("node {:d} -parent-> {:d}, expect {:d}\n",
                        k->id(), k->_parent ? k->_parent->id() : -1, n.id());
                throw "mis-parented node";
            }
            if (t._less(k->_keys[0].v, n._keys[i].v)) {
                fmt::print("node {:d}.{:d}, separation key {}, kid has {}\n", n.id(), k->id(),
                        (int)n._keys[i].v, (int)k->_keys[0].v);
                throw "separation key mismatch";
            }

            int min = 0;
            validate_node(t, *k, prev_key, min, false);
            if (t._less(n._keys[i].v, K(min)) || t._less(K(min), n._keys[i].v)) {
                fmt::print("node {:d}.[{:d}]{:d}, separation key {}, min {}\n",
                        n.id(), i, k->id(), (int)n._keys[i].v, min);
                if (strict_separation_key || t._less(K(min), n._keys[i].v)) {
                    throw "separation key screw";
                }
            }
        }
    }
}

template <typename K, typename T, typename L, size_t NS>
void validator<K, T, L, NS>::validate_list(const tree& t) {
    int prev = 0;

    node* lh = t.left_leaf_slow();
    node* rh = t.right_leaf_slow();

    if (lh != t._left) {
        fmt::print("left {:d}, slow {:d}\n", t._left->id(), lh->id());
        throw "list broken";
    }

    if (!(lh->_flags & node::NODE_LEFTMOST)) {
        fmt::print("left {:d} is not marked as such {}\n", t._left->id(), t._left->_flags);;
        throw "list broken";
    }

    if (rh != t._right) {
        fmt::print("right {:d}, slow {:d}\n", t._right->id(), rh->id());
        throw "list broken";
    }

    if (!(rh->_flags & node::NODE_RIGHTMOST)) {
        fmt::print("right {:d} is not marked as such {}\n", t._right->id(), t._right->_flags);;
        throw "list broken";
    }

    node* r = lh;
    while (1) {
        node *ln;

        if (!r->is_rightmost()) {
            ln = r->get_next();
            if (ln->get_prev() != r) {
                fmt::print("next leaf {:d} points to {:d}, expect {:d}\n", ln->id(), ln->get_prev()->id(), r->id());
                throw "list broken";
            }
        } else if (r->_rightmost_tree != &t) {
            fmt::print("right leaf doesn't point to tree\n");
            throw "list broken";
        }

        if (!r->is_leftmost()) {
            ln = r->get_prev();
            if (ln->get_next() != r) {
                fmt::print("prev leaf {:d} points to {:d}, expect {:d}\n", ln->id(), ln->get_next()->id(), r->id());
                throw "list broken";
            }
        } else if (r->_kids[0]._leftmost_tree != &t) {
            fmt::print("left leaf doesn't point to tree\n");
            throw "list broken";
        }

        if (r->_num_keys > 0 && t._less(r->_keys[0].v, K(prev))) {
            fmt::print("list misorder on element {:d}, keys {}..., prev {:d}\n", r->id(), (int)r->_keys[0].v, prev);
            throw "list broken";
        }

        if (!r->is_root() && r->_parent != nullptr) {
            const auto p = r->_parent;
            int i = p->index_for(r->_keys[0].v, t._less);
            if (i > 0) {
                if (p->_kids[i - 1].n != r->get_prev()) {
                    fmt::print("list misorder on parent check: node {:d}.{:d}, parent prev {:d}, list prev {:d}\n",
                            p->id(), r->id(), p->_kids[i - 1].n->id(), r->get_prev()->id());
                    throw "list broken";
                }
            }
            if (i < p->_num_keys - 1) {
                if (p->_kids[i + 1].n != r->get_next()) {
                    fmt::print("list misorder on parent check: node {:d}.{:d}, parent next {:d}, list next {:d}\n",
                            p->id(), r->id(), p->_kids[i + 1].n->id(), r->get_next()->id());
                    throw "list broken";
                }
            }
        }

        if (r->_num_keys > 0) {
            prev = (int)r->_keys[r->_num_keys - 1].v;
        }

        if (r != t._left && r != t._right && (r->_flags & (node::NODE_LEFTMOST | node::NODE_RIGHTMOST))) {
            fmt::print("middle {:d} is marked as left/right {}\n", r->id(), r->_flags);;
            throw "list broken";
        }

        if (r->is_rightmost()) {
            break;
        }

        r = r->get_next();
    }
}

template <typename K, typename T, typename L, size_t NS>
void validator<K, T, L, NS>::validate(const tree& t) {
    try {
        validate_list(t);
        int min = 0, prev = 0;
        if (t._root->_root_tree != &t) {
            fmt::print("root doesn't point to tree\n");
            throw "root broken";
        }

        validate_node(t, *t._root, prev, min, true);
    } catch (...) {
        print_tree(t, '|');
        fmt::print("[ ");
        node* lh = t._left;
        while (1) {
            fmt::print(" {:d}", lh->id());
            if (lh->is_rightmost()) {
                break;
            }
            lh = lh->get_next();
        }
        fmt::print("]\n");
        throw;
    }
}

template <typename K, typename T, typename Less, size_t NodeSize>
class iterator_checker {
    using tree = class tree<K, T, Less, NodeSize, key_search::both, with_debug::yes>;

    validator<K, T, Less, NodeSize>& _tv;
    tree& _t;
    typename tree::iterator _fwd, _fend;
    T _fprev;

public:
    iterator_checker(validator<K, T, Less, NodeSize>& tv, tree& t) : _tv(tv), _t(t),
            _fwd(t.begin()), _fend(t.end()) {
    }

    bool step() {
        try {
            return forward_check();
        } catch(...) {
            _tv.print_tree(_t, ':');
            throw;
        }
    }

    bool here(const K& k) {
        return _fwd != _fend && _fwd->match_key(k);
    }

private:
    bool forward_check() {
        if (_fwd == _fend) {
            return false;
        }
        _fwd++;
        if (_fwd == _fend) {
            return false;
        }
        T val = *_fwd;
        _fwd++;
        if (_fwd == _fend) {
            return false;
        }
        _fwd--;
        if (val != *_fwd) {
            fmt::print("Iterator broken, {:d} != {:d}\n", val, *_fwd);
            throw "iterator";
        }
        if (val < _fprev) {
            fmt::print("Iterator broken, {:d} < {:d}\n", val, _fprev);
            throw "iterator";
        }
        _fprev = val;

        return true;
    }
};

} // namespace

