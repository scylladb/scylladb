/*
 * Copyright (C) 2016 ScyllaDB
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

#include <boost/intrusive/set.hpp>

namespace bi = boost::intrusive;

typedef bi::rbtree_algorithms<bi::rbtree_node_traits<void*, true>> algo;

class intrusive_set_member_hook : public bi::set_member_hook<bi::optimize_size<true>> {
public:
    intrusive_set_member_hook() = default;
    intrusive_set_member_hook(intrusive_set_member_hook&& o) noexcept {
        algo::replace_node(o.this_ptr(), this_ptr());
        algo::init(o.this_ptr());
    }
};

template<typename Elem,
         intrusive_set_member_hook Elem::* PtrToMember,
         typename Comparator>
class intrusive_set final {
    using set_type = bi::set<Elem,
                             bi::member_hook<Elem, intrusive_set_member_hook, PtrToMember>,
                             bi::compare<Comparator>>;
public:
    typedef Elem value_type;
    typedef typename set_type::iterator iterator;
    typedef typename set_type::const_iterator const_iterator;
    typedef typename set_type::reverse_iterator reverse_iterator;
    typedef typename set_type::const_reverse_iterator const_reverse_iterator;
private:
    set_type _set;
public:
    intrusive_set(Comparator c) : _set(std::move(c)) { }
    Comparator key_comp() const { return _set.key_comp(); }
    iterator begin() { return _set.begin(); }
    const_iterator begin() const { return _set.begin(); }
    iterator end() { return _set.end(); }
    const_iterator end() const { return _set.end(); }
    reverse_iterator rbegin() { return _set.rbegin(); }
    const_reverse_iterator rbegin() const { return _set.rbegin(); }
    reverse_iterator rend() { return _set.rend(); }
    const_reverse_iterator rend() const { return _set.rend(); }
    iterator lower_bound(const Elem &key) { return _set.lower_bound(key); }
    template<class KeyType, class KeyTypeKeyCompare>
    iterator upper_bound(const KeyType& key, KeyTypeKeyCompare comp) { return _set.upper_bound(key, comp); }
    template<class KeyType, class KeyTypeKeyCompare>
    const_iterator upper_bound(const KeyType& key, KeyTypeKeyCompare comp) const { return _set.upper_bound(key, comp); }
    const_iterator lower_bound(const Elem &key) const { return _set.lower_bound(key); }
    Elem* unlink_leftmost_without_rebalance() { return _set.unlink_leftmost_without_rebalance(); }
    iterator insert_before(const_iterator pos, Elem& value) { return _set.insert_before(pos, value); }
    template<class Disposer>
    void clear_and_dispose(Disposer disposer) { _set.clear_and_dispose(disposer); }
    template <class Cloner, class Disposer>
    void clone_from(const intrusive_set &src, Cloner cloner, Disposer disposer) {
        _set.clone_from(src._set, cloner, disposer);
    }
    iterator find(const Elem &key) { return _set.find(key); }
    const_iterator find(const Elem &key) const { return _set.find(key); }
    template<class KeyType, class KeyTypeKeyCompare>
    iterator find(const KeyType &key, KeyTypeKeyCompare comp) { return _set.find(key, comp); }
    template<class KeyType, class KeyTypeKeyCompare>
    const_iterator find(const KeyType &key, KeyTypeKeyCompare comp) const { return _set.find(key, comp); }
    iterator insert(const_iterator hint, Elem& value) { return _set.insert(hint, value); }
    template<class Disposer>
    iterator erase_and_dispose(const_iterator i, Disposer disposer) {
        return _set.erase_and_dispose(i, disposer);
    }
    iterator erase(const_iterator i) { return _set.erase(i); }
    iterator erase(const_iterator b, const_iterator e) { return _set.erase(b, e); }
    template<class Disposer>
    iterator erase_and_dispose(const_iterator b, const_iterator e, Disposer disposer) {
        return _set.erase_and_dispose(b, e, disposer);
    }
    bool empty() const { return _set.empty(); }
    auto size() const { return _set.size(); }
};
