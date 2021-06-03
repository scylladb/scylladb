/*
 * Copyright (C) 2016-present ScyllaDB
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

/*
 * (C) Copyright Ion Gaztanaga 2013-2014
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 */

#pragma once

#include <boost/intrusive/set.hpp>
#include <iterator>
#include <boost/intrusive/parent_from_member.hpp>

namespace bi = boost::intrusive;

typedef bi::rbtree_algorithms<bi::rbtree_node_traits<void*, true>> algo;

class intrusive_set_external_comparator_member_hook : public bi::set_member_hook<bi::optimize_size<true>, bi::link_mode<bi::auto_unlink>> {
public:
    intrusive_set_external_comparator_member_hook() = default;
    intrusive_set_external_comparator_member_hook(intrusive_set_external_comparator_member_hook&& o) noexcept {
        algo::replace_node(o.this_ptr(), this_ptr());
        algo::init(o.this_ptr());
    }
};

template<typename Elem,
         intrusive_set_external_comparator_member_hook Elem::* PtrToMember>
class intrusive_set_external_comparator final {
    typedef boost::intrusive::mhtraits<Elem, intrusive_set_external_comparator_member_hook, PtrToMember> value_traits;
    typedef typename value_traits::node_traits node_traits;
    typedef typename node_traits::node_ptr node_ptr;

    using boost_iterator = typename bi::tree_iterator<value_traits, false>;
    using boost_const_iterator = typename bi::tree_iterator<value_traits, true>;
public:
    typedef Elem value_type;

    struct iterator : boost_iterator {
        using boost_iterator::boost_iterator;
        iterator(boost_iterator it) noexcept : boost_iterator(it) { }
        iterator(const iterator& other) = default;
        iterator(iterator&& other) noexcept : boost_iterator(other) { }
        iterator& operator=(const iterator& other) = default;
        iterator& operator=(iterator&& other) noexcept {
            return *this = other;
        }
        iterator& operator++() {
            boost_iterator::operator++();
            return *this;
        }
        iterator operator++(int) {
            return iterator(boost_iterator::operator++(int()));
        }
        iterator unconst() const {
            return *this;
        }
    };
    struct const_iterator : boost_const_iterator {
        using boost_const_iterator::boost_const_iterator;
        const_iterator(boost_const_iterator it) noexcept : boost_const_iterator(it) { }
        const_iterator(const const_iterator& other) = default;
        const_iterator(const iterator& other) : boost_const_iterator(boost_iterator(other)) { }
        const_iterator(const_iterator&& other) noexcept : boost_const_iterator(other) { }
        const_iterator& operator=(const const_iterator& other) = default;
        const_iterator& operator=(const_iterator&& other) noexcept {
            return *this = other;
        }
        const_iterator& operator++() {
            boost_const_iterator::operator++();
            return *this;
        }
        const_iterator operator++(int) {
            return const_iterator(boost_const_iterator::operator++(int()));
        }
        iterator unconst() const {
            return iterator(boost_const_iterator::unconst());
        }
    };

    typedef typename std::reverse_iterator<iterator> reverse_iterator;
    typedef typename std::reverse_iterator<const_iterator> const_reverse_iterator;

private:
    intrusive_set_external_comparator_member_hook _header;
    static const value_traits _value_traits;

    struct key_of_value {
        typedef Elem type;
        Elem& operator()(Elem& t) { return t; }
    };

    template <typename Comparator>
    struct key_node_comparator {
        Comparator _cmp;
        const value_traits& _value_traits;
        key_node_comparator(Comparator cmp, const value_traits& value_traits) : _cmp(cmp), _value_traits(value_traits) { }
        bool operator()(const node_ptr& a, const node_ptr& b) {
            return _cmp(*_value_traits.to_value_ptr(a), *_value_traits.to_value_ptr(b));
        }
        template <typename T1>
        bool operator()(const node_ptr& a, const T1& b) {
            return _cmp(*_value_traits.to_value_ptr(a), b);
        }
        template <typename T1>
        bool operator()(const T1& a, const node_ptr& b) {
            return _cmp(a, *_value_traits.to_value_ptr(b));
        }
        template <typename T1, typename T2>
        bool operator()(const T1& a, const T2& b) {
            return _cmp(a, b);
        }
    };

    using const_value_traits_ptr = typename std::pointer_traits<typename value_traits::node_ptr>::template rebind<const value_traits>;

    static const_value_traits_ptr priv_value_traits_ptr() {
        return bi::pointer_traits<const_value_traits_ptr>::pointer_to(_value_traits);
    }
    template <typename Comparator>
    key_node_comparator<Comparator> key_node_comp(Comparator comp) const {
        return key_node_comparator<Comparator>(comp, _value_traits);
    }
    iterator insert_unique_commit(Elem& value, const algo::insert_commit_data &commit_data) {
        node_ptr to_insert(_value_traits.to_node_ptr(value));
        algo::insert_unique_commit(_header.this_ptr(), to_insert, commit_data);
        return iterator(to_insert, priv_value_traits_ptr());
    }
public:
    intrusive_set_external_comparator() { algo::init_header(_header.this_ptr()); }
    intrusive_set_external_comparator(intrusive_set_external_comparator&& o) noexcept {
        algo::init_header(_header.this_ptr());
        algo::swap_tree(_header.this_ptr(), node_ptr(o._header.this_ptr()));
    }
    static iterator iterator_to(Elem& e) { return iterator(_value_traits.to_node_ptr(e), priv_value_traits_ptr()); }
    // Returns container of e, assuming is_only_member(e).
    static intrusive_set_external_comparator& container_of_only_member(Elem& e) {
        auto header_ptr = static_cast<intrusive_set_external_comparator_member_hook*>(
            algo::node_traits::get_parent(_value_traits.to_node_ptr(e)));
        return *boost::intrusive::get_parent_from_member(header_ptr, &intrusive_set_external_comparator::_header);
    }
    static bool is_root(Elem& e) {
        auto node = _value_traits.to_node_ptr(e);
        auto e_parent = algo::node_traits::get_parent(node);
        return algo::node_traits::get_parent(e_parent) == node;
    }
    // Returns true if and only if e is the only member of the tree.
    static bool is_only_member(Elem& e) {
        auto node = _value_traits.to_node_ptr(e);
        return is_root(e) && !algo::node_traits::get_left(node) && !algo::node_traits::get_right(node);
    }
    iterator begin() { return iterator(algo::begin_node(_header.this_ptr()), priv_value_traits_ptr()); }
    const_iterator begin() const { return const_iterator(algo::begin_node(_header.this_ptr()), priv_value_traits_ptr()); }
    iterator end() { return iterator(algo::end_node(_header.this_ptr()), priv_value_traits_ptr()); }
    const_iterator end() const { return const_iterator(algo::end_node(_header.this_ptr()), priv_value_traits_ptr()); }
    reverse_iterator rbegin() { return reverse_iterator(end()); }
    const_reverse_iterator rbegin() const { return const_reverse_iterator(end()); }
    reverse_iterator rend() { return reverse_iterator(begin()); }
    const_reverse_iterator rend() const { return const_reverse_iterator(begin()); }
    template<class Disposer>
    void clear_and_dispose(Disposer disposer) {
        algo::clear_and_dispose(_header.this_ptr(),
                                [&disposer] (const node_ptr& p) {
                                    disposer(_value_traits.to_value_ptr(p));
                                });
        algo::init_header(_header.this_ptr());
    }
    bool empty() const { return algo::unique(_header.this_ptr()); }

    // WARNING: this method has O(N) time complexity, use with care
    auto calculate_size() const { return algo::size(_header.this_ptr()); }
    iterator erase(const_iterator i) {
        const_iterator ret(i);
        ++ret;
        node_ptr to_erase(i.pointed_node());
        algo::erase(_header.this_ptr(), to_erase);
        algo::init(to_erase);
        return ret.unconst();
    }
    iterator erase(const_iterator b, const_iterator e) {
        while (b != e) {
            erase(b++);
        }
        return b.unconst();
    }
    template<class Disposer>
    iterator erase_and_dispose(const_iterator i, Disposer disposer) {
        node_ptr to_erase(i.pointed_node());
        iterator ret(erase(i));
        disposer(_value_traits.to_value_ptr(to_erase));
        return ret;
    }
    template<class Disposer>
    iterator erase_and_dispose(const_iterator b, const_iterator e, Disposer disposer) {
        while (b != e) {
            erase_and_dispose(b++, disposer);
        }
        return b.unconst();
    }
    template <class Cloner, class Disposer>
    void clone_from(const intrusive_set_external_comparator &src, Cloner cloner, Disposer disposer) {
        clear_and_dispose(disposer);
        if (!src.empty()) {
            auto rollback = defer([this, &disposer] { this->clear_and_dispose(disposer); });
            algo::clone(src._header.this_ptr(),
                        _header.this_ptr(),
                        [&cloner] (const node_ptr& p) {
                            return _value_traits.to_node_ptr(*cloner(*_value_traits.to_value_ptr(p)));
                        },
                        [&disposer] (const node_ptr& p) {
                            disposer(_value_traits.to_value_ptr(p));
                        });
            rollback.cancel();
        }
    }
    Elem* unlink_leftmost_without_rebalance() {
        node_ptr to_be_disposed(algo::unlink_leftmost_without_rebalance(_header.this_ptr()));
        if(!to_be_disposed)
            return 0;
        algo::init(to_be_disposed);
        return _value_traits.to_value_ptr(to_be_disposed);
    }
    iterator insert_before(const_iterator pos, Elem& value) {
        node_ptr to_insert(_value_traits.to_node_ptr(value));
        return iterator(algo::insert_before(_header.this_ptr(), pos.pointed_node(), to_insert), priv_value_traits_ptr());
    }
    template<class KeyType, class KeyTypeKeyCompare>
    iterator upper_bound(const KeyType& key, KeyTypeKeyCompare comp) {
        return iterator(algo::upper_bound(_header.this_ptr(), key, key_node_comp(comp)), priv_value_traits_ptr());
    }
    template<class KeyType, class KeyTypeKeyCompare>
    const_iterator upper_bound(const KeyType& key, KeyTypeKeyCompare comp) const {
        return const_iterator(algo::upper_bound(_header.this_ptr(), key, key_node_comp(comp)), priv_value_traits_ptr());
    }
    template<class KeyType, class KeyTypeKeyCompare>
    iterator lower_bound(const KeyType &key, KeyTypeKeyCompare comp) {
        return iterator(algo::lower_bound(_header.this_ptr(), key, key_node_comp(comp)), priv_value_traits_ptr());
    }
    template<class KeyType, class KeyTypeKeyCompare>
    const_iterator lower_bound(const KeyType &key, KeyTypeKeyCompare comp) const {
        return const_iterator(algo::lower_bound(_header.this_ptr(), key, key_node_comp(comp)), priv_value_traits_ptr());
    }
    template<class KeyType, class KeyTypeKeyCompare>
    iterator find(const KeyType &key, KeyTypeKeyCompare comp) {
        return iterator(algo::find(_header.this_ptr(), key, key_node_comp(comp)), priv_value_traits_ptr());
    }
    template<class KeyType, class KeyTypeKeyCompare>
    const_iterator find(const KeyType &key, KeyTypeKeyCompare comp) const {
        return const_iterator(algo::find(_header.this_ptr(), key, key_node_comp(comp)), priv_value_traits_ptr());
    }
    template<class ElemCompare>
    iterator insert(const_iterator hint, Elem& value, ElemCompare cmp) {
        return insert_check(hint, value, std::move(cmp)).first;
    }
    template<class ElemCompare>
    std::pair<iterator, bool> insert_check(const_iterator hint, Elem& value, ElemCompare cmp) {
        algo::insert_commit_data commit_data;
        std::pair<node_ptr, bool> ret =
            algo::insert_unique_check(_header.this_ptr(),
                                      hint.pointed_node(),
                                      key_of_value()(value),
                                      key_node_comp(cmp),
                                      commit_data);
        return ret.second ? std::make_pair(insert_unique_commit(value, commit_data), true)
                          : std::make_pair(iterator(ret.first, priv_value_traits_ptr()), false);
    }
};

template<typename Elem,
         intrusive_set_external_comparator_member_hook Elem::* PtrToMember>
const typename intrusive_set_external_comparator<Elem, PtrToMember>::value_traits intrusive_set_external_comparator<Elem, PtrToMember>::_value_traits;
