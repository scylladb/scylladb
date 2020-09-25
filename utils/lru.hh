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

#include <boost/intrusive/list.hpp>
#include <seastar/core/memory.hh>

class evictable {
    friend class lru;
    using lru_link_type = boost::intrusive::list_member_hook<
        boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    lru_link_type _lru_link;
protected:
    // Prevent destruction via evictable pointer. LRU is not aware of allocation strategy.
    ~evictable() = default;
public:
    evictable() = default;
    evictable(evictable&& o) noexcept;
    evictable& operator=(evictable&&) noexcept = default;

    virtual void on_evicted() noexcept = 0;

    bool is_linked() const {
        return _lru_link.is_linked();
    }

    void unlink_from_lru() {
        _lru_link.unlink();
    }
};

class lru {
private:
    friend class evictable;
    using lru_type = boost::intrusive::list<evictable,
        boost::intrusive::member_hook<evictable, evictable::lru_link_type, &evictable::_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    lru_type _list;
public:
    using reclaiming_result = seastar::memory::reclaiming_result;

    ~lru() {
        _list.clear_and_dispose([] (evictable* e) {
            e->on_evicted();
        });
    }

    void remove(evictable& e) noexcept {
        _list.erase(_list.iterator_to(e));
    }

    void add(evictable& e) noexcept {
        _list.push_back(e);
    }

    void touch(evictable& e) noexcept {
        remove(e);
        add(e);
    }

    // Evicts a single element from the LRU
    reclaiming_result evict() noexcept {
        if (_list.empty()) {
            return reclaiming_result::reclaimed_nothing;
        }
        evictable& e = _list.front();
        _list.pop_front();
        e.on_evicted();
        return reclaiming_result::reclaimed_something;
    }

    // Evicts all elements.
    // May stall the reactor, use only in tests.
    void evict_all() {
        while (evict() == reclaiming_result::reclaimed_something) {}
    }
};

inline
evictable::evictable(evictable&& o) noexcept {
    if (o._lru_link.is_linked()) {
        auto prev = o._lru_link.prev_;
        o._lru_link.unlink();
        lru::lru_type::node_algorithms::link_after(prev, _lru_link.this_ptr());
    }
}
