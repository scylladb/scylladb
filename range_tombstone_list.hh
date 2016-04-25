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

#include "range_tombstone.hh"

class range_tombstone_list final {
    using range_tombstones_type = range_tombstone::container_type;
private:
    range_tombstones_type _tombstones;
public:
    struct copy_comparator_only { };
    range_tombstone_list(const schema& s)
        : _tombstones(range_tombstone::compare(s))
    { }
    range_tombstone_list(const range_tombstone_list& x, copy_comparator_only)
        : _tombstones(x._tombstones.key_comp())
    { }
    range_tombstone_list(const range_tombstone_list&);
    range_tombstone_list& operator=(range_tombstone_list&) = delete;
    range_tombstone_list(range_tombstone_list&&) = default;
    range_tombstone_list& operator=(range_tombstone_list&&) = default;
    ~range_tombstone_list();
    size_t size() const {
        return _tombstones.size();
    }
    bool empty() const {
        return _tombstones.empty();
    }
    range_tombstones_type& tombstones() {
        return _tombstones;
    }
    auto begin() const {
        return _tombstones.begin();
    }
    auto end() const {
        return _tombstones.end();
    }
    void apply(const schema& s, const range_tombstone& rt) {
        apply(s, rt.start, rt.start_kind, rt.end, rt.end_kind, rt.tomb);
    }
    void apply(const schema& s, range_tombstone&& rt) {
        apply(s, std::move(rt.start), rt.start_kind, std::move(rt.end), rt.end_kind, std::move(rt.tomb));
    }
    void apply(const schema& s, clustering_key_prefix start, bound_kind start_kind,
               clustering_key_prefix end, bound_kind end_kind, tombstone tomb);
    tombstone search_tombstone_covering(const schema& s, const clustering_key_prefix& key) const;
    // Erases the range tombstones for which filter returns true.
    template <typename Pred>
    void erase_where(Pred filter) {
        static_assert(std::is_same<bool, std::result_of_t<Pred(const range_tombstone&)>>::value,
                      "bad Pred signature");
        auto it = begin();
        while (it != end()) {
            if (filter(*it)) {
                it = _tombstones.erase_and_dispose(it, current_deleter<range_tombstone>());
            } else {
                ++it;
            }
        }
    }
private:
    void insert_from(const schema& s, range_tombstones_type::iterator it, clustering_key_prefix start,
                     bound_kind start_kind, clustering_key_prefix end, bound_kind end_kind, tombstone tomb);
};
