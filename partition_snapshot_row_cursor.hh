/*
 * Copyright (C) 2017 ScyllaDB
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

#include "partition_version.hh"

// Allows iterating over rows of mutation_partition represented by given partition_snapshot.
//
// The cursor initially has a position before all rows and is not pointing at any row.
// To position the cursor, use advance_to().
//
// All methods should be called with the region of the snapshot locked. The cursor is invalidated
// when that lock section is left, or if the snapshot is modified.
//
// When the cursor is invalidated, it still maintains its previous position. It can be brought
// back to validity by calling maybe_refresh(), or advance_to().
//
class partition_snapshot_row_cursor final {
    struct position_in_version {
        mutation_partition::rows_type::iterator it;
        mutation_partition::rows_type::iterator end;
        int version_no;

        struct less_compare {
            rows_entry::tri_compare _cmp;
        public:
            explicit less_compare(const schema& s) : _cmp(s) { }
            bool operator()(const position_in_version& a, const position_in_version& b) {
                auto res = _cmp(*a.it, *b.it);
                return res > 0 || (res == 0 && a.version_no > b.version_no);
            }
        };
    };

    const schema& _schema;
    logalloc::region& _region;
    partition_snapshot& _snp;
    std::vector<position_in_version> _heap;
    std::vector<position_in_version> _current_row;
    position_in_partition _position;
    uint64_t _last_reclaim_count = 0;
    size_t _last_versions_count = 0;

    // Removes the next row from _heap and puts it into _current_row
    void recreate_current_row() {
        position_in_version::less_compare heap_less(_schema);
        position_in_partition::equal_compare eq(_schema);
        do {
            boost::range::pop_heap(_heap, heap_less);
            _current_row.push_back(_heap.back());
            _heap.pop_back();
        } while (!_heap.empty() && eq(_current_row[0].it->position(), _heap[0].it->position()));
        _position = position_in_partition(_current_row[0].it->position());
    }
public:
    partition_snapshot_row_cursor(const schema& s, logalloc::region& region, partition_snapshot& snp)
        : _schema(s)
        , _region(region)
        , _snp(snp)
        , _position(position_in_partition::static_row_tag_t{})
    { }
    bool has_up_to_date_row_from_latest_version() const {
        return up_to_date() && _current_row[0].version_no == 0;
    }
    mutation_partition::rows_type::iterator get_iterator_in_latest_version() const {
        return _current_row[0].it;
    }
    bool up_to_date() const {
        return _region.reclaim_counter() == _last_reclaim_count && _last_versions_count == _snp.version_count();
    }

    // Brings back the cursor to validity.
    // Can be only called when cursor is pointing at a row.
    //
    // Semantically equivalent to:
    //
    //   advance_to(position());
    //
    // but avoids work if not necessary.
    bool maybe_refresh() {
        if (!up_to_date()) {
            return advance_to(_position);
        }
        return true;
    }

    // Moves the cursor to the first entry with position >= pos.
    //
    // The caller must ensure that such entry exists.
    //
    // Returns true iff there can't be any clustering row entries
    // between lower_bound (inclusive) and the entry to which the cursor
    // was advanced.
    //
    // May be called when cursor is not valid.
    // The cursor is valid after the call.
    // Must be called under reclaim lock.
    bool advance_to(position_in_partition_view lower_bound) {
        rows_entry::compare less(_schema);
        position_in_version::less_compare heap_less(_schema);
        _heap.clear();
        _current_row.clear();
        int version_no = 0;
        for (auto&& v : _snp.versions()) {
            auto& rows = v.partition().clustered_rows();
            auto pos = rows.lower_bound(lower_bound, less);
            auto end = rows.end();
            if (pos != end) {
                _heap.push_back({pos, end, version_no});
            }
            ++version_no;
        }
        boost::range::make_heap(_heap, heap_less);
        _last_reclaim_count = _region.reclaim_counter();
        _last_versions_count = _snp.version_count();
        bool found = no_clustering_row_between(_schema, lower_bound, _heap[0].it->position());
        recreate_current_row();
        return found;
    }

    // Advances the cursor to the next row.
    // If there is no next row, returns false and the cursor is no longer pointing at a row.
    // Can be only called on a valid cursor pointing at a row.
    bool next() {
        position_in_version::less_compare heap_less(_schema);
        assert(up_to_date());
        for (auto&& curr : _current_row) {
            ++curr.it;
            if (curr.it != curr.end) {
                _heap.push_back(curr);
                boost::range::push_heap(_heap, heap_less);
            }
        }
        _current_row.clear();
        if (_heap.empty()) {
            return false;
        }
        recreate_current_row();
        return true;
    }

    // Can be called only when cursor is valid and pointing at a row.
    bool continuous() const { return bool(_current_row[0].it->continuous()); }

    // Can be called only when cursor is valid and pointing at a row.
    bool dummy() const { return bool(_current_row[0].it->dummy()); }

    // Can be called only when cursor is valid and pointing at a row, and !dummy().
    const clustering_key& key() const { return _current_row[0].it->key(); }

    // Can be called only when cursor is valid and pointing at a row.
    mutation_fragment row() const {
        auto it = _current_row.begin();
        auto mf = mutation_fragment(clustering_row(*it->it));
        auto& cr = mf.as_mutable_clustering_row();
        for (++it; it != _current_row.end(); ++it) {
            cr.apply(_schema, *it->it);
        }
        return mf;
    }

    // Can be called when cursor is pointing at a row, even when invalid.
    const position_in_partition& position() const {
        return _position;
    }

    bool is_in_latest_version() const;
    bool previous_row_in_latest_version_has_key(const clustering_key_prefix& key) const;
    void set_continuous(bool val);
};

inline
bool partition_snapshot_row_cursor::is_in_latest_version() const {
    return _current_row[0].version_no == 0;
}

inline
bool partition_snapshot_row_cursor::previous_row_in_latest_version_has_key(const clustering_key_prefix& key) const {
    if (_current_row[0].it == _snp.version()->partition().clustered_rows().begin()) {
        return false;
    }
    auto prev_it = _current_row[0].it;
    --prev_it;
    clustering_key_prefix::equality eq(_schema);
    return eq(prev_it->key(), key);
}

inline
void partition_snapshot_row_cursor::set_continuous(bool val) {
    _current_row[0].it->set_continuous(val);
}
