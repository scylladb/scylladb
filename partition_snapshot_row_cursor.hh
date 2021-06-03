/*
 * Copyright (C) 2017-present ScyllaDB
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
#include "row_cache.hh"
#include "utils/small_vector.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>

class partition_snapshot_row_cursor;

// A non-owning reference to a row inside partition_snapshot which
// maintains it's position and thus can be kept across reference invalidation points.
class partition_snapshot_row_weakref final {
    mutation_partition::rows_type::iterator _it;
    partition_snapshot::change_mark _change_mark;
    position_in_partition _pos = position_in_partition::min();
    bool _in_latest = false;
public:
    partition_snapshot_row_weakref() = default;
    // Makes this object point to a row pointed to by given partition_snapshot_row_cursor.
    explicit partition_snapshot_row_weakref(const partition_snapshot_row_cursor&);
    explicit partition_snapshot_row_weakref(std::nullptr_t) {}
    partition_snapshot_row_weakref(partition_snapshot& snp, mutation_partition::rows_type::iterator it, bool in_latest)
        : _it(it)
        , _change_mark(snp.get_change_mark())
        , _pos(it->position())
        , _in_latest(in_latest)
    { }
    partition_snapshot_row_weakref& operator=(const partition_snapshot_row_cursor&);
    partition_snapshot_row_weakref& operator=(std::nullptr_t) noexcept {
        _change_mark = {};
        return *this;
    }
    // Returns true iff the pointer is pointing at a row.
    explicit operator bool() const { return _change_mark != partition_snapshot::change_mark(); }
public:
    // Returns the position of the row.
    // Call only when pointing at a row.
    const position_in_partition& position() const { return _pos; }
    // Returns true iff the object is valid.
    bool valid(partition_snapshot& snp) { return snp.get_change_mark() == _change_mark; }
    // Call only when valid.
    bool is_in_latest_version() const { return _in_latest; }
    // Brings the object back to validity and returns true iff the snapshot contains the row.
    // When not pointing at a row, returns false.
    bool refresh(partition_snapshot& snp) {
        auto snp_cm = snp.get_change_mark();
        if (snp_cm == _change_mark) {
            return true;
        }
        if (!_change_mark) {
            return false;
        }
        _change_mark = snp_cm;
        rows_entry::tri_compare cmp(*snp.schema());
        _in_latest = true;
        for (auto&& v : snp.versions()) {
            auto& rows = v.partition().clustered_rows();
            _it = rows.find(_pos, cmp);
            if (_it != rows.end()) {
                return true;
            }
            _in_latest = false;
        }
        return false;
    }
    rows_entry* operator->() const {
        return &*_it;
    }
    rows_entry& operator*() const {
        return *_it;
    }
};

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
// Insertion of row entries after cursor's position invalidates the cursor.
// Exceptions thrown from mutators invalidate the cursor.
//
class partition_snapshot_row_cursor final {
    friend class partition_snapshot_row_weakref;
    struct position_in_version {
        mutation_partition::rows_type::iterator it;
        int version_no;
        bool unique_owner;

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
    partition_snapshot& _snp;
    utils::small_vector<position_in_version, 2> _heap;
    utils::small_vector<position_in_version, 2> _current_row;
    std::optional<mutation_partition::rows_type::iterator> _latest_it;
    bool _continuous{};
    bool _dummy{};
    const bool _unique_owner;
    position_in_partition _position;
    partition_snapshot::change_mark _change_mark;

    // Removes the next row from _heap and puts it into _current_row
    bool recreate_current_row() {
        _current_row.clear();
        if (_heap.empty()) {
            return false;
        }
        position_in_version::less_compare heap_less(_schema);
        position_in_partition::equal_compare eq(_schema);
        _continuous = false;
        _dummy = true;
        do {
            boost::range::pop_heap(_heap, heap_less);
            memory::on_alloc_point();
            rows_entry& e = *_heap.back().it;
            _dummy &= bool(e.dummy());
            _continuous |= bool(e.continuous());
            _current_row.push_back(_heap.back());
            _heap.pop_back();
        } while (!_heap.empty() && eq(_current_row[0].it->position(), _heap[0].it->position()));

        if (boost::algorithm::any_of(_heap, [] (auto&& v) { return v.it->continuous(); })) {
            // FIXME: Optimize by dropping dummy() entries.
            _continuous = true;
        }

        _position = position_in_partition(_current_row[0].it->position());
        return true;
    }

    void prepare_heap(position_in_partition_view lower_bound) {
        memory::on_alloc_point();
        rows_entry::tri_compare cmp(_schema);
        position_in_version::less_compare heap_less(_schema);
        _heap.clear();
        _latest_it.reset();
        int version_no = 0;
        bool unique_owner = _unique_owner;
        bool first = true;
        for (auto&& v : _snp.versions()) {
            unique_owner = unique_owner && (first || !v.is_referenced());
            auto& rows = v.partition().clustered_rows();
            auto pos = rows.lower_bound(lower_bound, cmp);
            if (first) {
                _latest_it = pos;
            }
            if (pos) {
                _heap.push_back({pos, version_no, unique_owner});
            }
            ++version_no;
            first = false;
        }
        boost::range::make_heap(_heap, heap_less);
        _change_mark = _snp.get_change_mark();
    }

    // Advances the cursor to the next row.
    // The @keep denotes whether the entries should be kept in partition version.
    // If there is no next row, returns false and the cursor is no longer pointing at a row.
    // Can be only called on a valid cursor pointing at a row.
    // When throws, the cursor is invalidated and its position is not changed.
    bool advance(bool keep) {
        memory::on_alloc_point();
        position_in_version::less_compare heap_less(_schema);
        assert(iterators_valid());
        for (auto&& curr : _current_row) {
            if (!keep && curr.unique_owner) {
                curr.it = curr.it.erase_and_dispose(current_deleter<rows_entry>());
            } else {
                ++curr.it;
            }
            if (curr.version_no == 0) {
                _latest_it = curr.it;
            }
            if (curr.it) {
                _heap.push_back(curr);
                boost::range::push_heap(_heap, heap_less);
            }
        }
        return recreate_current_row();
    }

    bool is_in_latest_version() const noexcept { return _current_row[0].version_no == 0; }

public:
    partition_snapshot_row_cursor(const schema& s, partition_snapshot& snp, bool unique_owner = false)
        : _schema(s)
        , _snp(snp)
        , _unique_owner(unique_owner)
        , _position(position_in_partition::static_row_tag_t{})
    { }

    mutation_partition::rows_type::iterator get_iterator_in_latest_version() const {
        assert(_latest_it);
        return *_latest_it;
    }

    // Returns true iff the iterators obtained since the cursor was last made valid
    // are still valid. Note that this doesn't mean that the cursor itself is valid.
    bool iterators_valid() const {
        return _snp.get_change_mark() == _change_mark;
    }

    // Marks the iterators as valid without refreshing them.
    // Call only when the iterators are known to be valid.
    void force_valid() {
        _change_mark = _snp.get_change_mark();
    }

    // Advances cursor to the first entry with position >= pos, if such entry exists.
    // Otherwise returns false and the cursor is left not pointing at a row and invalid.
    bool maybe_advance_to(position_in_partition_view pos) {
        prepare_heap(pos);
        return recreate_current_row();
    }

    // Brings back the cursor to validity.
    // Can be only called when cursor is pointing at a row.
    //
    // Semantically equivalent to:
    //
    //   advance_to(position());
    //
    // but avoids work if not necessary.
    //
    // Changes to attributes of the current row (e.g. continuity) don't have to be reflected.
    bool maybe_refresh() {
        if (!iterators_valid()) {
            return advance_to(_position);
        }
        // Refresh latest version's iterator in case there was an insertion
        // before it and after cursor's position. There cannot be any
        // insertions for non-latest versions, so we don't have to update them.
        if (!is_in_latest_version()) {
            rows_entry::tri_compare cmp(_schema);
            position_in_version::less_compare heap_less(_schema);
            auto& rows = _snp.version()->partition().clustered_rows();
            bool match;
            auto it = rows.lower_bound(_position, match, cmp);
            _latest_it = it;
            auto heap_i = boost::find_if(_heap, [](auto&& v) { return v.version_no == 0; });
            if (!it) {
                if (heap_i != _heap.end()) {
                    _heap.erase(heap_i);
                    boost::range::make_heap(_heap, heap_less);
                }
            } else if (match) {
                _current_row.insert(_current_row.begin(), position_in_version{it, 0});
                if (heap_i != _heap.end()) {
                    _heap.erase(heap_i);
                    boost::range::make_heap(_heap, heap_less);
                }
            } else {
                if (heap_i != _heap.end()) {
                    heap_i->it = it;
                    boost::range::make_heap(_heap, heap_less);
                } else {
                    _heap.push_back({it, 0});
                    boost::range::push_heap(_heap, heap_less);
                }
            }
        }
        return true;
    }

    // Brings back the cursor to validity, pointing at the first row with position not smaller
    // than the current position. Returns false iff no such row exists.
    // Assumes that rows are not inserted into the snapshot (static). They can be removed.
    bool maybe_refresh_static() {
        if (!iterators_valid()) {
            return maybe_advance_to(_position);
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
    // When throws, the cursor is invalidated and its position is not changed.
    bool advance_to(position_in_partition_view lower_bound) {
        prepare_heap(lower_bound);
        bool found = no_clustering_row_between(_schema, lower_bound, _heap[0].it->position());
        recreate_current_row();
        return found;
    }

    bool next() { return advance(true); }

    bool erase_and_advance() { return advance(false); }

    // Can be called when cursor is pointing at a row.
    bool continuous() const { return _continuous; }

    // Can be called when cursor is pointing at a row.
    bool dummy() const { return _dummy; }

    // Can be called only when cursor is valid and pointing at a row, and !dummy().
    const clustering_key& key() const { return _position.key(); }

    // Can be called only when cursor is valid and pointing at a row.
    clustering_row row() const {
        clustering_row cr(key());
        consume_row([&] (const deletable_row& row) {
            cr.apply(_schema, row);
        });
        return cr;
    }

    // Can be called only when cursor is valid and pointing at a row.
    deletable_row& latest_row() const noexcept {
        return _current_row[0].it->row();
    }

    // Can be called only when cursor is valid and pointing at a row.
    // Monotonic exception guarantees.
    template <typename Consumer>
    requires std::is_invocable_v<Consumer, deletable_row>
    void consume_row(Consumer&& consumer) {
        for (position_in_version& v : _current_row) {
            if (v.unique_owner) {
                consumer(std::move(v.it->row()));
            } else {
                consumer(deletable_row(_schema, v.it->row()));
            }
        }
    }

    // Can be called only when cursor is valid and pointing at a row.
    template <typename Consumer>
    requires std::is_invocable_v<Consumer, const deletable_row&>
    void consume_row(Consumer&& consumer) const {
        for (const position_in_version& v : _current_row) {
            consumer(v.it->row());
        }
    }

    // Returns memory footprint of row entries under the cursor.
    // Can be called only when cursor is valid and pointing at a row.
    size_t memory_usage() const {
        size_t result = 0;
        for (const position_in_version& v : _current_row) {
            result += v.it->memory_usage(_schema);
        }
        return result;
    }

    struct ensure_result {
        rows_entry& row;
        bool inserted = false;
    };

    // Makes sure that a rows_entry for the row under the cursor exists in the latest version.
    // Doesn't change logical value or continuity of the snapshot.
    // Can be called only when cursor is valid and pointing at a row.
    // The cursor remains valid after the call and points at the same row as before.
    ensure_result ensure_entry_in_latest() {
        auto&& rows = _snp.version()->partition().clustered_rows();
        auto latest_i = get_iterator_in_latest_version();
        rows_entry& latest = *latest_i;
        if (is_in_latest_version()) {
            if (_snp.at_latest_version()) {
                _snp.tracker()->touch(latest);
            }
            return {latest, false};
        } else {
            // Copy row from older version because rows in evictable versions must
            // hold values which are independently complete to be consistent on eviction.
            auto e = current_allocator().construct<rows_entry>(_schema, *_current_row[0].it);
            e->set_continuous(latest_i && latest_i->continuous());
            _snp.tracker()->insert(*e);
            rows.insert_before(latest_i, *e);
            return {*e, true};
        }
    }

    // Returns a pointer to rows_entry with given position in latest version or
    // creates a neutral one, provided that it belongs to a continuous range.
    // Otherwise returns nullptr.
    // Doesn't change logical value of mutation_partition or continuity of the snapshot.
    // The cursor doesn't have to be valid.
    // The cursor is invalid after the call.
    // Assumes the snapshot is evictable and not populated by means other than ensure_entry_if_complete().
    // Subsequent calls to ensure_entry_if_complete() must be given strictly monotonically increasing
    // positions unless iterators are invalidated across the calls.
    std::optional<ensure_result> ensure_entry_if_complete(position_in_partition_view pos) {
        position_in_partition::less_compare less(_schema);
        if (!iterators_valid() || less(position(), pos)) {
            auto has_entry = maybe_advance_to(pos);
            assert(has_entry); // evictable snapshots must have a dummy after all rows.
        }
        {
            position_in_partition::equal_compare eq(_schema);
            if (eq(position(), pos)) {
                if (dummy()) {
                    return std::nullopt;
                }
                return ensure_entry_in_latest();
            } else if (!continuous()) {
                return std::nullopt;
            }
        }
        auto&& rows = _snp.version()->partition().clustered_rows();
        auto latest_i = get_iterator_in_latest_version();
        auto e = current_allocator().construct<rows_entry>(_schema, pos, is_dummy(!pos.is_clustering_row()),
            is_continuous(latest_i && latest_i->continuous()));
        _snp.tracker()->insert(*e);
        rows.insert_before(latest_i, *e);
        return ensure_result{*e, true};
    }

    // Brings the entry pointed to by the cursor to the front of the LRU
    // Cursor must be valid and pointing at a row.
    void touch() {
        // We cannot bring entries from non-latest versions to the front because that
        // could result violate ordering invariant for the LRU, which states that older versions
        // must be evicted first. Needed to keep the snapshot consistent.
        if (_snp.at_latest_version() && is_in_latest_version()) {
            _snp.tracker()->touch(*get_iterator_in_latest_version());
        }
    }

    // Can be called when cursor is pointing at a row, even when invalid.
    const position_in_partition& position() const {
        return _position;
    }

    friend std::ostream& operator<<(std::ostream& out, const partition_snapshot_row_cursor& cur) {
        out << "{cursor: position=" << cur._position << ", cont=" << cur.continuous() << ", ";
        if (!cur.iterators_valid()) {
            return out << " iterators invalid}";
        }
        out << "current=[";
        bool first = true;
        for (auto&& v : cur._current_row) {
            if (!first) {
                out << ", ";
            }
            first = false;
            out << v.version_no;
        }
        out << "], heap=[\n  ";
        first = true;
        for (auto&& v : cur._heap) {
            if (!first) {
                out << ",\n  ";
            }
            first = false;
            out << "{v=" << v.version_no << ", pos=" << v.it->position() << ", cont=" << v.it->continuous() << "}";
        }
        out << "], latest_iterator=[";
        if (cur._latest_it) {
            mutation_partition::rows_type::iterator i = *cur._latest_it;
            if (!i) {
                out << "end";
            } else {
                out << i->position();
            }
        } else {
            out << "<none>";
        }
        return out << "]}";
    };
};

inline
partition_snapshot_row_weakref::partition_snapshot_row_weakref(const partition_snapshot_row_cursor& c)
    : _it(c._current_row[0].it)
    , _change_mark(c._change_mark)
    , _pos(c._position)
    , _in_latest(c.is_in_latest_version())
{ }

inline
partition_snapshot_row_weakref& partition_snapshot_row_weakref::operator=(const partition_snapshot_row_cursor& c) {
    auto tmp = partition_snapshot_row_weakref(c);
    this->~partition_snapshot_row_weakref();
    new (this) partition_snapshot_row_weakref(std::move(tmp));
    return *this;
}
