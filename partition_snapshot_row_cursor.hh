/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/partition_version.hh"
#include "row_cache.hh"
#include "utils/assert.hh"
#include "utils/small_vector.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <fmt/core.h>

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
    // Sets the iterator in latest version for the current position.
    void set_latest(mutation_partition::rows_type::iterator it) {
        _it = std::move(it);
        _in_latest = true;
    }
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
            auto rows = v.partition().clustered_rows();
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
// Range tombstone information is accessible via range_tombstone() and range_tombstone_for_row()
// functions. range_tombstone() returns the tombstone for the interval which strictly precedes
// the current row, and range_tombstone_for_row() returns the information for the row itself.
// If the interval which precedes the row is not continuous, then range_tombstone() is empty.
// If range_tombstone() is not empty then the interval is continuous.
class partition_snapshot_row_cursor final {
    friend class partition_snapshot_row_weakref;
    struct position_in_version {
        mutation_partition::rows_type::iterator it;
        utils::immutable_collection<mutation_partition::rows_type> rows;
        int version_no;
        const schema* schema;
        bool unique_owner = false;
        is_continuous continuous = is_continuous::no; // Range continuity in the direction of lower keys (in cursor schema domain).

        // Range tombstone in the direction of lower keys (in cursor schema domain).
        // Excludes the row. In the reverse mode, the row may have a different range tombstone.
        tombstone rt;
    };

    const schema& _schema; // query domain
    partition_snapshot& _snp;

    // _heap contains iterators which are ahead of the cursor.
    // _current_row contains iterators which are directly below the cursor.
    utils::small_vector<position_in_version, 2> _heap; // query domain order
    utils::small_vector<position_in_version, 2> _current_row;

    // For !_reversed cursors points to the entry which
    // is the lower_bound() of the current position in table schema order.
    // For _reversed cursors it can be either lower_bound() in table order
    // or lower_bound() in cursor's order, so should not be relied upon.
    // if current entry is in the latest version then _latest_it points to it,
    // also in _reversed mode.
    std::optional<mutation_partition::rows_type::iterator> _latest_it;

    // Continuity and range tombstone corresponding to ranges which are not represented in _heap because the cursor
    // went pass all the entries in those versions.
    bool _background_continuity = false;
    tombstone _background_rt;

    bool _continuous{};
    bool _dummy{};
    const bool _unique_owner;
    const bool _reversed;
    const bool _digest_requested;
    tombstone _range_tombstone;
    tombstone _range_tombstone_for_row;
    position_in_partition _position; // table domain
    partition_snapshot::change_mark _change_mark;

    position_in_partition_view to_table_domain(position_in_partition_view pos) const {
        if (_reversed) [[unlikely]] {
            return pos.reversed();
        }
        return pos;
    }

    position_in_partition_view to_query_domain(position_in_partition_view pos) const {
        if (_reversed) [[unlikely]] {
            return pos.reversed();
        }
        return pos;
    }

    struct version_heap_less_compare {
        rows_entry::tri_compare _cmp;
        partition_snapshot_row_cursor& _cur;
    public:
        explicit version_heap_less_compare(partition_snapshot_row_cursor& cur)
            : _cmp(cur._schema)
            , _cur(cur)
        { }

        bool operator()(const position_in_version& a, const position_in_version& b) {
            auto res = _cmp(_cur.to_query_domain(a.it->position()), _cur.to_query_domain(b.it->position()));
            return res > 0 || (res == 0 && a.version_no > b.version_no);
        }
    };

    // Removes the next row from _heap and puts it into _current_row
    bool recreate_current_row() {
        _current_row.clear();
        _continuous = _background_continuity;
        _range_tombstone = _background_rt;
        _range_tombstone_for_row = _background_rt;
        _dummy = true;
        if (_heap.empty()) {
            if (_reversed) {
                _position = position_in_partition::before_all_clustered_rows();
            } else {
                _position = position_in_partition::after_all_clustered_rows();
            }
            return false;
        }
        version_heap_less_compare heap_less(*this);
        position_in_partition::equal_compare eq(*_snp.schema());
        do {
            boost::range::pop_heap(_heap, heap_less);
            memory::on_alloc_point();
            position_in_version& v = _heap.back();
            rows_entry& e = *v.it;
            if (_digest_requested) {
                e.row().cells().prepare_hash(*v.schema, column_kind::regular_column);
            }
            _dummy &= bool(e.dummy());
            _continuous |= bool(v.continuous);
            _range_tombstone_for_row.apply(e.range_tombstone());
            if (v.continuous) {
                _range_tombstone.apply(v.rt);
            }
            _current_row.push_back(v);
            _heap.pop_back();
        } while (!_heap.empty() && eq(_current_row[0].it->position(), _heap[0].it->position()));

        // FIXME: Optimize by dropping dummy() entries.
        for (position_in_version& v : _heap) {
            _continuous |= bool(v.continuous);
            if (v.continuous) {
                _range_tombstone.apply(v.rt);
                _range_tombstone_for_row.apply(v.rt);
            }
        }

        _position = position_in_partition(_current_row[0].it->position());
        return true;
    }

    // lower_bound is in the query schema domain
    void prepare_heap(position_in_partition_view lower_bound) {
        lower_bound = to_table_domain(lower_bound);
        memory::on_alloc_point();
        rows_entry::tri_compare cmp(*_snp.schema());
        version_heap_less_compare heap_less(*this);
        _heap.clear();
        _latest_it.reset();
        _background_continuity = false;
        _background_rt = {};
        int version_no = 0;
        bool unique_owner = _unique_owner;
        bool first = true;
        for (auto&& v : _snp.versions()) {
            unique_owner = unique_owner && (first || !v.is_referenced());
            auto rows = v.partition().clustered_rows();
            auto pos = rows.lower_bound(lower_bound, cmp);
            if (first) {
                _latest_it = pos;
            }
            if (pos) {
                is_continuous cont;
                tombstone rt;
                if (_reversed) [[unlikely]] {
                    if (cmp(pos->position(), lower_bound) != 0) {
                        cont = pos->continuous();
                        rt = pos->range_tombstone();
                        if (pos != rows.begin()) {
                            --pos;
                        } else {
                            _background_continuity |= bool(cont);
                            if (cont) {
                                _background_rt = rt;
                            }
                            pos = {};
                        }
                    } else {
                        auto next_entry = std::next(pos);
                        if (next_entry == rows.end()) {
                            // Positions past last dummy are complete since mutation sources
                            // can't contain any keys which are larger.
                            cont = is_continuous::yes;
                            rt = {};
                        } else {
                            cont = next_entry->continuous();
                            rt = next_entry->range_tombstone();
                        }
                    }
                } else {
                    cont = pos->continuous();
                    rt = pos->range_tombstone();
                }
                if (pos) [[likely]] {
                    _heap.emplace_back(position_in_version{pos, std::move(rows), version_no, v.get_schema().get(), unique_owner, cont, rt});
                }
            } else {
                if (_reversed) [[unlikely]] {
                    if (!rows.empty()) {
                        pos = std::prev(rows.end());
                    } else {
                        _background_continuity = true;
                    }
                } else {
                    _background_continuity = true; // Default continuity past the last entry
                }
                if (pos) [[likely]] {
                    _heap.emplace_back(position_in_version{pos, std::move(rows), version_no, v.get_schema().get(), unique_owner, is_continuous::yes});
                }
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
        version_heap_less_compare heap_less(*this);
        SCYLLA_ASSERT(iterators_valid());
        for (auto&& curr : _current_row) {
            if (!keep && curr.unique_owner) {
                mutation_partition::rows_type::key_grabber kg(curr.it);
                kg.release(current_deleter<rows_entry>());
                if (_reversed && curr.it) [[unlikely]] {
                    if (curr.rows.begin() == curr.it) {
                        _background_continuity |= bool(curr.it->continuous());
                        if (curr.it->continuous()) {
                            _background_rt.apply(curr.it->range_tombstone());
                        }
                        curr.it = {};
                    } else {
                        curr.continuous = curr.it->continuous();
                        curr.rt = curr.it->range_tombstone();
                        --curr.it;
                    }
                }
            } else {
                if (_reversed) [[unlikely]] {
                    if (curr.rows.begin() == curr.it) {
                        _background_continuity |= bool(curr.it->continuous());
                        if (curr.it->continuous()) {
                            _background_rt.apply(curr.it->range_tombstone());
                        }
                        curr.it = {};
                    } else {
                        curr.continuous = curr.it->continuous();
                        curr.rt = curr.it->range_tombstone();
                        --curr.it;
                    }
                } else {
                    ++curr.it;
                    if (curr.it) {
                        curr.continuous = curr.it->continuous();
                        curr.rt = curr.it->range_tombstone();
                    }
                }
            }
            if (curr.it) {
                if (curr.version_no == 0) {
                    _latest_it = curr.it;
                }
                _heap.push_back(curr);
                boost::range::push_heap(_heap, heap_less);
            }
        }
        return recreate_current_row();
    }

    bool is_in_latest_version() const noexcept { return at_a_row() && _current_row[0].version_no == 0; }

public:
    // When reversed is true then the cursor will operate in reversed direction.
    // When reversed, s must be a reversed schema relative to snp->schema()
    // Positions and fragments accepted and returned by the cursor are from the domain of s.
    // Iterators are from the table's schema domain.
    partition_snapshot_row_cursor(const schema& s, partition_snapshot& snp, bool unique_owner = false, bool reversed = false, bool digest_requested = false)
        : _schema(s)
        , _snp(snp)
        , _unique_owner(unique_owner)
        , _reversed(reversed)
        , _digest_requested(digest_requested)
        , _position(position_in_partition::static_row_tag_t{})
    { }

    // If is_in_latest_version() then this returns an iterator to the entry under cursor in the latest version.
    mutation_partition::rows_type::iterator get_iterator_in_latest_version() const {
        SCYLLA_ASSERT(_latest_it);
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
    // If no such entry exists, the cursor is positioned at an extreme position in the direction of
    // the cursor (min for reversed cursor, max for forward cursor) and not pointing at a row
    // but still valid.
    //
    // continuous() is always valid after the call, even if not pointing at a row.
    // Returns true iff the cursor is pointing at a row after the call.
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
            auto pos = position_in_partition(position()); // advance_to() modifies position() so copy
            return advance_to(pos);
        }
        // Refresh latest version's iterator in case there was an insertion
        // before it and after cursor's position. There cannot be any
        // insertions for non-latest versions, so we don't have to update them.
        if (!is_in_latest_version()) {
            rows_entry::tri_compare cmp(*_snp.schema());
            version_heap_less_compare heap_less(*this);
            auto rows = _snp.version()->partition().clustered_rows();
            bool match;
            auto it = rows.lower_bound(_position, match, cmp);
            _latest_it = it;
            auto heap_i = boost::find_if(_heap, [](auto&& v) { return v.version_no == 0; });

            is_continuous cont;
            tombstone rt;
            if (it) {
                cont = it->continuous();
                rt = it->range_tombstone();
                if (_reversed) [[unlikely]] {
                    if (!match) {
                        // lower_bound() in reverse order points to predecessor of it unless the keys are equal.
                        if (it == rows.begin()) {
                            if (it->continuous()) {
                                _background_continuity = true;
                                _background_rt.apply(it->range_tombstone());
                            }
                            it = {};
                        } else {
                            --it;
                        }
                    } else {
                        // We can put anything in the match case since this continuity will not be used
                        // when advancing the cursor. Same applies to rt.
                        cont = is_continuous::no;
                        rt = {};
                    }
                }
            } else {
                _background_continuity = true; // Default continuity
            }

            if (!it) {
                if (heap_i != _heap.end()) {
                    _heap.erase(heap_i);
                    boost::range::make_heap(_heap, heap_less);
                }
            } else if (match) {
                _current_row.insert(_current_row.begin(), position_in_version{
                    it, std::move(rows), 0, _snp.version()->get_schema().get(), _unique_owner, cont, rt});
                if (heap_i != _heap.end()) {
                    _heap.erase(heap_i);
                    boost::range::make_heap(_heap, heap_less);
                }
            } else {
                if (heap_i != _heap.end()) {
                    heap_i->it = it;
                    heap_i->continuous = cont;
                    heap_i->rt = rt;
                    boost::range::make_heap(_heap, heap_less);
                } else {
                    _heap.push_back(position_in_version{
                        it, std::move(rows), 0, _snp.version()->get_schema().get(), _unique_owner, cont, rt});
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
            return maybe_advance_to(position());
        }
        return true;
    }

    // Moves the cursor to the first entry with position >= pos.
    // If no such entry exists, the cursor is still moved, although
    // it won't be pointing at a row. Still, continuous() will be valid.
    //
    // Returns true iff there can't be any clustering row entries
    // between lower_bound (inclusive) and the position to which the cursor
    // was advanced.
    //
    // May be called when cursor is not valid.
    // The cursor is valid after the call.
    // Must be called under reclaim lock.
    // When throws, the cursor is invalidated and its position is not changed.
    bool advance_to(position_in_partition_view lower_bound) {
        maybe_advance_to(lower_bound);
        return no_clustering_row_between_weak(_schema, lower_bound, position());
    }

    // Call only when valid.
    // Returns true iff the cursor is pointing at a row.
    bool at_a_row() const { return !_current_row.empty(); }

    // Advances to the next row, if any.
    // If there is no next row, advances to the extreme position in the direction of the cursor
    // (position_in_partition::before_all_clustering_rows() or position_in_partition::after_all_clustering_rows)
    // and does not point at a row.
    // Information about the range, continuous() and range_tombstone(), is still valid in this case.
    // Call only when valid, not necessarily pointing at a row.
    bool next() { return advance(true); }

    bool erase_and_advance() { return advance(false); }

    // Can be called when cursor is pointing at a row.
    // Returns true iff the key range adjacent to the cursor's position from the side of smaller keys
    // is marked as continuous.
    bool continuous() const { return _continuous; }

    // Can be called when cursor is valid, not necessarily pointing at a row.
    // Returns the range tombstone for the key range adjacent to the cursor's position from the side of smaller keys.
    // Excludes the range for the row itself. That information is returned by range_tombstone_for_row().
    // It's possible that range_tombstone() is empty and range_tombstone_for_row() is not empty.
    tombstone range_tombstone() const { return _range_tombstone; }

    // Can be called when cursor is pointing at a row.
    // Returns the range tombstone covering the row under the cursor.
    tombstone range_tombstone_for_row() const { return _range_tombstone_for_row; }

    // Can be called when cursor is pointing at a row.
    bool dummy() const { return _dummy; }

    // Can be called only when cursor is valid and pointing at a row, and !dummy().
    const clustering_key& key() const { return _position.key(); }

    // Can be called only when cursor is valid and pointing at a row.
    clustering_row row() const {
        // Note: if the precondition ("cursor is valid and pointing at a row") is fulfilled
        // then _current_row is not empty, so the below is valid.
        clustering_row cr(key(), deletable_row(_schema, *_current_row[0].schema, _current_row[0].it->row()));
        for (size_t i = 1; i < _current_row.size(); ++i) {
            cr.apply(_schema, *_current_row[i].schema, _current_row[i].it->row());
        }
        return cr;
    }

    const schema& latest_row_schema() const noexcept {
        return *_current_row[0].schema;
    }

    // Can be called only when cursor is valid and pointing at a row.
    deletable_row& latest_row() const noexcept {
        return _current_row[0].it->row();
    }

    // Can be called only when cursor is valid and pointing at a row.
    void latest_row_prepare_hash() const {
        _current_row[0].it->row().cells().prepare_hash(*_current_row[0].schema, column_kind::regular_column);
    }

    // Can be called only when cursor is valid and pointing at a row.
    // Monotonic exception guarantees.
    template <typename Consumer>
    requires std::is_invocable_v<Consumer, deletable_row&&>
    void consume_row(Consumer&& consumer) {
        for (position_in_version& v : _current_row) {
            if (v.unique_owner && (_schema.version() == v.schema->version())) [[likely]] {
                consumer(std::move(v.it->row()));
            } else {
                consumer(deletable_row(_schema, *v.schema, v.it->row()));
            }
        }
    }

    // Returns memory footprint of row entries under the cursor.
    // Can be called only when cursor is valid and pointing at a row.
    size_t memory_usage() const {
        size_t result = 0;
        for (const position_in_version& v : _current_row) {
            result += v.it->memory_usage(*v.schema);
        }
        return result;
    }

    struct ensure_result {
        rows_entry& row;
        mutation_partition_v2::rows_type::iterator it;
        bool inserted = false;
    };

    // Makes sure that a rows_entry for the row under the cursor exists in the latest version.
    // Doesn't change logical value or continuity of the snapshot.
    // Can be called only when cursor is valid and pointing at a row.
    // The cursor remains valid after the call and points at the same row as before.
    // Use only with evictable snapshots.
    ensure_result ensure_entry_in_latest() {
        auto&& rows = _snp.version()->partition().mutable_clustered_rows();
        if (is_in_latest_version()) {
            auto latest_i = get_iterator_in_latest_version();
            rows_entry& latest = *latest_i;
            if (_snp.at_latest_version()) {
                _snp.tracker()->touch(latest);
            }
            return {latest, latest_i, false};
        } else {
            // Copy row from older version because rows in evictable versions must
            // hold values which are independently complete to be consistent on eviction.
            auto e = [&] {
                if (!at_a_row()) {
                    return alloc_strategy_unique_ptr<rows_entry>(
                            current_allocator().construct<rows_entry>(*_snp.schema(), _position,
                                                                      is_dummy(!_position.is_clustering_row()), is_continuous::no));
                } else {
                    return alloc_strategy_unique_ptr<rows_entry>(
                            current_allocator().construct<rows_entry>(*_snp.schema(), *_current_row[0].schema, *_current_row[0].it));
                }
            }();
            rows_entry& re = *e;
            if (_reversed) { // latest_i is not reliably a successor
                re.set_continuous(false);
                e->set_range_tombstone(range_tombstone_for_row());
                rows_entry::tri_compare cmp(*_snp.schema());
                auto res = rows.insert(std::move(e), cmp);
                if (auto l = std::next(res.first); l != rows.end() && l->continuous()) {
                    re.set_continuous(true);
                    re.set_range_tombstone(l->range_tombstone());
                }
                if (res.second) {
                    _snp.tracker()->insert(re);
                }
                return {*res.first, res.first, res.second};
            } else {
                auto latest_i = get_iterator_in_latest_version();
                if (latest_i && latest_i->continuous()) {
                    e->set_continuous(true);
                    // See the "information monotonicity" rule.
                    e->set_range_tombstone(latest_i->range_tombstone());
                } else {
                    e->set_continuous(false);
                    e->set_range_tombstone(range_tombstone_for_row());
                }
                auto i = rows.insert_before(latest_i, std::move(e));
                _snp.tracker()->insert(re);
                return {re, i, true};
            }
        }
    }

    // Returns a pointer to rows_entry with given position in latest version or
    // creates a neutral one, provided that it belongs to a continuous range.
    // Otherwise returns nullptr.
    // Doesn't change logical value of mutation_partition or continuity of the snapshot.
    // The cursor doesn't have to be valid.
    // The cursor is invalid after the call.
    // When returns an engaged optional, the attributes of the cursor: continuous() and range_tombstone()
    // are valid, as if the cursor was advanced to the requested position.
    // Assumes the snapshot is evictable and not populated by means other than ensure_entry_if_complete().
    // Subsequent calls to ensure_entry_if_complete() or advance_to() must be given weakly monotonically increasing
    // positions unless iterators are invalidated across the calls.
    // The cursor must not be a reversed-order cursor.
    // Use only with evictable snapshots.
    std::optional<ensure_result> ensure_entry_if_complete(position_in_partition_view pos) {
        if (_reversed) { // latest_i is unreliable
            throw_with_backtrace<std::logic_error>("ensure_entry_if_complete() called on reverse cursor");
        }
        position_in_partition::less_compare less(_schema);
        if (!iterators_valid() || less(position(), pos)) {
            auto has_entry = maybe_advance_to(pos);
            SCYLLA_ASSERT(has_entry); // evictable snapshots must have a dummy after all rows.
        }
        auto&& rows = _snp.version()->partition().mutable_clustered_rows();
        auto latest_i = get_iterator_in_latest_version();
        position_in_partition::equal_compare eq(_schema);
        if (eq(position(), pos)) {
            // Check if entry was already inserted by previous call to ensure_entry_if_complete()
            if (latest_i != rows.begin()) {
                auto prev_i = std::prev(latest_i);
                if (eq(prev_i->position(), pos)) {
                    return ensure_result{*prev_i, prev_i, false};
                }
            }
            return ensure_entry_in_latest();
        } else if (!continuous()) {
            return std::nullopt;
        }
        // Check if entry was already inserted by previous call to ensure_entry_if_complete()
        if (latest_i != rows.begin()) {
            auto prev_i = std::prev(latest_i);
            if (eq(prev_i->position(), pos)) {
                return ensure_result{*prev_i, prev_i, false};
            }
        }
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(*_snp.version()->get_schema(), pos,
                    is_dummy(!pos.is_clustering_row()),
                    is_continuous::no));
        if (latest_i && latest_i->continuous()) {
            e->set_continuous(true);
            e->set_range_tombstone(latest_i->range_tombstone()); // See the "information monotonicity" rule.
        } else {
            // Even if the range in the latest version is not continuous, the row itself is assumed to be complete,
            // so it must inherit the current range tombstone.
            e->set_range_tombstone(range_tombstone());
        }
        auto e_i = rows.insert_before(latest_i, std::move(e));
        _snp.tracker()->insert(*e_i);
        return ensure_result{*e_i, e_i, true};
    }

    // Brings the entry pointed to by the cursor to the front of the LRU
    // Cursor must be valid and pointing at a row.
    // Use only with evictable snapshots.
    void touch() {
        // We cannot bring entries from non-latest versions to the front because that
        // could result violate ordering invariant for the LRU, which states that older versions
        // must be evicted first. Needed to keep the snapshot consistent.
        if (_snp.at_latest_version() && is_in_latest_version()) {
            _snp.tracker()->touch(*get_iterator_in_latest_version());
        }
    }

    // Position of the cursor in the cursor schema domain.
    // Can be called when cursor is pointing at a row, even when invalid, or when valid.
    position_in_partition_view position() const {
        return to_query_domain(_position);
    }

    // Position of the cursor in the table schema domain.
    // Can be called when cursor is pointing at a row, even when invalid, or when valid.
    position_in_partition_view table_position() const {
        return _position;
    }

    friend fmt::formatter<partition_snapshot_row_cursor>;
};

template <> struct fmt::formatter<partition_snapshot_row_cursor> : fmt::formatter<string_view> {
    auto format(const  partition_snapshot_row_cursor& cur, fmt::format_context& ctx) const {
        auto out = ctx.out();
        out = fmt::format_to(out, "{{cursor: position={}, cont={}, rt={}}}",
                   cur._position, cur.continuous(), cur.range_tombstone());
        if (cur.range_tombstone() != cur.range_tombstone_for_row()) {
            out = fmt::format_to(out, ", row_rt={}", cur.range_tombstone_for_row());
        }
        out = fmt::format_to(out, ", ");
        if (cur._reversed) {
            out = fmt::format_to(out, "reversed, ");
        }
        if (!cur.iterators_valid()) {
            return fmt::format_to(out, " iterators invalid}}");
        }
        out = fmt::format_to(out, "snp={}, current=[", fmt::ptr(&cur._snp));
        bool first = true;
        for (auto&& v : cur._current_row) {
            if (!first) {
                out = fmt::format_to(out, ", ");
            }
            first = false;
            out = fmt::format_to(out, "{{v={}, pos={}, cont={}, rt={}, row_rt={}}}",
                       v.version_no, v.it->position(), v.continuous, v.rt, v.it->range_tombstone());
        }
        out = fmt::format_to(out, "], heap[\n  ");
        first = true;
        for (auto&& v : cur._heap) {
            if (!first) {
                out = fmt::format_to(out, ",\n  ");
            }
            first = false;
            out = fmt::format_to(out, "{{v={}, pos={}, cont={}, rt={}, row_rt={}}}",
                       v.version_no, v.it->position(), v.continuous, v.rt, v.it->range_tombstone());
        }
        out = fmt::format_to(out, "], latest_iterator=[");
        if (cur._latest_it) {
            mutation_partition::rows_type::iterator i = *cur._latest_it;
            if (!i) {
                out = fmt::format_to(out, "end");
            } else {
                out = fmt::format_to(out, "{}", i->position());
            }
        } else {
            out = fmt::format_to(out, "<none>");
        }
        return fmt::format_to(out, "]}}");
    }
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
