/*
 * Copyright (C) 2014-present ScyllaDB
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

#include <iosfwd>
#include <map>
#include <boost/intrusive/set.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/intrusive/parent_from_member.hpp>

#include <seastar/core/bitset-iter.hh>
#include <seastar/util/optimized_optional.hh>

#include "schema_fwd.hh"
#include "tombstone.hh"
#include "keys.hh"
#include "position_in_partition.hh"
#include "atomic_cell_or_collection.hh"
#include "query-result.hh"
#include "mutation_partition_view.hh"
#include "mutation_partition_visitor.hh"
#include "utils/managed_vector.hh"
#include "hashing_partition_visitor.hh"
#include "range_tombstone_list.hh"
#include "clustering_key_filter.hh"
#include "utils/intrusive_btree.hh"
#include "utils/preempt.hh"
#include "utils/managed_ref.hh"
#include "utils/compact-radix-tree.hh"

class mutation_fragment;

struct cell_hash {
    using size_type = uint64_t;
    static constexpr size_type no_hash = 0;

    size_type hash = no_hash;

    explicit operator bool() const noexcept {
        return hash != no_hash;
    }
};

template<>
struct appending_hash<cell_hash> {
    template<typename Hasher>
    void operator()(Hasher& h, const cell_hash& ch) const {
        feed_hash(h, ch.hash);
    }
};

using cell_hash_opt = seastar::optimized_optional<cell_hash>;

struct cell_and_hash {
    atomic_cell_or_collection cell;
    mutable cell_hash_opt hash;

    cell_and_hash() = default;
    cell_and_hash(cell_and_hash&&) noexcept = default;
    cell_and_hash& operator=(cell_and_hash&&) noexcept = default;

    cell_and_hash(atomic_cell_or_collection&& cell, cell_hash_opt hash)
        : cell(std::move(cell))
        , hash(hash)
    { }
};

class compaction_garbage_collector;

//
// Container for cells of a row. Cells are identified by column_id.
//
// All cells must belong to a single column_kind. The kind is not stored
// for space-efficiency reasons. Whenever a method accepts a column_kind,
// the caller must always supply the same column_kind.
//
//
class row {
    friend class size_calculator;
    using size_type = std::make_unsigned_t<column_id>;
    size_type _size = 0;
    using sparse_array_type = compact_radix_tree::tree<cell_and_hash, column_id>;
    sparse_array_type _cells;
public:
    row();
    ~row();
    row(const schema&, column_kind, const row&);
    row(row&& other) noexcept;
    row& operator=(row&& other) noexcept;
    size_t size() const { return _size; }
    bool empty() const { return _size == 0; }

    const atomic_cell_or_collection& cell_at(column_id id) const;

    // Returns a pointer to cell's value or nullptr if column is not set.
    const atomic_cell_or_collection* find_cell(column_id id) const;
    // Returns a pointer to cell's value and hash or nullptr if column is not set.
    const cell_and_hash* find_cell_and_hash(column_id id) const;

    template<typename Func>
    void remove_if(Func&& func) {
        _cells.weed([func, this] (column_id id, cell_and_hash& cah) {
            if (!func(id, cah.cell)) {
                return false;
            }

            _size--;
            return true;
        });
    }

private:
    template<typename Func>
    void consume_with(Func&&);

    // Func obeys the same requirements as for for_each_cell below.
    template<typename Func, typename MaybeConstCellAndHash>
    static constexpr auto maybe_invoke_with_hash(Func& func, column_id id, MaybeConstCellAndHash& c_a_h) {
        if constexpr (std::is_invocable_v<Func, column_id, const cell_and_hash&>) {
            return func(id, c_a_h);
        } else {
            return func(id, c_a_h.cell);
        }
    }

public:
    // Calls Func(column_id, cell_and_hash&) or Func(column_id, atomic_cell_and_collection&)
    // for each cell in this row, depending on the concrete Func type.
    // noexcept if Func doesn't throw.
    template<typename Func>
    void for_each_cell(Func&& func) {
        _cells.walk([func] (column_id id, cell_and_hash& cah) {
            maybe_invoke_with_hash(func, id, cah);
            return true;
        });
    }

    template<typename Func>
    void for_each_cell(Func&& func) const {
        _cells.walk([func] (column_id id, const cell_and_hash& cah) {
            maybe_invoke_with_hash(func, id, cah);
            return true;
        });
    }

    template<typename Func>
    void for_each_cell_until(Func&& func) const {
        _cells.walk([func] (column_id id, const cell_and_hash& cah) {
            return maybe_invoke_with_hash(func, id, cah) != stop_iteration::yes;
        });
    }

    // Merges cell's value into the row.
    // Weak exception guarantees.
    void apply(const column_definition& column, const atomic_cell_or_collection& cell, cell_hash_opt hash = cell_hash_opt());

    // Merges cell's value into the row.
    // Weak exception guarantees.
    void apply(const column_definition& column, atomic_cell_or_collection&& cell, cell_hash_opt hash = cell_hash_opt());

    // Monotonic exception guarantees. In case of exception the sum of cell and this remains the same as before the exception.
    void apply_monotonically(const column_definition& column, atomic_cell_or_collection&& cell, cell_hash_opt hash = cell_hash_opt());

    // Adds cell to the row. The column must not be already set.
    void append_cell(column_id id, atomic_cell_or_collection cell);

    // Weak exception guarantees
    void apply(const schema&, column_kind, const row& src);
    // Weak exception guarantees
    void apply(const schema&, column_kind, row&& src);
    // Monotonic exception guarantees
    void apply_monotonically(const schema&, column_kind, row&& src);

    // Expires cells based on query_time. Expires tombstones based on gc_before
    // and max_purgeable. Removes cells covered by tomb.
    // Returns true iff there are any live cells left.
    bool compact_and_expire(
            const schema& s,
            column_kind kind,
            row_tombstone tomb,
            gc_clock::time_point query_time,
            can_gc_fn&,
            gc_clock::time_point gc_before,
            const row_marker& marker,
            compaction_garbage_collector* collector = nullptr);

    bool compact_and_expire(
            const schema& s,
            column_kind kind,
            row_tombstone tomb,
            gc_clock::time_point query_time,
            can_gc_fn&,
            gc_clock::time_point gc_before,
            compaction_garbage_collector* collector = nullptr);

    row difference(const schema&, column_kind, const row& other) const;

    bool equal(column_kind kind, const schema& this_schema, const row& other, const schema& other_schema) const;

    size_t external_memory_usage(const schema&, column_kind) const;

    cell_hash_opt cell_hash_for(column_id id) const;

    void prepare_hash(const schema& s, column_kind kind) const;
    void clear_hash() const;

    bool is_live(const schema&, column_kind kind, tombstone tomb = tombstone(), gc_clock::time_point now = gc_clock::time_point::min()) const;

    class printer {
        const schema& _schema;
        column_kind _kind;
        const row& _row;
    public:
        printer(const schema& s, column_kind k, const row& r) : _schema(s), _kind(k), _row(r) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const printer& p);
    };
    friend std::ostream& operator<<(std::ostream& os, const printer& p);
};

// Like row, but optimized for the case where the row doesn't exist (e.g. static rows)
class lazy_row {
    managed_ref<row> _row;
    static inline const row _empty_row;
public:
    lazy_row() = default;
    explicit lazy_row(row&& r) {
        if (!r.empty()) {
            _row = make_managed<row>(std::move(r));
        }
    }

    lazy_row(const schema& s, column_kind kind, const lazy_row& r) {
        if (!r.empty()) {
            _row = make_managed<row>(s, kind, *r._row);
        }
    }

    lazy_row(const schema& s, column_kind kind, const row& r) {
        if (!r.empty()) {
            _row = make_managed<row>(s, kind, r);
        }
    }

    row& maybe_create() {
        if (!_row) {
            _row = make_managed<row>();
        }
        return *_row;
    }

    const row& get_existing() const & {
        return *_row;
    }

    row& get_existing() & {
        return *_row;
    }

    row&& get_existing() && {
        return std::move(*_row);
    }

    const row& get() const {
        return _row ? *_row : _empty_row;
    }

    size_t size() const {
        if (!_row) {
            return 0;
        }
        return _row->size();
    }

    bool empty() const {
        if (!_row) {
            return true;
        }
        return _row->empty();
    }

    void reserve(column_id nr) {
        if (nr) {
            maybe_create();
        }
    }

    const atomic_cell_or_collection& cell_at(column_id id) const {
        if (!_row) {
            throw_with_backtrace<std::out_of_range>(format("Column not found for id = {:d}", id));
        } else {
            return _row->cell_at(id);
        }
    }

    // Returns a pointer to cell's value or nullptr if column is not set.
    const atomic_cell_or_collection* find_cell(column_id id) const {
        if (!_row) {
            return nullptr;
        }
        return _row->find_cell(id);
    }

    // Returns a pointer to cell's value and hash or nullptr if column is not set.
    const cell_and_hash* find_cell_and_hash(column_id id) const {
        if (!_row) {
            return nullptr;
        }
        return _row->find_cell_and_hash(id);
    }

    // Calls Func(column_id, cell_and_hash&) or Func(column_id, atomic_cell_and_collection&)
    // for each cell in this row, depending on the concrete Func type.
    // noexcept if Func doesn't throw.
    template<typename Func>
    void for_each_cell(Func&& func) {
        if (!_row) {
            return;
        }
        _row->for_each_cell(std::forward<Func>(func));
    }

    template<typename Func>
    void for_each_cell(Func&& func) const {
        if (!_row) {
            return;
        }
        _row->for_each_cell(std::forward<Func>(func));
    }

    template<typename Func>
    void for_each_cell_until(Func&& func) const {
        if (!_row) {
            return;
        }
        _row->for_each_cell_until(std::forward<Func>(func));
    }

    // Merges cell's value into the row.
    // Weak exception guarantees.
    void apply(const column_definition& column, const atomic_cell_or_collection& cell, cell_hash_opt hash = cell_hash_opt()) {
        maybe_create().apply(column, cell, std::move(hash));
    }

    // Merges cell's value into the row.
    // Weak exception guarantees.
    void apply(const column_definition& column, atomic_cell_or_collection&& cell, cell_hash_opt hash = cell_hash_opt()) {
        maybe_create().apply(column, std::move(cell), std::move(hash));
    }

    // Monotonic exception guarantees. In case of exception the sum of cell and this remains the same as before the exception.
    void apply_monotonically(const column_definition& column, atomic_cell_or_collection&& cell, cell_hash_opt hash = cell_hash_opt()) {
        maybe_create().apply_monotonically(column, std::move(cell), std::move(hash));
    }

    // Adds cell to the row. The column must not be already set.
    void append_cell(column_id id, atomic_cell_or_collection cell) {
        maybe_create().append_cell(id, std::move(cell));
    }

    // Weak exception guarantees
    void apply(const schema& s, column_kind kind, const row& src) {
        if (src.empty()) {
            return;
        }
        maybe_create().apply(s, kind, src);
    }

    // Weak exception guarantees
    void apply(const schema& s, column_kind kind, const lazy_row& src) {
        if (src.empty()) {
            return;
        }
        maybe_create().apply(s, kind, src.get_existing());
    }

    // Weak exception guarantees
    void apply(const schema& s, column_kind kind, row&& src) {
        if (src.empty()) {
            return;
        }
        maybe_create().apply(s, kind, std::move(src));
    }

    // Monotonic exception guarantees
    void apply_monotonically(const schema& s, column_kind kind, row&& src) {
        if (src.empty()) {
            return;
        }
        maybe_create().apply_monotonically(s, kind, std::move(src));
    }

    // Monotonic exception guarantees
    void apply_monotonically(const schema& s, column_kind kind, lazy_row&& src) {
        if (src.empty()) {
            return;
        }
        if (!_row) {
            _row = std::move(src._row);
            return;
        }
        get_existing().apply_monotonically(s, kind, std::move(src.get_existing()));
    }

    // Expires cells based on query_time. Expires tombstones based on gc_before
    // and max_purgeable. Removes cells covered by tomb.
    // Returns true iff there are any live cells left.
    bool compact_and_expire(
            const schema& s,
            column_kind kind,
            row_tombstone tomb,
            gc_clock::time_point query_time,
            can_gc_fn& can_gc,
            gc_clock::time_point gc_before,
            const row_marker& marker,
            compaction_garbage_collector* collector = nullptr);

    bool compact_and_expire(
            const schema& s,
            column_kind kind,
            row_tombstone tomb,
            gc_clock::time_point query_time,
            can_gc_fn& can_gc,
            gc_clock::time_point gc_before,
            compaction_garbage_collector* collector = nullptr);

    lazy_row difference(const schema& s, column_kind kind, const lazy_row& other) const {
        if (!_row) {
            return lazy_row();
        }
        if (!other._row) {
            return lazy_row(s, kind, *_row);
        }
        return lazy_row(_row->difference(s, kind, *other._row));
    }

    bool equal(column_kind kind, const schema& this_schema, const lazy_row& other, const schema& other_schema) const {
        bool e1 = empty();
        bool e2 = other.empty();
        if (e1 && e2) {
            return true;
        }
        if (e1 != e2) {
            return false;
        }
        // both non-empty
        return _row->equal(kind, this_schema, *other._row, other_schema);
    }

    size_t external_memory_usage(const schema& s, column_kind kind) const {
        if (!_row) {
            return 0;
        }
        return _row.external_memory_usage() + _row->external_memory_usage(s, kind);
    }

    cell_hash_opt cell_hash_for(column_id id) const {
        if (!_row) {
            return cell_hash_opt{};
        }
        return _row->cell_hash_for(id);
    }

    void prepare_hash(const schema& s, column_kind kind) const {
        if (!_row) {
            return;
        }
        _row->prepare_hash(s, kind);
    }

    void clear_hash() const {
        if (!_row) {
            return;
        }
        _row->clear_hash();
    }

    bool is_live(const schema& s, column_kind kind, tombstone tomb = tombstone(), gc_clock::time_point now = gc_clock::time_point::min()) const {
        if (!_row) {
            return false;
        }
        return _row->is_live(s, kind, tomb, now);
    }

    class printer {
        const schema& _schema;
        column_kind _kind;
        const lazy_row& _row;
    public:
        printer(const schema& s, column_kind k, const lazy_row& r) : _schema(s), _kind(k), _row(r) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const printer& p);
    };
};

// Used to return the timestamp of the latest update to the row
struct max_timestamp {
    api::timestamp_type max = api::missing_timestamp;

    void update(api::timestamp_type ts) {
        max = std::max(max, ts);
    }
};

template<>
struct appending_hash<row> {
    static constexpr int null_hash_value = 0xbeefcafe;
    template<typename Hasher>
    void operator()(Hasher& h, const row& cells, const schema& s, column_kind kind, const query::column_id_vector& columns, max_timestamp& max_ts) const;
};

class row_marker;
int compare_row_marker_for_merge(const row_marker& left, const row_marker& right) noexcept;

class row_marker {
    static constexpr gc_clock::duration no_ttl { 0 };
    static constexpr gc_clock::duration dead { -1 };
    static constexpr gc_clock::time_point no_expiry { gc_clock::duration(0) };
    api::timestamp_type _timestamp = api::missing_timestamp;
    gc_clock::duration _ttl = no_ttl;
    gc_clock::time_point _expiry = no_expiry;
public:
    row_marker() = default;
    explicit row_marker(api::timestamp_type created_at) : _timestamp(created_at) { }
    row_marker(api::timestamp_type created_at, gc_clock::duration ttl, gc_clock::time_point expiry)
        : _timestamp(created_at), _ttl(ttl), _expiry(expiry)
    { }
    explicit row_marker(tombstone deleted_at)
        : _timestamp(deleted_at.timestamp), _ttl(dead), _expiry(deleted_at.deletion_time)
    { }
    bool is_missing() const {
        return _timestamp == api::missing_timestamp;
    }
    bool is_live() const {
        return !is_missing() && _ttl != dead;
    }
    bool is_live(tombstone t, gc_clock::time_point now) const {
        if (is_missing() || _ttl == dead) {
            return false;
        }
        if (_ttl != no_ttl && _expiry <= now) {
            return false;
        }
        return _timestamp > t.timestamp;
    }
    // Can be called only when !is_missing().
    bool is_dead(gc_clock::time_point now) const {
        if (_ttl == dead) {
            return true;
        }
        return _ttl != no_ttl && _expiry <= now;
    }
    // Can be called only when is_live().
    bool is_expiring() const {
        return _ttl != no_ttl;
    }
    // Can be called only when is_expiring().
    gc_clock::duration ttl() const {
        return _ttl;
    }
    // Can be called only when is_expiring().
    gc_clock::time_point expiry() const {
        return _expiry;
    }
    // Should be called when is_dead() or is_expiring().
    // Safe to be called when is_missing().
    // When is_expiring(), returns the the deletion time of the marker when it finally expires.
    gc_clock::time_point deletion_time() const {
        return _ttl == dead ? _expiry : _expiry - _ttl;
    }
    api::timestamp_type timestamp() const {
        return _timestamp;
    }
    void apply(const row_marker& rm) {
        if (compare_row_marker_for_merge(*this, rm) < 0) {
            *this = rm;
        }
    }
    // Expires cells and tombstones. Removes items covered by higher level
    // tombstones.
    // Returns true if row marker is live.
    bool compact_and_expire(tombstone tomb, gc_clock::time_point now,
            can_gc_fn& can_gc, gc_clock::time_point gc_before, compaction_garbage_collector* collector = nullptr);
    // Consistent with feed_hash()
    bool operator==(const row_marker& other) const {
        if (_timestamp != other._timestamp) {
            return false;
        }
        if (is_missing()) {
            return true;
        }
        if (_ttl != other._ttl) {
            return false;
        }
        return _ttl == no_ttl || _expiry == other._expiry;
    }
    bool operator!=(const row_marker& other) const {
        return !(*this == other);
    }
    // Consistent with operator==()
    template<typename Hasher>
    void feed_hash(Hasher& h) const {
        ::feed_hash(h, _timestamp);
        if (!is_missing()) {
            ::feed_hash(h, _ttl);
            if (_ttl != no_ttl) {
                ::feed_hash(h, _expiry);
            }
        }
    }
    friend std::ostream& operator<<(std::ostream& os, const row_marker& rm);
};

template<>
struct appending_hash<row_marker> {
    template<typename Hasher>
    void operator()(Hasher& h, const row_marker& m) const {
        m.feed_hash(h);
    }
};

class shadowable_tombstone {
    tombstone _tomb;
public:

    explicit shadowable_tombstone(api::timestamp_type timestamp, gc_clock::time_point deletion_time)
            : _tomb(timestamp, deletion_time) {
    }

    explicit shadowable_tombstone(tombstone tomb = tombstone())
            : _tomb(std::move(tomb)) {
    }

    std::strong_ordering operator<=>(const shadowable_tombstone& t) const {
        return _tomb <=> t._tomb;
    }

    bool operator==(const shadowable_tombstone&) const = default;
    bool operator!=(const shadowable_tombstone&) const = default;

    explicit operator bool() const {
        return bool(_tomb);
    }

    const tombstone& tomb() const {
        return _tomb;
    }

    // A shadowable row tombstone is valid only if the row has no live marker. In other words,
    // the row tombstone is only valid as long as no newer insert is done (thus setting a
    // live row marker; note that if the row timestamp set is lower than the tombstone's,
    // then the tombstone remains in effect as usual). If a row has a shadowable tombstone
    // with timestamp Ti and that row is updated with a timestamp Tj, such that Tj > Ti
    // (and that update sets the row marker), then the shadowable tombstone is shadowed by
    // that update. A concrete consequence is that if the update has cells with timestamp
    // lower than Ti, then those cells are preserved (since the deletion is removed), and
    // this is contrary to a regular, non-shadowable row tombstone where the tombstone is
    // preserved and such cells are removed.
    bool is_shadowed_by(const row_marker& marker) const {
        return marker.is_live() && marker.timestamp() > _tomb.timestamp;
    }

    void maybe_shadow(tombstone t, row_marker marker) noexcept {
        if (is_shadowed_by(marker)) {
            _tomb = std::move(t);
        }
    }

    void apply(tombstone t) noexcept {
        _tomb.apply(t);
    }

    void apply(shadowable_tombstone t) noexcept {
        _tomb.apply(t._tomb);
    }

    friend std::ostream& operator<<(std::ostream& out, const shadowable_tombstone& t) {
        if (t) {
            return out << "{shadowable tombstone: timestamp=" << t.tomb().timestamp
                   << ", deletion_time=" << t.tomb().deletion_time.time_since_epoch().count()
                   << "}";
        } else {
            return out << "{shadowable tombstone: none}";
        }
    }
};

template<>
struct appending_hash<shadowable_tombstone> {
    template<typename Hasher>
    void operator()(Hasher& h, const shadowable_tombstone& t) const {
        feed_hash(h, t.tomb());
    }
};

/*
The rules for row_tombstones are as follows:
  - The shadowable tombstone is always >= than the regular one;
  - The regular tombstone works as expected;
  - The shadowable tombstone doesn't erase or compact away the regular
    row tombstone, nor dead cells;
  - The shadowable tombstone can erase live cells, but only provided they
    can be recovered (e.g., by including all cells in a MV update, both
    updated cells and pre-existing ones);
  - The shadowable tombstone can be erased or compacted away by a newer
    row marker.
*/
class row_tombstone {
    tombstone _regular;
    shadowable_tombstone _shadowable; // _shadowable is always >= _regular
public:
    explicit row_tombstone(tombstone regular, shadowable_tombstone shadowable)
            : _regular(std::move(regular))
            , _shadowable(std::move(shadowable)) {
    }

    explicit row_tombstone(tombstone regular)
            : row_tombstone(regular, shadowable_tombstone(regular)) {
    }

    row_tombstone() = default;

    std::strong_ordering operator<=>(const row_tombstone& t) const {
        return _shadowable <=> t._shadowable;
    }
    bool operator==(const row_tombstone& t) const {
        return _shadowable == t._shadowable;
    }
    bool operator!=(const row_tombstone& t) const {
        return _shadowable != t._shadowable;
    }

    explicit operator bool() const {
        return bool(_shadowable);
    }

    const tombstone& tomb() const {
        return _shadowable.tomb();
    }

    const gc_clock::time_point max_deletion_time() const {
        return std::max(_regular.deletion_time, _shadowable.tomb().deletion_time);
    }

    const tombstone& regular() const {
        return _regular;
    }

    const shadowable_tombstone& shadowable() const {
        return _shadowable;
    }

    bool is_shadowable() const {
        return _shadowable.tomb() > _regular;
    }

    void maybe_shadow(const row_marker& marker) noexcept {
        _shadowable.maybe_shadow(_regular, marker);
    }

    void apply(tombstone regular) noexcept {
        _shadowable.apply(regular);
        _regular.apply(regular);
    }

    void apply(shadowable_tombstone shadowable, row_marker marker) noexcept {
        _shadowable.apply(shadowable.tomb());
        _shadowable.maybe_shadow(_regular, marker);
    }

    void apply(row_tombstone t, row_marker marker) noexcept {
        _regular.apply(t._regular);
        _shadowable.apply(t._shadowable);
        _shadowable.maybe_shadow(_regular, marker);
    }

    friend std::ostream& operator<<(std::ostream& out, const row_tombstone& t) {
        if (t) {
            return out << "{row_tombstone: " << t._regular << (t.is_shadowable() ? t._shadowable : shadowable_tombstone()) << "}";
        } else {
            return out << "{row_tombstone: none}";
        }
    }
};

template<>
struct appending_hash<row_tombstone> {
    template<typename Hasher>
    void operator()(Hasher& h, const row_tombstone& t) const {
        feed_hash(h, t.regular());
        if (t.is_shadowable()) {
            feed_hash(h, t.shadowable());
        }
    }
};

class deletable_row final {
    row_tombstone _deleted_at;
    row_marker _marker;
    row _cells;
public:
    deletable_row() {}
    deletable_row(const schema& s, const deletable_row& other)
        : _deleted_at(other._deleted_at)
        , _marker(other._marker)
        , _cells(s, column_kind::regular_column, other._cells)
    { }
    deletable_row(row_tombstone&& tomb, row_marker&& marker, row&& cells)
        : _deleted_at(std::move(tomb)), _marker(std::move(marker)), _cells(std::move(cells))
    {}

    void apply(tombstone deleted_at) {
        _deleted_at.apply(deleted_at);
    }

    void apply(shadowable_tombstone deleted_at) {
        _deleted_at.apply(deleted_at, _marker);
    }

    void apply(row_tombstone deleted_at) {
        _deleted_at.apply(deleted_at, _marker);
    }

    void apply(const row_marker& rm) {
        _marker.apply(rm);
        maybe_shadow();
    }

    void remove_tombstone() {
        _deleted_at = {};
    }

    void maybe_shadow() {
        _deleted_at.maybe_shadow(_marker);
    }

    // Weak exception guarantees. After exception, both src and this will commute to the same value as
    // they would should the exception not happen.
    void apply(const schema& s, deletable_row&& src);
    void apply_monotonically(const schema& s, deletable_row&& src);
public:
    row_tombstone deleted_at() const { return _deleted_at; }
    api::timestamp_type created_at() const { return _marker.timestamp(); }
    row_marker& marker() { return _marker; }
    const row_marker& marker() const { return _marker; }
    const row& cells() const { return _cells; }
    row& cells() { return _cells; }
    bool equal(column_kind, const schema& s, const deletable_row& other, const schema& other_schema) const;
    bool is_live(const schema& s, tombstone base_tombstone = tombstone(), gc_clock::time_point query_time = gc_clock::time_point::min()) const;
    bool empty() const { return !_deleted_at && _marker.is_missing() && !_cells.size(); }
    deletable_row difference(const schema&, column_kind, const deletable_row& other) const;

    class printer {
        const schema& _schema;
        const deletable_row& _deletable_row;
    public:
        printer(const schema& s, const deletable_row& r) : _schema(s), _deletable_row(r) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const printer& p);
    };
    friend std::ostream& operator<<(std::ostream& os, const printer& p);
};

class cache_tracker;

class rows_entry {
    using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    friend class size_calculator;
    intrusive_b::member_hook _link;
    clustering_key _key;
    deletable_row _row;
    lru_link_type _lru_link;
    struct flags {
        // _before_ck and _after_ck encode position_in_partition::weight
        bool _before_ck : 1;
        bool _after_ck : 1;
        bool _continuous : 1; // See doc of is_continuous.
        bool _dummy : 1;
        // Marks a dummy entry which is after_all_clustered_rows() position.
        // Needed so that eviction, which can't use comparators, can check if it's dealing with it.
        bool _last_dummy : 1;
        flags() : _before_ck(0), _after_ck(0), _continuous(true), _dummy(false), _last_dummy(false) { }
    } _flags{};
public:
    using lru_type = bi::list<rows_entry,
        bi::member_hook<rows_entry, rows_entry::lru_link_type, &rows_entry::_lru_link>,
        bi::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.

    void unlink_from_lru() noexcept { _lru_link.unlink(); }
    struct last_dummy_tag {};
    explicit rows_entry(clustering_key&& key)
        : _key(std::move(key))
    { }
    explicit rows_entry(const clustering_key& key)
        : _key(key)
    { }
    rows_entry(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous)
        : _key(pos.key())
    {
        _flags._last_dummy = bool(dummy) && pos.is_after_all_clustered_rows(s);
        _flags._dummy = bool(dummy);
        _flags._continuous = bool(continuous);
        _flags._before_ck = pos.is_before_key();
        _flags._after_ck = pos.is_after_key();
    }
    rows_entry(const schema& s, last_dummy_tag, is_continuous continuous)
        : rows_entry(s, position_in_partition_view::after_all_clustered_rows(), is_dummy::yes, continuous)
    { }
    rows_entry(const clustering_key& key, deletable_row&& row)
        : _key(key), _row(std::move(row))
    { }
    rows_entry(const schema& s, const clustering_key& key, const deletable_row& row)
        : _key(key), _row(s, row)
    { }
    rows_entry(rows_entry&& o) noexcept;
    rows_entry(const schema& s, const rows_entry& e)
        : _key(e._key)
        , _row(s, e._row)
        , _flags(e._flags)
    { }
    // Valid only if !dummy()
    clustering_key& key() {
        return _key;
    }
    // Valid only if !dummy()
    const clustering_key& key() const {
        return _key;
    }
    deletable_row& row() {
        return _row;
    }
    const deletable_row& row() const {
        return _row;
    }
    position_in_partition_view position() const {
        return position_in_partition_view(partition_region::clustered, bound_weight(_flags._after_ck - _flags._before_ck), &_key);
    }

    is_continuous continuous() const { return is_continuous(_flags._continuous); }
    void set_continuous(bool value) { _flags._continuous = value; }
    void set_continuous(is_continuous value) { set_continuous(bool(value)); }
    is_dummy dummy() const { return is_dummy(_flags._dummy); }
    bool is_last_dummy() const { return _flags._last_dummy; }
    void set_dummy(bool value) { _flags._dummy = value; }
    void set_dummy(is_dummy value) { _flags._dummy = bool(value); }
    void replace_with(rows_entry&& other) noexcept;

    void apply(row_tombstone t) {
        _row.apply(t);
    }
    void apply_monotonically(const schema& s, rows_entry&& e) {
        _row.apply(s, std::move(e._row));
    }
    bool empty() const {
        return _row.empty();
    }
    struct tri_compare {
        position_in_partition::tri_compare _c;
        explicit tri_compare(const schema& s) : _c(s) {}

        std::strong_ordering operator()(const rows_entry& e1, const rows_entry& e2) const {
            return _c(e1.position(), e2.position());
        }
        std::strong_ordering operator()(const clustering_key& key, const rows_entry& e) const {
            return _c(position_in_partition_view::for_key(key), e.position());
        }
        std::strong_ordering operator()(const rows_entry& e, const clustering_key& key) const {
            return _c(e.position(), position_in_partition_view::for_key(key));
        }
        std::strong_ordering operator()(const rows_entry& e, position_in_partition_view p) const {
            return _c(e.position(), p);
        }
        std::strong_ordering operator()(position_in_partition_view p, const rows_entry& e) const {
            return _c(p, e.position());
        }
        std::strong_ordering operator()(position_in_partition_view p1, position_in_partition_view p2) const {
            return _c(p1, p2);
        }
    };
    struct compare {
        tri_compare _c;
        explicit compare(const schema& s) : _c(s) {}

        template <typename K1, typename K2>
        bool operator()(const K1& k1, const K2& k2) const { return _c(k1, k2) < 0; }
    };
    bool equal(const schema& s, const rows_entry& other) const;
    bool equal(const schema& s, const rows_entry& other, const schema& other_schema) const;

    size_t memory_usage(const schema&) const;
    void on_evicted(cache_tracker&) noexcept;

    class printer {
        const schema& _schema;
        const rows_entry& _rows_entry;
    public:
        printer(const schema& s, const rows_entry& r) : _schema(s), _rows_entry(r) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const printer& p);
    };
    friend std::ostream& operator<<(std::ostream& os, const printer& p);

    using container_type = intrusive_b::tree<rows_entry, &rows_entry::_link, rows_entry::tri_compare, 12, 20, intrusive_b::key_search::linear>;
};

struct mutation_application_stats {
    uint64_t row_hits = 0;
    uint64_t row_writes = 0;

    mutation_application_stats& operator+=(const mutation_application_stats& other) {
        row_hits += other.row_hits;
        row_writes += other.row_writes;
        return *this;
    }
};

// Represents a set of writes made to a single partition.
//
// The object is schema-dependent. Each instance is governed by some
// specific schema version. Accessors require a reference to the schema object
// of that version.
//
// There is an operation of addition defined on mutation_partition objects
// (also called "apply"), which gives as a result an object representing the
// sum of writes contained in the addends. For instances governed by the same
// schema, addition is commutative and associative.
//
// In addition to representing writes, the object supports specifying a set of
// partition elements called "continuity". This set can be used to represent
// lack of information about certain parts of the partition. It can be
// specified which ranges of clustering keys belong to that set. We say that a
// key range is continuous if all keys in that range belong to the continuity
// set, and discontinuous otherwise. By default everything is continuous.
// The static row may be also continuous or not.
// Partition tombstone is always continuous.
//
// Continuity is ignored by instance equality. It's also transient, not
// preserved by serialization.
//
// Continuity is represented internally using flags on row entries. The key
// range between two consecutive entries (both ends exclusive) is continuous
// if and only if rows_entry::continuous() is true for the later entry. The
// range starting after the last entry is assumed to be continuous. The range
// corresponding to the key of the entry is continuous if and only if
// rows_entry::dummy() is false.
//
// Adding two fully-continuous instances gives a fully-continuous instance.
// Continuity doesn't affect how the write part is added.
//
// Addition of continuity is not commutative in general, but is associative.
// The default continuity merging rules are those required by MVCC to
// preserve its invariants. For details, refer to "Continuity merging rules" section
// in the doc in partition_version.hh.
class mutation_partition final {
public:
    using rows_type = rows_entry::container_type;
    friend class size_calculator;
private:
    tombstone _tombstone;
    lazy_row _static_row;
    bool _static_row_continuous = true;
    rows_type _rows;
    // Contains only strict prefixes so that we don't have to lookup full keys
    // in both _row_tombstones and _rows.
    range_tombstone_list _row_tombstones;
#ifdef SEASTAR_DEBUG
    table_schema_version _schema_version;
#endif

    friend class converting_mutation_partition_applier;
public:
    struct copy_comparators_only {};
    struct incomplete_tag {};
    // Constructs an empty instance which is fully discontinuous except for the partition tombstone.
    mutation_partition(incomplete_tag, const schema& s, tombstone);
    static mutation_partition make_incomplete(const schema& s, tombstone t = {}) {
        return mutation_partition(incomplete_tag(), s, t);
    }
    mutation_partition(schema_ptr s)
        : _rows()
        , _row_tombstones(*s)
#ifdef SEASTAR_DEBUG
        , _schema_version(s->version())
#endif
    { }
    mutation_partition(mutation_partition& other, copy_comparators_only)
        : _rows()
        , _row_tombstones(other._row_tombstones, range_tombstone_list::copy_comparator_only())
#ifdef SEASTAR_DEBUG
        , _schema_version(other._schema_version)
#endif
    { }
    mutation_partition(mutation_partition&&) = default;
    mutation_partition(const schema& s, const mutation_partition&);
    mutation_partition(const mutation_partition&, const schema&, query::clustering_key_filter_ranges);
    mutation_partition(mutation_partition&&, const schema&, query::clustering_key_filter_ranges);
    ~mutation_partition();
    static mutation_partition& container_of(rows_type&);
    mutation_partition& operator=(mutation_partition&& x) noexcept;
    bool equal(const schema&, const mutation_partition&) const;
    bool equal(const schema& this_schema, const mutation_partition& p, const schema& p_schema) const;
    bool equal_continuity(const schema&, const mutation_partition&) const;
    // Consistent with equal()
    template<typename Hasher>
    void feed_hash(Hasher& h, const schema& s) const {
        hashing_partition_visitor<Hasher> v(h, s);
        accept(s, v);
    }

    class printer {
        const schema& _schema;
        const mutation_partition& _mutation_partition;
    public:
        printer(const schema& s, const mutation_partition& mp) : _schema(s), _mutation_partition(mp) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const printer& p);
    };
    friend std::ostream& operator<<(std::ostream& os, const printer& p);
public:
    // Makes sure there is a dummy entry after all clustered rows. Doesn't affect continuity.
    // Doesn't invalidate iterators.
    void ensure_last_dummy(const schema&);
    bool static_row_continuous() const { return _static_row_continuous; }
    void set_static_row_continuous(bool value) { _static_row_continuous = value; }
    bool is_fully_continuous() const;
    void make_fully_continuous();
    // Sets or clears continuity of clustering ranges between existing rows.
    void set_continuity(const schema&, const position_range& pr, is_continuous);
    // Returns clustering row ranges which have continuity matching the is_continuous argument.
    clustering_interval_set get_continuity(const schema&, is_continuous = is_continuous::yes) const;
    // Returns true iff all keys from given range are marked as continuous, or range is empty.
    bool fully_continuous(const schema&, const position_range&);
    // Returns true iff all keys from given range are marked as not continuous and range is not empty.
    bool fully_discontinuous(const schema&, const position_range&);
    // Returns true iff all keys from given range have continuity membership as specified by is_continuous.
    bool check_continuity(const schema&, const position_range&, is_continuous) const;
    // Frees elements of the partition in batches.
    // Returns stop_iteration::yes iff there are no more elements to free.
    // Continuity is unspecified after this.
    stop_iteration clear_gently(cache_tracker*) noexcept;
    // Applies mutation_fragment.
    // The fragment must be goverened by the same schema as this object.
    void apply(const schema& s, const mutation_fragment&);
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(const schema& schema, const clustering_key_prefix& prefix, tombstone t);
    void apply_delete(const schema& schema, range_tombstone rt);
    void apply_delete(const schema& schema, clustering_key_prefix&& prefix, tombstone t);
    void apply_delete(const schema& schema, clustering_key_prefix_view prefix, tombstone t);
    // Equivalent to applying a mutation with an empty row, created with given timestamp
    void apply_insert(const schema& s, clustering_key_view, api::timestamp_type created_at);
    void apply_insert(const schema& s, clustering_key_view, api::timestamp_type created_at,
                      gc_clock::duration ttl, gc_clock::time_point expiry);
    // prefix must not be full
    void apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t);
    void apply_row_tombstone(const schema& schema, range_tombstone rt);
    //
    // Applies p to current object.
    //
    // Commutative when this_schema == p_schema. If schemas differ, data in p which
    // is not representable in this_schema is dropped, thus apply() loses commutativity.
    //
    // Weak exception guarantees.
    void apply(const schema& this_schema, const mutation_partition& p, const schema& p_schema,
            mutation_application_stats& app_stats);
    // Use in case this instance and p share the same schema.
    // Same guarantees as apply(const schema&, mutation_partition&&, const schema&);
    void apply(const schema& s, mutation_partition&& p, mutation_application_stats& app_stats);
    // Same guarantees and constraints as for apply(const schema&, const mutation_partition&, const schema&).
    void apply(const schema& this_schema, mutation_partition_view p, const schema& p_schema,
            mutation_application_stats& app_stats);

    // Applies p to this instance.
    //
    // Monotonic exception guarantees. In case of exception the sum of p and this remains the same as before the exception.
    // This instance and p are governed by the same schema.
    //
    // Must be provided with a pointer to the cache_tracker, which owns both this and p.
    //
    // Returns stop_iteration::no if the operation was preempted before finished, and stop_iteration::yes otherwise.
    // On preemption the sum of this and p stays the same (represents the same set of writes), and the state of this
    // object contains at least all the writes it contained before the call (monotonicity). It may contain partial writes.
    // Also, some progress is always guaranteed (liveness).
    //
    // The operation can be driven to completion like this:
    //
    //   while (apply_monotonically(..., is_preemtable::yes) == stop_iteration::no) { }
    //
    // If is_preemptible::no is passed as argument then stop_iteration::no is never returned.
    stop_iteration apply_monotonically(const schema& s, mutation_partition&& p, cache_tracker*,
            mutation_application_stats& app_stats, is_preemptible = is_preemptible::no);
    stop_iteration apply_monotonically(const schema& s, mutation_partition&& p, const schema& p_schema,
            mutation_application_stats& app_stats, is_preemptible = is_preemptible::no);

    // Weak exception guarantees.
    // Assumes this and p are not owned by a cache_tracker.
    void apply_weak(const schema& s, const mutation_partition& p, const schema& p_schema,
            mutation_application_stats& app_stats);
    void apply_weak(const schema& s, mutation_partition&&,
            mutation_application_stats& app_stats);
    void apply_weak(const schema& s, mutation_partition_view p, const schema& p_schema,
            mutation_application_stats& app_stats);

    // Converts partition to the new schema. When succeeds the partition should only be accessed
    // using the new schema.
    //
    // Strong exception guarantees.
    void upgrade(const schema& old_schema, const schema& new_schema);
private:
    void insert_row(const schema& s, const clustering_key& key, deletable_row&& row);
    void insert_row(const schema& s, const clustering_key& key, const deletable_row& row);

    uint32_t do_compact(const schema& s,
        gc_clock::time_point now,
        const std::vector<query::clustering_range>& row_ranges,
        bool always_return_static_content,
        bool reverse,
        uint64_t row_limit,
        can_gc_fn&);

    // Calls func for each row entry inside row_ranges until func returns stop_iteration::yes.
    // Removes all entries for which func didn't return stop_iteration::no or wasn't called at all.
    // Removes all entries that are empty, check rows_entry::empty().
    // If reversed is true, func will be called on entries in reverse order. In that case row_ranges
    // must be already in reverse order.
    template<bool reversed, typename Func>
    void trim_rows(const schema& s,
        const std::vector<query::clustering_range>& row_ranges,
        Func&& func);
public:
    // Performs the following:
    //   - throws out data which doesn't belong to row_ranges
    //   - expires cells and tombstones based on query_time
    //   - drops cells covered by higher-level tombstones (compaction)
    //   - leaves at most row_limit live rows
    //
    // Note: a partition with a static row which has any cell live but no
    // clustered rows still counts as one row, according to the CQL row
    // counting rules.
    //
    // Returns the count of CQL rows which remained. If the returned number is
    // smaller than the row_limit it means that there was no more data
    // satisfying the query left.
    //
    // The row_limit parameter must be > 0.
    //
    uint64_t compact_for_query(const schema& s, gc_clock::time_point query_time,
        const std::vector<query::clustering_range>& row_ranges, bool always_return_static_content,
        bool reversed, uint64_t row_limit);

    // Performs the following:
    //   - expires cells based on compaction_time
    //   - drops cells covered by higher-level tombstones
    //   - drops expired tombstones which timestamp is before max_purgeable
    void compact_for_compaction(const schema& s, can_gc_fn&,
        gc_clock::time_point compaction_time);

    // Returns the minimal mutation_partition that when applied to "other" will
    // create a mutation_partition equal to the sum of other and this one.
    // This and other must both be governed by the same schema s.
    mutation_partition difference(schema_ptr s, const mutation_partition& other) const;

    // Returns a subset of this mutation holding only information relevant for given clustering ranges.
    // Range tombstones will be trimmed to the boundaries of the clustering ranges.
    mutation_partition sliced(const schema& s, const query::clustering_row_ranges&) const;

    // Returns true if the mutation_partition represents no writes.
    bool empty() const;
public:
    deletable_row& clustered_row(const schema& s, const clustering_key& key);
    deletable_row& clustered_row(const schema& s, clustering_key&& key);
    deletable_row& clustered_row(const schema& s, clustering_key_view key);
    deletable_row& clustered_row(const schema& s, position_in_partition_view pos, is_dummy, is_continuous);
    // Throws if the row already exists or if the row was not inserted to the
    // last position (one or more greater row already exists).
    // Weak exception guarantees.
    deletable_row& append_clustered_row(const schema& s, position_in_partition_view pos, is_dummy, is_continuous);
public:
    tombstone partition_tombstone() const { return _tombstone; }
    lazy_row& static_row() { return _static_row; }
    const lazy_row& static_row() const { return _static_row; }
    // return a set of rows_entry where each entry represents a CQL row sharing the same clustering key.
    const rows_type& clustered_rows() const { return _rows; }
    const range_tombstone_list& row_tombstones() const { return _row_tombstones; }
    rows_type& clustered_rows() { return _rows; }
    range_tombstone_list& row_tombstones() { return _row_tombstones; }
    const row* find_row(const schema& s, const clustering_key& key) const;
    tombstone range_tombstone_for_row(const schema& schema, const clustering_key& key) const;
    row_tombstone tombstone_for_row(const schema& schema, const clustering_key& key) const;
    // Can be called only for non-dummy entries
    row_tombstone tombstone_for_row(const schema& schema, const rows_entry& e) const;
    boost::iterator_range<rows_type::const_iterator> range(const schema& schema, const query::clustering_range& r) const;
    rows_type::const_iterator lower_bound(const schema& schema, const query::clustering_range& r) const;
    rows_type::const_iterator upper_bound(const schema& schema, const query::clustering_range& r) const;
    rows_type::iterator lower_bound(const schema& schema, const query::clustering_range& r);
    rows_type::iterator upper_bound(const schema& schema, const query::clustering_range& r);
    boost::iterator_range<rows_type::iterator> range(const schema& schema, const query::clustering_range& r);
    // Returns an iterator range of rows_entry, with only non-dummy entries.
    auto non_dummy_rows() const {
        return boost::make_iterator_range(_rows.begin(), _rows.end())
            | boost::adaptors::filtered([] (const rows_entry& e) { return bool(!e.dummy()); });
    }
    void accept(const schema&, mutation_partition_visitor&) const;

    // Returns the number of live CQL rows in this partition.
    //
    // Note: If no regular rows are live, but there's something live in the
    // static row, the static row counts as one row. If there is at least one
    // regular row live, static row doesn't count.
    //
    uint64_t live_row_count(const schema&,
        gc_clock::time_point query_time = gc_clock::time_point::min()) const;

    bool is_static_row_live(const schema&,
        gc_clock::time_point query_time = gc_clock::time_point::min()) const;

    uint64_t row_count() const;

    size_t external_memory_usage(const schema&) const;
private:
    template<typename Func>
    void for_each_row(const schema& schema, const query::clustering_range& row_range, bool reversed, Func&& func) const;
    friend class counter_write_query_result_builder;

    void check_schema(const schema& s) const {
#ifdef SEASTAR_DEBUG
        assert(s.version() == _schema_version);
#endif
    }
};

inline
mutation_partition& mutation_partition::container_of(rows_type& rows) {
    return *boost::intrusive::get_parent_from_member(&rows, &mutation_partition::_rows);
}
