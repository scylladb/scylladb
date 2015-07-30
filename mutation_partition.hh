/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#pragma once

#include <iostream>
#include <map>
#include <boost/intrusive/set.hpp>
#include <boost/range/iterator_range.hpp>

#include "schema.hh"
#include "keys.hh"
#include "atomic_cell.hh"
#include "query-result-writer.hh"
#include "mutation_partition_view.hh"

// Container for cells of a row. Cells are identified by column_id.
//
// Can be used as a range of std::pair<column_id, atomic_cell_or_collection>.
//
class row {
    using map_type = std::map<column_id, atomic_cell_or_collection>;
    map_type _cells;
public:
    using value_type = map_type::value_type;
    using iterator = map_type::iterator;
    using const_iterator = map_type::const_iterator;
public:
    iterator begin() { return _cells.begin(); }
    iterator end() { return _cells.end(); }
    const_iterator begin() const { return _cells.begin(); }
    const_iterator end() const { return _cells.end(); }
    size_t size() const { return _cells.size(); }

    // Returns a reference to cell's value or throws std::out_of_range
    const atomic_cell_or_collection& cell_at(column_id id) const { return _cells.at(id); }

    // Returns a pointer to cell's value or nullptr if column is not set.
    const atomic_cell_or_collection* find_cell(column_id id) const;
public:
    // Merges cell's value into the row.
    void apply(const column_definition& column, atomic_cell_or_collection cell);

    // Adds cell to the row. The column must not be already set.
    void append_cell(column_id id, atomic_cell_or_collection cell);

    // Merges given cell into the row.
    template <typename ColumnDefinitionResolver>
    void apply(column_id id, atomic_cell_or_collection cell, ColumnDefinitionResolver&& resolver) {
        auto i = _cells.lower_bound(id);
        if (i == _cells.end() || i->first != id) {
            _cells.emplace_hint(i, id, std::move(cell));
        } else {
            merge_column(resolver(id), i->second, std::move(cell));
        }
    }

    // Expires cells based on query_time. Removes cells covered by tomb.
    // Returns true iff there are any live cells left.
    template <typename ColumnDefinitionResolver>
    bool compact_and_expire(tombstone tomb, gc_clock::time_point query_time, ColumnDefinitionResolver&& resolver) {
        bool any_live = false;
        for (auto it = _cells.begin(); it != _cells.end(); ) {
            auto& entry = *it;
            bool erase = false;
            const column_definition& def = resolver(entry.first);
            if (def.is_atomic()) {
                atomic_cell_view cell = entry.second.as_atomic_cell();
                if (cell.is_covered_by(tomb)) {
                    erase = true;
                } else if (cell.has_expired(query_time)) {
                    entry.second = atomic_cell::make_dead(cell.timestamp(), cell.deletion_time());
                } else {
                    any_live |= cell.is_live();
                }
            } else {
                auto&& cell = entry.second.as_collection_mutation();
                auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
                auto m_view = ctype->deserialize_mutation_form(cell);
                collection_type_impl::mutation m = m_view.materialize();
                any_live |= m.compact_and_expire(tomb, query_time);
                if (m.cells.empty() && m.tomb <= tomb) {
                    erase = true;
                } else {
                    entry.second = ctype->serialize_mutation_form(m);
                }
            }
            if (erase) {
                it = _cells.erase(it);
            } else {
                ++it;
            }
        }
        return any_live;
    }
};

std::ostream& operator<<(std::ostream& os, const row::value_type& rv);
std::ostream& operator<<(std::ostream& os, const row& r);

class row_marker {
    static constexpr gc_clock::duration no_ttl { 0 };
    static constexpr gc_clock::duration dead { -1 };
    api::timestamp_type _timestamp = api::missing_timestamp;
    gc_clock::duration _ttl = no_ttl;
    gc_clock::time_point _expiry;
public:
    row_marker() = default;
    row_marker(api::timestamp_type created_at) : _timestamp(created_at) { }
    row_marker(api::timestamp_type created_at, gc_clock::duration ttl, gc_clock::time_point expiry)
        : _timestamp(created_at), _ttl(ttl), _expiry(expiry)
    { }
    row_marker(tombstone deleted_at)
        : _timestamp(deleted_at.timestamp), _ttl(dead), _expiry(deleted_at.deletion_time)
    { }
    bool is_missing() const {
        return _timestamp == api::missing_timestamp;
    }
    bool is_live(tombstone t, gc_clock::time_point now) const {
        if (is_missing() || _ttl == dead) {
            return false;
        }
        if (_ttl != no_ttl && _expiry < now) {
            return false;
        }
        return _timestamp > t.timestamp;
    }
    // Can be called only when !is_missing().
    bool is_dead(gc_clock::time_point now) const {
        if (_ttl == dead) {
            return true;
        }
        return _ttl != no_ttl && _expiry < now;
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
    // Can be called only when is_dead().
    gc_clock::time_point deletion_time() const {
        return _ttl == dead ? _expiry : _expiry - _ttl;
    }
    api::timestamp_type timestamp() const {
        return _timestamp;
    }
    void apply(const row_marker& rm) {
        if (_timestamp <= rm._timestamp) {
            *this = rm;
        }
    }
    bool compact_and_expire(tombstone tomb, gc_clock::time_point now) {
        if (is_missing()) {
            return false;
        }
        if (_timestamp <= tomb.timestamp) {
            _timestamp = api::missing_timestamp;
            return false;
        }
        if (_ttl != no_ttl && _expiry < now) {
            _expiry -= _ttl;
            _ttl = dead;
            return false;
        }
        return true;
    }
    bool operator==(const row_marker& other) const {
        if (_timestamp != other._timestamp || _ttl != other._ttl) {
            return false;
        }
        return _ttl == no_ttl || _expiry == other._expiry;
    }
    bool operator!=(const row_marker& other) const {
        return !(*this == other);
    }
    friend std::ostream& operator<<(std::ostream& os, const row_marker& rm);
};

class deletable_row final {
    tombstone _deleted_at;
    row_marker _marker;
    row _cells;
public:
    deletable_row() {}

    void apply(tombstone deleted_at) {
        _deleted_at.apply(deleted_at);
    }

    void apply(api::timestamp_type created_at) {
        _marker.apply(row_marker(created_at));
    }
public:
    tombstone deleted_at() const { return _deleted_at; }
    api::timestamp_type created_at() const { return _marker.timestamp(); }
    row_marker& marker() { return _marker; }
    const row_marker& marker() const { return _marker; }
    const row& cells() const { return _cells; }
    row& cells() { return _cells; }
    friend std::ostream& operator<<(std::ostream& os, const deletable_row& dr);
    bool equal(const schema& s, const deletable_row& other) const;
    bool is_live(const schema& s, tombstone base_tombstone, gc_clock::time_point query_time) const;
};

class row_tombstones_entry : public boost::intrusive::set_base_hook<> {
    clustering_key_prefix _prefix;
    tombstone _t;
public:
    row_tombstones_entry(clustering_key_prefix&& prefix, tombstone t)
        : _prefix(std::move(prefix))
        , _t(std::move(t))
    { }
    clustering_key_prefix& prefix() {
        return _prefix;
    }
    const clustering_key_prefix& prefix() const {
        return _prefix;
    }
    tombstone& t() {
        return _t;
    }
    const tombstone& t() const {
        return _t;
    }
    void apply(tombstone t) {
        _t.apply(t);
    }
    struct compare {
        clustering_key_prefix::less_compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const row_tombstones_entry& e1, const row_tombstones_entry& e2) const {
            return _c(e1._prefix, e2._prefix);
        }
        bool operator()(const clustering_key_prefix& prefix, const row_tombstones_entry& e) const {
            return _c(prefix, e._prefix);
        }
        bool operator()(const row_tombstones_entry& e, const clustering_key_prefix& prefix) const {
            return _c(e._prefix, prefix);
        }
    };
    template <typename Comparator>
    struct delegating_compare {
        Comparator _c;
        delegating_compare(Comparator&& c) : _c(std::move(c)) {}
        template <typename Comparable>
        bool operator()(const Comparable& prefix, const row_tombstones_entry& e) const {
            return _c(prefix, e._prefix);
        }
        template <typename Comparable>
        bool operator()(const row_tombstones_entry& e, const Comparable& prefix) const {
            return _c(e._prefix, prefix);
        }
    };
    template <typename Comparator>
    static auto key_comparator(Comparator&& c) {
        return delegating_compare<Comparator>(std::move(c));
    }

    friend std::ostream& operator<<(std::ostream& os, const row_tombstones_entry& rte);
    bool equal(const schema& s, const row_tombstones_entry& other) const;
};

class rows_entry : public boost::intrusive::set_base_hook<> {
    clustering_key _key;
    deletable_row _row;
public:
    rows_entry(clustering_key&& key)
        : _key(std::move(key))
    { }
    rows_entry(const clustering_key& key)
        : _key(key)
    { }
    rows_entry(const rows_entry& e)
        : _key(e._key)
        , _row(e._row)
    { }
    clustering_key& key() {
        return _key;
    }
    const clustering_key& key() const {
        return _key;
    }
    deletable_row& row() {
        return _row;
    }
    const deletable_row& row() const {
        return _row;
    }
    void apply(tombstone t) {
        _row.apply(t);
    }
    struct compare {
        clustering_key::less_compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const rows_entry& e1, const rows_entry& e2) const {
            return _c(e1._key, e2._key);
        }
        bool operator()(const clustering_key& key, const rows_entry& e) const {
            return _c(key, e._key);
        }
        bool operator()(const rows_entry& e, const clustering_key& key) const {
            return _c(e._key, key);
        }
        bool operator()(const clustering_key_view& key, const rows_entry& e) const {
            return _c(key, e._key);
        }
        bool operator()(const rows_entry& e, const clustering_key_view& key) const {
            return _c(e._key, key);
        }
    };
    template <typename Comparator>
    struct delegating_compare {
        Comparator _c;
        delegating_compare(Comparator&& c) : _c(std::move(c)) {}
        template <typename Comparable>
        bool operator()(const Comparable& v, const rows_entry& e) const {
            return _c(v, e._key);
        }
        template <typename Comparable>
        bool operator()(const rows_entry& e, const Comparable& v) const {
            return _c(e._key, v);
        }
    };
    template <typename Comparator>
    static auto key_comparator(Comparator&& c) {
        return delegating_compare<Comparator>(std::move(c));
    }
    friend std::ostream& operator<<(std::ostream& os, const rows_entry& re);
    bool equal(const schema& s, const rows_entry& other) const;
};

namespace db {
template<typename T>
class serializer;
}


class mutation_partition final {
    // FIXME: using boost::intrusive because gcc's std::set<> does not support heterogeneous lookup yet
    using rows_type = boost::intrusive::set<rows_entry, boost::intrusive::compare<rows_entry::compare>>;
    using row_tombstones_type = boost::intrusive::set<row_tombstones_entry, boost::intrusive::compare<row_tombstones_entry::compare>>;
private:
    tombstone _tombstone;
    row _static_row;
    rows_type _rows;
    // Contains only strict prefixes so that we don't have to lookup full keys
    // in both _row_tombstones and _rows.
    // FIXME: using boost::intrusive because gcc's std::set<> does not support heterogeneous lookup yet
    row_tombstones_type _row_tombstones;

    template<typename T>
    friend class db::serializer;
    friend class mutation_partition_applier;
public:
    mutation_partition(schema_ptr s)
        : _rows(rows_entry::compare(*s))
        , _row_tombstones(row_tombstones_entry::compare(*s))
    { }
    mutation_partition(mutation_partition&&) = default;
    mutation_partition(const mutation_partition&);
    ~mutation_partition();
    mutation_partition& operator=(const mutation_partition& x);
    mutation_partition& operator=(mutation_partition&& x) = default;
    bool equal(const schema& s, const mutation_partition&) const;
    friend std::ostream& operator<<(std::ostream& os, const mutation_partition& mp);
public:
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(const schema& schema, const exploded_clustering_prefix& prefix, tombstone t);
    void apply_delete(const schema& schema, clustering_key&& key, tombstone t);
    void apply_delete(const schema& schema, clustering_key_view key, tombstone t);
    // Equivalent to applying a mutation with an empty row, created with given timestamp
    void apply_insert(const schema& s, clustering_key_view, api::timestamp_type created_at);
    // prefix must not be full
    void apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t);
    void apply(const schema& schema, const mutation_partition& p);
    void apply(const schema& schema, mutation_partition_view);
public:
    // Performs the following:
    //   - throws out data which doesn't belong to row_ranges
    //   - expires cells based on query_time
    //   - drops cells covered by higher-level tombstones (compaction)
    //   - leaves at most row_limit live rows
    //
    // FIXME: Should also perform tombstone GC.
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
    uint32_t compact_for_query(const schema& s, gc_clock::time_point query_time,
        const std::vector<query::clustering_range>& row_ranges, uint32_t row_limit);
public:
    deletable_row& clustered_row(const clustering_key& key);
    deletable_row& clustered_row(clustering_key&& key);
    deletable_row& clustered_row(const schema& s, const clustering_key_view& key);
public:
    tombstone partition_tombstone() const { return _tombstone; }
    row& static_row() { return _static_row; }
    const row& static_row() const { return _static_row; }
    // return a set of rows_entry where each entry represents a CQL row sharing the same clustering key.
    const rows_type& clustered_rows() const { return _rows; }
    const row_tombstones_type& row_tombstones() const { return _row_tombstones; }
    const row* find_row(const clustering_key& key) const;
    const rows_entry* find_entry(const schema& schema, const clustering_key_prefix& key) const;
    tombstone range_tombstone_for_row(const schema& schema, const clustering_key& key) const;
    tombstone tombstone_for_row(const schema& schema, const clustering_key& key) const;
    tombstone tombstone_for_row(const schema& schema, const rows_entry& e) const;
    boost::iterator_range<rows_type::const_iterator> range(const schema& schema, const query::range<clustering_key_prefix>& r) const;
    // Returns at most "limit" rows. The limit must be greater than 0.
    void query(query::result::partition_writer& pw, const schema& s, gc_clock::time_point now, uint32_t limit = query::max_rows) const;

    // Returns the number of live CQL rows in this partition.
    //
    // Note: If no regular rows are live, but there's something live in the
    // static row, the static row counts as one row. If there is at least one
    // regular row live, static row doesn't count.
    //
    size_t live_row_count(const schema&,
        gc_clock::time_point query_time = gc_clock::time_point::min()) const;

    bool is_static_row_live(const schema&,
        gc_clock::time_point query_time = gc_clock::time_point::min()) const;
private:
    template<typename Func>
    void for_each_row(const schema& schema, const query::range<clustering_key_prefix>& row_range, bool reversed, Func&& func) const;
};
