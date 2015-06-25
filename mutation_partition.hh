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
};

std::ostream& operator<<(std::ostream& os, const row::value_type& rv);
std::ostream& operator<<(std::ostream& os, const row& r);

class deletable_row final {
    tombstone _deleted_at;
    api::timestamp_type _created_at = api::missing_timestamp;
    row _cells;
public:
    deletable_row() {}

    void apply(tombstone deleted_at) {
        _deleted_at.apply(deleted_at);
    }

    void apply(api::timestamp_type created_at) {
        _created_at = std::max(_created_at, created_at);
    }
public:
    tombstone deleted_at() const { return _deleted_at; }
    api::timestamp_type created_at() const { return _created_at; }
    const row& cells() const { return _cells; }
    row& cells() { return _cells; }
    friend std::ostream& operator<<(std::ostream& os, const deletable_row& dr);
    bool equal(const schema& s, const deletable_row& other) const;
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
    void query(query::result::partition_writer& pw, const schema& s, const query::partition_slice& slice, uint32_t limit = query::max_rows) const;
};
