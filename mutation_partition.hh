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

// FIXME: Encapsulate
using row = std::map<column_id, atomic_cell_or_collection>;

std::ostream& operator<<(std::ostream& os, const row::value_type& rv);
std::ostream& operator<<(std::ostream& os, const row& r);

// FIXME: Encapsulate
struct deletable_row final {
    tombstone t;
    api::timestamp_type created_at = api::missing_timestamp;
    row cells;

    void apply(tombstone t_) {
        t.apply(t_);
    }

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
    tombstone partition_tombstone() const { return _tombstone; }
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(schema_ptr schema, const exploded_clustering_prefix& prefix, tombstone t);
    void apply_delete(schema_ptr schema, clustering_key&& key, tombstone t);
    // prefix must not be full
    void apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t);
    void apply(schema_ptr schema, const mutation_partition& p);
    row& static_row() { return _static_row; }
    // return a set of rows_entry where each entry represents a CQL row sharing the same clustering key.
    const rows_type& clustered_rows() const { return _rows; }
    const row& static_row() const { return _static_row; }
    const row_tombstones_type& row_tombstones() const { return _row_tombstones; }
    deletable_row& clustered_row(const clustering_key& key);
    deletable_row& clustered_row(clustering_key&& key);
    deletable_row& clustered_row(const schema& s, const clustering_key_view& key);
    const row* find_row(const clustering_key& key) const;
    const rows_entry* find_entry(schema_ptr schema, const clustering_key_prefix& key) const;
    tombstone range_tombstone_for_row(const schema& schema, const clustering_key& key) const;
    tombstone tombstone_for_row(const schema& schema, const clustering_key& key) const;
    tombstone tombstone_for_row(const schema& schema, const rows_entry& e) const;
    friend std::ostream& operator<<(std::ostream& os, const mutation_partition& mp);
    boost::iterator_range<rows_type::const_iterator> range(const schema& schema, const query::range<clustering_key_prefix>& r) const;
    // Returns at most "limit" rows. The limit must be greater than 0.
    void query(const schema& s, const query::partition_slice& slice, uint32_t limit, query::result::partition_writer& pw) const;
public:
    bool equal(const schema& s, const mutation_partition&) const;
};
