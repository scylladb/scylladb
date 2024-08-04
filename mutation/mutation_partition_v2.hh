/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <iosfwd>
#include <boost/intrusive/set.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/intrusive/parent_from_member.hpp>

#include <seastar/core/bitset-iter.hh>
#include <seastar/util/optimized_optional.hh>

#include "mutation_partition.hh"

// is_evictable::yes means that the object is part of an evictable snapshots in MVCC,
// and non-evictable one otherwise.
// See docs/dev/mvcc.md for more details.
using is_evictable = bool_class<class evictable_tag>;

// Represents a set of writes made to a single partition.
//
// Like mutation_partition, but intended to be used in cache/memtable
// so the tradeoffs are different. This representation must be memory-efficient
// and must support incremental eviction of its contents. It is used in MVCC so
// algorithms for merging must respect MVCC invariants. See docs/dev/mvcc.md.
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
class mutation_partition_v2 final {
public:
    using rows_type = rows_entry::container_type;
    friend class size_calculator;
private:
    tombstone _tombstone;
    lazy_row _static_row;
    bool _static_row_continuous = true;
    rows_type _rows;
#ifdef SEASTAR_DEBUG
    table_schema_version _schema_version;
#endif

    friend class converting_mutation_partition_applier;
public:
    struct copy_comparators_only {};
    struct incomplete_tag {};
    // Constructs an empty instance which is fully discontinuous except for the partition tombstone.
    mutation_partition_v2(incomplete_tag, const schema& s, tombstone);
    static mutation_partition_v2 make_incomplete(const schema& s, tombstone t = {}) {
        return mutation_partition_v2(incomplete_tag(), s, t);
    }
    mutation_partition_v2(const schema& s)
        : _rows()
#ifdef SEASTAR_DEBUG
        , _schema_version(s.version())
#endif
    { }
    mutation_partition_v2(mutation_partition_v2& other, copy_comparators_only)
        : _rows()
#ifdef SEASTAR_DEBUG
        , _schema_version(other._schema_version)
#endif
    { }
    mutation_partition_v2(mutation_partition_v2&&) = default;
    // Assumes that p is fully continuous.
    mutation_partition_v2(const schema& s, mutation_partition&& p);
    mutation_partition_v2(const schema& s, const mutation_partition_v2&);
    // Assumes that p is fully continuous.
    mutation_partition_v2(const schema& s, const mutation_partition& p);
    ~mutation_partition_v2();
    static mutation_partition_v2& container_of(rows_type&);
    mutation_partition_v2& operator=(mutation_partition_v2&& x) noexcept;
    bool equal(const schema&, const mutation_partition_v2&) const;
    bool equal(const schema& this_schema, const mutation_partition_v2& p, const schema& p_schema) const;
    bool equal_continuity(const schema&, const mutation_partition_v2&) const;
    // Consistent with equal()
    template<typename Hasher>
    void feed_hash(Hasher& h, const schema& s) const {
        hashing_partition_visitor<Hasher> v(h, s);
        accept(s, v);
    }

    class printer {
        const schema& _schema;
        const mutation_partition_v2& _mutation_partition;
    public:
        printer(const schema& s, const mutation_partition_v2& mp) : _schema(s), _mutation_partition(mp) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend fmt::formatter<printer>;
    };
    friend fmt::formatter<printer>;
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
    // Applies p to current object.
    // Weak exception guarantees.
    // Assumes this and p are not owned by a cache_tracker and non-evictable.
    void apply(const schema& s, mutation_partition&&);
    void apply(const schema& s, mutation_partition_v2&& p, cache_tracker* = nullptr, is_evictable evictable = is_evictable::no);

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
    // If returns stop_iteration::yes, then the sum of this and p is NO LONGER the same as before the call,
    // the state of p is undefined and should not be used for reading.
    //
    // The operation can be driven to completion like this:
    //
    //   apply_resume res;
    //   while (apply_monotonically(..., is_preemtable::yes, &res) == stop_iteration::no) { }
    //
    // If is_preemptible::no is passed as argument then stop_iteration::no is never returned.
    //
    // If is_preemptible::yes is passed, apply_resume must also be passed,
    // same instance each time until stop_iteration::yes is returned.
    stop_iteration apply_monotonically(const schema& this_schema, const schema& p_schema, mutation_partition_v2&& p,
            cache_tracker*, mutation_application_stats& app_stats, preemption_check, apply_resume&, is_evictable);

    // Converts partition to the new schema. When succeeds the partition should only be accessed
    // using the new schema.
    //
    // Strong exception guarantees.
    void upgrade(const schema& old_schema, const schema& new_schema);

    // Transforms this instance into a minimal one which still represents the same set of writes.
    // Does not garbage collect expired data, so the result is clock-independent and
    // should produce the same result on all replicas.
    // has_redundant_dummies(*this) is guaranteed to be false after this.
    void compact(const schema&, cache_tracker*);

    mutation_partition as_mutation_partition(const schema&) const;
private:
    // Erases the entry if it's safe to do so without changing the logical state of the partition.
    rows_type::iterator maybe_drop(const schema&, cache_tracker*, rows_type::iterator, mutation_application_stats&);
    void insert_row(const schema& s, const clustering_key& key, deletable_row&& row);
    void insert_row(const schema& s, const clustering_key& key, const deletable_row& row);
public:
    // Returns true if the mutation_partition_v2 represents no writes.
    bool empty() const;
public:
    deletable_row& clustered_row(const schema& s, const clustering_key& key);
    deletable_row& clustered_row(const schema& s, clustering_key&& key);
    deletable_row& clustered_row(const schema& s, clustering_key_view key);
    deletable_row& clustered_row(const schema& s, position_in_partition_view pos, is_dummy, is_continuous);
    rows_entry& clustered_rows_entry(const schema& s, position_in_partition_view pos, is_dummy, is_continuous);
    rows_entry& clustered_row(const schema& s, position_in_partition_view pos, is_dummy);
    // Throws if the row already exists or if the row was not inserted to the
    // last position (one or more greater row already exists).
    // Weak exception guarantees.
    deletable_row& append_clustered_row(const schema& s, position_in_partition_view pos, is_dummy, is_continuous);
public:
    tombstone partition_tombstone() const { return _tombstone; }
    lazy_row& static_row() { return _static_row; }
    const lazy_row& static_row() const { return _static_row; }

    // return a set of rows_entry where each entry represents a CQL row sharing the same clustering key.
    const rows_type& clustered_rows() const noexcept { return _rows; }
    utils::immutable_collection<rows_type> clustered_rows() noexcept { return _rows; }
    rows_type& mutable_clustered_rows() noexcept { return _rows; }

    const row* find_row(const schema& s, const clustering_key& key) const;
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
        SCYLLA_ASSERT(s.version() == _schema_version);
#endif
    }
};

inline
mutation_partition_v2& mutation_partition_v2::container_of(rows_type& rows) {
    return *boost::intrusive::get_parent_from_member(&rows, &mutation_partition_v2::_rows);
}

// Returns true iff the mutation contains dummy rows which are redundant,
// meaning that they can be removed without affecting the set of writes represented by the mutation.
bool has_redundant_dummies(const mutation_partition_v2&);

template <> struct fmt::formatter<mutation_partition_v2::printer> : fmt::formatter<string_view> {
    auto format(const mutation_partition_v2::printer&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
