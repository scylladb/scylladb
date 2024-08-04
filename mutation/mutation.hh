/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iosfwd>

#include "mutation_partition.hh"
#include "keys.hh"
#include "schema/schema_fwd.hh"
#include "utils/assert.hh"
#include "utils/hashing.hh"
#include "mutation_fragment_v2.hh"
#include "mutation_consumer.hh"
#include "range_tombstone_change_generator.hh"
#include "mutation/mutation_consumer_concepts.hh"
#include "utils/preempt.hh"

#include <seastar/util/optimized_optional.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

struct mutation_consume_cookie {
    using crs_iterator_type = mutation_partition::rows_type::iterator;
    using rts_iterator_type = range_tombstone_list::iterator;

    struct clustering_iterators {
        crs_iterator_type crs_begin;
        crs_iterator_type crs_end;
        rts_iterator_type rts_begin;
        rts_iterator_type rts_end;
        range_tombstone_change_generator rt_gen;

        clustering_iterators(const schema& s, crs_iterator_type crs_b, crs_iterator_type crs_e, rts_iterator_type rts_b, rts_iterator_type rts_e)
            : crs_begin(std::move(crs_b)), crs_end(std::move(crs_e)), rts_begin(std::move(rts_b)), rts_end(std::move(rts_e)), rt_gen(s) { }
    };

    schema_ptr schema;
    bool partition_start_consumed = false;
    bool static_row_consumed = false;
    // only used when reverse == consume_in_reverse::yes
    bool reversed_range_tombstone = false;
    std::unique_ptr<clustering_iterators> iterators;
};

template<typename Result>
struct mutation_consume_result {
    stop_iteration stop;
    Result result;
    mutation_consume_cookie cookie;
};

template<>
struct mutation_consume_result<void> {
    stop_iteration stop;
    mutation_consume_cookie cookie;
};

class mutation final {
private:
    struct data {
        schema_ptr _schema;
        dht::decorated_key _dk;
        mutation_partition _p;

        data(dht::decorated_key&& key, schema_ptr&& schema);
        data(partition_key&& key, schema_ptr&& schema);
        data(schema_ptr&& schema, dht::decorated_key&& key, const mutation_partition& mp);
        data(schema_ptr&& schema, dht::decorated_key&& key, mutation_partition&& mp);
    };
    std::unique_ptr<data> _ptr;
private:
    mutation() = default;
    explicit operator bool() const { return bool(_ptr); }
    friend class optimized_optional<mutation>;
public:
    mutation(schema_ptr schema, dht::decorated_key key)
        : _ptr(std::make_unique<data>(std::move(key), std::move(schema)))
    { }
    mutation(schema_ptr schema, partition_key key_)
        : _ptr(std::make_unique<data>(std::move(key_), std::move(schema)))
    { }
    mutation(schema_ptr schema, dht::decorated_key key, const mutation_partition& mp)
        : _ptr(std::make_unique<data>(std::move(schema), std::move(key), mp))
    { }
    mutation(schema_ptr schema, dht::decorated_key key, mutation_partition&& mp)
        : _ptr(std::make_unique<data>(std::move(schema), std::move(key), std::move(mp)))
    { }
    mutation(const mutation& m)
    {
        if (m._ptr) {
            _ptr = std::make_unique<data>(schema_ptr(m.schema()), dht::decorated_key(m.decorated_key()), m.partition());
        }
    }
    mutation(mutation&&) = default;
    mutation& operator=(mutation&& x) = default;
    mutation& operator=(const mutation& m);

    void set_static_cell(const column_definition& def, atomic_cell_or_collection&& value);
    void set_static_cell(const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const clustering_key& key, const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection&& value);
    void set_cell(const clustering_key_prefix& prefix, const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_cell(const clustering_key_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value);

    // Upgrades this mutation to a newer schema. The new schema must
    // be obtained using only valid schema transformation:
    //  * primary key column count must not change
    //  * column types may only change to those with compatible representations
    //
    // After upgrade, mutation's partition should only be accessed using the new schema. User must
    // ensure proper isolation of accesses.
    //
    // Strong exception guarantees.
    //
    // Note that the conversion may lose information, it's possible that m1 != m2 after:
    //
    //   auto m2 = m1;
    //   m2.upgrade(s2);
    //   m2.upgrade(m1.schema());
    //
    void upgrade(const schema_ptr&);

    const partition_key& key() const { return _ptr->_dk._key; };
    const dht::decorated_key& decorated_key() const { return _ptr->_dk; };
    dht::ring_position ring_position() const { return { decorated_key() }; }
    const dht::token& token() const { return _ptr->_dk._token; }
    const schema_ptr& schema() const { return _ptr->_schema; }
    const mutation_partition& partition() const { return _ptr->_p; }
    mutation_partition& partition() { return _ptr->_p; }
    const table_id& column_family_id() const { return _ptr->_schema->id(); }
    // Consistent with hash<canonical_mutation>
    bool operator==(const mutation&) const;
public:
    // Consumes the mutation's content.
    //
    // The mutation is in a moved-from alike state after consumption.
    // There are tree ways to consume the mutation:
    // * consume_in_reverse::no - consume in forward order, as defined by the
    //   schema.
    // * consume_in_reverse::yes - consume in reverse order, as if the schema
    //   had the opposite clustering order. This effectively reverses the
    //   mutation's content, according to the native reverse order[1].
    //
    // For definition of [1] and [2] see docs/dev/reverse-reads.md.
    //
    // The consume operation is pausable and resumable:
    // * To pause return stop_iteration::yes from one of the consume() methods;
    // * The consume will now stop and return;
    // * To resume call consume again and pass the cookie member of the returned
    //   mutation_consume_result as the cookie parameter;
    //
    // Note that `consume_end_of_partition()` and `consume_end_of_stream()`
    // will be called each time the consume is stopping, regardless of whether
    // you are pausing or the consumption is ending for good.
    template<FlattenedConsumerV2 Consumer>
    auto consume(Consumer& consumer, consume_in_reverse reverse, mutation_consume_cookie cookie = {}) && -> mutation_consume_result<decltype(consumer.consume_end_of_stream())>;

    template<FlattenedConsumerV2 Consumer>
    auto consume_gently(Consumer& consumer, consume_in_reverse reverse, mutation_consume_cookie cookie = {}) && -> future<mutation_consume_result<decltype(consumer.consume_end_of_stream())>>;

    // See mutation_partition::live_row_count()
    uint64_t live_row_count(gc_clock::time_point query_time = gc_clock::time_point::min()) const;

    void apply(mutation&&);
    void apply(const mutation&);
    void apply(const mutation_fragment&);

    mutation operator+(const mutation& other) const;
    mutation& operator+=(const mutation& other);
    mutation& operator+=(mutation&& other);

    // Returns a subset of this mutation holding only information relevant for given clustering ranges.
    // Range tombstones will be trimmed to the boundaries of the clustering ranges.
    mutation sliced(const query::clustering_row_ranges&) const;

    // Returns a mutation which contains the same writes but in a minimal form.
    // Drops data covered by tombstones.
    // Does not drop expired tombstones.
    // Does not expire TTLed data.
    mutation compacted() const;

    size_t memory_usage(const ::schema& s) const;
};

inline std::vector<mutation> make_mutation_vector(mutation&& m) {
    std::vector<mutation> ret;
    ret.emplace_back(std::move(m));
    return ret;
}

template<consume_in_reverse reverse, FlattenedConsumerV2 Consumer>
std::optional<stop_iteration> consume_clustering_fragments(schema_ptr s, mutation_partition& partition, Consumer& consumer, mutation_consume_cookie& cookie, is_preemptible preempt = is_preemptible::no) {
    constexpr bool crs_in_reverse = reverse == consume_in_reverse::yes;
    // We can read the range_tombstone_list in reverse order in consume_in_reverse::yes mode
    // since we deoverlap range_tombstones on insertion.
    constexpr bool rts_in_reverse = reverse == consume_in_reverse::yes;

    using crs_type = mutation_partition::rows_type;
    using crs_iterator_type = std::conditional_t<crs_in_reverse, std::reverse_iterator<crs_type::iterator>, crs_type::iterator>;
    using rts_type = range_tombstone_list;
    using rts_iterator_type = std::conditional_t<rts_in_reverse, std::reverse_iterator<rts_type::iterator>, rts_type::iterator>;

    if (!cookie.schema) {
        if constexpr (reverse == consume_in_reverse::yes) {
            cookie.schema = s->make_reversed();
        } else {
            cookie.schema = s;
        }
    }
    s = cookie.schema;

    if (!cookie.iterators) {
        auto& crs = partition.mutable_clustered_rows();
        auto& rts = partition.mutable_row_tombstones();
        cookie.iterators = std::make_unique<mutation_consume_cookie::clustering_iterators>(*s, crs.begin(), crs.end(), rts.begin(), rts.end());
    }

    crs_iterator_type crs_it, crs_end;
    rts_iterator_type rts_it, rts_end;

    if constexpr (crs_in_reverse) {
        crs_it = std::reverse_iterator(cookie.iterators->crs_end);
        crs_end = std::reverse_iterator(cookie.iterators->crs_begin);
    } else {
        crs_it = cookie.iterators->crs_begin;
        crs_end = cookie.iterators->crs_end;
    }

    if constexpr (rts_in_reverse) {
        rts_it = std::reverse_iterator(cookie.iterators->rts_end);
        rts_end = std::reverse_iterator(cookie.iterators->rts_begin);
    } else {
        rts_it = cookie.iterators->rts_begin;
        rts_end = cookie.iterators->rts_end;
    }

    auto flush_tombstones = [&] (position_in_partition_view pos) {
        cookie.iterators->rt_gen.flush(pos, [&] (range_tombstone_change rt) {
            consumer.consume(std::move(rt));
        });
    };

    stop_iteration stop = stop_iteration::no;

    position_in_partition::tri_compare cmp(*s);

    while (!stop && (crs_it != crs_end || rts_it != rts_end)) {
        // Dummy rows are part of the in-memory representation but should be
        // invisible to reads.
        if (crs_it != crs_end && crs_it->dummy()) {
            ++crs_it;
            continue;
        }
        bool emit_rt = rts_it != rts_end;
        if (rts_it != rts_end) {
            if (reverse == consume_in_reverse::yes && !cookie.reversed_range_tombstone) {
                rts_it->tombstone().reverse();
                cookie.reversed_range_tombstone = true;
            }
            if (crs_it != crs_end) {
                const auto cmp_res = cmp(rts_it->position(), crs_it->position());
                emit_rt = cmp_res < 0;
            }
        }
        if (emit_rt) {
            flush_tombstones(rts_it->position());
            cookie.iterators->rt_gen.consume(std::move(rts_it->tombstone()));
            ++rts_it;
            cookie.reversed_range_tombstone = false;
        } else {
            flush_tombstones(crs_it->position());
            stop = consumer.consume(clustering_row(std::move(*crs_it)));
            ++crs_it;
        }
        if (preempt && need_preempt()) {
            break;
        }
    }

    if constexpr (crs_in_reverse) {
        cookie.iterators->crs_begin = crs_end.base();
        cookie.iterators->crs_end = crs_it.base();
    } else {
        cookie.iterators->crs_begin = crs_it;
        cookie.iterators->crs_end = crs_end;
    }

    if constexpr (rts_in_reverse) {
        cookie.iterators->rts_begin = rts_end.base();
        cookie.iterators->rts_end = rts_it.base();
    } else {
        cookie.iterators->rts_begin = rts_it;
        cookie.iterators->rts_end = rts_end;
    }

    if (!stop) {
      if (crs_it == crs_end && rts_it == rts_end) {
        flush_tombstones(position_in_partition::after_all_clustered_rows());
      } else {
        SCYLLA_ASSERT(preempt && need_preempt());
        return std::nullopt;
      }
    }

    return stop;
}

template<FlattenedConsumerV2 Consumer>
auto mutation::consume(Consumer& consumer, consume_in_reverse reverse, mutation_consume_cookie cookie) &&
        -> mutation_consume_result<decltype(consumer.consume_end_of_stream())> {
    auto& partition = _ptr->_p;

    if (!cookie.partition_start_consumed) {
        consumer.consume_new_partition(_ptr->_dk);
        if (partition.partition_tombstone()) {
            consumer.consume(partition.partition_tombstone());
        }
        cookie.partition_start_consumed = true;
    }

    stop_iteration stop = stop_iteration::no;
    if (!cookie.static_row_consumed && !partition.static_row().empty()) {
        stop = consumer.consume(static_row(std::move(partition.static_row().get_existing())));
    }

    cookie.static_row_consumed = true;

    if (reverse == consume_in_reverse::yes) {
        stop = *consume_clustering_fragments<consume_in_reverse::yes>(_ptr->_schema, partition, consumer, cookie);
    } else {
        stop = *consume_clustering_fragments<consume_in_reverse::no>(_ptr->_schema, partition, consumer, cookie);
    }

    const auto stop_consuming = consumer.consume_end_of_partition();
    using consume_res_type = decltype(consumer.consume_end_of_stream());
    if constexpr (std::is_same_v<consume_res_type, void>) {
        consumer.consume_end_of_stream();
        return mutation_consume_result<void>{stop_consuming, std::move(cookie)};
    } else {
        return mutation_consume_result<consume_res_type>{stop_consuming, consumer.consume_end_of_stream(), std::move(cookie)};
    }
}

template<FlattenedConsumerV2 Consumer>
auto mutation::consume_gently(Consumer& consumer, consume_in_reverse reverse, mutation_consume_cookie cookie) &&
        -> future<mutation_consume_result<decltype(consumer.consume_end_of_stream())>> {
    auto& partition = _ptr->_p;

    if (!cookie.partition_start_consumed) {
        consumer.consume_new_partition(_ptr->_dk);
        if (partition.partition_tombstone()) {
            consumer.consume(partition.partition_tombstone());
        }
        cookie.partition_start_consumed = true;
    }

    stop_iteration stop = stop_iteration::no;
    if (!cookie.static_row_consumed && !partition.static_row().empty()) {
        stop = consumer.consume(static_row(std::move(partition.static_row().get_existing())));
    }

    cookie.static_row_consumed = true;

    std::optional<stop_iteration> stop_opt;
    if (reverse == consume_in_reverse::yes) {
        while (!(stop_opt = consume_clustering_fragments<consume_in_reverse::yes>(_ptr->_schema, partition, consumer, cookie, is_preemptible::yes))) {
            co_await yield();
        }
    } else {
        while (!(stop_opt = consume_clustering_fragments<consume_in_reverse::no>(_ptr->_schema, partition, consumer, cookie, is_preemptible::yes))) {
            co_await yield();
        }
    }
    stop = *stop_opt;

    const auto stop_consuming = consumer.consume_end_of_partition();
    using consume_res_type = decltype(consumer.consume_end_of_stream());
    if constexpr (std::is_same_v<consume_res_type, void>) {
        consumer.consume_end_of_stream();
        co_return mutation_consume_result<void>{stop_consuming, std::move(cookie)};
    } else {
        co_return mutation_consume_result<consume_res_type>{stop_consuming, consumer.consume_end_of_stream(), std::move(cookie)};
    }
}

struct mutation_equals_by_key {
    bool operator()(const mutation& m1, const mutation& m2) const {
        return m1.schema() == m2.schema()
                && m1.decorated_key().equal(*m1.schema(), m2.decorated_key());
    }
};

struct mutation_hash_by_key {
    size_t operator()(const mutation& m) const {
        auto dk_hash = std::hash<dht::decorated_key>();
        return dk_hash(m.decorated_key());
    }
};

struct mutation_decorated_key_less_comparator {
    bool operator()(const mutation& m1, const mutation& m2) const;
};

using mutation_opt = optimized_optional<mutation>;

// Consistent with operator==()
// Consistent across the cluster, so should not rely on particular
// serialization format, only on actual data stored.
template<>
struct appending_hash<mutation> {
    template<typename Hasher>
    void operator()(Hasher& h, const mutation& m) const {
        const schema& s = *m.schema();
        feed_hash(h, m.key(), s);
        m.partition().feed_hash(h, s);
    }
};

inline
void apply(mutation_opt& dst, mutation&& src) {
    if (!dst) {
        dst = std::move(src);
    } else {
        dst->apply(std::move(src));
    }
}

inline
void apply(mutation_opt& dst, mutation_opt&& src) {
    if (src) {
        apply(dst, std::move(*src));
    }
}

inline
void apply(mutation& dst, mutation_opt&& src) {
    if (src) {
        dst.apply(std::move(*src));
    }
}

inline
void apply(mutation& dst, const mutation_opt& src) {
    if (src) {
        dst.apply(*src);
    }
}

// Returns a range into partitions containing mutations covered by the range.
// partitions must be sorted according to decorated key.
// range must not wrap around.
boost::iterator_range<std::vector<mutation>::const_iterator> slice(
    const std::vector<mutation>& partitions,
    const dht::partition_range&);

// Reverses the mutation as if it was created with a schema with reverse
// clustering order. The resulting mutation will contain a reverse schema too.
mutation reverse(mutation mut);

template <> struct fmt::formatter<mutation> : fmt::formatter<string_view> {
    auto format(const mutation&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

// Splits the source mutation into multiple mutations so that their size
// does not exceed the max_size limit.
// The size of a mutation is calculated as the sum of the memory_usage()
// of its constituent mutation_fragments.
// The function doesn't split rows into cells, one big row can
// lead to the creation of a mutation larger than max_size.
// Due to the difference in calculating sizes for mutations and their fragments,
// the actual size of the output mutation may be larger than max_size. It is recommended
// to pass half of the required value as max_size; such a margin should ensure
// that the condition is met.
future<> split_mutation(mutation source, std::vector<mutation>& target, size_t max_size);
