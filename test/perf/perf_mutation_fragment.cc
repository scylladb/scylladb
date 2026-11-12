/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/defer.hh>

#include "test/lib/simple_schema.hh"
#include "test/perf/perf.hh"

#include "mutation/mutation_fragment.hh"
#include "mutation/collection_mutation.hh"
#include "schema/schema_builder.hh"
#include "types/map.hh"
#include "utils/xx_hasher.hh"
#include "compaction/compaction_garbage_collector.hh"
#include "types/set.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation_query.hh"
#include "query/query-result.hh"
#include <random>

namespace tests {

class clustering_row {
    mutable simple_schema _schema;
    perf::reader_concurrency_semaphore_wrapper _semaphore;
    reader_permit _permit;
    clustering_key _key;

    bytes _value_4;
    bytes _value_4k;
    bytes _value_1M;

    mutation_fragment _row_4;
    mutation_fragment _row_4k;
    mutation_fragment _row_1M;

public:
    clustering_row()
        : _semaphore(__FILE__)
        , _permit(_semaphore.make_permit())
        , _key(_schema.make_ckey(0))
        , _value_4(4, 'a')
        , _value_4k(4 * 1024, 'b')
        , _value_1M(1024 * 1024, 'c')
        , _row_4(_schema.make_row_from_serialized_value(_permit, _key, _value_4))
        , _row_4k(_schema.make_row_from_serialized_value(_permit, _key, _value_4k))
        , _row_1M(_schema.make_row_from_serialized_value(_permit, _key, _value_1M))
    { }

    schema_ptr schema() const { return _schema.schema(); }
    reader_permit permit() const { return _permit; }

    mutation_fragment& clustering_row_4() {
        return _row_4;
    }

    mutation_fragment& clustering_row_4k() {
        return _row_4k;
    }

    mutation_fragment& clustering_row_1M() {
        return _row_1M;
    }

    mutation_fragment make_clustering_row_4() const {
        return _schema.make_row_from_serialized_value(_permit, _key, _value_4);
    }

    mutation_fragment make_clustering_row_4k() const {
        return _schema.make_row_from_serialized_value(_permit, _key, _value_4k);
    }

    mutation_fragment make_clustering_row_1M() const {
        return _schema.make_row_from_serialized_value(_permit, _key, _value_1M);
    }
};

PERF_TEST_F(clustering_row, make_4)
{
    auto mf = make_clustering_row_4();
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, make_4k)
{
    auto mf = make_clustering_row_4k();
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, make_1M)
{
    auto mf = make_clustering_row_1M();
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, copy_4)
{
    auto mf = mutation_fragment(*schema(), permit(), clustering_row_4());
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, copy_4k)
{
    auto mf = mutation_fragment(*schema(), permit(), clustering_row_4k());
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, copy_1M)
{
    auto mf = mutation_fragment(*schema(), permit(), clustering_row_1M());
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, hash_4)
{
    clustering_row_4().mutate_as_clustering_row(*schema(), [&] (::clustering_row& cr) mutable {
        cr.cells().prepare_hash(*schema(), column_kind::regular_column);
        perf_tests::do_not_optimize(cr);
        cr.cells().clear_hash();
    });
}

PERF_TEST_F(clustering_row, hash_4k)
{
    clustering_row_4k().mutate_as_clustering_row(*schema(), [&] (::clustering_row& cr) mutable {
        cr.cells().prepare_hash(*schema(), column_kind::regular_column);
        perf_tests::do_not_optimize(cr);
        cr.cells().clear_hash();
    });
}

PERF_TEST_F(clustering_row, hash_1M)
{
    clustering_row_1M().mutate_as_clustering_row(*schema(), [&] (::clustering_row& cr) mutable {
        cr.cells().prepare_hash(*schema(), column_kind::regular_column);
        perf_tests::do_not_optimize(cr);
        cr.cells().clear_hash();
    });
}

// appending_hash<row> over one map<int, blob> cell, the path query digests take (via query::digester,
// which adds variant dispatch) when the cell carries no cached hash (merged or sstable-sourced rows).
class collection_hash {
    schema_ptr _s;
    const column_definition* _def;
    query::column_id_vector _columns;
    row _row_10;
    row _row_500;
    row _row_50k;

    static row make_row(const column_definition& def, unsigned elements) {
        const bytes blob(32, 'v');
        collection_mutation_writer w({});
        for (unsigned e = 0; e < elements; ++e) {
            w.push_back(managed_bytes_view(int32_type->decompose(int32_t(e))),
                    atomic_cell::make_live(*bytes_type, api::new_timestamp(), blob, atomic_cell::collection_member::yes));
        }
        row r;
        r.apply(def, atomic_cell_or_collection(std::move(w).finish()));
        return r;
    }

public:
    collection_hash()
        : _s(schema_builder(this_smp_shard_count(), "ks", "cf")
                          .with_column("pk", int32_type, column_kind::partition_key)
                          .with_column("m", map_type_impl::get_instance(int32_type, bytes_type, true))
                          .build())
        , _def(_s->get_column_definition(to_bytes("m")))
        , _columns({_def->id})
        , _row_10(make_row(*_def, 10))
        , _row_500(make_row(*_def, 500))
        , _row_50k(make_row(*_def, 50000)) {
        for (auto* r : {&_row_10, &_row_500, &_row_50k}) {
            const auto ref = hash_reference(*r);
            if (hash<xx_hasher>(*r) != ref || hash<buffered_xx_hasher>(*r) != ref) {
                throw std::runtime_error("collection digest differs from per-cell xx_hasher");
            }
        }
    }

    const row& row_10() const {
        return _row_10;
    }
    const row& row_500() const {
        return _row_500;
    }
    const row& row_50k() const {
        return _row_50k;
    }

    template <typename Hasher>
    uint64_t hash(const row& r) const {
        Hasher h;
        max_timestamp ts;
        appending_hash<row>()(h, r, *_s, column_kind::regular_column, _columns, ts);
        return h.finalize_uint64();
    }

    // The digest with an unbuffered per-cell xx_hasher, as before buffering.
    uint64_t hash_reference(const row& r) const {
        xx_hasher cellh;
        feed_hash(cellh, *r.find_cell(_def->id), *_def);
        xx_hasher h;
        feed_hash(h, cellh.finalize_uint64());
        return h.finalize_uint64();
    }

    void prepare_hash(const row& r) const {
        r.prepare_hash(*_s, column_kind::regular_column);
        perf_tests::stop_measuring_time();
        r.clear_hash();
        perf_tests::start_measuring_time();
    }

    // Reference, not a bound: one XXH64 pass over the serialized cell, the cost
    // of a bulk hash of the cell bytes.
    uint64_t hash_raw(const row& r) const {
        xx_hasher h;
        for (bytes_view frag : fragment_range(r.find_cell(_def->id)->as_collection_mutation().data)) {
            h.update(reinterpret_cast<const char*>(frag.data()), frag.size());
        }
        return h.finalize_uint64();
    }
};

// appending_hash<row> buffers collection cells whatever the outer hasher, so xx_ and buffered_
// differ only in hashing the cell digest; compare xx_ across builds for the per-cell cost.
PERF_TEST_F(collection_hash, xx_10) {
    perf_tests::do_not_optimize(hash<xx_hasher>(row_10()));
}
PERF_TEST_F(collection_hash, buffered_10) {
    perf_tests::do_not_optimize(hash<buffered_xx_hasher>(row_10()));
}
PERF_TEST_F(collection_hash, raw_10) {
    perf_tests::do_not_optimize(hash_raw(row_10()));
}

PERF_TEST_F(collection_hash, xx_500) {
    perf_tests::do_not_optimize(hash<xx_hasher>(row_500()));
}
PERF_TEST_F(collection_hash, buffered_500) {
    perf_tests::do_not_optimize(hash<buffered_xx_hasher>(row_500()));
}
PERF_TEST_F(collection_hash, raw_500) {
    perf_tests::do_not_optimize(hash_raw(row_500()));
}

PERF_TEST_F(collection_hash, xx_50k) {
    perf_tests::do_not_optimize(hash<xx_hasher>(row_50k()));
}
PERF_TEST_F(collection_hash, buffered_50k) {
    perf_tests::do_not_optimize(hash<buffered_xx_hasher>(row_50k()));
}
PERF_TEST_F(collection_hash, raw_50k) {
    perf_tests::do_not_optimize(hash_raw(row_50k()));
}

// The cached-hash path: cache reads fill it with row::prepare_hash().
PERF_TEST_F(collection_hash, prepare_500) {
    prepare_hash(row_500());
}
PERF_TEST_F(collection_hash, prepare_50k) {
    prepare_hash(row_50k());
}

// appending_hash<row> over five small int cells, each hashed by its own xx_hasher;
// compare across builds for the per-cell cost.
class narrow_row_hash {
    schema_ptr _s;
    query::column_id_vector _columns;
    row _row;

public:
    narrow_row_hash()
        : _s(schema_builder(this_smp_shard_count(), "ks", "cf")
                          .with_column("pk", int32_type, column_kind::partition_key)
                          .with_column("v1", int32_type)
                          .with_column("v2", int32_type)
                          .with_column("v3", int32_type)
                          .with_column("v4", int32_type)
                          .with_column("v5", int32_type)
                          .build()) {
        for (auto& def : _s->regular_columns()) {
            _columns.push_back(def.id);
            _row.apply(def, atomic_cell_or_collection(atomic_cell::make_live(*int32_type, api::new_timestamp(), int32_type->decompose(int32_t(def.id)))));
        }
    }

    uint64_t hash() const {
        xx_hasher h;
        max_timestamp ts;
        appending_hash<row>()(h, _row, *_s, column_kind::regular_column, _columns, ts);
        return h.finalize_uint64();
    }
};

PERF_TEST_F(narrow_row_hash, xx) {
    perf_tests::do_not_optimize(hash());
}

// A ~1 MB set<blob> (10k 90-byte elements) and near-copies (an append, a rewrite, an expiry):
// the passes reads and read repair make over a large collection cell, timed one by one.
class large_set {
protected:
    data_type _type = set_type_impl::get_instance(bytes_type, true);
    schema_ptr _s = schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("s", _type)
            .build();
    const column_definition& _def = *_s->get_column_definition(to_bytes("s"));
    collection_mutation _big;
    collection_mutation _delta;
    collection_mutation _merged;
    collection_mutation _changed; // one element rewritten
    collection_mutation _expired; // element 9/10 in expired: ~90% copied verbatim, the rest per element
    std::optional<frozen_mutation> _frozen;
    std::optional<mutation> _m;

    static collection_mutation make(const std::vector<int64_t>& keys, api::timestamp_type ts, bool expired = false) {
        collection_mutation_writer w({});
        for (auto k : keys) {
            auto e = long_type->decompose(k); // big-endian, so positive keys sort as integers
            e.resize(90);
            w.push_back(managed_bytes_view(e), expired
                    ? atomic_cell::make_live(*empty_type, ts, bytes_view(), gc_clock::time_point(gc_clock::duration(1)), gc_clock::duration(1), atomic_cell::collection_member::yes)
                    : atomic_cell::make_live(*empty_type, ts, bytes_view(), atomic_cell::collection_member::yes));
        }
        return std::move(w).finish();
    }

public:
    large_set() : large_set(10000) {}
    explicit large_set(size_t n) {
        std::mt19937_64 rng(42);
        std::vector<int64_t> keys(n);
        for (auto& k : keys) {
            k = int64_t(rng() >> 2);
        }
        std::ranges::sort(keys);
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
        if (std::ranges::binary_search(keys, keys[keys.size() / 2] + 1)) {
            throw std::logic_error("large_set: delta key must be a new element");
        }
        _big = make(keys, 1);
        _delta = make({keys[keys.size() / 2] + 1}, 2);
        _merged = merge(*_type, _big, _delta);
        _changed = merge(*_type, _big, make({keys[keys.size() / 2]}, 2));
        _expired = merge(*_type, _big, make({keys[keys.size() * 9 / 10]}, 2, /* expired */ true));
        mutation m(_s, partition_key::from_single_value(*_s, int32_type->decompose(int32_t(0))));
        m.set_clustered_cell(clustering_key::make_empty(), _def, atomic_cell_or_collection(collection_mutation(collection_mutation_view(_big))));
        _frozen.emplace(freeze(m));
        _m.emplace(std::move(m));
    }
};

// Read repair merges near-identical replica results.
PERF_TEST_F(large_set, merge_near) { perf_tests::do_not_optimize(merge(*_type, _merged, _big)); }
PERF_TEST_F(large_set, merge_near_general) {
    auto restore = seastar::defer([saved = std::exchange(collection_fast_path_min_size, std::numeric_limits<size_t>::max())] () noexcept {
        collection_fast_path_min_size = saved;
    });
    perf_tests::do_not_optimize(merge(*_type, _merged, _big));
}
PERF_TEST_F(large_set, difference_same) { perf_tests::do_not_optimize(difference(*_type, _big, _big)); }
PERF_TEST_F(large_set, difference_delta) { perf_tests::do_not_optimize(difference(*_type, _merged, _big)); }
PERF_TEST_F(large_set, difference_changed) { perf_tests::do_not_optimize(difference(*_type, _changed, _big)); }
PERF_TEST_F(large_set, copy) { perf_tests::do_not_optimize(collection_mutation(collection_mutation_view(_big))); }
PERF_TEST_F(large_set, merge) { perf_tests::do_not_optimize(merge(*_type, _big, _delta)); }
PERF_TEST_F(large_set, compact) {
    perf_tests::do_not_optimize(compact_and_expire(_big, _def.id, *_type, row_tombstone(), gc_clock::now(), always_gc, gc_clock::time_point::min(), nullptr));
}
// The variant row::compact_and_expire() uses; no copy of an unchanged cell.
PERF_TEST_F(large_set, compact_if_changed) {
    perf_tests::do_not_optimize(compact_and_expire_if_changed(_big, _def.id, *_type, row_tombstone(), gc_clock::now(), always_gc, gc_clock::time_point::min(), nullptr));
}
// Long expired, so purged as a real read would.
PERF_TEST_F(large_set, compact_expired) {
    perf_tests::do_not_optimize(compact_and_expire(_expired, _def.id, *_type, row_tombstone(), gc_clock::now(), always_gc, gc_clock::now(), nullptr));
}
PERF_TEST_F(large_set, serialize_for_cql) { perf_tests::do_not_optimize(serialize_for_cql(*_type, _big)); }
PERF_TEST_F(large_set, last_update) { perf_tests::do_not_optimize(collection_mutation_view(_big).last_update(*_type)); }
PERF_TEST_F(large_set, hash) {
    buffered_xx_hasher h;
    feed_hash(h, collection_mutation_view(_big), _def);
    perf_tests::do_not_optimize(h.finalize_uint64());
}
// Read repair: replica results arrive frozen and are unfrozen (and refrozen).
PERF_TEST_F(large_set, unfreeze) { perf_tests::do_not_optimize(_frozen->unfreeze(_s)); }
PERF_TEST_F(large_set, freeze) { perf_tests::do_not_optimize(freeze(*_m)); }

class large_set_100k : public large_set {
public:
    large_set_100k() : large_set(100000) {}
};

// A read of one row holding the set, through compact_for_query<query_result_builder> (includes the mutation copy).
static query::result query_large_set(const mutation& m, query::result_request request) {
    auto slice = m.schema()->full_slice();
    return query_mutation(mutation(m), slice, query::max_rows, gc_clock::now(), query::result_options{request, query::digest_algorithm::xxHash});
}

PERF_TEST_F(large_set, query_digest) { perf_tests::do_not_optimize(query_large_set(*_m, query::result_request::only_digest)); }
PERF_TEST_F(large_set, query_data) { perf_tests::do_not_optimize(query_large_set(*_m, query::result_request::result_and_digest)); }
PERF_TEST_F(large_set_100k, query_digest) { perf_tests::do_not_optimize(query_large_set(*_m, query::result_request::only_digest)); }
PERF_TEST_F(large_set_100k, query_data) { perf_tests::do_not_optimize(query_large_set(*_m, query::result_request::result_and_digest)); }

}
