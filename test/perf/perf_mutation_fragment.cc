/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/perf_tests.hh>

#include "test/lib/simple_schema.hh"
#include "test/perf/perf.hh"

#include "mutation/mutation_fragment.hh"
#include "mutation/collection_mutation.hh"
#include "schema/schema_builder.hh"
#include "types/map.hh"
#include "utils/xx_hasher.hh"

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
}
