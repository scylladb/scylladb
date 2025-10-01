/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/test_runner.hh>

#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "sstables/mx/types.hh"
#include "sstables/trie/bti_key_translation.hh"

struct lcb_mismatch_test {
    schema_ptr _s;
    std::vector<sstables::clustering_info> keys;
    lcb_mismatch_test() {
        int n_columns = 16;
        auto builder = schema_builder("ks", "t")
            .with_column("pk", int32_type, column_kind::partition_key);
        for (int i = 0; i <= n_columns; ++i) {
            builder.with_column(bytes(fmt::format("c{}", i).c_str()), int32_type, column_kind::clustering_key);
        }
        _s = builder.build();
        std::vector<data_value> components;
        for (int i = 0; i < n_columns - 1; ++i) {
            components.push_back(data_value(int32_t(0)));
        }
        for (int i = 0; i < 100; ++i) {
            components.push_back(data_value(int32_t(i)));
            keys.push_back({
                sstables::clustering_info{
                    clustering_key_prefix::from_deeply_exploded(*_s, components),
                    sstables::bound_kind_m::clustering
                }
            });
            components.pop_back();
        }
    }
};

PERF_TEST_F(lcb_mismatch_test, lcb_mismatch) {
    for (size_t i = 1; i < keys.size(); ++i) {
        auto a = sstables::trie::lazy_comparable_bytes_from_clustering_position(*_s, keys[i - 1]);
        auto b = sstables::trie::lazy_comparable_bytes_from_clustering_position(*_s, keys[i]);
        auto [offset, ptr] = sstables::trie::lcb_mismatch(a.begin(), b.begin());
        perf_tests::do_not_optimize(offset);
        perf_tests::do_not_optimize(ptr);
    }
}

