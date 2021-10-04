/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "mutation_reader.hh"
#include "test/lib/sstable_utils.hh"

class cql_test_env;

using populate_fn = std::function<mutation_source(schema_ptr s, const std::vector<mutation>&)>;
using populate_fn_ex = std::function<mutation_source(schema_ptr s, const std::vector<mutation>&, gc_clock::time_point)>;

// Must be run in a seastar thread
void run_mutation_source_tests(populate_fn populate, cql_test_env* test_env = nullptr, bool with_partition_range_forwarding = true);
void run_mutation_source_tests(populate_fn_ex populate, cql_test_env* test_env = nullptr, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_plain(populate_fn_ex populate, cql_test_env* test_env = nullptr, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_downgrade(populate_fn_ex populate, cql_test_env* test_env = nullptr, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_upgrade(populate_fn_ex populate, cql_test_env* test_env = nullptr, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_reverse(populate_fn_ex populate, cql_test_env* test_env = nullptr, bool with_partition_range_forwarding = true);

enum are_equal { no, yes };

// Calls the provided function on mutation pairs, equal and not equal. Is supposed
// to exercise all potential ways two mutations may differ.
void for_each_mutation_pair(schema_registry& registry, std::function<void(const mutation&, const mutation&, are_equal)>);

// Calls the provided function on mutations. Is supposed to exercise as many differences as possible.
void for_each_mutation(schema_registry& registry, std::function<void(const mutation&)>);

// Returns true if mutations in schema s1 can be upgraded to s2.
inline bool can_upgrade_schema(schema_ptr from, schema_ptr to) {
    return from->is_counter() == to->is_counter();
}

class random_mutation_generator {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    struct generate_counters_tag { };
    using generate_counters = bool_class<generate_counters_tag>;
    using generate_uncompactable = bool_class<class generate_uncompactable_tag>;

    // With generate_uncompactable::yes, the mutation will be uncompactable, that
    // is no higher level tombstone will cover lower level tombstones and no
    // tombstone will cover data, i.e. compacting the mutation will not result
    // in any changes.
    explicit random_mutation_generator(schema_registry& registry, generate_counters, local_shard_only lso = local_shard_only::yes,
            generate_uncompactable uc = generate_uncompactable::no);
    ~random_mutation_generator();
    mutation operator()();
    // Generates n mutations sharing the same schema nad sorted by their decorated keys.
    std::vector<mutation> operator()(size_t n);
    schema_ptr schema() const;
    clustering_key make_random_key();
    std::vector<dht::decorated_key> make_partition_keys(size_t n);
    std::vector<query::clustering_range> make_random_ranges(unsigned n_ranges);
};

bytes make_blob(size_t blob_size);

void for_each_schema_change(std::function<void(schema_ptr, const std::vector<mutation>&,
                                               schema_ptr, const std::vector<mutation>&)>);

void compare_readers(const schema&, flat_mutation_reader authority, flat_mutation_reader tested);
void compare_readers(const schema&, flat_mutation_reader authority, flat_mutation_reader tested, const std::vector<position_range>& fwd_ranges);
