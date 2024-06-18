/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/mutation_reader_fwd.hh"
#include "test/lib/simple_schema.hh"

using populate_fn = std::function<mutation_source(schema_ptr s, const std::vector<mutation>&)>;
using populate_fn_ex = std::function<mutation_source(schema_ptr s, const std::vector<mutation>&, gc_clock::time_point)>;

// Must be run in a seastar thread
void run_mutation_source_tests(populate_fn populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_plain(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_plain_basic(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_plain_reader_conversion(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_plain_fragments_monotonic(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_plain_read_back(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_reverse(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_reverse_basic(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_reverse_reader_conversion(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_reverse_fragments_monotonic(populate_fn_ex populate, bool with_partition_range_forwarding = true);
void run_mutation_source_tests_reverse_read_back(populate_fn_ex populate, bool with_partition_range_forwarding = true);

enum are_equal { no, yes };

// Calls the provided function on mutation pairs, equal and not equal. Is supposed
// to exercise all potential ways two mutations may differ.
void for_each_mutation_pair(std::function<void(const mutation&, const mutation&, are_equal)>);

// Calls the provided function on mutations. Is supposed to exercise as many differences as possible.
void for_each_mutation(std::function<void(const mutation&)>);

// Returns true if mutations in schema s1 can be upgraded to s2.
inline bool can_upgrade_schema(schema_ptr from, schema_ptr to) {
    return from->is_counter() == to->is_counter();
}

// Merge mutations that have the same key.
// The returned vector has mutations with unique keys.
// run_mutation_source_tests() might pass in multiple mutations for the same key.
// Some tests need these deduplicated, which is what this method does.
std::vector<mutation> squash_mutations(std::vector<mutation> mutations);

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
    explicit random_mutation_generator(generate_counters, local_shard_only lso = local_shard_only::yes,
            generate_uncompactable uc = generate_uncompactable::no, std::optional<uint32_t> seed_opt = std::nullopt, const char* ks_name="ks", const char* cf_name="cf");
    random_mutation_generator(generate_counters gc, uint32_t seed)
            : random_mutation_generator(gc, local_shard_only::yes, generate_uncompactable::no, seed) {}
    ~random_mutation_generator();
    mutation operator()();
    // Generates n mutations sharing the same schema nad sorted by their decorated keys.
    std::vector<mutation> operator()(size_t n);
    schema_ptr schema() const;
    clustering_key make_random_key();
    range_tombstone make_random_range_tombstone();
    std::vector<dht::decorated_key> make_partition_keys(size_t n);
    std::vector<query::clustering_range> make_random_ranges(unsigned n_ranges);
    // Sets the number of distinct clustering keys which will be used in generated mutations.
    void set_key_cardinality(size_t);
};

bytes make_blob(size_t blob_size);

void for_each_schema_change(std::function<void(schema_ptr, const std::vector<mutation>&,
                                               schema_ptr, const std::vector<mutation>&)>);

void compare_readers(const schema&, mutation_reader authority, mutation_reader tested, bool exact = false);
void compare_readers(const schema&, mutation_reader authority, mutation_reader tested, const std::vector<position_range>& fwd_ranges);

// Forward `r` to each range in `fwd_ranges` and consume all fragments produced by `r` in these ranges.
// Build a mutation out of these fragments.
//
// Assumes that for each subsequent `r1`, `r2` in `fwd_ranges`, `r1.end() <= r2.start()`.
// Must be run in a seastar::thread.
mutation forwardable_reader_to_mutation(mutation_reader r, const std::vector<position_range>& fwd_ranges);
