/*
 * Copyright (C) 2018-present ScyllaDB
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

/// Helper functions for creating and populating a test table.
///
/// See the various `create_test_table()` overloads for more details.

#include "test/lib/cql_test_env.hh"

namespace test {

class partition_content_generator {
public:
    struct row {
        clustering_key key;
        bytes val;
    };

    virtual ~partition_content_generator() = default;

    // Allowed to generate duplicates.
    virtual partition_key generate_partition_key(const schema& schema) = 0;
    virtual bool has_static_row() = 0;
    virtual bytes generate_static_row(const schema& schema, const partition_key& pk) = 0;
    virtual int clustering_row_count() = 0;
    // Allowed to generate duplicates.
    virtual row generate_row(const schema& schema, const partition_key& pk) = 0;
    // Ranges can overlap. `rows` is the list of ck values the partition has, sorted.
    // Open bounds are not allowed.
    virtual query::clustering_row_ranges generate_delete_ranges(const schema& schema, const std::vector<clustering_key>& rows) = 0;
};

class population_generator {
public:
    virtual ~population_generator() = default;
    virtual size_t partition_count() = 0;
    virtual std::unique_ptr<partition_content_generator> make_partition_content_generator() = 0;
};

struct partition_description {
    const dht::decorated_key dkey;
    bool has_static_row = false;
    // List of clustering keys, sorted.
    std::vector<clustering_key> live_rows;
    std::vector<clustering_key> dead_rows;
    // List of deleted clustering ranges, may overlap, sorted.
    query::clustering_row_ranges range_tombstones;

    explicit partition_description(dht::decorated_key dkey)
        : dkey(std::move(dkey)) {
    }
};

struct population_description {
    schema_ptr schema;
    // Sorted by ring order.
    // Exact number of generated partitions may differ from that returned by
    // `population_generator::partition_count()` as the generator is allowed
    // to generate duplicate partitions.
    std::vector<partition_description> partitions;
};

struct partition_configuration {
    std::optional<std::uniform_int_distribution<int>> static_row_size_dist;
    std::uniform_int_distribution<int> clustering_row_count_dist;
    std::uniform_int_distribution<int> clustering_row_size_dist;
    std::uniform_int_distribution<int> range_deletion_count_dist;
    std::uniform_int_distribution<int> range_deletion_size_dist; // how many keys a range should include
    int count;
};

using generate_blob_function = noncopyable_function<bytes(const schema& schema, size_t size, const partition_key& pk,
        const clustering_key* const ck)>;

/// Return those keys that overlap with at least one range.
///
/// \param keys sorted list of ck values.
/// \param ranges sorted and de-duplicated list of ranges.
std::vector<clustering_key> slice_keys(const schema& schema, const std::vector<clustering_key>& keys, const query::clustering_row_ranges& ranges);

/// Create and populate a test table (beginner version).
///
/// The keyspace and table are created as:
///
///     CREATE KEYSPACE
///         ${ks_name}
///     WITH
///         REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
///
///     CREATE TABLE
///         ${ks_name}.${table_name}
///         (pk int, ck int, s blob static, v blob, PRIMARY KEY(pk, ck))
///
/// The table is populated with partitions 0..partition_count and each partition
/// is populated with clustering rows 0..row_per_partition_count.
/// No static row (column) is written.
/// For each row `v` contains: `pk ^ ck`.
///
/// \returns the schema and a vector of the written partition keys (decorated).
/// The vector is sorted by ring order.
std::pair<schema_ptr, std::vector<dht::decorated_key>> create_test_table(cql_test_env& env, const sstring& ks_name, const sstring& table_name,
        int partition_count = 10 * smp::count, int row_per_partition_count = 10);

/// Create and populate a test table (advanced version).
///
/// Uses the same schema as the "beginner version".
/// Populates the table according to the passed in
/// `population_distribution configuration`.
/// Values for the column `v` and `s` are generated with the passed in
/// `gen_blob` function.
/// Allows for generating non-trivial random population in a controlled way.
/// The partition configurations will be processed in a random order, using a
/// deterministic pseudo-random engine. Passing the same seed will yield the
/// same population.
///
/// \returns the description of the generated population.
population_description create_test_table(cql_test_env& env, const sstring& ks_name, const sstring& table_name, uint32_t seed,
        std::vector<partition_configuration> part_configs, generate_blob_function gen_blob);

/// Create and populate a test table (expert version).
///
/// Uses the same schema as the "beginner version".
/// Allows for a fully customized population of the test table.
///
/// \returns the description of the generated population.
population_description create_test_table(cql_test_env& env, const sstring& ks_name, const sstring& table_name,
        std::unique_ptr<population_generator> pop_gen);

} // namespace test
