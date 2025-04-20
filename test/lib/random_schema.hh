/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"
#include "test/lib/data_model.hh"

///
/// Random schema and random data generation related utilities.
///

class cql_test_env;

namespace tests {

class random_schema_specification {
public:
    using compress_sstable = bool_class<class compress_sstable_tag>;
private:
    sstring _keyspace_name;
public:
    explicit random_schema_specification(sstring keyspace_name) : _keyspace_name(std::move(keyspace_name)) { }
    virtual ~random_schema_specification() = default;
    // Should be the same for all invocations
    const sstring& keyspace_name() const { return _keyspace_name; }
    // Should be unique on the instance level.
    virtual sstring table_name(std::mt19937& engine) = 0;
    // Should be unique on the instance level.
    virtual sstring udt_name(std::mt19937& engine) = 0;
    virtual std::vector<data_type> partition_key_columns(std::mt19937& engine) = 0;
    virtual std::vector<data_type> clustering_key_columns(std::mt19937& engine) = 0;
    virtual std::vector<data_type> regular_columns(std::mt19937& engine) = 0;
    virtual std::vector<data_type> static_columns(std::mt19937& engine) = 0;
    virtual compress_sstable& compress() = 0;
};

/// Helper class that can generate a subset of all valid combination of types.
///
/// Can be used to implement concrete random schema specifications.
/// TODO: counters
class type_generator {
public:
    using is_multi_cell = bool_class<class is_multi_cell_tag>;

private:
    using generator = std::function<data_type(std::mt19937&, is_multi_cell)>;

private:
    random_schema_specification& _spec;
    std::vector<generator> _generators;

public:
    explicit type_generator(random_schema_specification& spec);
    // This is captured.
    type_generator(type_generator&&) = delete;

    data_type operator()(std::mt19937& engine, is_multi_cell multi_cell);
};

/// The default random schema specification.
///
/// Warning: reusing the same keyspace_name across specs can lead to user
/// defined type clashes.
std::unique_ptr<random_schema_specification> make_random_schema_specification(
        sstring keyspace_name,
        std::uniform_int_distribution<size_t> partition_column_count_dist = std::uniform_int_distribution<size_t>(1, 4),
        std::uniform_int_distribution<size_t> clustering_column_count_dist = std::uniform_int_distribution<size_t>(0, 4),
        std::uniform_int_distribution<size_t> regular_column_count_dist = std::uniform_int_distribution<size_t>(1, 4),
        std::uniform_int_distribution<size_t> static_column_count_dist = std::uniform_int_distribution<size_t>(0, 4),
        random_schema_specification::compress_sstable compress = random_schema_specification::compress_sstable::yes);

/// Generate values for any type.
///
/// Values sizes:
/// * string types (ascii, utf8, bytes):
///     - 95.0% [   0,    32) characters.
///     -  4.5% [  32,   100) characters.
///     -  0.4% [ 100,  1000) characters.
///     -  0.1% [1000, 10000) characters.
/// * collections: max 16 elements.
/// * frozen collections: max 4 elements.
/// For native types, the intent is to cover the entire value range.
/// TODO: counters
class value_generator {
public:
    using atomic_value_generator = std::function<data_value(std::mt19937&, size_t, size_t)>;
    using generator = std::function<data_model::mutation_description::value(std::mt19937&)>;

    static const size_t no_size_in_bytes_limit{std::numeric_limits<size_t>::max()};

private:
    std::unordered_map<const abstract_type*, atomic_value_generator> _regular_value_generators;
    std::unordered_map<const abstract_type*, size_t> _regular_value_min_sizes;

public:
    value_generator();
    value_generator(value_generator&&) = delete;

    /// Only for atomic types.
    size_t min_size(const abstract_type& type);

    atomic_value_generator get_atomic_value_generator(const abstract_type& type);
    // Generate a value for the given type, according to the provided size constraints.
    // Controlling the size of values only really works with string-like types and collections of these.
    data_value generate_atomic_value(std::mt19937& engine, const abstract_type& type, size_t max_size_in_bytes = no_size_in_bytes_limit);
    data_value generate_atomic_value(std::mt19937& engine, const abstract_type& type, size_t min_size_in_bytes, size_t max_size_in_bytes);

    generator get_generator(const abstract_type& type);
    data_model::mutation_description::value generate_value(std::mt19937& engine, const abstract_type& type);
};

enum class timestamp_destination {
    partition_tombstone,
    row_marker,
    cell_timestamp,
    collection_cell_timestamp,
    row_tombstone,
    collection_tombstone,
    range_tombstone,
};

/// Functor that generates timestamps for various destinations.
using timestamp_generator = std::function<api::timestamp_type(std::mt19937& engine, timestamp_destination destination,
        api::timestamp_type min_timestamp)>;

/// The default timestamp generator.
///
/// Generates fully random timestamps in the range:
///     [api::min_timestamp, api::max_timestamp]
/// Ignores timestamp destination.
timestamp_generator default_timestamp_generator();

/// Use this to generate mutations that cannot be compacted
///
/// Tombstones will not cover lower level tombstones, or data.
timestamp_generator uncompactible_timestamp_generator(uint32_t seed);

struct expiry_info {
    gc_clock::duration ttl;
    gc_clock::time_point expiry_point;
};

/// Functor that generates expiry for various destinations.
/// A disengaged optional means the cell doesn't expire. When the destination is
/// a tombstone, gc_clock::now() + schema::gc_grace_seconds() will be used as
/// the expiry instead.
/// The `expiry_info::ttl` is always ignored for tombstone destinations (because
/// they have a fixed ttl as determined by `schema::gc_grace_seconds()`.
using expiry_generator = std::function<std::optional<expiry_info>(std::mt19937& engine, timestamp_destination destination)>;

/// Always returns disengaged optionals.
expiry_generator no_expiry_expiry_generator();

/// Utility class wrapping a randomly generated schema.
///
/// The schema is generated when the class is constructed.
/// The generation is deterministic, the same seed will generate the same schema.
class random_schema {
    schema_ptr _schema;

private:
    static data_model::mutation_description::key make_key(uint32_t n, value_generator& gen, schema::const_iterator_range_type columns,
            size_t max_size_in_bytes);
    data_model::mutation_description::key make_partition_key(uint32_t n, value_generator& gen) const;
    data_model::mutation_description::key make_clustering_key(uint32_t n, value_generator& gen) const;

public:
    /// Create a random schema.
    ///
    /// Passing the same seed and spec will yield the same schema. Part of this
    /// guarantee rests on the spec, which, if a custom one is used, should
    /// make sure to honor this guarantee.
    random_schema(uint32_t seed, random_schema_specification& spec);

    schema_ptr schema() const {
        return _schema;
    }

    sstring cql() const;

    /// Create the generated schema as a table via CQL.
    ///
    /// Along with all its dependencies, like UDTs.
    /// The underlying schema_ptr instance is replaced with the one from the
    /// local table instance.
    future<> create_with_cql(cql_test_env& env);

    /// Make a partition key which is n-th in some arbitrary sequence of keys.
    ///
    /// There is no particular order for the keys, they're not in ring order.
    /// This method is deterministic, the pair of the seed used to generate the
    /// schema and `n` will map to the same generated value.
    data_model::mutation_description::key make_pkey(uint32_t n);

    /// Make n partition keys.
    ///
    /// Keys are in ring order.
    /// This method is deterministic, the pair of the seed used to generate the
    /// schema and `n` will map to the same generated values.
    std::vector<data_model::mutation_description::key> make_pkeys(size_t n);

    /// Make a clustering key which is n-th in some arbitrary sequence of keys.
    ///
    /// There is no particular order for the keys, they're not in clustering order.
    /// This method is deterministic, the pair of the seed used to generate the
    /// schema and `n` will map to the same generated value.
    data_model::mutation_description::key make_ckey(uint32_t n);

    /// Make up to n clustering keys.
    ///
    /// Key are in clustering order.
    /// This method is deterministic, the pair of the seed used to generate the
    /// schema and `n` will map to the same generated values.
    /// Fewer than n keys may be returned if the schema limits the clustering keys space.
    std::vector<data_model::mutation_description::key> make_ckeys(size_t n);

    data_model::mutation_description new_mutation(data_model::mutation_description::key pkey);

    /// Make a new mutation with a key produced via `make_pkey(n)`.
    data_model::mutation_description new_mutation(uint32_t n);

    /// Set the partition tombstone
    void set_partition_tombstone(std::mt19937& engine, data_model::mutation_description& md,
            timestamp_generator ts_gen = default_timestamp_generator(),
            expiry_generator exp_gen = no_expiry_expiry_generator());

    void add_row(std::mt19937& engine, data_model::mutation_description& md, data_model::mutation_description::key ckey,
            timestamp_generator ts_gen = default_timestamp_generator(),
            expiry_generator exp_gen = no_expiry_expiry_generator());

    /// Add a new row with a key produced via `make_ckey(n)`.
    void add_row(std::mt19937& engine, data_model::mutation_description& md, uint32_t n, timestamp_generator ts_gen = default_timestamp_generator(),
            expiry_generator exp_gen = no_expiry_expiry_generator());

    void add_static_row(std::mt19937& engine, data_model::mutation_description& md, timestamp_generator ts_gen = default_timestamp_generator(),
            expiry_generator exp_gen = no_expiry_expiry_generator());

    void delete_range(
            std::mt19937& engine,
            data_model::mutation_description& md,
            interval<data_model::mutation_description::key> range,
            timestamp_generator ts_gen = default_timestamp_generator(),
            expiry_generator exp_gen = no_expiry_expiry_generator());
};

/// Generate random mutations using the random schema.
///
/// `clustering_row_count_dist` and `range_tombstone_count_dist` will be used to
/// generate the respective counts for *each* partition. These params are
/// ignored if the schema has no clustering columns.
/// Mutations are returned in ring order. Does not contain duplicate partitions.
/// Futurized to avoid stalls.
future<std::vector<mutation>> generate_random_mutations(
        uint32_t seed,
        tests::random_schema& random_schema,
        timestamp_generator ts_gen = default_timestamp_generator(),
        expiry_generator exp_gen = no_expiry_expiry_generator(),
        std::uniform_int_distribution<size_t> partition_count_dist = std::uniform_int_distribution<size_t>(8, 16),
        std::uniform_int_distribution<size_t> clustering_row_count_dist = std::uniform_int_distribution<size_t>(16, 128),
        std::uniform_int_distribution<size_t> range_tombstone_count_dist = std::uniform_int_distribution<size_t>(4, 16));

future<std::vector<mutation>> generate_random_mutations(
        tests::random_schema& random_schema,
        timestamp_generator ts_gen = default_timestamp_generator(),
        expiry_generator exp_gen = no_expiry_expiry_generator(),
        std::uniform_int_distribution<size_t> partition_count_dist = std::uniform_int_distribution<size_t>(8, 16),
        std::uniform_int_distribution<size_t> clustering_row_count_dist = std::uniform_int_distribution<size_t>(16, 128),
        std::uniform_int_distribution<size_t> range_tombstone_count_dist = std::uniform_int_distribution<size_t>(4, 16));

/// Generate exactly partition_count partitions. See the more general overload above.
future<std::vector<mutation>> generate_random_mutations(tests::random_schema& random_schema, size_t partition_count);

} // namespace tests
