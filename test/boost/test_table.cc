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

#include "test/lib/test_table.hh"
#include "test/lib/log.hh"

#include <boost/range/adaptor/map.hpp>

#include "test/lib/cql_assertions.hh"

namespace test {

std::vector<clustering_key> slice_keys(const schema& schema, const std::vector<clustering_key>& keys, const query::clustering_row_ranges& ranges) {
    if (keys.empty() || ranges.empty()) {
        return {};
    }

    const auto tri_cmp = clustering_key::tri_compare(schema);
    std::vector<clustering_key> sliced_keys;

    auto keys_it = keys.begin();
    const auto keys_end = keys.end();
    auto ranges_it = ranges.begin();
    const auto ranges_end = ranges.end();

    while (keys_it != keys_end && ranges_it != ranges_end) {
        if (ranges_it->before(*keys_it, tri_cmp)) {
            ++keys_it; // Key is before current range: skip.
            continue;
        }
        if (ranges_it->contains(*keys_it, tri_cmp)) {
            sliced_keys.push_back(*keys_it++);
        } else {
            // Key is after current range: go to next range.
            ++ranges_it;
            continue;
        }
    }

    return sliced_keys;
}

std::pair<schema_ptr, std::vector<dht::decorated_key>> create_test_table(cql_test_env& env, const sstring& ks_name, const sstring& table_name,
        int partition_count, int row_per_partition_count) {
    class simple_population_generator : public population_generator {
        class simple_partition_content_generator : public partition_content_generator {
            const int _pk;
            int _ck = 0;
            const int _row_count;

        public:
            explicit simple_partition_content_generator(int pk, int row_count) : _pk(pk), _row_count(row_count) {
            }
            virtual partition_key generate_partition_key(const schema& schema) override {
                return partition_key::from_single_value(schema, serialized(_pk));
            }
            virtual bool has_static_row() override {
                return false;
            }
            virtual bytes generate_static_row(const schema&, const partition_key&) override {
                return {};
            }
            virtual int clustering_row_count() override {
                return _row_count;
            }
            virtual row generate_row(const schema& schema, const partition_key&) override {
                const auto ck = _ck++;
                const auto data = _pk ^ ck;
                auto buf = bytes(bytes::initialized_later{}, sizeof(int));
                std::copy_n(&data, 1, reinterpret_cast<int*>(buf.data()));
                return row{clustering_key::from_single_value(schema, serialized(ck)), std::move(buf)};
            }
            virtual query::clustering_row_ranges generate_delete_ranges(const schema&, const std::vector<clustering_key>&) override {
                return {};
            }
        };
        const int _partition_count;
        const int _row_count;
        int _pk = 0;
    public:
        explicit simple_population_generator(int partition_count, int row_count) : _partition_count(partition_count), _row_count(row_count) {
        }
        virtual size_t partition_count() override {
            return _partition_count;
        }
        virtual std::unique_ptr<partition_content_generator> make_partition_content_generator() override {
            return std::make_unique<simple_partition_content_generator>(_pk++, _row_count);
        }
    };
    auto pop_desc = create_test_table(env, ks_name, table_name,
            std::make_unique<simple_population_generator>(partition_count, row_per_partition_count));
    std::vector<dht::decorated_key> pkeys;
    pkeys.reserve(pop_desc.partitions.size());
    for (auto& part : pop_desc.partitions) {
        pkeys.emplace_back(std::move(part.dkey));
    }
    return std::pair(std::move(pop_desc.schema), std::move(pkeys));
}

population_description create_test_table(cql_test_env& env, const sstring& ks_name, const sstring& table_name, uint32_t seed,
        std::vector<partition_configuration> part_configs, generate_blob_function gen_blob) {
    class configurable_random_partition_content_generator : public test::partition_content_generator {
        std::mt19937& _engine;
        partition_configuration& _config;
        generate_blob_function& _gen_blob;
        std::uniform_int_distribution<int> _key_dist;

    public:
        configurable_random_partition_content_generator(std::mt19937& engine, partition_configuration& config, generate_blob_function& gen_blob)
            : _engine(engine)
            , _config(config)
            , _gen_blob(gen_blob)
            , _key_dist(std::numeric_limits<int>::min(), std::numeric_limits<int>::max()) {
        }
        virtual partition_key generate_partition_key(const schema& schema) override {
            return partition_key::from_single_value(schema, serialized(_key_dist(_engine)));
        }
        virtual bool has_static_row() override {
            return _config.static_row_size_dist.has_value();
        }
        virtual bytes generate_static_row(const schema& schema, const partition_key& pk) override {
            return _gen_blob(schema, (*_config.static_row_size_dist)(_engine), pk, nullptr);
        }
        virtual int clustering_row_count() override {
            return _config.clustering_row_count_dist(_engine);
        }
        virtual row generate_row(const schema& schema, const partition_key& pk) override {
            auto ck = clustering_key::from_single_value(schema, serialized(_key_dist(_engine)));
            auto value = _gen_blob(schema, _config.clustering_row_size_dist(_engine), pk, &ck);
            return row{std::move(ck), std::move(value)};
        }
        virtual query::clustering_row_ranges generate_delete_ranges(const schema& schema, const std::vector<clustering_key>& rows) override {
            const auto count = _config.range_deletion_count_dist(_engine);
            if (!count) {
                return {};
            }

            std::uniform_int_distribution<int> index_dist(0, rows.size() - 1);
            std::uniform_int_distribution<int> inclusive_dist(0, 1);

            using range_bound = query::clustering_range::bound;

            query::clustering_row_ranges ranges;
            for (auto i = 0; i < count - 1; ++i) {
                const auto size = _config.range_deletion_size_dist(_engine);
                if (size == 0) {
                    continue;
                }
                if (size >= int(rows.size())) {
                    ranges.emplace_back(range_bound(rows.front(), true), range_bound(rows.back(), true));
                    continue;
                }

                const bool b1_inclusive = inclusive_dist(_engine);
                const bool b2_inclusive = size_t(size) == rows.size() - 1 ? true : inclusive_dist(_engine);

                auto param = std::uniform_int_distribution<int>::param_type(0, rows.size() - size - !b1_inclusive - !b2_inclusive);
                const auto b1 = index_dist(_engine, param);

                const auto b2 = b1 + size - 1 + !b1_inclusive + !b2_inclusive;
                ranges.emplace_back(range_bound(rows.at(b1), b1_inclusive), range_bound(rows.at(b2), b2_inclusive));
            }
            return ranges;
        }
    };

    class configurable_random_population_generator : public test::population_generator {
        std::mt19937 _engine;
        std::vector<partition_configuration> _part_configs;
        size_t _count;
        generate_blob_function _gen_blob;

    public:
        configurable_random_population_generator(uint32_t seed, std::vector<partition_configuration> part_configs, generate_blob_function gen_blob)
            : _engine(seed)
            , _part_configs(std::move(part_configs))
            , _count(boost::accumulate(_part_configs, size_t(0), [] (size_t c, const partition_configuration& part_config) {
                    return c + part_config.count;
            }))
            , _gen_blob(std::move(gen_blob)) {
        }
        virtual size_t partition_count() override {
            return _count;
        }
        virtual std::unique_ptr<partition_content_generator> make_partition_content_generator() override {
            const auto index = std::uniform_int_distribution<int>(0, _part_configs.size() - 1)(_engine);
            auto& partition_config = _part_configs.at(index);
            auto gen = std::make_unique<configurable_random_partition_content_generator>(_engine, partition_config, _gen_blob);
            if (!--partition_config.count) {
                std::swap(partition_config, _part_configs.back());
                _part_configs.pop_back();
            }
            return gen;
        }
    };
    return create_test_table(env, ks_name, table_name, std::make_unique<configurable_random_population_generator>(seed, std::move(part_configs),
                std::move(gen_blob)));
}

namespace {

static size_t maybe_generate_static_row(cql_test_env& env, const schema& schema,partition_content_generator& part_gen,
        const cql3::prepared_cache_key_type& static_insert_id, const partition_description& part_desc) {
    if (!part_gen.has_static_row()) {
        return 0;
    }

    auto value = part_gen.generate_static_row(schema, part_desc.dkey.key());
    const auto size = value.size();

    env.execute_prepared(static_insert_id, {{
            cql3::raw_value::make_value(to_bytes(part_desc.dkey.key().get_component(schema, 0))),
            cql3::raw_value::make_value(serialized(std::move(value)))}}).get();
    return size;
}

struct clustering_row_generation_result {
    size_t written_bytes = 0;
    std::vector<clustering_key> live_rows;
    std::vector<clustering_key> dead_rows;
    query::clustering_row_ranges range_tombstones;
};

class clustering_range_less_compare {
    clustering_key::tri_compare _tri_cmp;
public:
    explicit clustering_range_less_compare(const schema& schema) : _tri_cmp(schema) {
    }
    bool operator()(const query::clustering_range& a, const query::clustering_range& b) const {
        if (!a.start() || !b.start()) {
            return !a.start();
        }
        if (auto res = _tri_cmp(a.start()->value(), b.start()->value()); res != 0) {
            return res < 0;
        }
        return a.start()->is_inclusive() < b.start()->is_inclusive();
    }
};

static clustering_row_generation_result generate_clustering_rows(
        cql_test_env& env,
        const schema& schema,
        partition_content_generator& part_gen,
        const cql3::prepared_cache_key_type& clustering_insert_id,
        const std::array<std::array<cql3::prepared_cache_key_type, 2>, 2>& clustering_delete_id_mappings,
        const partition_description& part_desc) {
    // If the partition has no static row and no clustering rows it will not exist.
    // Make sure each partition has at least one row.
    const auto clustering_row_count = std::max(int(!part_desc.has_static_row), part_gen.clustering_row_count());
    if (!clustering_row_count) {
        return clustering_row_generation_result{};
    }

    size_t written_bytes = 0;

    // The generator is allowed to produce duplicates.
    // Use a set to de-duplicate rows (and while at it keep them sorted).
    std::set<clustering_key, clustering_key::less_compare> written_rows{clustering_key::less_compare(schema)};

    for (auto j = 0; j < clustering_row_count; ++j) {
        auto [key, value] = part_gen.generate_row(schema, part_desc.dkey.key());

        written_bytes += key.external_memory_usage();
        written_bytes += value.size();

        env.execute_prepared(clustering_insert_id, {{
                cql3::raw_value::make_value(to_bytes(part_desc.dkey.key().get_component(schema, 0))),
                cql3::raw_value::make_value(to_bytes(key.get_component(schema, 0))),
                cql3::raw_value::make_value(serialized(std::move(value)))}}).get();
        written_rows.insert(std::move(key));
    }

    std::vector<clustering_key> rows;
    rows.reserve(written_rows.size());
    std::move(written_rows.begin(), written_rows.end(), std::back_inserter(rows));

    auto delete_ranges = part_gen.generate_delete_ranges(schema, rows);
    for (const auto& range : delete_ranges) {
        const auto delete_id = clustering_delete_id_mappings[range.start()->is_inclusive()][range.end()->is_inclusive()];
        env.execute_prepared(delete_id, {{
                cql3::raw_value::make_value(to_bytes(part_desc.dkey.key().get_component(schema, 0))),
                cql3::raw_value::make_value(to_bytes(range.start()->value().get_component(schema, 0))),
                cql3::raw_value::make_value(to_bytes(range.end()->value().get_component(schema, 0)))}}).get();
    }

    std::sort(delete_ranges.begin(), delete_ranges.end(), clustering_range_less_compare(schema));

    const auto tri_cmp = clustering_key::tri_compare(schema);

    const auto deleted_ranges_deoverlapped = query::clustering_range::deoverlap(delete_ranges, tri_cmp);

    std::vector<clustering_key> live_rows, dead_rows;
    auto ranges_it = deleted_ranges_deoverlapped.cbegin();
    const auto ranges_end = deleted_ranges_deoverlapped.cend();

    for (auto& row : rows) {
        while (ranges_it != ranges_end && ranges_it->after(row, tri_cmp)) {
            ++ranges_it;
        }
        if (ranges_it == ranges_end) {
            live_rows.push_back(std::move(row));
            continue;
        }
        if (ranges_it->before(row, tri_cmp)) {
            live_rows.push_back(std::move(row));
            continue;
        }
        if (ranges_it->contains(row, tri_cmp)) {
            dead_rows.push_back(std::move(row));
            continue;
        }
    }

    return clustering_row_generation_result{written_bytes, std::move(live_rows), std::move(dead_rows), std::move(delete_ranges)};
}

static std::vector<clustering_key> merge_and_deduplicate_rows(const schema& schema, const std::vector<clustering_key>& a,
        const std::vector<clustering_key>& b) {
    std::vector<clustering_key> merged_rows;
    merged_rows.reserve(a.size() + b.size());
    std::merge(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(merged_rows), clustering_key::less_compare(schema));
    merged_rows.erase(std::unique(merged_rows.begin(), merged_rows.end(), clustering_key::equality(schema)), merged_rows.end());
    return merged_rows;
}

static query::clustering_row_ranges merge_ranges(const schema& schema, const query::clustering_row_ranges& a, const query::clustering_row_ranges& b) {
    query::clustering_row_ranges merged_ranges;
    merged_ranges.reserve(a.size() + b.size());
    std::merge(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(merged_ranges), clustering_range_less_compare(schema));
    return merged_ranges;
}

struct partition_generation_result {
    size_t written_partition_count = 0;
    size_t written_row_count = 0;
    size_t written_rt_count = 0;
    size_t written_bytes = 0;
    std::vector<partition_description> partitions;
};

static partition_generation_result generate_partitions(
        cql_test_env& env,
        const schema& schema,
        population_generator& pop_gen,
        const cql3::prepared_cache_key_type& static_insert_id,
        const cql3::prepared_cache_key_type& clustering_insert_id,
        const std::array<std::array<cql3::prepared_cache_key_type, 2>, 2>& clustering_delete_id_mappings) {

    size_t written_row_count = 0;
    size_t written_rt_count = 0;
    size_t written_bytes = 0;

    // The generator is allowed to produce duplicates.
    // Use a map to deduplicate partitions (and while at it keep them sorted).
    std::map<dht::decorated_key, partition_description, dht::decorated_key::less_comparator> partitions(
            dht::decorated_key::less_comparator(schema.shared_from_this()));

    const auto part_count = pop_gen.partition_count();
    for (size_t i = 0; i < part_count; ++i) {
        auto part_gen = pop_gen.make_partition_content_generator();
        partition_description part_desc(dht::decorate_key(schema, part_gen->generate_partition_key(schema)));
        written_bytes += part_desc.dkey.key().external_memory_usage();

        {
            auto size = maybe_generate_static_row(env, schema, *part_gen, static_insert_id, part_desc);
            written_bytes += size;
            part_desc.has_static_row = size > 0;
        }
        {
            auto res = generate_clustering_rows(env, schema, *part_gen, clustering_insert_id, clustering_delete_id_mappings, part_desc);
            written_bytes += res.written_bytes;
            written_row_count += std::max(res.live_rows.size() + res.dead_rows.size(), size_t(part_desc.has_static_row));
            written_rt_count += res.range_tombstones.size();
            part_desc.live_rows = std::move(res.live_rows);
            part_desc.dead_rows = std::move(res.dead_rows);
            part_desc.range_tombstones = std::move(res.range_tombstones);
        }

        auto it = partitions.find(part_desc.dkey);
        if (it == partitions.end()) {
            auto key = part_desc.dkey;
            partitions.emplace(std::move(key), std::move(part_desc));
        } else {
            it->second.has_static_row |= part_desc.has_static_row;
            it->second.live_rows = merge_and_deduplicate_rows(schema, part_desc.live_rows, it->second.live_rows);
            it->second.dead_rows = merge_and_deduplicate_rows(schema, part_desc.dead_rows, it->second.dead_rows);
            it->second.range_tombstones = merge_ranges(schema, part_desc.range_tombstones, it->second.range_tombstones);
        }
    }

    std::vector<partition_description> partition_vec;
    partition_vec.reserve(partitions.size());
    auto partition_values = partitions | boost::adaptors::map_values;
    std::move(partition_values.begin(), partition_values.end(), std::back_inserter(partition_vec));

    return partition_generation_result{part_count, written_row_count, written_rt_count, written_bytes, std::move(partition_vec)};
}

} // anonymous namespace

population_description create_test_table(cql_test_env& env, const sstring& ks_name, const sstring& table_name,
        std::unique_ptr<population_generator> pop_gen) {
    env.execute_cql(format("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}};", ks_name)).get();
    env.execute_cql(format("CREATE TABLE {}.{} (pk int, ck int, s blob static, v blob, PRIMARY KEY(pk, ck));", ks_name, table_name)).get();

    const auto static_insert_id = env.prepare(format("INSERT INTO {}.{} (\"pk\", \"s\") VALUES (?, ?);", ks_name, table_name)).get0();
    const auto clustering_insert_id = env.prepare(format("INSERT INTO {}.{} (\"pk\", \"ck\", \"v\") VALUES (?, ?, ?);", ks_name, table_name)).get0();

    const auto clustering_delete_id_open_open = env.prepare(format("DELETE FROM {}.{} WHERE pk = ? AND ck > ? AND ck < ?;", ks_name,
                table_name)).get0();
    const auto clustering_delete_id_open_closed = env.prepare(format("DELETE FROM {}.{} WHERE pk = ? AND ck > ? AND ck <= ?;", ks_name,
                table_name)).get0();
    const auto clustering_delete_id_closed_open = env.prepare(format("DELETE FROM {}.{} WHERE pk = ? AND ck >= ? AND ck < ?;", ks_name,
                table_name)).get0();
    const auto clustering_delete_id_closed_closed = env.prepare(format("DELETE FROM {}.{} WHERE pk = ? AND ck >= ? AND ck <= ?;", ks_name,
                table_name)).get0();

    // Indexing with `range_bound::is_inclusive()` will select the correct id to use.
    const std::array<std::array<cql3::prepared_cache_key_type, 2>, 2> clustering_delete_id_mappings = {{
        {clustering_delete_id_open_open, clustering_delete_id_open_closed},
        {clustering_delete_id_closed_open, clustering_delete_id_closed_closed}}};

    population_description pop_desc;

    pop_desc.schema = env.local_db().find_column_family(ks_name, table_name).schema();

    testlog.info("Populating test data...");

    auto res = generate_partitions(env, *pop_desc.schema, *pop_gen, static_insert_id, clustering_insert_id, clustering_delete_id_mappings);
    pop_desc.partitions = std::move(res.partitions);

    uint64_t live_row_count = 0;
    uint64_t dead_row_count = 0;
    for (auto& part : pop_desc.partitions) {
        live_row_count += std::max(part.live_rows.size(), uint64_t(part.has_static_row));
        dead_row_count += part.dead_rows.size();
        testlog.trace("Partition {}, has_static_rows={}, rows={}, (of which live={} and dead={})",
                part.dkey,
                part.has_static_row,
                part.live_rows.size() + part.dead_rows.size(),
                part.live_rows.size(),
                part.dead_rows.size());
    }

    // We don't expect this to fail, this is here more to validate our
    // expectation of the population, then the correctness of writes.
    auto msg = env.execute_cql(format("SELECT COUNT(*) FROM {}.{}", ks_name, table_name)).get0();
    assert_that(msg).is_rows().with_rows({{serialized(int64_t(live_row_count))}});

    testlog.info("Done. Population summary: written {} partitions, {} rows, {} range tombstones and {} bytes;"
            " have (after de-duplication) {} partitions, {} live rows and {} dead rows.",
            res.written_partition_count,
            res.written_row_count,
            res.written_rt_count,
            res.written_bytes,
            pop_desc.partitions.size(),
            live_row_count,
            dead_row_count);

    return pop_desc;
}

} // namespace test
