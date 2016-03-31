/*
 * Copyright 2015 Cloudius Systems
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

#include "schema_builder.hh"
#include "mutation_reader_assertions.hh"
#include "mutation_source_test.hh"

// partitions must be sorted by decorated key
static void require_no_token_duplicates(const std::vector<mutation>& partitions) {
    std::experimental::optional<dht::token> last_token;
    for (auto&& p : partitions) {
        const dht::decorated_key& key = p.decorated_key();
        if (last_token && key.token() == *last_token) {
            BOOST_FAIL("token duplicate detected");
        }
        last_token = key.token();
    }
}

static void test_range_queries(populate_fn populate) {
    BOOST_TEST_MESSAGE("Testing range queries");

    auto s = schema_builder("ks", "cf")
        .with_column("key", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    auto make_partition_mutation = [s] (bytes key) -> mutation {
        mutation m(partition_key::from_single_value(*s, key), s);
        m.set_clustered_cell(clustering_key::make_empty(*s), "v", data_value(bytes("v1")), 1);
        return m;
    };

    int partition_count = 300;

    std::vector<mutation> partitions;
    for (int i = 0; i < partition_count; ++i) {
        partitions.emplace_back(
            make_partition_mutation(to_bytes(sprint("key_%d", i))));
    }

    std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());
    require_no_token_duplicates(partitions);

    dht::decorated_key key_before_all = partitions.front().decorated_key();
    partitions.erase(partitions.begin());

    dht::decorated_key key_after_all = partitions.back().decorated_key();
    partitions.pop_back();

    auto ds = populate(s, partitions);

    auto test_slice = [&] (query::range<dht::ring_position> r) {
        BOOST_TEST_MESSAGE(sprint("Testing range %s", r));
        assert_that(ds(s, r))
            .produces(slice(partitions, r))
            .produces_end_of_stream();
    };

    auto inclusive_token_range = [&] (size_t start, size_t end) {
        return query::partition_range::make(
            {dht::ring_position::starting_at(partitions[start].token())},
            {dht::ring_position::ending_at(partitions[end].token())});
    };

    test_slice(query::partition_range::make(
        {key_before_all, true}, {partitions.front().decorated_key(), true}));

    test_slice(query::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), true}));

    test_slice(query::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), false}));

    test_slice(query::partition_range::make(
        {dht::ring_position::starting_at(key_before_all.token())},
        {dht::ring_position::ending_at(partitions.front().token())}));

    test_slice(query::partition_range::make(
        {dht::ring_position::ending_at(key_before_all.token())},
        {dht::ring_position::ending_at(partitions.front().token())}));

    test_slice(query::partition_range::make(
        {dht::ring_position::ending_at(key_before_all.token())},
        {dht::ring_position::starting_at(partitions.front().token())}));

    test_slice(query::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, true}));

    test_slice(query::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, false}));

    test_slice(query::partition_range::make(
        {partitions.back().decorated_key(), false}, {key_after_all, false}));

    test_slice(query::partition_range::make(
        {dht::ring_position::starting_at(partitions.back().token())},
        {dht::ring_position::ending_at(key_after_all.token())}));

    test_slice(query::partition_range::make(
        {dht::ring_position::starting_at(partitions.back().token())},
        {dht::ring_position::starting_at(key_after_all.token())}));

    test_slice(query::partition_range::make(
        {dht::ring_position::ending_at(partitions.back().token())},
        {dht::ring_position::starting_at(key_after_all.token())}));

    test_slice(query::partition_range::make(
        {partitions[0].decorated_key(), false},
        {partitions[1].decorated_key(), true}));

    test_slice(query::partition_range::make(
        {partitions[0].decorated_key(), true},
        {partitions[1].decorated_key(), false}));

    test_slice(query::partition_range::make(
        {partitions[1].decorated_key(), true},
        {partitions[3].decorated_key(), false}));

    test_slice(query::partition_range::make(
        {partitions[1].decorated_key(), false},
        {partitions[3].decorated_key(), true}));

    test_slice(query::partition_range::make_ending_with(
        {partitions[3].decorated_key(), true}));

    test_slice(query::partition_range::make_starting_with(
        {partitions[partitions.size() - 4].decorated_key(), true}));

    test_slice(inclusive_token_range(0, 0));
    test_slice(inclusive_token_range(1, 1));
    test_slice(inclusive_token_range(2, 4));
    test_slice(inclusive_token_range(127, 128));
    test_slice(inclusive_token_range(128, 128));
    test_slice(inclusive_token_range(128, 129));
    test_slice(inclusive_token_range(127, 129));
    test_slice(inclusive_token_range(partitions.size() - 1, partitions.size() - 1));

    test_slice(inclusive_token_range(0, partitions.size() - 1));
    test_slice(inclusive_token_range(0, partitions.size() - 2));
    test_slice(inclusive_token_range(0, partitions.size() - 3));
    test_slice(inclusive_token_range(0, partitions.size() - 128));

    test_slice(inclusive_token_range(1, partitions.size() - 1));
    test_slice(inclusive_token_range(2, partitions.size() - 1));
    test_slice(inclusive_token_range(3, partitions.size() - 1));
    test_slice(inclusive_token_range(128, partitions.size() - 1));
}

void run_mutation_source_tests(populate_fn populate) {
    test_range_queries(populate);
}

struct mutation_sets {
    std::vector<std::vector<mutation>> equal;
    std::vector<std::vector<mutation>> unequal;
    mutation_sets(){}
};

static tombstone new_tombstone() {
    return { api::new_timestamp(), gc_clock::now() };
}

static mutation_sets generate_mutation_sets() {
    using mutations = std::vector<mutation>;
    mutation_sets result;

    {
        auto common_schema = schema_builder("ks", "test")
                .with_column("pk_col", bytes_type, column_kind::partition_key)
                .with_column("ck_col_1", bytes_type, column_kind::clustering_key)
                .with_column("ck_col_2", bytes_type, column_kind::clustering_key)
                .with_column("regular_col_1", bytes_type)
                .with_column("regular_col_2", bytes_type)
                .with_column("static_col_1", bytes_type, column_kind::static_column)
                .with_column("static_col_2", bytes_type, column_kind::static_column);

        auto s1 = common_schema
                .with_column("regular_col_1_s1", bytes_type) // will have id in between common columns
                .build();

        auto s2 = common_schema
                .with_column("regular_col_1_s2", bytes_type) // will have id in between common columns
                .build();

        // Differing keys
        result.unequal.emplace_back(mutations{
            mutation(partition_key::from_single_value(*s1, to_bytes("key1")), s1),
            mutation(partition_key::from_single_value(*s2, to_bytes("key2")), s2)
        });

        auto m1 = mutation(partition_key::from_single_value(*s1, to_bytes("key1")), s1);
        auto m2 = mutation(partition_key::from_single_value(*s2, to_bytes("key1")), s2);

        result.equal.emplace_back(mutations{m1, m2});

        clustering_key ck1 = clustering_key::from_deeply_exploded(*s1, {data_value(bytes("ck1_0")), data_value(bytes("ck1_1"))});
        clustering_key ck2 = clustering_key::from_deeply_exploded(*s1, {data_value(bytes("ck2_0")), data_value(bytes("ck2_1"))});
        auto ttl = gc_clock::duration(1);

        {
            auto tomb = new_tombstone();
            m1.partition().apply(tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply(tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto tomb = new_tombstone();
            m1.partition().apply_delete(*s1, ck2, tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_delete(*s1, ck2, tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto tomb = new_tombstone();
            auto key = clustering_key_prefix::from_deeply_exploded(*s1, {data_value(bytes("ck2_0"))});
            m1.partition().apply_row_tombstone(*s1, key, tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_row_tombstone(*s1, key, tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = api::new_timestamp();
            m1.set_clustered_cell(ck1, "regular_col_1", data_value(bytes("regular_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck1, "regular_col_1", data_value(bytes("regular_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = api::new_timestamp();
            m1.set_clustered_cell(ck1, "regular_col_2", data_value(bytes("regular_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck1, "regular_col_2", data_value(bytes("regular_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = api::new_timestamp();
            m1.partition().apply_insert(*s1, ck2, ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_insert(*s1, ck2, ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = api::new_timestamp();
            m1.set_clustered_cell(ck2, "regular_col_1", data_value(bytes("ck2_regular_col_1_value")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck2, "regular_col_1", data_value(bytes("ck2_regular_col_1_value")), ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = api::new_timestamp();
            m1.set_static_cell("static_col_1", data_value(bytes("static_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_static_cell("static_col_1", data_value(bytes("static_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = api::new_timestamp();
            m1.set_static_cell("static_col_2", data_value(bytes("static_col_value")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_static_cell("static_col_2", data_value(bytes("static_col_value")), ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = api::new_timestamp();
            m1.set_clustered_cell(ck2, "regular_col_1_s1", data_value(bytes("x")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck2, "regular_col_1_s2", data_value(bytes("x")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
        }
    }

    return result;
}

static const mutation_sets& get_mutation_sets() {
    static thread_local const auto ms = generate_mutation_sets();
    return ms;
}

void for_each_mutation_pair(std::function<void(const mutation&, const mutation&, are_equal)> callback) {
    auto&& ms = get_mutation_sets();
    for (auto&& mutations : ms.equal) {
        auto i = mutations.begin();
        assert(i != mutations.end());
        const mutation& first = *i++;
        while (i != mutations.end()) {
            callback(first, *i, are_equal::yes);
            ++i;
        }
    }
    for (auto&& mutations : ms.unequal) {
        auto i = mutations.begin();
        assert(i != mutations.end());
        const mutation& first = *i++;
        while (i != mutations.end()) {
            callback(first, *i, are_equal::no);
            ++i;
        }
    }
}

void for_each_mutation(std::function<void(const mutation&)> callback) {
    auto&& ms = get_mutation_sets();
    for (auto&& mutations : ms.equal) {
        for (auto&& m : mutations) {
            callback(m);
        }
    }
    for (auto&& mutations : ms.unequal) {
        for (auto&& m : mutations) {
            callback(m);
        }
    }
}

bytes make_blob(size_t blob_size) {
    static thread_local std::independent_bits_engine<std::default_random_engine, 8, uint8_t> random_bytes;
    bytes big_blob(bytes::initialized_later(), blob_size);
    for (auto&& b : big_blob) {
        b = random_bytes();
    }
    return big_blob;
};

class random_mutation_generator::impl {
    friend class random_mutation_generator;
    const size_t _external_blob_size = 128; // Should be enough to force use of external bytes storage
    const column_id column_count = row::max_vector_size * 2;
    std::mt19937 _gen;
    schema_ptr _schema;
    std::vector<bytes> _blobs;

    static gc_clock::time_point expiry_dist(auto& gen) {
        static thread_local std::uniform_int_distribution<int> dist(0, 2);
        return gc_clock::time_point() + std::chrono::seconds(dist(gen));
    }

public:
    schema_ptr make_schema() {
        auto builder = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck1", bytes_type, column_kind::clustering_key)
                .with_column("ck2", bytes_type, column_kind::clustering_key);

        // Create enough columns so that row can overflow its vector storage
        for (column_id i = 0; i < column_count; ++i) {
            {
                auto column_name = sprint("v%d", i);
                builder.with_column(to_bytes(column_name), bytes_type, column_kind::regular_column);
            }
            {
                auto column_name = sprint("s%d", i);
                builder.with_column(to_bytes(column_name), bytes_type, column_kind::static_column);
            }
        }

        return builder.build();
    }

    impl() {
        _schema = make_schema();

        for (int i = 0; i < 1024; ++i) {
            _blobs.emplace_back(make_blob(_external_blob_size));
        }

        std::random_device rd;
        // In case of errors, replace the seed with a fixed value to get a deterministic run.
        auto seed = rd();
        BOOST_TEST_MESSAGE(sprint("Random seed: %s", seed));
        _gen = std::mt19937(seed);
    }

    mutation operator()() {
        std::uniform_int_distribution<column_id> column_count_dist(1, column_count);
        std::uniform_int_distribution<column_id> column_id_dist(0, column_count - 1);
        std::uniform_int_distribution<size_t> value_blob_index_dist(0, 2);
        std::normal_distribution<> ck_index_dist(_blobs.size() / 2, 1.5);
        std::uniform_int_distribution<int> bool_dist(0, 1);

        std::uniform_int_distribution<api::timestamp_type> timestamp_dist(api::min_timestamp, api::min_timestamp + 2); // 3 values

        auto pkey = partition_key::from_single_value(*_schema, _blobs[0]);
        mutation m(pkey, _schema);

        auto set_random_cells = [&] (row& r, column_kind kind) {
            auto columns_to_set = column_count_dist(_gen);
            for (column_id i = 0; i < columns_to_set; ++i) {
                // FIXME: generate expiring cells
                auto cell = bool_dist(_gen)
                            ? atomic_cell::make_live(timestamp_dist(_gen), _blobs[value_blob_index_dist(_gen)])
                            : atomic_cell::make_dead(timestamp_dist(_gen), expiry_dist(_gen));
                r.apply(_schema->column_at(kind, column_id_dist(_gen)), std::move(cell));
            }
        };

        auto random_tombstone = [&] {
            return tombstone(timestamp_dist(_gen), expiry_dist(_gen));
        };

        auto random_row_marker = [&] {
            static thread_local std::uniform_int_distribution<int> dist(0, 3);
            switch (dist(_gen)) {
                case 0: return row_marker();
                case 1: return row_marker(random_tombstone());
                case 2: return row_marker(timestamp_dist(_gen));
                case 3: return row_marker(timestamp_dist(_gen), std::chrono::seconds(1), expiry_dist(_gen));
                default: assert(0);
            }
        };

        if (bool_dist(_gen)) {
            m.partition().apply(random_tombstone());
        }

        set_random_cells(m.partition().static_row(), column_kind::static_column);

        auto random_blob = [&] {
            return _blobs[std::min(_blobs.size() - 1, static_cast<size_t>(std::max(0.0, ck_index_dist(_gen))))];
        };

        auto row_count_dist = [&] (auto& gen) {
            static thread_local std::normal_distribution<> dist(32, 1.5);
            return static_cast<size_t>(std::min(100.0, std::max(0.0, dist(gen))));
        };

        size_t row_count = row_count_dist(_gen);
        for (size_t i = 0; i < row_count; ++i) {
            auto ckey = clustering_key::from_exploded(*_schema, {random_blob(), random_blob()});
            deletable_row& row = m.partition().clustered_row(ckey);
            set_random_cells(row.cells(), column_kind::regular_column);
            row.marker() = random_row_marker();
        }

        size_t range_tombstone_count = row_count_dist(_gen);
        for (size_t i = 0; i < range_tombstone_count; ++i) {
            auto key = clustering_key::from_exploded(*_schema, {random_blob()});
            m.partition().apply_row_tombstone(*_schema, key, random_tombstone());
        }
        return m;
    }
};

random_mutation_generator::~random_mutation_generator() {}

random_mutation_generator::random_mutation_generator()
    : _impl(std::make_unique<random_mutation_generator::impl>())
{ }

mutation random_mutation_generator::operator()() {
    return (*_impl)();
}

schema_ptr random_mutation_generator::schema() const {
    return _impl->_schema;
}