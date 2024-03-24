/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/key_utils.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/random_utils.hh"
#include "dht/i_partitioner.hh"

namespace tests {

namespace {

template<typename RawKey, typename DecoratedKey, typename Comparator>
std::vector<DecoratedKey> generate_keys(
        size_t n,
        schema_ptr s,
        Comparator cmp,
        const std::vector<data_type>& types,
        std::function<std::optional<DecoratedKey>(const RawKey&)> decorate_fun,
        bool allow_prefixes,
        std::optional<key_size> size) {
    auto keys = std::set<DecoratedKey, Comparator>(cmp);
    const auto effective_size = size.value_or(tests::key_size{1, 128});

    std::mt19937 engine(tests::random::get_int<uint32_t>());
    std::uniform_int_distribution<size_t> component_count_dist(1, types.size());
    tests::value_generator value_gen;

    std::vector<data_value> components;
    components.reserve(types.size());

    while (keys.size() != n) {
        components.clear();
        auto component_count = allow_prefixes ? component_count_dist(engine) : types.size();
        for (size_t i = 0; i < component_count; ++i) {
            components.emplace_back(value_gen.generate_atomic_value(engine, *types.at(i), effective_size.min, effective_size.max));
        }
        auto raw_key = RawKey::from_deeply_exploded(*s, components);
        // discard empty keys on the off chance that we generate one
        if (raw_key.is_empty() || (types.size() == 1 && raw_key.begin(*s)->empty())) {
            continue;
        }
        if constexpr (std::is_same_v<RawKey, DecoratedKey>) {
            keys.emplace(std::move(raw_key));
        } else if (auto decorated_key_opt = decorate_fun(raw_key); decorated_key_opt) {
            keys.emplace(std::move(*decorated_key_opt));
        }
    }

    return std::vector<DecoratedKey>(keys.begin(), keys.end());
}

}

std::vector<dht::decorated_key> generate_partition_keys(size_t n, schema_ptr s, std::optional<shard_id> shard, std::optional<key_size> size) {
    return generate_keys<partition_key, dht::decorated_key, dht::decorated_key::less_comparator>(
            n,
            s,
            dht::decorated_key::less_comparator(s),
            s->partition_key_type()->types(),
            [s, shard, tokens = std::set<dht::token>()] (const partition_key& pkey) mutable -> std::optional<dht::decorated_key> {
                auto dkey = dht::decorate_key(*s, pkey);
                if (shard && *shard != dht::static_shard_of(*s, dkey.token())) {
                    return {};
                }
                if (!tokens.insert(dkey.token()).second) {
                    return {};
                }
                return dkey;
            },
            false,
            size);
}

std::vector<dht::decorated_key> generate_partition_keys(size_t n, schema_ptr s, local_shard_only lso, std::optional<key_size> size) {
    return generate_partition_keys(n, std::move(s), lso == local_shard_only::yes ? std::optional(this_shard_id()) : std::nullopt, size);
}

dht::decorated_key generate_partition_key(schema_ptr s, std::optional<shard_id> shard, std::optional<key_size> size) {
    auto&& keys = generate_partition_keys(1, std::move(s), shard, size);
    return std::move(keys.front());
}

dht::decorated_key generate_partition_key(schema_ptr s, local_shard_only lso, std::optional<key_size> size) {
    return generate_partition_key(std::move(s), lso == local_shard_only::yes ? std::optional(this_shard_id()) : std::nullopt, size);
}

std::vector<clustering_key> generate_clustering_keys(size_t n, schema_ptr s, bool allow_prefixes, std::optional<key_size> size) {
    return generate_keys<clustering_key, clustering_key, clustering_key::less_compare>(
            n,
            s,
            clustering_key::less_compare(*s),
            s->clustering_key_type()->types(),
            {},
            allow_prefixes,
            size);
}

clustering_key generate_clustering_key(schema_ptr s, bool allow_prefix, std::optional<key_size> size) {
    auto&& keys = generate_clustering_keys(1, std::move(s), allow_prefix, size);
    return std::move(keys.front());
}

__attribute__((no_sanitize("undefined")))
int64_t d2t(double d) {
    // Double to unsigned long conversion will overflow if the
    // input is greater than numeric_limits<long>::max(), so divide by two and
    // multiply again later.
    auto scale = std::numeric_limits<unsigned long>::max();
    return static_cast<unsigned long>(d * static_cast<double>(scale >> 1)) << 1;
};

} // namespace tests
