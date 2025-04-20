/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "redis/mutation_utils.hh"
#include "types/types.hh"
#include "service/storage_proxy.hh"
#include "schema/schema.hh"
#include <seastar/core/format.hh>
#include "redis/keyspace_utils.hh"
#include "redis/options.hh"
#include "mutation/mutation.hh"
#include "service_permit.hh"

using namespace seastar;

namespace redis {

atomic_cell make_cell(const schema_ptr schema,
        const abstract_type& type,
        bytes_view value,
        long cttl = 0)
{

    if (cttl > 0) {
        auto ttl = std::chrono::seconds(cttl);
        return atomic_cell::make_live(type, api::new_timestamp(), value, gc_clock::now() + ttl, ttl, atomic_cell::collection_member::no);
    }   
    auto ttl = schema->default_time_to_live();
    if (ttl.count() > 0) {
        return atomic_cell::make_live(type, api::new_timestamp(), value, gc_clock::now() + ttl, ttl, atomic_cell::collection_member::no);
    }   
    return atomic_cell::make_live(type, api::new_timestamp(), value, atomic_cell::collection_member::no);
}  


future<> write_hashes(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, bytes&& field, bytes&& data, long ttl, service_permit permit) {
    db::timeout_clock::time_point timeout = db::timeout_clock::now() + options.get_write_timeout();

    auto schema = get_schema(proxy, options.get_keyspace_name(), redis::HASHes);
    const column_definition& column = *schema->get_column_definition(redis::DATA_COLUMN_NAME);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto ckey = clustering_key::from_single_value(*schema, field);
    auto m = mutation(schema, std::move(pkey));
    auto cell = make_cell(schema, *(column.type.get()), data, ttl);
    m.set_clustered_cell(ckey, column, std::move(cell));

    auto write_consistency_level = options.get_write_consistency_level();
    return proxy.mutate(std::vector<mutation> {std::move(m)}, write_consistency_level, timeout, nullptr, permit, db::allow_per_partition_rate_limit::yes);
}


mutation make_mutation(service::storage_proxy& proxy, const redis_options& options, bytes&& key, bytes&& data, long ttl) {
    auto schema = get_schema(proxy, options.get_keyspace_name(), redis::STRINGs);
    const column_definition& column = *schema->get_column_definition(redis::DATA_COLUMN_NAME);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto m = mutation(schema, std::move(pkey));
    auto cell = make_cell(schema, *(column.type.get()), data, ttl);
    m.set_clustered_cell(clustering_key::make_empty(), column, std::move(cell));
    return m;
}

future<> write_strings(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, bytes&& data, long ttl, service_permit permit) {
    db::timeout_clock::time_point timeout = db::timeout_clock::now() + options.get_write_timeout();
    auto m = make_mutation(proxy, options, std::move(key), std::move(data), ttl);
    auto write_consistency_level = options.get_write_consistency_level();
    return proxy.mutate(std::vector<mutation> {std::move(m)}, write_consistency_level, timeout, nullptr, permit, db::allow_per_partition_rate_limit::yes);
}


mutation make_tombstone(service::storage_proxy& proxy, const redis_options& options, const sstring& cf_name, const bytes& key) {
    auto schema = get_schema(proxy, options.get_keyspace_name(), cf_name);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto m = mutation(schema, std::move(pkey));
    m.partition().apply(tombstone { api::new_timestamp(), gc_clock::now() }); 
    return m;
}

future<> delete_objects(service::storage_proxy& proxy, redis::redis_options& options, std::vector<bytes>&& keys, service_permit permit) {
    db::timeout_clock::time_point timeout = db::timeout_clock::now() + options.get_write_timeout();
    auto write_consistency_level = options.get_write_consistency_level();
    std::vector<sstring> tables { redis::STRINGs, redis::LISTs, redis::HASHes, redis::SETs, redis::ZSETs }; 
    auto remove = [&proxy, timeout, write_consistency_level, permit, &options, keys = std::move(keys)] (const sstring& cf_name) {
        return parallel_for_each(keys.begin(), keys.end(), [&proxy, timeout, write_consistency_level, &options, permit, cf_name] (const bytes& key) {
            auto m = make_tombstone(proxy, options, cf_name, key);
            return proxy.mutate(std::vector<mutation> {std::move(m)}, write_consistency_level, timeout, nullptr, permit, db::allow_per_partition_rate_limit::yes);
        });
    };  
    return parallel_for_each(tables.begin(), tables.end(), remove);
}

future<> delete_fields(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, std::vector<bytes>&& fields, service_permit permit) {
    db::timeout_clock::time_point timeout = db::timeout_clock::now() + options.get_write_timeout();
    auto write_consistency_level = options.get_write_consistency_level();
    auto schema = get_schema(proxy, options.get_keyspace_name(), redis::HASHes);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto ts = api::new_timestamp();
    auto clk = gc_clock::now();
    std::vector<mutation> mutations;
    for (auto& field : fields) {
        auto ckey = clustering_key::from_single_value(*schema, field);
        auto m = mutation(schema, pkey);
        m.partition().apply_delete(*schema, ckey, tombstone { ts, clk });
        mutations.push_back(m);
    }
    return proxy.mutate(mutations, write_consistency_level, timeout, nullptr, permit, db::allow_per_partition_rate_limit::yes);
}

}
