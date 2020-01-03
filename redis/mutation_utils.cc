/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#include "redis/mutation_utils.hh"
#include "types.hh"
#include "service/storage_service.hh"
#include "schema.hh"
#include "database.hh"
#include <seastar/core/print.hh>
#include "redis/keyspace_utils.hh"
#include "redis/locks_context.hh"
#include "redis/options.hh"
#include "mutation.hh"
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
    return redis::with_strings_locked_key(redis::get_locks_context(options.db_index()), key, timeout, 
        [timeout, &proxy, &options, key = std::move(key), data = std::move(data), ttl, permit] () mutable {
        auto m = make_mutation(proxy, options, std::move(key), std::move(data), ttl);
        auto write_consistency_level = options.get_write_consistency_level();
        return proxy.mutate(std::vector<mutation> {std::move(m)}, write_consistency_level, timeout, nullptr, permit);
    });
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
    struct context {
        const sstring table;
        locks_map& locks;
        context(const sstring table, locks_map& locks_ref) : table(table), locks(locks_ref) {}
    };
    auto& locks_context = get_locks_context(options.db_index());
    std::vector<context> contexts {
        {redis::STRINGs, locks_context._strings_locks },
        {redis::LISTs, locks_context._lists_locks },
        {redis::HASHes, locks_context._hashes_locks },
        {redis::SETs, locks_context._sets_locks },
        {redis::ZSETs, locks_context._zsets_locks }
    }; 
    auto remove = [&proxy, timeout, write_consistency_level, permit, &options, keys = std::move(keys)] (context& ctx) mutable {
        return parallel_for_each(keys.begin(), keys.end(), [&proxy, timeout, write_consistency_level, &options, permit, &ctx] (const bytes& key) mutable {
            return redis::with_locked_key(ctx.locks, key, timeout, [&proxy, timeout, write_consistency_level, &options, key, permit, &ctx] () mutable {
                auto m = make_tombstone(proxy, options, ctx.table, key);
                return proxy.mutate(std::vector<mutation> {std::move(m)}, write_consistency_level, timeout, nullptr, permit);
            });
        });
    };
    return do_with(std::move(contexts), [remove = std::move(remove)] (auto& contexts) mutable {
        return parallel_for_each(contexts.begin(), contexts.end(), std::move(remove));
    });
}

}
