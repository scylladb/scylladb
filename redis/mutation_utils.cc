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
#include "service/storage_proxy.hh"
#include "schema.hh"
#include "database.hh"
#include <seastar/core/print.hh>
#include "redis/keyspace_utils.hh"
#include "redis/options.hh"
#include "mutation.hh"
#include "service_permit.hh"
#include "partition_slice_builder.hh"

using namespace seastar;

namespace redis {

atomic_cell make_cell(const schema_ptr schema,
        const abstract_type& type,
        bytes_view value,
        api::timestamp_type ts,
        long cttl = 0)
{

    if (cttl > 0) {
        auto ttl = std::chrono::seconds(cttl);
        return atomic_cell::make_live(type, ts, value, gc_clock::now() + ttl, ttl, atomic_cell::collection_member::no);
    }   
    auto ttl = schema->default_time_to_live();
    if (ttl.count() > 0) {
        return atomic_cell::make_live(type, ts, value, gc_clock::now() + ttl, ttl, atomic_cell::collection_member::no);
    }   
    return atomic_cell::make_live(type, ts, value, atomic_cell::collection_member::no);
}  

// cas_request sub-class for write_strings
class write_strings_cas_request : public service::cas_request {
private:
    service::storage_proxy& _proxy;
    schema_ptr _schema;
    bytes _key;
    bytes _data;
    long _ttl;

public:
    write_strings_cas_request(service::storage_proxy& proxy, schema_ptr schema, bytes key, bytes data, long ttl)
    : _proxy(proxy), _schema(schema), _key(std::move(key)), _data(std::move(data)), _ttl(ttl)  {}
    virtual ~write_strings_cas_request() = default;
    virtual std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr, const query::partition_slice& slice, api::timestamp_type ts) override {
        const column_definition& column = *_schema->get_column_definition(redis::DATA_COLUMN_NAME);
        auto pkey = partition_key::from_single_value(*_schema, _key);
        auto m = mutation(_schema, std::move(pkey));
        auto cell = make_cell(_schema, *(column.type.get()), _data, ts, _ttl);
        m.set_clustered_cell(clustering_key::make_empty(), column, std::move(cell));
        return m;
    }
};


future<> write_hashes(service::storage_proxy& proxy, redis::redis_options& options, bytes&& key, bytes&& field, bytes&& data, long ttl, service_permit permit) {
    db::timeout_clock::time_point timeout = db::timeout_clock::now() + options.get_write_timeout();

    auto schema = get_schema(proxy, options.get_keyspace_name(), redis::HASHes);
    const column_definition& column = *schema->get_column_definition(redis::DATA_COLUMN_NAME);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto ckey = clustering_key::from_single_value(*schema, field);
    auto m = mutation(schema, std::move(pkey));
    auto cell = make_cell(schema, *(column.type.get()), data, api::new_timestamp(), ttl);
    m.set_clustered_cell(ckey, column, std::move(cell));

    auto write_consistency_level = options.get_write_consistency_level();
    return proxy.mutate(std::vector<mutation> {std::move(m)}, write_consistency_level, timeout, nullptr, permit);
}


mutation make_mutation(service::storage_proxy& proxy, const redis_options& options, bytes&& key, bytes&& data, long ttl) {
    auto schema = get_schema(proxy, options.get_keyspace_name(), redis::STRINGs);
    const column_definition& column = *schema->get_column_definition(redis::DATA_COLUMN_NAME);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto m = mutation(schema, std::move(pkey));
    auto cell = make_cell(schema, *(column.type.get()), data, api::new_timestamp(), ttl);
    m.set_clustered_cell(clustering_key::make_empty(), column, std::move(cell));
    return m;
}

future<> write_strings(service::storage_proxy& proxy, redis::redis_options& options, bytes key, bytes data, long ttl, service_permit permit) {
    // construct cas_request
    db::timeout_clock::time_point timeout = db::timeout_clock::now() + options.get_write_timeout();
    auto schema = get_schema(proxy, options.get_keyspace_name(), redis::STRINGs);
    const column_definition& column = *schema->get_column_definition(redis::DATA_COLUMN_NAME);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto dk = dht::decorate_key(*schema, std::move(pkey));
    auto partition_range = dht::partition_range::make_singular(dk);
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    auto write_consistency_level = options.get_write_consistency_level();
    auto ssg = options.get_smp_service_group();

    service::client_state& client_state = options.get_client_state();
    auto trace_state = tracing::trace_state_ptr();
    auto desired_shard = service::storage_proxy::cas_shard(*schema, dk.token());
    if (desired_shard == this_shard_id()) {
        auto request = seastar::make_shared<write_strings_cas_request>(proxy, schema, std::move(key), std::move(data), ttl);
        return proxy.cas(schema, request, nullptr, partition_ranges,
            {timeout, std::move(permit), client_state, trace_state},
            db::consistency_level::LOCAL_SERIAL, db::consistency_level::LOCAL_QUORUM, timeout, timeout).then([] (bool is_applied) mutable {
            assert(is_applied); //XXX: handle failed condition
            return make_ready_future<>();
        });
    } else {
        return proxy.container().invoke_on(desired_shard, ssg,
                    [cs = client_state.move_to_other_shard(),
                     dk = dk,
                     ks = schema->ks_name(),
                     cf = schema->cf_name(),
                     gt =  tracing::global_trace_state_ptr(trace_state),
                     permit = std::move(permit),
                     key = std::move(key),
                     data = std::move(data),
                     ttl = ttl,
                     partition_ranges = std::move(partition_ranges),
                     timeout = std::move(timeout)]
                    (service::storage_proxy& proxy) mutable {
            return do_with(cs.get(), [&proxy, dk = std::move(dk), ks = std::move(ks), cf = std::move(cf),
                                      trace_state = tracing::trace_state_ptr(gt),
                                      permit = std::move(permit),
                                      key = std::move(key),
                                      data = std::move(data),
                                      ttl = ttl,
                                      partition_ranges = std::move(partition_ranges),
                                      timeout = std::move(timeout)]
                                      (service::client_state& client_state) mutable {
                auto schema = proxy.get_db().local().find_schema(ks, cf);
                //FIXME: A corresponding FIXME can be found in transport/server.cc when a message must be bounced
                // to another shard - once it is solved, this place can use a similar solution. Instead of passing
                // empty_service_permit() to the background operation, the current permit's lifetime should be prolonged,
                // so that it's destructed only after all background operations are finished as well.
                auto request = seastar::make_shared<write_strings_cas_request>(proxy, schema, std::move(key), std::move(data), ttl);
                return proxy.cas(schema, request, nullptr, partition_ranges,
                    {timeout, std::move(permit), client_state, trace_state},
                    db::consistency_level::LOCAL_SERIAL, db::consistency_level::LOCAL_QUORUM, timeout, timeout).then([] (bool is_applied) mutable {
                    assert(is_applied); //XXX: handle failed condition
                    return make_ready_future<>();
                });
            });
        });
    }
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
            return proxy.mutate(std::vector<mutation> {std::move(m)}, write_consistency_level, timeout, nullptr, permit);
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
    return proxy.mutate(mutations, write_consistency_level, timeout, nullptr, permit);
}

}
