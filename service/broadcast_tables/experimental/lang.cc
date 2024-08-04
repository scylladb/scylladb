/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "lang.hh"

#include <seastar/core/future.hh>

#include "data_dictionary/data_dictionary.hh"
#include "db/system_keyspace.hh"
#include "service/broadcast_tables/experimental/query_result.hh"
#include "service/raft/raft_group0_client.hh"
#include "partition_slice_builder.hh"
#include "query-request.hh"
#include "cache_temperature.hh"
#include "service/storage_proxy.hh"

namespace service::broadcast_tables {

bool is_broadcast_table_statement(const sstring& keyspace, const sstring& column_family) {
    return keyspace == db::system_keyspace::NAME && column_family == db::system_keyspace::BROADCAST_KV_STORE;
}

future<query_result> execute(service::raft_group0_client& group0_client, const query& query) {
    auto group0_cmd = group0_client.prepare_command(broadcast_table_query{query}, "broadcast_tables query");
    auto guard = group0_client.create_result_guard(group0_cmd.new_state_id);
    co_await group0_client.add_entry_unguarded(std::move(group0_cmd), nullptr);
    co_return guard.get();
}


static
std::pair<lw_shared_ptr<::query::read_command>, dht::partition_range>
prepare_read_command(storage_proxy& proxy, const schema& schema, const bytes& key) {
    auto slice = partition_slice_builder(schema).build();
    auto partition_key = partition_key::from_single_value(schema, key);
    dht::ring_position ring_position(dht::get_token(schema, partition_key), partition_key);
    auto range = dht::partition_range::make_singular(ring_position);
    return {make_lw_shared<::query::read_command>(
            schema.id(),
            schema.version(),
            slice,
            proxy.get_max_result_size(slice),
            ::query::tombstone_limit(proxy.get_tombstone_limit())
        ), range};
}

static
const atomic_cell_view
get_atomic_cell(const schema_ptr& schema, mutation& mutation, const bytes& name) {
    const auto* column_definition = schema->get_column_definition(name);
    return mutation.partition().clustered_row(*schema, clustering_key::make_empty()).cells().cell_at(column_definition->id).as_atomic_cell(*column_definition);
}

future<query_result> execute_broadcast_table_query(
    storage_proxy& proxy,
    const query& query,
    utils::UUID cmd_id) {
    // std::bind_front is used to avoid losing the captures in coroutines.
    return std::visit(make_visitor(
        std::bind_front([] (storage_proxy& proxy, const service::broadcast_tables::select_query& q) -> future<query_result> {
            const auto schema = db::system_keyspace::broadcast_kv_store();

            // Read mutations
            const auto [read_cmd, range] = prepare_read_command(proxy, *schema, q.key);
            auto [rs, _] = co_await proxy.query_mutations_locally(schema, read_cmd, range, db::no_timeout);

            if (rs->partitions().empty()) {
                co_return query_result_select{};
            }

            SCYLLA_ASSERT(rs->partitions().size() == 1); // In this version only one value per partition key is allowed.

            const auto& p = rs->partitions()[0];
            auto mutation = p.mut().unfreeze(schema);
            const auto cell = get_atomic_cell(schema, mutation, "value");

            co_return query_result_select{
                    .value = cell.value().linearize()
                };
        }, std::ref(proxy)),
        std::bind_front([] (storage_proxy& proxy, utils::UUID cmd_id, const broadcast_tables::update_query& q) -> future<query_result> {
            const auto schema = db::system_keyspace::broadcast_kv_store();

            // Read mutations
            const auto [read_cmd, range] = prepare_read_command(proxy, *schema, q.key);
            auto [rs, _] = co_await proxy.query_mutations_locally(schema, read_cmd, range, db::no_timeout);

            bool found = !rs->partitions().empty();

            SCYLLA_ASSERT(!found || rs->partitions().size() == 1); // In this version at most one value per partition key is allowed.

            auto new_mutation = found
                ? rs->partitions()[0].mut().unfreeze(schema)
                : mutation(schema, partition_key::from_single_value(*schema, to_bytes(q.key)));

            const auto cell = found
                ? std::optional<atomic_cell_view>{get_atomic_cell(schema, new_mutation, "value")}
                : std::nullopt;

            const auto previous_value = found
                ? bytes_opt{cell->value().linearize()}
                : std::nullopt;

            bool is_conditional = q.value_condition.has_value();
            bool is_applied = !is_conditional || *q.value_condition == previous_value;

            if (is_applied) {
                auto old_ts = found ? cell->timestamp() : api::min_timestamp;
                auto from_state_id = utils::UUID_gen::micros_timestamp(cmd_id);
                auto ts = std::max(from_state_id, old_ts + 1);

                auto value = utf8_type->deserialize(q.new_value);
                new_mutation.set_clustered_cell(clustering_key::make_empty(), "value", std::move(value), ts);
                co_await proxy.mutate_locally(new_mutation, {}, {});
            }

            if (is_conditional) {
                co_return query_result_conditional_update{
                        .is_applied = is_applied,
                        .previous_value = previous_value
                    };
            } else {
                co_return query_result_none{};
            }
        }, std::ref(proxy), cmd_id)),
        query.q
    );
}

}
