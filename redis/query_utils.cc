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


#include "redis/query_utils.hh"
#include "redis/options.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "dht/i_partitioner.hh"
#include "partition_slice_builder.hh"
#include "query-result-reader.hh"
#include "gc_clock.hh"
#include "service_permit.hh"
#include "redis/keyspace_utils.hh"

namespace redis {
class strings_result_builder {
    lw_shared_ptr<strings_result> _data;
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
private:
    void add_cell(const column_definition& col, const std::optional<query::result_atomic_cell_view>& cell)
    {
        if (cell) {
            cell->value().with_linearized([this, &col, &cell] (bytes_view cell_view) {
                auto&& dv = col.type->deserialize_value(cell_view);
                auto&& d = dv.serialize_nonnull();
                _data->_result = std::move(d);
                if (cell->expiry().has_value()) {
                    _data->_ttl = cell->expiry().value() - gc_clock::now();
                }
                _data->_has_result = true;
            });
        }
    }
public:
    strings_result_builder(lw_shared_ptr<strings_result> data, const schema_ptr schema, const query::partition_slice& ps)
        : _data(data)
        , _partition_slice(ps)
        , _schema(schema)
    {
    }
    void accept_new_partition(const partition_key& key, uint32_t row_count) {}
    void accept_new_partition(uint32_t row_count) {}
    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row)
    {
        auto row_iterator = row.iterator();
        for (auto&& id : _partition_slice.regular_columns) {
            add_cell(_schema->regular_column_at(id), row_iterator.next_atomic_cell());
        }
    }
    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

future<lw_shared_ptr<strings_result>> read_strings(service::storage_proxy& proxy, const redis_options& options, const bytes& key, service_permit permit) {
    auto schema = get_schema(proxy, options.get_keyspace_name(), redis::STRINGs);
    auto ps = partition_slice_builder(*schema).build();
    query::read_command cmd(schema->id(), schema->version(), ps, 1, gc_clock::now(), std::nullopt, 1); 
    auto pkey = partition_key::from_single_value(*schema, key);
    auto partition_range = dht::partition_range::make_singular(dht::decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    auto read_consistency_level = options.get_read_consistency_level();
    db::timeout_clock::time_point timeout = db::timeout_clock::now() + options.get_read_timeout();
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), read_consistency_level, {timeout, permit, service::client_state::for_internal_calls()}).then([ps, schema] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = make_lw_shared<strings_result>();
            v.consume(ps, strings_result_builder(pd, schema, ps));
            return pd;
        }); 
    }); 
}

}
