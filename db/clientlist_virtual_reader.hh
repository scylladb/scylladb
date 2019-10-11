/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "schema.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/shared_ptr.hh>

namespace cql_transport {
class cql_server;
}  // namespace cql_transport

namespace db::system_keyspace {
    struct client_data;
}  // namespace system_keyspace

namespace db::clientlist {

class clientlist_mutation_reader final : public flat_mutation_reader::impl {
    schema_ptr _schema;
    const dht::partition_range* _prange;
    const query::partition_slice& _slice;
    flat_mutation_reader_opt _mutation_fragmentator_opt;
private:
    future<std::vector<system_keyspace::client_data>>
    filter_and_sort_by_port(std::vector<system_keyspace::client_data>&& clients) const;

    future<> maybe_setup_mutation_fragmentator();
public:
    clientlist_mutation_reader(
            schema_ptr schema,
            const dht::partition_range& prange,
            const query::partition_slice& slice);
            
    future<> fill_buffer(db::timeout_clock::time_point) override;
    void next_partition() override;
    future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override;
    future<> fast_forward_to(position_range, db::timeout_clock::time_point) override;
    size_t buffer_size() const override;
};

struct virtual_reader {
    flat_mutation_reader operator()(schema_ptr schema,
            const dht::partition_range& range,
            const query::partition_slice& slice) {
        return make_flat_mutation_reader<clientlist_mutation_reader>(schema, range, slice);
    }
};

}  // namespace db::clientlist

