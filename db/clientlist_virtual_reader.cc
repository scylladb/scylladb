/*
 * Copyright (C) 2019 ScyllaDB
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

#include "db/clientlist_virtual_reader.hh"

#include "api/api.hh"
#include "db/system_keyspace.hh"
#include "db/timeout_clock.hh"
#include "dht/i_partitioner.hh"
#include "mutation_fragment.hh"
#include "range.hh"
#include "service/storage_service.hh"
#include "transport/server.hh"

#include <algorithm>
#include <unordered_map>
#include <vector>

namespace db::clientlist {

namespace {
class clients_reducer {
    std::vector<system_keyspace::client_data> _val;
public:
    future<> operator()(std::vector<system_keyspace::client_data>&& rhs) {
        // Move the smaller vector into the bigger one
        if (_val.size() < rhs.size()) {
            _val.swap(rhs);
        }
        _val.reserve(_val.size() + rhs.size());
        _val.insert(_val.end(), std::make_move_iterator(rhs.begin()), std::make_move_iterator(rhs.end()));
        return make_ready_future<>();
    }

    std::vector<system_keyspace::client_data> get() && {
        return std::move(_val);
    }
};

class ck_comparator {
    schema_ptr _schema;
public:
    using port_type = decltype(system_keyspace::client_data::port);

    ck_comparator(schema_ptr schema) : _schema(schema) {}

    int operator()(const clustering_key_prefix& lhs, const clustering_key_prefix& rhs) {
        return clustering_key_prefix::prefix_equal_tri_compare(*_schema)(lhs, rhs);
    }
    int operator()(const port_type& lhs, const clustering_key_prefix& rhs) {
        const auto lhs_bytes = data_type_for<port_type>()->decompose(lhs);
        return operator()(clustering_key_prefix::from_single_value(*_schema, lhs_bytes), rhs);
    }
    int operator()(const clustering_key_prefix& lhs, const port_type& rhs) {
        const auto rhs_bytes = data_type_for<port_type>()->decompose(rhs);
        return operator()(lhs, clustering_key_prefix::from_single_value(*_schema, rhs_bytes));
    }
};
}  // anon. namespace

static future<std::vector<system_keyspace::client_data>> get_all_clients_data(
        distributed<cql_transport::cql_server>& distributed_cql_server) {
    return distributed_cql_server.map_reduce(clients_reducer{},
            [] (const cql_transport::cql_server& srv) {
        const auto& conn_list = srv.get_connections_list();
        return do_with(std::vector<system_keyspace::client_data>{},
                [&conn_list] (auto& per_shard_result) {
            return do_for_each(conn_list.begin(), conn_list.end(),
                    [&per_shard_result] (const auto& conn) {
                system_keyspace::client_data cd;
                cd.ip = conn.get_client_state().get_client_address().addr();
                cd.port = conn.get_client_state().get_client_port();
                cd.connection_stage = conn.get_client_state().get_auth_state();
                cd.protocol_version = conn.get_protocol_version();
                cd.username = conn.get_client_state().user()->name;

                per_shard_result.emplace_back(std::move(cd));
            }).then([psr = std::move(per_shard_result)] {
                return make_ready_future<std::vector<system_keyspace::client_data>>(std::move(psr));
            });
        });
    });
}

// Clusters clients by IP and sorts them by port number within clusters.
future<std::vector<system_keyspace::client_data>>
clientlist_mutation_reader::filter_and_sort_by_port(std::vector<system_keyspace::client_data>&& clients) const {
    std::vector<system_keyspace::client_data> sorted_clients;
    sorted_clients.reserve(clients.size());
    
    std::unordered_map<
            decltype(system_keyspace::client_data::ip),
            std::vector<system_keyspace::client_data>> pk_clusters;
    
    for (auto&& client : clients) {
        const auto pk = partition_key::from_single_value(*_schema, inet_addr_type->decompose(client.ip));
        const dht::token calculated_token = dht::global_partitioner().get_token(*_schema, pk);
        if (dht::shard_of(calculated_token) != engine().cpu_id()) {
            continue;
        }
        for (auto& range : _slice.row_ranges(*_schema, pk)) {
            const auto ck_bytes = data_type_for<decltype(system_keyspace::client_data::port
                    )>()->decompose(client.port);
            const auto ck_prefix = clustering_key_prefix::from_single_value(*_schema, ck_bytes);

            if (range.contains(ck_prefix, ck_comparator(_schema))) {
                pk_clusters[client.ip].emplace_back(std::move(client));
                break;
            }
        }
    }

    for (auto& [ip, cluster] : pk_clusters) {
        if (_slice.options.contains(query::partition_slice::option::reversed)) {
            std::sort(
                    cluster.begin(), cluster.end(),
                    [] (const auto& client1, const auto& client2) { return client1.port > client2.port; });
        } else {
            std::sort(
                    cluster.begin(), cluster.end(),
                    [] (const auto& client1, const auto& client2) { return client1.port < client2.port; });
        }
        sorted_clients.insert(
                sorted_clients.end(),
                make_move_iterator(cluster.begin()),
                make_move_iterator(cluster.end()));
    }
    return make_ready_future<std::vector<system_keyspace::client_data>>(std::move(sorted_clients));
}

future<> clientlist_mutation_reader::maybe_setup_mutation_fragmentator() {
    if (_mutation_fragmentator_opt) {
        return make_ready_future<>();
    }

    return smp::submit_to(0, [] {
        // It must be run on shard 0. Shards with id>=1 see: get_dist_cql_server()==nullptr
        return std::ref(*service::get_local_storage_service().get_dist_cql_server());
    }).then([] (distributed<cql_transport::cql_server>& distributed_cql_server) {
        return get_all_clients_data(distributed_cql_server);
    }).then([this] (auto&& client_data_vec) {
        return filter_and_sort_by_port(std::move(client_data_vec)).then([this] (auto&& sorted_client_data_vec) {
            return do_with(
                    std::vector<mutation>{},
                    [this, scdv=std::move(sorted_client_data_vec)] (auto&& mutations_vec) mutable {
                mutations_vec.reserve(scdv.size());

                std::for_each(scdv.begin(), scdv.end(), [this, &mutations_vec] (auto&& client) {
                    mutations_vec.emplace_back(db::system_keyspace::make_client_mutation(std::move(client)));
                });
                if (!mutations_vec.empty()) {
                    _mutation_fragmentator_opt = flat_mutation_reader_from_mutations(std::move(mutations_vec));
                }
                return make_ready_future<>();
            });
        });
    });
}

clientlist_mutation_reader::clientlist_mutation_reader(
        schema_ptr schema,
        const dht::partition_range& prange,
        const query::partition_slice& slice)
    : impl(schema)
    , _schema(std::move(schema))
    , _prange(&prange)
    , _slice(slice) {}

future<> clientlist_mutation_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    // PKs returned from different shards must be distinct, otherwise combining reader
    // hangs up. Now: one client can maintain concurrent connections with many different
    // shards but his IP belongs to partition range of a particular (possibly yet another)
    // shard. That's why every shard must traverse every connection and return only the
    // clients with IPs that fall within *this shard's* partition range.

    return with_timeout(timeout, maybe_setup_mutation_fragmentator()).then([this, timeout] {
        if (!_mutation_fragmentator_opt) {
            // Setup of `_mutation_fragmentator_opt' failed => there are no connections
            _end_of_stream = true;
            return make_ready_future<>();
        }
        return do_until([this, timeout] { return is_end_of_stream() || is_buffer_full(); }, [this, timeout] {
            return _mutation_fragmentator_opt->consume_pausable([this] (mutation_fragment mf) {
                push_mutation_fragment(std::move(mf));
                return stop_iteration(is_buffer_full());
            }, timeout).then([this] {
                if (_mutation_fragmentator_opt->is_end_of_stream() && _mutation_fragmentator_opt->is_buffer_empty()) {
                    _end_of_stream = true;
                    _mutation_fragmentator_opt = std::nullopt;
                }
            });
        });
    });
}

void clientlist_mutation_reader::next_partition() {
    // This method should never be needed.
    api::unimplemented();
}

future<> clientlist_mutation_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    // This method should never be needed.
    api::unimplemented();
    return make_ready_future<>();
}

future<> clientlist_mutation_reader::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    // This method should never be needed.
    api::unimplemented();
    return make_ready_future<>();
}

size_t clientlist_mutation_reader::buffer_size() const {
    if (_mutation_fragmentator_opt) {
        return flat_mutation_reader::impl::buffer_size() + _mutation_fragmentator_opt->buffer_size();
    }
    return flat_mutation_reader::impl::buffer_size();
}

} // namespace db::clientlist
