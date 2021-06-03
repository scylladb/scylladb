/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "messaging_service.hh"
#include "message/messaging_service.hh"
#include <seastar/rpc/rpc_types.hh>
#include "api/api-doc/messaging_service.json.hh"
#include <iostream>
#include <sstream>

using namespace httpd::messaging_service_json;
using namespace netw;

namespace api {

using shard_info = messaging_service::shard_info;
using msg_addr = messaging_service::msg_addr;

static const int32_t num_verb = static_cast<int32_t>(messaging_verb::LAST);

std::vector<message_counter> map_to_message_counters(
        const std::unordered_map<gms::inet_address, unsigned long>& map) {
    std::vector<message_counter> res;
    for (auto i : map) {
        res.push_back(message_counter());
        res.back().key = boost::lexical_cast<sstring>(i.first);
        res.back().value = i.second;
    }
    return res;
}

/**
 * Return a function that performs a map_reduce on messaging_service
 * For each instance it calls its foreach_client method set the value
 * according to a function that it gets as a parameter.
 *
 */
future_json_function get_client_getter(sharded<netw::messaging_service>& ms, std::function<uint64_t(const shard_info&)> f) {
    return [&ms, f](std::unique_ptr<request> req) {
        using map_type = std::unordered_map<gms::inet_address, uint64_t>;
        auto get_shard_map = [f](messaging_service& ms) {
            std::unordered_map<gms::inet_address, unsigned long> map;
            ms.foreach_client([&map, f] (const msg_addr& id, const shard_info& info) {
                map[id.addr] = f(info);
            });
            return map;
        };
        return ms.map_reduce0(get_shard_map, map_type(), map_sum<map_type>).
                then([](map_type&& map) {
            return make_ready_future<json::json_return_type>(map_to_message_counters(map));
        });
    };
}

future_json_function get_server_getter(sharded<netw::messaging_service>& ms, std::function<uint64_t(const rpc::stats&)> f) {
    return [&ms, f](std::unique_ptr<request> req) {
        using map_type = std::unordered_map<gms::inet_address, uint64_t>;
        auto get_shard_map = [f](messaging_service& ms) {
            std::unordered_map<gms::inet_address, unsigned long> map;
            ms.foreach_server_connection_stats([&map, f] (const rpc::client_info& info, const rpc::stats& stats) mutable {
                map[gms::inet_address(info.addr.addr())] = f(stats);
            });
            return map;
        };
        return ms.map_reduce0(get_shard_map, map_type(), map_sum<map_type>).
                then([](map_type&& map) {
            return make_ready_future<json::json_return_type>(map_to_message_counters(map));
        });
    };
}

void set_messaging_service(http_context& ctx, routes& r, sharded<netw::messaging_service>& ms) {
    get_timeout_messages.set(r, get_client_getter(ms, [](const shard_info& c) {
        return c.get_stats().timeout;
    }));

    get_sent_messages.set(r, get_client_getter(ms, [](const shard_info& c) {
        return c.get_stats().sent_messages;
    }));

    get_replied_messages.set(r, get_client_getter(ms, [](const shard_info& c) {
        return c.get_stats().replied;
    }));

    get_dropped_messages.set(r, get_client_getter(ms, [](const shard_info& c) {
        // We don't have the same drop message mechanism
        // as origin has.
        // hence we can always return 0
        return 0;
    }));

    get_exception_messages.set(r, get_client_getter(ms, [](const shard_info& c) {
        return c.get_stats().exception_received;
    }));

    get_pending_messages.set(r, get_client_getter(ms, [](const shard_info& c) {
        return c.get_stats().pending;
    }));

    get_respond_pending_messages.set(r, get_server_getter(ms, [](const rpc::stats& c) {
        return c.pending;
    }));

    get_respond_completed_messages.set(r, get_server_getter(ms, [](const rpc::stats& c) {
        return c.sent_messages;
    }));

    get_version.set(r, [&ms](const_req req) {
        return ms.local().get_raw_version(req.get_query_param("addr"));
    });

    get_dropped_messages_by_ver.set(r, [&ms](std::unique_ptr<request> req) {
        shared_ptr<std::vector<uint64_t>> map = make_shared<std::vector<uint64_t>>(num_verb);

        return ms.map_reduce([map](const uint64_t* local_map) mutable {
            for (auto i = 0; i < num_verb; i++) {
                (*map)[i]+= local_map[i];
            }
        },[](messaging_service& ms) {
            return make_ready_future<const uint64_t*>(ms.get_dropped_messages());
        }).then([map]{
            std::vector<verb_counter> res;
            for (auto i : verb_counter::verb_wrapper::all_items()) {
                verb_counter c;
                messaging_verb v = i; // for type safety we use messaging_verb values
                auto idx = static_cast<uint32_t>(v);
                if (idx >= map->size()) {
                    throw std::runtime_error(format("verb index out of bounds: {:d}, map size: {:d}", idx, map->size()));
                }
                if ((*map)[idx] > 0) {
                    c.count = (*map)[idx];
                    c.verb = i;
                    res.push_back(c);
                }
            }
            return make_ready_future<json::json_return_type>(res);
        });
    });
}

void unset_messaging_service(http_context& ctx, routes& r) {
    get_timeout_messages.unset(r);
    get_sent_messages.unset(r);
    get_replied_messages.unset(r);
    get_dropped_messages.unset(r);
    get_exception_messages.unset(r);
    get_pending_messages.unset(r);
    get_respond_pending_messages.unset(r);
    get_respond_completed_messages.unset(r);
    get_version.unset(r);
    get_dropped_messages_by_ver.unset(r);
}

}

