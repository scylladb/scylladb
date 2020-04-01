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

#pragma once

#include "auth/service.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "redis/query_processor.hh"
#include "redis/request.hh"
#include "cql3/values.hh"
#include "auth/authenticator.hh"
#include "timeout_config.hh"
#include <memory>
#include <seastar/net/tls.hh>
#include <seastar/core/semaphore.hh>
#include "seastar/core/distributed.hh"
#include "seastar/core/seastar.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/estimated_histogram.hh"
#include "redis/protocol_parser.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/options.hh"
#include  "service/client_state.hh"
#include "redis/stats.hh"

db::consistency_level make_consistency_level(const sstring&);

namespace db {
class config;
};
class redis_exception;

namespace redis_transport {

struct redis_server_config {
    ::timeout_config _timeout_config;
    size_t _max_request_size;
    db::consistency_level _read_consistency_level;
    db::consistency_level _write_consistency_level;
    size_t _total_redis_db_count;
};

class redis_server {
    std::vector<server_socket> _listeners;
    distributed<service::storage_proxy>& _proxy;
    distributed<redis::query_processor>& _query_processor;
    redis_server_config _config;
    size_t _max_request_size;
    semaphore _memory_available;
    redis::stats _stats;
private:
    uint64_t _requests_blocked_memory = 0;
    auth::service& _auth_service;
    size_t _total_redis_db_count;
public:
    redis_server(distributed<service::storage_proxy>& proxy, distributed<redis::query_processor>& qp, auth::service& auth_service, redis_server_config config);
    future<> listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> = {}, bool keepalive = false);
    future<> do_accepts(int which, bool keepalive, socket_address server_addr);
    future<> stop();
public:
    struct result {
        result(redis::redis_message&& m) : _data(make_foreign(std::make_unique<redis::redis_message>(std::move(m)))) {}
        foreign_ptr<std::unique_ptr<redis::redis_message>> _data;
        inline lw_shared_ptr<scattered_message<char>> make_message()  {
            return _data->message();
        }
    };
    using response_type = result;
private:
    class fmt_visitor;
    friend class connection;
    class connection : public boost::intrusive::list_base_hook<> {
       // using result = redis_server::result;
        redis_server& _server;
        socket_address _server_addr;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        redis_protocol_parser _parser;
        seastar::gate _pending_requests_gate;
        //service::client_state _client_state;
        redis::redis_options _options;
        future<> _ready_to_respond = make_ready_future<>();
        unsigned _request_cpu = 0;
    private:
        enum class tracing_request_type : uint8_t {
            not_requested,
            no_write_on_close,
            write_on_close
        };  

        using execution_stage_type = inheriting_concrete_execution_stage<
                future<redis_server::result>,
                redis_server::connection*,
                redis::request&&,
                redis::redis_options&,
                service_permit
        >;
        static thread_local execution_stage_type _process_request_stage;
    public:
        connection(redis_server& server, socket_address server_addr, connected_socket&& fd, socket_address addr);
        ~connection();
        future<> process();
        future<> process_request();
        void write_reply(const redis_exception&);
        void write_reply(redis_server::result result);
        future<> shutdown();
    private:
        const ::timeout_config& timeout_config() { return _server.timeout_config(); }
        friend class process_request_executor;
        future<result> process_request_one(redis::request&& request, redis::redis_options&, service_permit permit);
        future<result> process_request_internal();
    };

private:
    bool _stopping = false;
    promise<> _all_connections_stopped;
    future<> _stopped = _all_connections_stopped.get_future();
    boost::intrusive::list<connection> _connections_list;
private:
    void maybe_idle() {
        if (_stopping && !_stats._connections_being_accepted && !_stats._current_connections) {
            _all_connections_stopped.set_value();
        }
    }
    const ::timeout_config& timeout_config() { return _config._timeout_config; }
};
}
