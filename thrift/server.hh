/*
 * Copyright (C) 2014 ScyllaDB
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

#ifndef APPS_SEASTAR_THRIFT_SERVER_HH_
#define APPS_SEASTAR_THRIFT_SERVER_HH_

#include "core/reactor.hh"
#include "core/distributed.hh"
#include "cql3/query_processor.hh"
#include "timeout_config.hh"
#include <seastar/core/gate.hh>
#include <memory>
#include <cstdint>
#include <boost/intrusive/list.hpp>

class thrift_server;
class thrift_stats;
class database;

namespace cassandra {

static const sstring thrift_version = "20.1.0";

class CassandraCobSvIfFactory;

}

namespace apache { namespace thrift { namespace protocol {

class TProtocolFactory;
class TProtocol;

}}}

namespace apache { namespace thrift { namespace async {

class TAsyncProcessor;
class TAsyncProcessorFactory;

}}}

namespace apache { namespace thrift { namespace transport {

class TMemoryBuffer;

}}}

namespace auth {
class service;
}

struct thrift_server_config {
    ::timeout_config timeout_config;
    uint64_t max_request_size;
};

class thrift_server {
    class connection : public boost::intrusive::list_base_hook<> {
        struct fake_transport;
        thrift_server& _server;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        temporary_buffer<char> _in_tmp;
        boost::shared_ptr<fake_transport> _transport;
        boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> _input;
        boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> _output;
        boost::shared_ptr<apache::thrift::protocol::TProtocol> _in_proto;
        boost::shared_ptr<apache::thrift::protocol::TProtocol> _out_proto;
        boost::shared_ptr<apache::thrift::async::TAsyncProcessor> _processor;
        promise<> _processor_promise;
    public:
        connection(thrift_server& server, connected_socket&& fd, socket_address addr);
        ~connection();
        connection(connection&&);
        future<> process();
        future<> read();
        future<> write();
        void shutdown();
    private:
        future<> process_one_request();
    };
private:
    std::vector<server_socket> _listeners;
    std::unique_ptr<thrift_stats> _stats;
    boost::shared_ptr<::cassandra::CassandraCobSvIfFactory> _handler_factory;
    std::unique_ptr<apache::thrift::protocol::TProtocolFactory> _protocol_factory;
    boost::shared_ptr<apache::thrift::async::TAsyncProcessorFactory> _processor_factory;
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _requests_served = 0;
    thrift_server_config _config;
    boost::intrusive::list<connection> _connections_list;
    seastar::gate _stop_gate;
public:
    thrift_server(distributed<database>& db, distributed<cql3::query_processor>& qp, auth::service&, thrift_server_config config);
    ~thrift_server();
    future<> listen(ipv4_addr addr, bool keepalive);
    future<> stop();
    void do_accepts(int which, bool keepalive);
    uint64_t total_connections() const;
    uint64_t current_connections() const;
    uint64_t requests_served() const;

private:
    void maybe_retry_accept(int which, bool keepalive, std::exception_ptr ex);
};

#endif /* APPS_SEASTAR_THRIFT_SERVER_HH_ */
