/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef APPS_SEASTAR_THRIFT_SERVER_HH_
#define APPS_SEASTAR_THRIFT_SERVER_HH_

#include "core/reactor.hh"
#include "core/distributed.hh"
#include <memory>
#include <cstdint>

class thrift_server;
class thrift_stats;
class database;

namespace org { namespace apache { namespace cassandra {

static const sstring thrift_version = "20.1.0";

class CassandraCobSvIfFactory;

}}}

namespace apache { namespace thrift { namespace protocol {

class TProtocolFactory;

}}}

namespace apache { namespace thrift { namespace async {

class TAsyncProcessorFactory;

}}}


class thrift_server {
    std::vector<server_socket> _listeners;
    std::unique_ptr<thrift_stats> _stats;
    boost::shared_ptr<org::apache::cassandra::CassandraCobSvIfFactory> _handler_factory;
    std::unique_ptr<apache::thrift::protocol::TProtocolFactory> _protocol_factory;
    boost::shared_ptr<apache::thrift::async::TAsyncProcessorFactory> _processor_factory;
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _requests_served = 0;
public:
    thrift_server(distributed<database>& db);
    ~thrift_server();
    future<> listen(ipv4_addr addr);
    void do_accepts(int which);
    class connection;
    uint64_t total_connections() const;
    uint64_t current_connections() const;
    uint64_t requests_served() const;
};

#endif /* APPS_SEASTAR_THRIFT_SERVER_HH_ */
