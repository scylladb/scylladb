/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef APPS_SEASTAR_THRIFT_SERVER_HH_
#define APPS_SEASTAR_THRIFT_SERVER_HH_

#include "core/reactor.hh"
#include <memory>
#include <cstdint>

class thrift_server;
class thrift_stats;

class thrift_server {
    std::vector<server_socket> _listeners;
    std::unique_ptr<thrift_stats> _stats;
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _requests_served = 0;
public:
    thrift_server();
    future<> listen(ipv4_addr addr);
    void do_accepts(int which);
    class connection;
    uint64_t total_connections() const;
    uint64_t current_connections() const;
    uint64_t requests_served() const;
};

#endif /* APPS_SEASTAR_THRIFT_SERVER_HH_ */
