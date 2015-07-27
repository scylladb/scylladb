/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#ifndef CQL_SERVER_HH
#define CQL_SERVER_HH

#include "core/reactor.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "core/distributed.hh"
#include <memory>

namespace scollectd {

class registrations;

}

class database;

class cql_server {
    std::vector<server_socket> _listeners;
    distributed<service::storage_proxy>& _proxy;
    distributed<cql3::query_processor>& _query_processor;
    std::unique_ptr<scollectd::registrations> _collectd_registrations;
private:
    scollectd::registrations setup_collectd();
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
public:
    cql_server(distributed<service::storage_proxy>& proxy, distributed<cql3::query_processor>& qp);
    future<> listen(ipv4_addr addr);
    void do_accepts(int which);
    future<> stop();
private:
    class fmt_visitor;
    class connection;
    class response;
    friend class type_codec;
};

#endif
