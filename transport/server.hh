/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#ifndef CQL_SERVER_HH
#define CQL_SERVER_HH

#include "core/reactor.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "core/distributed.hh"

class database;

class cql_server {
    std::vector<server_socket> _listeners;
    service::storage_proxy _proxy;
    cql3::query_processor _query_processor;
public:
    cql_server(distributed<database>& db);
    future<> listen(ipv4_addr addr);
    void do_accepts(int which);
private:
    class connection;
    class response;
};

#endif
