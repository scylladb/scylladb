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
    distributed<service::storage_proxy>& _proxy;
    distributed<cql3::query_processor>& _query_processor;
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
