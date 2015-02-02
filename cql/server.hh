/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#ifndef CQL_SERVER_HH
#define CQL_SERVER_HH

#include "core/reactor.hh"

class database;

class cql_server {
    std::vector<server_socket> _listeners;
public:
    cql_server(database& db);
    future<> listen(ipv4_addr addr);
    void do_accepts(int which);
private:
    class connection;
    class response;
};

#endif
