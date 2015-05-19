/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef API_API_HH_
#define API_API_HH_

#include "http/httpd.hh"
#include "database.hh"

namespace api {

struct http_context {
    http_server_control http_server;
    distributed<database>& db;
    http_context(distributed<database>& _db) : db(_db) {}
};

future<> set_server(http_context& ctx);

}

#endif /* API_API_HH_ */
