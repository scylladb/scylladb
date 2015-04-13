/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef API_API_HH_
#define API_API_HH_

#include "http/httpd.hh"

namespace api {

struct http_context {
    http_server_control http_server;
};

future<> set_server(http_context& ctx);

}

#endif /* API_API_HH_ */
