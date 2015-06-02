/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef API_API_HH_
#define API_API_HH_

#include "http/httpd.hh"
#include "database.hh"
#include <boost/lexical_cast.hpp>
namespace api {

struct http_context {
    http_server_control http_server;
    distributed<database>& db;
    http_context(distributed<database>& _db) : db(_db) {}
};

future<> set_server(http_context& ctx);

template<class T>
std::vector<sstring> container_to_vec(const T& container) {
    std::vector<sstring> res;
    for (auto i : container) {
        res.push_back(boost::lexical_cast<std::string>(i));
    }
    return res;
}

}

#endif /* API_API_HH_ */
