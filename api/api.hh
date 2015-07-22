/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "http/httpd.hh"
#include "json/json_elements.hh"
#include "database.hh"
#include "service/storage_proxy.hh"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

namespace api {

struct http_context {
    sstring api_dir;
    http_server_control http_server;
    distributed<database>& db;
    distributed<service::storage_proxy>& sp;
    http_context(distributed<database>& _db, distributed<service::storage_proxy>&
            _sp) : db(_db), sp(_sp) {}
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

template<class T>
std::vector<T> map_to_key_value(const std::map<sstring, sstring>& map) {
    std::vector<T> res;
    for (auto i : map) {
        res.push_back(T());
        res.back().key = i.first;
        res.back().value = i.second;
    }
    return res;
}

template<class T, class MAP>
std::vector<T>& map_to_key_value(const MAP& map, std::vector<T>& res) {
    for (auto i : map) {
        T val;
        val.key = boost::lexical_cast<std::string>(i.first);
        val.value = boost::lexical_cast<std::string>(i.second);
        res.push_back(val);
    }
    return res;
}
template <typename T, typename S = T>
T map_sum(T&& dest, const S& src) {
    for (auto i : src) {
        dest[i.first] += i.second;
    }
    return dest;
}

template <typename MAP>
std::vector<sstring> map_keys(const MAP& map) {
    std::vector<sstring> res;
    for (const auto& i : map) {
        res.push_back(boost::lexical_cast<std::string>(i.first));
    }
    return res;
}

/**
 * General sstring splitting function
 */
inline std::vector<sstring> split(const sstring& text, const char* separator) {
    if (text == "") {
        return std::vector<sstring>();
    }
    std::vector<sstring> tokens;
    return boost::split(tokens, text, boost::is_any_of(separator));
}

/**
 * Split a column family parameter
 */
inline std::vector<sstring> split_cf(const sstring& cf) {
    return split(cf, ",");
}

/**
 * A helper function to sum values on an a distributed object that
 * has a get_stats method.
 *
 */
template<class T, class F>
future<json::json_return_type>  sum_stats(distributed<T>& d, uint64_t F::*f) {
    return d.map_reduce0([f](const T& p) {return p.get_stats().*f;}, 0,
            std::plus<uint64_t>()).then([](uint64_t val) {
        return make_ready_future<json::json_return_type>(val);
    });
}

}
