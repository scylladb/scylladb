/*
 * Copyright 2015 Cloudius Systems
 */

#include "gossiper.hh"
#include "api/api-doc/gossiper.json.hh"
#include <gms/gossiper.hh>
#include <vector>
#include <sstream>

namespace api {

template<class T>
std::vector<sstring> addr_to_vec(const T& container) {
    auto res = std::vector<sstring>(container.size());
    for (auto i : container) {
        std::stringstream ss;
        ss << i;
        res.push_back(ss.str());
    }
    return res;
}

void set_gossiper(http_context& ctx, routes& r) {
    httpd::gossiper_json::get_down_endpoint.set(r, [](std::unique_ptr<request> req) {
        return gms::get_unreachable_members().then([](std::set<gms::inet_address> res) {
            return make_ready_future<json::json_return_type>(addr_to_vec(res));
        });
    });

    httpd::gossiper_json::get_live_endpoint.set(r, [](std::unique_ptr<request> req) {
        return gms::get_live_members().then([](std::set<gms::inet_address> res) {
            return make_ready_future<json::json_return_type>(addr_to_vec(res));
        });
    });

    httpd::gossiper_json::get_endpoint_downtime.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address ep(req->param.at("addr").substr(1));
        return gms::get_endpoint_downtime(ep).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    httpd::gossiper_json::get_current_generation_number.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address ep(req->param.at("addr").substr(1));
        return gms::get_current_generation_number(ep).then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    httpd::gossiper_json::assassinate_endpoint.set(r, [](std::unique_ptr<request> req) {
        if (req->get_query_param("unsafe") != "True") {
            return gms::assassinate_endpoint(req->param.at("addr").substr(1)).then([] {
                    return make_ready_future<json::json_return_type>("");
            });
        }
        return gms::unsafe_assassinate_endpoint(req->param.at("addr").substr(1)).then([] {
                return make_ready_future<json::json_return_type>("");
        });
    });
}

}
