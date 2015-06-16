/*
 * Copyright 2015 Cloudius Systems
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include <service/storage_service.hh>
#include <db/commitlog/commitlog.hh>

namespace api {

void set_storage_service(http_context& ctx, routes& r) {
    httpd::storage_service_json::local_hostid.set(r, [](const_req req) {
        return "";
    });

    httpd::storage_service_json::get_tokens.set(r, [](std::unique_ptr<request> req) {
        return service::sorted_tokens().then([](const std::vector<dht::token>& tokens) {
            return make_ready_future<json::json_return_type>(container_to_vec(tokens));
        });
    });

    httpd::storage_service_json::get_node_tokens.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address addr(req->param["endpoint"]);
        return service::get_tokens(addr).then([](const std::vector<dht::token>& tokens) {
            return make_ready_future<json::json_return_type>(container_to_vec(tokens));
        });
    });

    httpd::storage_service_json::get_commitlog.set(r, [&ctx](const_req req) {
        return ctx.db.local().commitlog()->active_config().commit_log_location;
    });

    httpd::storage_service_json::get_token_endpoint.set(r, [](std::unique_ptr<request> req) {
        return service::get_token_to_endpoint().then([] (const std::map<dht::token, gms::inet_address>& tokens){
            std::vector<storage_service_json::mapper> res;
            for (auto i : tokens) {
                ss::mapper val;
                val.key = boost::lexical_cast<std::string>(i.first);
                val.value = boost::lexical_cast<std::string>(i.second);
                res.push_back(val);
            }
            return make_ready_future<json::json_return_type>(res);
        });
    });
}

}
