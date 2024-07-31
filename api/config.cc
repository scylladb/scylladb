/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "api/api.hh"
#include "api/config.hh"
#include "api/api-doc/config.json.hh"
#include "api/api-doc/storage_proxy.json.hh"
#include "api/api-doc/storage_service.json.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include <sstream>
#include <boost/algorithm/string/replace.hpp>
#include <seastar/http/exception.hh>

namespace api {
using namespace seastar::httpd;
namespace sp = httpd::storage_proxy_json;
namespace ss = httpd::storage_service_json;

template<class T>
json::json_return_type get_json_return_type(const T& val) {
    return json::json_return_type(val);
}

/*
 * As commented on db::seed_provider_type is not used
 * and probably never will.
 *
 * Just in case, we will return its name
 */
template<>
json::json_return_type get_json_return_type(const db::seed_provider_type& val) {
    return json::json_return_type(val.class_name);
}

std::string_view format_type(std::string_view type) {
    if (type == "int") {
        return "integer";
    }
    return type;
}

future<> get_config_swagger_entry(std::string_view name, const std::string& description, std::string_view type, bool& first, output_stream<char>& os) {
    std::stringstream ss;
    if (first) {
        first=false;
    } else {
        ss <<',';
    };
    ss << "\"/v2/config/" << name <<"\": {"
      "\"get\": {"
        "\"description\": \"" << boost::replace_all_copy(boost::replace_all_copy(boost::replace_all_copy(description,"\n","\\n"),"\"", "''"), "\t", " ") <<"\","
        "\"operationId\": \"find_config_"<< name <<"\","
        "\"produces\": ["
          "\"application/json\""
        "],"
        "\"tags\": [\"config\"],"
        "\"parameters\": ["
        "],"
        "\"responses\": {"
          "\"200\": {"
            "\"description\": \"Config value\","
             "\"schema\": {"
               "\"type\": \"" << format_type(type) << "\""
             "}"
          "},"
          "\"default\": {"
            "\"description\": \"unexpected error\","
            "\"schema\": {"
              "\"$ref\": \"#/definitions/ErrorModel\""
            "}"
          "}"
        "}"
      "}"
    "}";
    return os.write(ss.str());
}

namespace cs = httpd::config_json;

void set_config(std::shared_ptr < api_registry_builder20 > rb, http_context& ctx, routes& r, const db::config& cfg, bool first) {
    rb->register_function(r, [&cfg, first] (output_stream<char>& os) {
        return do_with(first, [&os, &cfg] (bool& first) {
            auto f = make_ready_future();
            for (auto&& cfg_ref : cfg.values()) {
                auto&& cfg = cfg_ref.get();
                f = f.then([&os, &first, &cfg] {
                    return get_config_swagger_entry(cfg.name(), std::string(cfg.desc()), cfg.type_name(), first, os);
                });
            }
            return f;
        });
    });

    cs::find_config_id.set(r, [&cfg] (const_req r) {
        auto id = r.get_path_param("id");
        for (auto&& cfg_ref : cfg.values()) {
            auto&& cfg = cfg_ref.get();
            if (id == cfg.name()) {
                return cfg.value_as_json();
            }
        }
        throw bad_param_exception(sstring("No such config entry: ") + id);
    });

    sp::get_rpc_timeout.set(r, [&cfg](const_req req)  {
        return cfg.request_timeout_in_ms()/1000.0;
    });

    sp::set_rpc_timeout.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(seastar::json::json_void());
    });

    sp::get_read_rpc_timeout.set(r, [&cfg](const_req req)  {
        return cfg.read_request_timeout_in_ms()/1000.0;
    });

    sp::set_read_rpc_timeout.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(seastar::json::json_void());
    });

    sp::get_write_rpc_timeout.set(r, [&cfg](const_req req)  {
        return cfg.write_request_timeout_in_ms()/1000.0;
    });

    sp::set_write_rpc_timeout.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(seastar::json::json_void());
    });

    sp::get_counter_write_rpc_timeout.set(r, [&cfg](const_req req)  {
        return cfg.counter_write_request_timeout_in_ms()/1000.0;
    });

    sp::set_counter_write_rpc_timeout.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(seastar::json::json_void());
    });

    sp::get_cas_contention_timeout.set(r, [&cfg](const_req req)  {
        return cfg.cas_contention_timeout_in_ms()/1000.0;
    });

    sp::set_cas_contention_timeout.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(seastar::json::json_void());
    });

    sp::get_range_rpc_timeout.set(r, [&cfg](const_req req)  {
        return cfg.range_request_timeout_in_ms()/1000.0;
    });

    sp::set_range_rpc_timeout.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(seastar::json::json_void());
    });

    sp::get_truncate_rpc_timeout.set(r, [&cfg](const_req req)  {
        return cfg.truncate_request_timeout_in_ms()/1000.0;
    });

    sp::set_truncate_rpc_timeout.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(seastar::json::json_void());
    });

    ss::get_all_data_file_locations.set(r, [&cfg](const_req req) {
        return container_to_vec(cfg.data_file_directories());
    });

    ss::get_saved_caches_location.set(r, [&cfg](const_req req) {
        return cfg.saved_caches_directory();
    });

}

void unset_config(http_context& ctx, routes& r) {
    cs::find_config_id.unset(r);
    sp::get_rpc_timeout.unset(r);
    sp::set_rpc_timeout.unset(r);
    sp::get_read_rpc_timeout.unset(r);
    sp::set_read_rpc_timeout.unset(r);
    sp::get_write_rpc_timeout.unset(r);
    sp::set_write_rpc_timeout.unset(r);
    sp::get_counter_write_rpc_timeout.unset(r);
    sp::set_counter_write_rpc_timeout.unset(r);
    sp::get_cas_contention_timeout.unset(r);
    sp::set_cas_contention_timeout.unset(r);
    sp::get_range_rpc_timeout.unset(r);
    sp::set_range_rpc_timeout.unset(r);
    sp::get_truncate_rpc_timeout.unset(r);
    sp::set_truncate_rpc_timeout.unset(r);
    ss::get_all_data_file_locations.unset(r);
    ss::get_saved_caches_location.unset(r);
}

}

