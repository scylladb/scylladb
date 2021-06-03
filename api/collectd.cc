/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "collectd.hh"
#include "api/api-doc/collectd.json.hh"
#include <seastar/core/scollectd.hh>
#include <seastar/core/scollectd_api.hh>
#include "endian.h"
#include <boost/range/irange.hpp>
#include <regex>

namespace api {

using namespace scollectd;
using namespace httpd;

using namespace json;
namespace cd = httpd::collectd_json;

static auto transformer(const std::vector<collectd_value>& values) {
    cd::collectd_value collected_value;
    for (auto v: values) {
        switch (v._type) {
        case scollectd::data_type::GAUGE:
            collected_value.values.push(v.d());
            break;
        case scollectd::data_type::DERIVE:
            collected_value.values.push(v.i());
            break;
        default:
            collected_value.values.push(v.ui());
            break;
        }
    }
    return collected_value;
}


static const char* str_to_regex(const sstring& v) {
    if (v != "") {
        return v.c_str();
    }
    return ".*";
}

void set_collectd(http_context& ctx, routes& r) {
    cd::get_collectd.set(r, [&ctx](std::unique_ptr<request> req) {

        auto id = ::make_shared<scollectd::type_instance_id>(req->param["pluginid"],
                req->get_query_param("instance"), req->get_query_param("type"),
                req->get_query_param("type_instance"));


        return do_with(std::vector<cd::collectd_value>(), [id] (auto& vec) {
            vec.resize(smp::count);
            return parallel_for_each(boost::irange(0u, smp::count), [&vec, id] (auto cpu) {
                return smp::submit_to(cpu, [id = *id] {
                    return scollectd::get_collectd_value(id);
                }).then([&vec, cpu] (auto res) {
                    vec[cpu] = transformer(res);
                });
            }).then([&vec] {
                return make_ready_future<json::json_return_type>(vec);
            });
        });
    });

    cd::get_collectd_items.set(r, [](const_req req) {
        std::vector<cd::collectd_metric_status> res;
        auto ids = scollectd::get_collectd_ids();
        for (auto i: ids) {
            cd::type_instance_id id;
            id.plugin = i.plugin();
            id.plugin_instance = i.plugin_instance();
            id.type = i.type();
            id.type_instance = i.type_instance();
            cd::collectd_metric_status it;
            it.id = id;
            it.enable = scollectd::is_enabled(i);
            res.push_back(it);
        }
        return res;
    });

    cd::enable_collectd.set(r, [](std::unique_ptr<request> req) -> future<json::json_return_type> {
        std::regex plugin(req->param["pluginid"].c_str());
        std::regex instance(str_to_regex(req->get_query_param("instance")));
        std::regex type(str_to_regex(req->get_query_param("type")));
        std::regex type_instance(str_to_regex(req->get_query_param("type_instance")));
        bool enable = strcasecmp(req->get_query_param("enable").c_str(), "true") == 0;
        return smp::invoke_on_all([enable, plugin, instance, type, type_instance]() {
            for (auto id: scollectd::get_collectd_ids()) {
                if (std::regex_match(std::string(id.plugin()), plugin) &&
                        std::regex_match(std::string(id.plugin_instance()), instance) &&
                        std::regex_match(std::string(id.type()), type) &&
                        std::regex_match(std::string(id.type_instance()), type_instance)) {
                    scollectd::enable(id, enable);
                }
            }
        }).then([] {
            return json::json_return_type(json_void());
        });
    });

    cd::enable_all_collectd.set(r, [](std::unique_ptr<request> req) -> future<json::json_return_type> {
        bool enable = strcasecmp(req->get_query_param("enable").c_str(), "true") == 0;
        return smp::invoke_on_all([enable] {
            for (auto id: scollectd::get_collectd_ids()) {
                scollectd::enable(id, enable);
            }
        }).then([] {
            return json::json_return_type(json_void());
        });
    });
}

}

