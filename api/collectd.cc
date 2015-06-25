/*
 * Copyright 2015 Cloudius Systems
 */

#include "collectd.hh"
#include "api/api-doc/collectd.json.hh"
#include "core/scollectd.hh"
#include "core/scollectd_api.hh"
#include "endian.h"
#include <boost/range/irange.hpp>

namespace api {

using namespace scollectd;
namespace cd = httpd::collectd_json;

static auto transformer(const std::vector<collectd_value>& values) {
    cd::collectd_value collected_value;
    for (auto v: values) {
        switch (v._type) {
        case scollectd::data_type::GAUGE:
            collected_value.values.push(v.u._d);
            break;
        case scollectd::data_type::DERIVE:
            collected_value.values.push(v.u._i);
            break;
        default:
            collected_value.values.push(v.u._ui);
            break;
        }
    }
    return collected_value;
}

void set_collectd(http_context& ctx, routes& r) {
    cd::get_collectd.set(r, [&ctx](std::unique_ptr<request> req) {

        auto id = make_shared<scollectd::type_instance_id>(req->param["pluginid"],
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
        std::vector<cd::type_instance_id> res;
        auto ids = scollectd::get_collectd_ids();
        for (auto i: ids) {
            cd::type_instance_id id;
            id.plugin = i.plugin();
            id.plugin_instance = i.plugin_instance();
            id.type = i.type();
            id.type_instance = i.type_instance();
            res.push_back(id);
        }
        return res;
    });
}

}

