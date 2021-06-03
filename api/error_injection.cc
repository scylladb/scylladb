/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "api/api-doc/error_injection.json.hh"
#include "api/api.hh"

#include <seastar/http/exception.hh>
#include "log.hh"
#include "utils/error_injection.hh"
#include "seastar/core/future-util.hh"

namespace api {

namespace hf = httpd::error_injection_json;

void set_error_injection(http_context& ctx, routes& r) {

    hf::enable_injection.set(r, [](std::unique_ptr<request> req) {
        sstring injection = req->param["injection"];
        bool one_shot = req->get_query_param("one_shot") == "True";
        auto& errinj = utils::get_local_injector();
        return errinj.enable_on_all(injection, one_shot).then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });

    hf::get_enabled_injections_on_all.set(r, [](std::unique_ptr<request> req) {
        auto& errinj = utils::get_local_injector();
        auto ret = errinj.enabled_injections_on_all();
        return make_ready_future<json::json_return_type>(ret);
    });

    hf::disable_injection.set(r, [](std::unique_ptr<request> req) {
        sstring injection = req->param["injection"];

        auto& errinj = utils::get_local_injector();
        return errinj.disable_on_all(injection).then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });

    hf::disable_on_all.set(r, [](std::unique_ptr<request> req) {
        auto& errinj = utils::get_local_injector();
        return errinj.disable_on_all().then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });

}

} // namespace api
