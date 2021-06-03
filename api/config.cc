/*
 * Copyright 2018-present ScyllaDB
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

#include "api/config.hh"
#include "api/api-doc/config.json.hh"
#include "db/config.hh"
#include "database.hh"
#include <sstream>
#include <boost/algorithm/string/replace.hpp>

namespace api {

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
    ss << "\"/config/" << name <<"\": {"
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

void set_config(std::shared_ptr < api_registry_builder20 > rb, http_context& ctx, routes& r) {
    rb->register_function(r, [&ctx] (output_stream<char>& os) {
        return do_with(true, [&os, &ctx] (bool& first) {
            auto f = make_ready_future();
            for (auto&& cfg_ref : ctx.db.local().get_config().values()) {
                auto&& cfg = cfg_ref.get();
                f = f.then([&os, &first, &cfg] {
                    return get_config_swagger_entry(cfg.name(), std::string(cfg.desc()), cfg.type_name(), first, os);
                });
            }
            return f;
        });
    });

    cs::find_config_id.set(r, [&ctx] (const_req r) {
        auto id = r.param["id"];
        for (auto&& cfg_ref : ctx.db.local().get_config().values()) {
            auto&& cfg = cfg_ref.get();
            if (id == cfg.name()) {
                return cfg.value_as_json();
            }
        }
        throw bad_param_exception(sstring("No such config entry: ") + id);
    });
}

}

