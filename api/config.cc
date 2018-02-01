/*
 * Copyright 2018 ScyllaDB
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

std::string format_type(const std::string& type) {
    if (type == "int") {
        return "integer";
    }
    return type;
}

future<> get_config_swagger_entry(const std::string& name, const std::string& description, const std::string& type, bool& first, output_stream<char>& os) {
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
#define _get_config_value(name, type, deflt, status, desc, ...) if (id == #name) {return get_json_return_type(ctx.db.local().get_config().name());}


#define _get_config_description(name, type, deflt, status, desc, ...) f = f.then([&os, &first] {return get_config_swagger_entry(#name, desc, #type, first, os);});

void set_config(std::shared_ptr < api_registry_builder20 > rb, http_context& ctx, routes& r) {
    rb->register_function(r, [] (output_stream<char>& os) {
        return do_with(true, [&os] (bool& first) {
            auto f = make_ready_future();
            _make_config_values(_get_config_description)
            return f;
        });
    });

    cs::find_config_id.set(r, [&ctx] (const_req r) {
        auto id = r.param["id"];
        _make_config_values(_get_config_value)
        throw bad_param_exception(sstring("No such config entry: ") + id);
    });
}

}

