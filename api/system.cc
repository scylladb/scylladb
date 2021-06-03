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

#include "api/api-doc/system.json.hh"
#include "api/api.hh"

#include <seastar/core/reactor.hh>
#include <seastar/http/exception.hh>
#include "log.hh"
#include "database.hh"

extern logging::logger apilog;

namespace api {

namespace hs = httpd::system_json;

void set_system(http_context& ctx, routes& r) {
    hs::get_system_uptime.set(r, [](const_req req) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(engine().uptime()).count();
    });

    hs::get_all_logger_names.set(r, [](const_req req) {
        return logging::logger_registry().get_all_logger_names();
    });

    hs::set_all_logger_level.set(r, [](const_req req) {
        try {
            logging::log_level level = boost::lexical_cast<logging::log_level>(std::string(req.get_query_param("level")));
            logging::logger_registry().set_all_loggers_level(level);
        } catch (boost::bad_lexical_cast& e) {
            throw bad_param_exception("Unknown logging level " + req.get_query_param("level"));
        }
        return json::json_void();
    });

    hs::get_logger_level.set(r, [](const_req req) {
        try {
            return logging::level_name(logging::logger_registry().get_logger_level(req.param["name"]));
        } catch (std::out_of_range& e) {
            throw bad_param_exception("Unknown logger name " + req.param["name"]);
        }
        // just to keep the compiler happy
        return sstring();
    });

    hs::set_logger_level.set(r, [](const_req req) {
        try {
            logging::log_level level = boost::lexical_cast<logging::log_level>(std::string(req.get_query_param("level")));
            logging::logger_registry().set_logger_level(req.param["name"], level);
        } catch (std::out_of_range& e) {
            throw bad_param_exception("Unknown logger name " + req.param["name"]);
        } catch (boost::bad_lexical_cast& e) {
            throw bad_param_exception("Unknown logging level " + req.get_query_param("level"));
        }
        return json::json_void();
    });

    hs::drop_sstable_caches.set(r, [&ctx](std::unique_ptr<request> req) {
        apilog.info("Dropping sstable caches");
        return ctx.db.invoke_on_all([] (database& db) {
            return db.drop_caches();
        }).then([] {
            apilog.info("Caches dropped");
            return json::json_return_type(json::json_void());
        });
    });
}

}
