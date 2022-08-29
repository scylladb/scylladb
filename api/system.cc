/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "api/api-doc/system.json.hh"
#include "api/api.hh"

#include <seastar/core/reactor.hh>
#include <seastar/http/exception.hh>
#include "log.hh"
#include "replica/database.hh"

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

    hs::write_log_message.set(r, [](const_req req) {
        try {
            logging::log_level level = boost::lexical_cast<logging::log_level>(std::string(req.get_query_param("level")));
            apilog.log(level, "/system/log: {}", std::string(req.get_query_param("message")));
        } catch (boost::bad_lexical_cast& e) {
            throw bad_param_exception("Unknown logging level " + req.get_query_param("level"));
        }
        return json::json_void();
    });

    hs::drop_sstable_caches.set(r, [&ctx](std::unique_ptr<request> req) {
        apilog.info("Dropping sstable caches");
        return ctx.db.invoke_on_all([] (replica::database& db) {
            return db.drop_caches();
        }).then([] {
            apilog.info("Caches dropped");
            return json::json_return_type(json::json_void());
        });
    });
}

}
