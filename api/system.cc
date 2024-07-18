/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "api/api_init.hh"
#include "api/api-doc/system.json.hh"
#include "api/api-doc/metrics.json.hh"
#include "replica/database.hh"
#include "sstables/sstables_manager.hh"

#include <rapidjson/document.h>
#include <seastar/core/reactor.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/relabel_config.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/http/short_streams.hh>

#include "log.hh"

extern logging::logger apilog;

namespace api {
using namespace seastar::httpd;

namespace hs = httpd::system_json;
namespace hm = httpd::metrics_json;

extern "C" void __attribute__((weak)) __llvm_profile_dump();
extern "C" const char * __attribute__((weak)) __llvm_profile_get_filename();
extern "C" void __attribute__((weak)) __llvm_profile_reset_counters();

void set_system(http_context& ctx, routes& r) {
    hm::get_metrics_config.set(r, [](const_req req) {
        std::vector<hm::metrics_config> res;
        res.resize(seastar::metrics::get_relabel_configs().size());
        size_t i = 0;
        for (auto&& r : seastar::metrics::get_relabel_configs()) {
            res[i].action = r.action;
            res[i].target_label = r.target_label;
            res[i].replacement = r.replacement;
            res[i].separator = r.separator;
            res[i].source_labels = r.source_labels;
            res[i].regex = r.expr.str();
            i++;
        }
        return res;
    });

    hm::set_metrics_config.set(r, [](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        rapidjson::Document doc;
        doc.Parse(req->content.c_str());
        if (!doc.IsArray()) {
            throw bad_param_exception("Expected a json array");
        }
        std::vector<seastar::metrics::relabel_config> relabels;
        relabels.resize(doc.Size());
        for (rapidjson::SizeType i = 0; i < doc.Size(); i++) {
            const auto& element = doc[i];
            if (element.HasMember("source_labels")) {
                std::vector<std::string> source_labels;
                source_labels.resize(element["source_labels"].Size());

                for (size_t j = 0; j < element["source_labels"].Size(); j++) {
                    source_labels[j] = element["source_labels"][j].GetString();
                }
                relabels[i].source_labels = source_labels;
            }
            if (element.HasMember("action")) {
                relabels[i].action = seastar::metrics::relabel_config_action(element["action"].GetString());
            }
            if (element.HasMember("replacement")) {
                relabels[i].replacement = element["replacement"].GetString();
            }
            if (element.HasMember("separator")) {
                relabels[i].separator = element["separator"].GetString();
            }
            if (element.HasMember("target_label")) {
                relabels[i].target_label = element["target_label"].GetString();
            }
            if (element.HasMember("regex")) {
                relabels[i].expr = element["regex"].GetString();
            }
        }
        return do_with(std::move(relabels), false, [](const std::vector<seastar::metrics::relabel_config>& relabels, bool& failed) {
            return smp::invoke_on_all([&relabels, &failed] {
                return metrics::set_relabel_configs(relabels).then([&failed](const metrics::metric_relabeling_result& result) {
                    if (result.metrics_relabeled_due_to_collision > 0) {
                        failed = true;
                    }
                    return;
                });
            }).then([&failed](){
                if (failed) {
                    throw bad_param_exception("conflicts found during relabeling");
                }
                return make_ready_future<json::json_return_type>(seastar::json::json_void());
            });
        });
    });

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
            return logging::level_name(logging::logger_registry().get_logger_level(req.get_path_param("name")));
        } catch (std::out_of_range& e) {
            throw bad_param_exception("Unknown logger name " + req.get_path_param("name"));
        }
        // just to keep the compiler happy
        return sstring();
    });

    hs::set_logger_level.set(r, [](const_req req) {
        try {
            logging::log_level level = boost::lexical_cast<logging::log_level>(std::string(req.get_query_param("level")));
            logging::logger_registry().set_logger_level(req.get_path_param("name"), level);
        } catch (std::out_of_range& e) {
            throw bad_param_exception("Unknown logger name " + req.get_path_param("name"));
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

    hs::dump_profile.set(r, [](std::unique_ptr<request> req) {
        if (!__llvm_profile_dump) {
            apilog.info("Profile will not be dumped, executable is not instrumented with profile dumping.");
            return make_ready_future<json::json_return_type>(json::json_return_type(json::json_void()));
        }
        sstring profile_dest(__llvm_profile_get_filename ? __llvm_profile_get_filename() : "disk");
        apilog.info("Dumping profile to {}", profile_dest);
        __llvm_profile_dump();
        if (__llvm_profile_reset_counters) {
            // If counters are not reset the profile dumping mechanism will issue a warning and exit
            // next time it is attempted. If the counters are reset, profiles can be accumulated
            // (if %m is present in LLVM_PROFILE_FILE pattern) so it can be dumped in stages or
            // multiple times during runtime.
            __llvm_profile_reset_counters();
        } else {
            apilog.warn("Could not reset profile counters, profile dumping will be skipped next time it is attempted");
        }
        apilog.info("Profile dumped to {}", profile_dest);
        return make_ready_future<json::json_return_type>(json::json_return_type(json::json_void()));
    }) ;

    hs::get_highest_supported_sstable_version.set(r, [&ctx] (const_req req) {
        auto& table = ctx.db.local().find_column_family("system", "local");
        return seastar::to_sstring(table.get_sstables_manager().get_highest_supported_format());
    });
}

}
