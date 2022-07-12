/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "compaction_manager.hh"
#include "compaction/compaction_manager.hh"
#include "api/api-doc/compaction_manager.json.hh"
#include "db/system_keyspace.hh"
#include "column_family.hh"
#include "unimplemented.hh"
#include "storage_service.hh"

#include <utility>

namespace api {

namespace cm = httpd::compaction_manager_json;
using namespace json;

static future<json::json_return_type> get_cm_stats(http_context& ctx,
        int64_t compaction_manager::stats::*f) {
    return ctx.db.map_reduce0([f](replica::database& db) {
        return db.get_compaction_manager().get_stats().*f;
    }, int64_t(0), std::plus<int64_t>()).then([](const int64_t& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}
static std::unordered_map<std::pair<sstring, sstring>, uint64_t, utils::tuple_hash> sum_pending_tasks(std::unordered_map<std::pair<sstring, sstring>, uint64_t, utils::tuple_hash>&& a,
        const std::unordered_map<std::pair<sstring, sstring>, uint64_t, utils::tuple_hash>& b) {
    for (auto&& i : b) {
        if (i.second) {
            a[i.first] += i.second;
        }
    }
    return std::move(a);
}


void set_compaction_manager(http_context& ctx, routes& r) {
    cm::get_compactions.set(r, [&ctx] (std::unique_ptr<request> req) {
        return ctx.db.map_reduce0([](replica::database& db) {
            std::vector<cm::summary> summaries;
            const compaction_manager& cm = db.get_compaction_manager();

            for (const auto& c : cm.get_compactions()) {
                cm::summary s;
                s.id = c.compaction_uuid.to_sstring();
                s.ks = c.ks_name;
                s.cf = c.cf_name;
                s.unit = "keys";
                s.task_type = sstables::compaction_name(c.type);
                s.completed = c.total_keys_written;
                s.total = c.total_partitions;
                summaries.push_back(std::move(s));
            }
            return summaries;
        }, std::vector<cm::summary>(), concat<cm::summary>).then([](const std::vector<cm::summary>& res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cm::get_pending_tasks_by_table.set(r, [&ctx] (std::unique_ptr<request> req) {
        return ctx.db.map_reduce0([&ctx](replica::database& db) {
            return do_with(std::unordered_map<std::pair<sstring, sstring>, uint64_t, utils::tuple_hash>(), [&ctx, &db](std::unordered_map<std::pair<sstring, sstring>, uint64_t, utils::tuple_hash>& tasks) {
                return do_for_each(db.get_column_families(), [&tasks](const std::pair<utils::UUID, seastar::lw_shared_ptr<replica::table>>& i) {
                    replica::table& cf = *i.second.get();
                    tasks[std::make_pair(cf.schema()->ks_name(), cf.schema()->cf_name())] = cf.get_compaction_strategy().estimated_pending_compactions(cf.as_table_state());
                    return make_ready_future<>();
                }).then([&tasks] {
                    return std::move(tasks);
                });
            });
        }, std::unordered_map<std::pair<sstring, sstring>, uint64_t, utils::tuple_hash>(), sum_pending_tasks).then(
                [](const std::unordered_map<std::pair<sstring, sstring>, uint64_t, utils::tuple_hash>& task_map) {
            std::vector<cm::pending_compaction> res;
            res.reserve(task_map.size());
            for (auto i : task_map) {
                cm::pending_compaction task;
                task.ks = i.first.first;
                task.cf = i.first.second;
                task.task = i.second;
                res.emplace_back(std::move(task));
            }
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cm::force_user_defined_compaction.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        // FIXME
        warn(unimplemented::cause::API);
        return make_ready_future<json::json_return_type>(json_void());
    });

    cm::stop_compaction.set(r, [&ctx] (std::unique_ptr<request> req) {
        auto type = req->get_query_param("type");
        return ctx.db.invoke_on_all([type] (replica::database& db) {
            auto& cm = db.get_compaction_manager();
            return cm.stop_compaction(type);
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    cm::stop_keyspace_compaction.set(r, [&ctx] (std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto ks_name = validate_keyspace(ctx, req->param);
        auto table_names = parse_tables(ks_name, ctx, req->query_parameters, "tables");
        if (table_names.empty()) {
            table_names = map_keys(ctx.db.local().find_keyspace(ks_name).metadata().get()->cf_meta_data());
        }
        auto type = req->get_query_param("type");
        co_await ctx.db.invoke_on_all([&ks_name, &table_names, type] (replica::database& db) {
            auto& cm = db.get_compaction_manager();
            return parallel_for_each(table_names, [&db, &cm, &ks_name, type] (sstring& table_name) {
                auto& t = db.find_column_family(ks_name, table_name);
                return cm.stop_compaction(type, &t.as_table_state());
            });
        });
        co_return json_void();
    });

    cm::get_pending_tasks.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, int64_t(0), [](replica::column_family& cf) {
            return cf.get_compaction_strategy().estimated_pending_compactions(cf.as_table_state());
        }, std::plus<int64_t>());
    });

    cm::get_completed_tasks.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cm_stats(ctx, &compaction_manager::stats::completed_tasks);
    });

    cm::get_total_compactions_completed.set(r, [] (std::unique_ptr<request> req) {
        // FIXME
        // We are currently dont have an API for compaction
        // so returning a 0 as the number of total compaction is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cm::get_bytes_compacted.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        // FIXME
        warn(unimplemented::cause::API);
        return make_ready_future<json::json_return_type>(0);
    });

    cm::get_compaction_history.set(r, [] (std::unique_ptr<request> req) {
        std::function<future<>(output_stream<char>&&)> f = [](output_stream<char>&& s) {
            return do_with(output_stream<char>(std::move(s)), true, [] (output_stream<char>& s, bool& first){
                return s.write("[").then([&s, &first] {
                    return db::system_keyspace::get_compaction_history([&s, &first](const db::system_keyspace::compaction_history_entry& entry) mutable {
                        cm::history h;
                        h.id = entry.id.to_sstring();
                        h.ks = std::move(entry.ks);
                        h.cf = std::move(entry.cf);
                        h.compacted_at = entry.compacted_at;
                        h.bytes_in = entry.bytes_in;
                        h.bytes_out =  entry.bytes_out;
                        for (auto it : entry.rows_merged) {
                            httpd::compaction_manager_json::row_merged e;
                            e.key = it.first;
                            e.value = it.second;
                            h.rows_merged.push(std::move(e));
                        }
                        auto fut = first ? make_ready_future<>() : s.write(", ");
                        first = false;
                        return fut.then([&s, h = std::move(h)] {
                            return formatter::write(s, h);
                        });
                    }).then([&s] {
                        return s.write("]").then([&s] {
                            return s.close();
                        });
                    });
                });
            });
        };
        return make_ready_future<json::json_return_type>(std::move(f));
    });

    cm::get_compaction_info.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        // FIXME
        warn(unimplemented::cause::API);
        std::vector<cm::compaction_info> res;
        return make_ready_future<json::json_return_type>(res);
    });

}

}

