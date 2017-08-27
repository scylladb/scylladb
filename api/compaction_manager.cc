/*
 * Copyright (C) 2015 ScyllaDB
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

#include "compaction_manager.hh"
#include "sstables/compaction_manager.hh"
#include "api/api-doc/compaction_manager.json.hh"
#include "db/system_keyspace.hh"
#include "column_family.hh"

namespace api {

namespace cm = httpd::compaction_manager_json;
using namespace json;

static future<json::json_return_type> get_cm_stats(http_context& ctx,
        int64_t compaction_manager::stats::*f) {
    return ctx.db.map_reduce0([f](database& db) {
        return db.get_compaction_manager().get_stats().*f;
    }, int64_t(0), std::plus<int64_t>()).then([](const int64_t& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

void set_compaction_manager(http_context& ctx, routes& r) {
    cm::get_compactions.set(r, [&ctx] (std::unique_ptr<request> req) {
        return ctx.db.map_reduce0([](database& db) {
            std::vector<cm::summary> summaries;
            const compaction_manager& cm = db.get_compaction_manager();

            for (const auto& c : cm.get_compactions()) {
                cm::summary s;
                s.ks = c->ks;
                s.cf = c->cf;
                s.unit = "keys";
                s.task_type = sstables::compaction_name(c->type);
                s.completed = c->total_keys_written;
                s.total = c->total_partitions;
                summaries.push_back(std::move(s));
            }
            return summaries;
        }, std::vector<cm::summary>(), concat<cm::summary>).then([](const std::vector<cm::summary>& res) {
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
        return ctx.db.invoke_on_all([type] (database& db) {
            auto& cm = db.get_compaction_manager();
            cm.stop_compaction(type);
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    cm::get_pending_tasks.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, int64_t(0), [](column_family& cf) {
            return cf.get_compaction_strategy().estimated_pending_compactions(cf);
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
        return db::system_keyspace::get_compaction_history().then([] (std::vector<db::system_keyspace::compaction_history_entry> history) {
            std::vector<cm::history> res;
            res.reserve(history.size());

            for (auto& entry : history) {
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
                res.push_back(std::move(h));
            }

            return make_ready_future<json::json_return_type>(res);
        });
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

