/*
 * Copyright 2015 Cloudius Systems
 */

#include "column_family.hh"
#include "api/api-doc/column_family.json.hh"
#include <vector>

namespace api {

using namespace std;
namespace cf = httpd::column_family_json;

auto get_uuid(const sstring& name, const database& db) {
    auto pos = name.find(':');
    return db.find_uuid(name.substr(0, pos), name.substr(pos + 1));
}
void set_column_family(http_context& ctx, routes& r) {
    cf::get_column_family_name.set(r, [&ctx] (const_req req){
        vector<sstring> res;
        for (auto i: ctx.db.local().get_column_families_mapping()) {
            res.push_back(i.first.first + ":" + i.first.second);
        }
        return res;
    });

    cf::get_column_family.set(r, [&ctx] (const_req req){
            vector<cf::column_family_info> res;
            for (auto i: ctx.db.local().get_column_families_mapping()) {
                cf::column_family_info info;
                info.ks = i.first.first;
                info.cf =  i.first.second;
                info.type = "ColumnFamilies";
                res.push_back(info);
            }
            return res;
        });

    cf::get_column_family_name_keyspace.set(r, [&ctx] (const_req req){
        vector<sstring> res;
        for (auto i = ctx.db.local().get_keyspaces().cbegin(); i!=  ctx.db.local().get_keyspaces().cend(); i++) {
            res.push_back(i->first);
        }
        return res;
    });

    cf::get_memtable_columns_count.set(r, [&ctx] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_columns_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_memtable_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_memtable_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_memtable_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtables_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_all_memtables_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtables_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_all_memtables_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtables_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_all_memtables_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_memtable_switch_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_switch_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_estimated_row_size_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_estimated_column_count_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_compression_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_compression_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_read_latency.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_read_latency.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_range_latency.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_range_latency.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_write_latency.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_write_latency.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_pending_flushes.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_pending_flushes.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_pending_compactions.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_pending_compactions.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_live_ss_table_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_live_ss_table_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_live_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_live_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_total_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_total_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_min_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        ////auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_min_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_max_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_max_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_mean_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_mean_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_bloom_filter_false_positives.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_bloom_filter_false_positives.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_recent_bloom_filter_false_positives.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_recent_bloom_filter_false_positives.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_bloom_filter_false_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_bloom_filter_false_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_recent_bloom_filter_false_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_recent_bloom_filter_false_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_bloom_filter_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_bloom_filter_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_bloom_filter_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_bloom_filter_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_index_summary_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_index_summary_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_compression_metadata_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_compression_metadata_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_speculative_retries.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_speculative_retries.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_key_cache_hit_rate.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_true_snapshots_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_true_snapshots_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_hit_out_of_range.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_row_cache_hit_out_of_range.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_hit.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_row_cache_hit.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_miss.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_row_cache_miss.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_cas_prepare.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_cas_propose.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_cas_commit.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_sstables_per_read_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_tombstone_scanned_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_live_scanned_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_col_update_time_delta_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

}
}
