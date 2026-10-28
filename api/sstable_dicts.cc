/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/coroutine.hh>

#include "api/api.hh"
#include "api/api-doc/storage_service.json.hh"
#include "api/sstable_dicts.hh"

#include "gms/feature_service.hh"
#include "replica/database.hh"
#include "sstable_dict_autotrainer.hh"
#include "sstables/dictionary_service.hh"
#include "sstables/sstables_manager.hh"
#include "utils/log.hh"

extern logging::logger apilog;

namespace api {

using namespace seastar::httpd;
using namespace json;
namespace ss = httpd::storage_service_json;

static
future<json::json_return_type>
rest_estimate_compression_ratios(http_context& ctx, sharded<sstables::dictionary_service>& dicts, std::unique_ptr<http::request> req) {
    auto gate_holder = dicts.local().hold_async_gate();
    if (!dicts.local().get_feature_service().sstable_compression_dicts) {
        apilog.warn("estimate_compression_ratios: called before the cluster feature was enabled");
        throw std::runtime_error("estimate_compression_ratios requires all nodes to support the SSTABLE_COMPRESSION_DICTS cluster feature");
    }
    auto ticket = co_await get_units(dicts.local().get_do_sample_sstables_concurrency_limiter(), 1);
    auto ks = get_query_param<sstring>(*req, "keyspace");
    auto cf = get_query_param<sstring>(*req, "cf");
    apilog.debug("estimate_compression_ratios: called with ks={} cf={}", ks, cf);

    auto s = ctx.db.local().find_column_family(ks, cf).schema();

    auto training_sample = co_await dicts.local().do_sample_sstables(s->id(), 4096, 4096);
    auto validation_sample = co_await dicts.local().do_sample_sstables(s->id(), 16*1024, 1024);
    apilog.debug("estimate_compression_ratios: got training sample with {} blocks and validation sample with {}", training_sample.size(), validation_sample.size());

    auto dict = co_await dicts.local().train_dict(std::move(training_sample));
    apilog.debug("estimate_compression_ratios: got dict of size {}", dict.size());

    std::vector<ss::compression_config_result> res;
    auto make_result = [](std::string_view name, int chunk_length_kb, std::string_view dict, int level, float ratio) -> ss::compression_config_result {
        ss::compression_config_result x;
        x.sstable_compression = sstring(name);
        x.chunk_length_in_kb = chunk_length_kb;
        x.dict = sstring(dict);
        x.level = level;
        x.ratio = ratio;
        return x;
    };

    using algorithm = compression_parameters::algorithm;
    for (const auto& algo : {algorithm::lz4_with_dicts, algorithm::zstd_with_dicts}) {
        for (const auto& chunk_size_kb : {1, 4, 16}) {
            std::vector<int> levels;
            if (algo == compressor::algorithm::zstd_with_dicts) {
                for (int i = 1; i <= 5; ++i) {
                    levels.push_back(i);
                }
            } else {
                levels.push_back(1);
            }
            for (auto level : levels) {
                auto algo_name = compression_parameters::algorithm_to_name(algo);
                auto m = std::map<sstring, sstring>{
                    {compression_parameters::CHUNK_LENGTH_KB, std::to_string(chunk_size_kb)},
                    {compression_parameters::SSTABLE_COMPRESSION, sstring(algo_name)},
                };
                if (algo == compressor::algorithm::zstd_with_dicts) {
                    m.insert(decltype(m)::value_type{sstring("compression_level"), sstring(std::to_string(level))});
                }
                auto params = compression_parameters(std::move(m));
                auto ratio_with_no_dict = co_await try_one_compression_config({}, s, params, validation_sample);
                auto ratio_with_past_dict = co_await try_one_compression_config(dicts.local().get_compressor_factory(), s, params, validation_sample);
                auto ratio_with_future_dict = co_await try_one_compression_config(dict, s, params, validation_sample);
                res.push_back(make_result(algo_name, chunk_size_kb, "none", level, ratio_with_no_dict));
                res.push_back(make_result(algo_name, chunk_size_kb, "past", level, ratio_with_past_dict));
                res.push_back(make_result(algo_name, chunk_size_kb, "future", level, ratio_with_future_dict));
            }
        }
    }

    co_return res;
}

static
future<json::json_return_type>
rest_retrain_dict(http_context& ctx, sharded<sstables::dictionary_service>& dicts, service::raft_group0_client& group0_client, std::unique_ptr<http::request> req) {
    auto gate_holder = dicts.local().hold_async_gate();
    if (!dicts.local().get_feature_service().sstable_compression_dicts) {
        apilog.warn("retrain_dict: called before the cluster feature was enabled");
        throw std::runtime_error("retrain_dict requires all nodes to support the SSTABLE_COMPRESSION_DICTS cluster feature");
    }
    auto ticket = co_await get_units(dicts.local().get_do_sample_sstables_concurrency_limiter(), 1);
    auto ks = get_query_param<sstring>(*req, "keyspace");
    auto cf = get_query_param<sstring>(*req, "cf");
    apilog.debug("retrain_dict: called with ks={} cf={}", ks, cf);
    const auto t_id = ctx.db.local().find_column_family(ks, cf).schema()->id();
    constexpr uint64_t chunk_size = 4096;
    constexpr uint64_t n_chunks = 4096;
    auto sample = co_await dicts.local().do_sample_sstables(t_id, chunk_size, n_chunks);
    apilog.debug("retrain_dict: got sample with {} blocks", sample.size());
    auto dict = co_await dicts.local().train_dict(std::move(sample));
    apilog.debug("retrain_dict: got dict of size {}", dict.size());
    co_await dicts.local().publish_new_sstable_dict(t_id, dict, group0_client);
    apilog.debug("retrain_dict: published new dict");
    co_return json_void();
}
void set_sstable_dicts(http_context& ctx, routes& r, sharded<sstables::dictionary_service>& dicts,
        service::raft_group0_client& group0_client) {
    ss::estimate_compression_ratios.set(r, [&ctx, &dicts] (std::unique_ptr<http::request> req) {
        return rest_estimate_compression_ratios(ctx, dicts, std::move(req));
    });

    ss::retrain_dict.set(r, [&ctx, &dicts, &group0_client] (std::unique_ptr<http::request> req) {
        return rest_retrain_dict(ctx, dicts, group0_client, std::move(req));
    });
}

void unset_sstable_dicts(http_context& ctx, routes& r) {
    ss::estimate_compression_ratios.unset(r);
    ss::retrain_dict.unset(r);
}

}
