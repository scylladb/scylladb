/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dict_autotrainer.hh"
#include "gms/feature_service.hh"
#include "service/storage_service.hh"
#include <seastar/core/sleep.hh>
#include "schema/schema_builder.hh"
#include "sstables/sstables_manager.hh"
#include <seastar/coroutine/maybe_yield.hh>

static seastar::logger alogger("sstable_dict_autotrainer");

sstable_dict_autotrainer::sstable_dict_autotrainer(service::storage_service& ss, service::raft_group0_client& rgc, config cfg)
    : _ss(ss)
    , _group0_client(rgc)
    , _cfg(std::move(cfg))
    , _fiber(run())
{}

future<> sstable_dict_autotrainer::tick() {
    alogger.debug("sstable_dict_autotrainer::tick(): entering");
    auto dict_aware_tables = std::vector<schema_ptr>();
    _ss.get_database().get_tables_metadata().for_each_table([&dict_aware_tables] (table_id id, lw_shared_ptr<replica::table> table) {
        if (is_system_keyspace(table->schema()->ks_name())) {
            return;
        }
        auto params = table->schema()->get_compressor_params();
        auto compression_algo = params.get_algorithm();
        if (compression_algo == compressor::algorithm::lz4_with_dicts || compression_algo == compressor::algorithm::zstd_with_dicts) {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{} is dict-aware", table->schema()->ks_name(), table->schema()->cf_name());
            dict_aware_tables.push_back(table->schema());
        } else {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{} is not dict-aware", table->schema()->ks_name(), table->schema()->cf_name());
        }
    });
    for (const auto& s : dict_aware_tables) {
        auto dict_name = fmt::format("sstables/{}", s->id());
        auto current_dict_timestamp = co_await _ss.get_system_keyspace().query_dict_timestamp(dict_name);
        if (current_dict_timestamp) {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{} has timestamp {}", s->ks_name(), s->cf_name(), current_dict_timestamp);
            auto delta = (db_clock::now() - *current_dict_timestamp);
            auto min_delta = std::chrono::duration<float>(_cfg.retrain_period_in_seconds());
            if (delta < min_delta) {
                alogger.debug("sstable_dict_autotrainer::tick(): {}.{} has dict younger than {}, not updating", s->ks_name(), s->cf_name(), min_delta);
                continue;
            }
        } else {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{} has no dict", s->ks_name(), s->cf_name());
        }
        alogger.debug("sstable_dict_autotrainer::tick(): attempting to update the dict for {}.{}", s->ks_name(), s->cf_name());
        auto estimated_size = co_await _ss.estimate_total_sstable_volume(s->id());
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: estimated_size={}", s->ks_name(), s->cf_name(), estimated_size);
        // constexpr uint64_t min_dataset_size = uint64_t(1)*1024*1024*1024;
        constexpr uint64_t min_dataset_size = uint64_t(32)*1024*1024;
        if (estimated_size < min_dataset_size) {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: dataset not big enough yet (< {}), giving up on the update", s->ks_name(), s->cf_name(), min_dataset_size);
            continue;
        }
        auto params = s->get_compressor_params();
        auto ticket = get_units(_ss.get_do_sample_sstables_concurrency_limiter(), 1);
        auto n_chunks = std::min<uint64_t>(4096, 128*1024*1024 / params.chunk_length());
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: sampling for training with chunk_length={}, n_chunks={}", s->ks_name(), s->cf_name(), params.chunk_length(), n_chunks);
        auto training_sample = co_await _ss.do_sample_sstables(s->id(), params.chunk_length(), n_chunks);
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: got training sample with {} chunks", s->ks_name(), s->cf_name(), training_sample.size());
        if (training_sample.size() < n_chunks) {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: not enough chunks, giving up on the update", s->ks_name(), s->cf_name());
            continue;
        }
        auto dict = co_await _ss.train_dict(std::move(training_sample));
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: trained dict of size {}", s->ks_name(), s->cf_name(), dict.size());
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: sampling for validation with chunk_length={}, n_chunks={}", s->ks_name(), s->cf_name(), params.chunk_length(), n_chunks);
        auto validation_sample = co_await _ss.do_sample_sstables(s->id(), params.chunk_length(), n_chunks);
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: got validation sample with {} chunks", s->ks_name(), s->cf_name(), validation_sample.size());
        if (validation_sample.size() < n_chunks) {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: not enough chunks, giving up on the update", s->ks_name(), s->cf_name());
            continue;
        }
        auto ratio_before = co_await try_one_compression_config(_ss.get_database().get_user_sstables_manager().get_compressor_factory(), s, params, validation_sample);
        auto ratio_after = co_await try_one_compression_config(dict, s, params, validation_sample);
        constexpr float threshold_fraction = 0.95;
        float threshold = threshold_fraction * ratio_before;
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: ratio_before={}, ratio_after={}, update_threshold={}", s->ks_name(), s->cf_name(), ratio_before, ratio_after, threshold);
        if (ratio_after > threshold) {
            alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: ratio_after below threshold. Discarding the new dict.", s->ks_name(), s->cf_name());
            continue;
        }
        alogger.debug("sstable_dict_autotrainer::tick(): {}.{}: Publishing the new dict.", s->ks_name(), s->cf_name());
        co_await _ss.publish_new_sstable_dict(s->id(), dict, _group0_client);
    }
}

future<> sstable_dict_autotrainer::run() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    alogger.debug("sstable_dict_autotrainer::run(): starting");
    std::default_random_engine rng(0);
    while (!_as.abort_requested()) {
        try {
            auto tick_period = std::chrono::duration<float>(_cfg.tick_period_in_seconds());
            auto tick_period_ms = std::chrono::duration_cast<std::chrono::milliseconds>(tick_period);
            co_await seastar::sleep_abortable(tick_period_ms, _as);
            if (_ss.is_raft_leader() && _ss.get_feature_service().sstable_compression_dicts) {
                co_await tick();
            }
        } catch (const abort_requested_exception&) {
            alogger.debug("sstable_dict_autotrainer::run(): exiting");
        } catch (...) {
            alogger.debug("sstable_dict_autotrainer::run(): tick() failed with: {}", std::current_exception());
        }
    }
}

future<> sstable_dict_autotrainer::stop() {
    _as.request_abort();
    return std::move(_fiber);
}

future<float> try_one_compression_config(
    sstable_compressor_factory& factory,
    schema_ptr initial_schema,
    const compression_parameters& params,
    const utils::chunked_vector<bytes>& validation_samples
) {
    auto modified_schema = schema_builder(initial_schema).set_compressor_params(params).build();
    auto compressor = co_await factory.make_compressor_for_writing(modified_schema);
    size_t raw_size = 0;
    size_t compressed_size = 0;
    std::vector<char> tmp;
    for (const auto& s : validation_samples) {
        auto chunk_len = params.chunk_length();
        for (size_t offset = 0; offset < s.size(); offset += chunk_len) {
            auto frag = std::string_view(reinterpret_cast<const char*>(s.data()), s.size()).substr(offset);
            frag = frag.substr(0, std::min<size_t>(chunk_len, frag.size()));
            raw_size += frag.size();
            tmp.resize(compressor->compress_max_size(frag.size()));
            compressed_size += compressor->compress(frag.data(), frag.size(), tmp.data(), tmp.size());
        }
        co_await coroutine::maybe_yield();
    }
    if (raw_size == 0) {
        co_return 0.0f;
    }
    co_return float(compressed_size) / raw_size;
}

future<float> try_one_compression_config(
    std::span<std::byte> dict,
    schema_ptr initial_schema,
    const compression_parameters& params,
    const utils::chunked_vector<bytes>& validation_samples
) {
    auto factory = make_sstable_compressor_factory();
    co_await factory->set_recommended_dict(initial_schema->id(), dict);
    co_return co_await try_one_compression_config(*factory, initial_schema, params, validation_samples);
}
