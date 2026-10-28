/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <algorithm>
#include <ranges>

#include "sstables/dictionary_service.hh"

#include "db/system_keyspace.hh"
#include "gms/feature_service.hh"
#include "locator/token_metadata.hh"
#include "message/messaging_service.hh"
#include "mutation/mutation.hh"
#include "replica/database.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "utils/log.hh"

#include "idl/sstable_dicts.dist.hh"

namespace sstables {

static logging::logger slogger("sstable_dictionary_service");

dictionary_service::dictionary_service(sharded<replica::database>& db, sharded<netw::messaging_service>& messaging, sharded<db::system_keyspace>& sys_ks, sharded<gms::feature_service>& feature_service)
    : _db(db)
    , _messaging(messaging)
    , _sys_ks(sys_ks)
    , _feature_service(feature_service)
{
    ser::sstable_dicts_rpc_verbs::register_estimate_sstable_volume(&_messaging.local(), [this] (table_id t_id) -> future<uint64_t> {
        co_return co_await _db.map_reduce0(seastar::coroutine::lambda([&] (replica::database& local_db) -> future<uint64_t> {
            uint64_t result = 0;
            auto& t = local_db.get_tables_metadata().get_table(t_id);
            auto snap = co_await t.take_sstable_set_snapshot();
            for (const auto& sst : snap) {
                result += sst.get()->data_size();
            }
            co_return result;
        }), uint64_t(0), std::plus());
    });
    ser::sstable_dicts_rpc_verbs::register_sample_sstables(&_messaging.local(), [this] (table_id table, uint64_t chunk_size, uint64_t n_chunks) -> future<utils::chunked_vector<temporary_buffer<char>>> {
        return _db.local().sample_data_files(table, chunk_size, n_chunks);
    });
}

future<> dictionary_service::stop() {
    if (!_group0_as.abort_requested()) {
        _group0_as.request_abort();
    }
    co_await _async_gate.close();
    co_await ser::sstable_dicts_rpc_verbs::unregister(&_messaging.local());
}

void dictionary_service::set_group0(service::raft_group0& group0) {
    _group0 = &group0;
}

replica::database& dictionary_service::get_database() const noexcept {
    return _db.local();
}

db::system_keyspace& dictionary_service::get_system_keyspace() const noexcept {
    return _sys_ks.local();
}

const gms::feature_service& dictionary_service::get_feature_service() const noexcept {
    return _feature_service.local();
}

bool dictionary_service::is_raft_leader() const noexcept {
    return _group0->joined_group0() && _group0->group0_server().is_leader();
}

sstable_compressor_factory& dictionary_service::get_compressor_factory() const noexcept {
    return _db.local().get_user_sstables_manager().get_compressor_factory();
}

semaphore& dictionary_service::get_do_sample_sstables_concurrency_limiter() {
    return _do_sample_sstables_concurrency_limiter;
}

future<uint64_t> dictionary_service::estimate_total_sstable_volume(table_id t, ignore_errors errors) {
    co_return co_await seastar::map_reduce(
        _db.local().get_token_metadata().get_host_ids(),
        [&] (auto h) -> future<uint64_t> {
            try {
                co_return co_await ser::sstable_dicts_rpc_verbs::send_estimate_sstable_volume(&_messaging.local(), h, t);
            }
            catch(...) {
                if (errors == ignore_errors::yes) {
                    // If the call failed we just return 0 for this one
                    slogger.info("call to estimate_total_sstable_volume failed for table {} and host {}, returning 0", t, h);
                    co_return 0;
                }
                throw;
            }
        },
        uint64_t(0),
        std::plus<uint64_t>()
    );
}

future<std::vector<std::byte>> dictionary_service::train_dict(utils::chunked_vector<temporary_buffer<char>> sample) {
    std::vector<std::vector<std::byte>> tmp;
    tmp.reserve(sample.size());
    for (const auto& s : sample) {
        auto v = std::as_bytes(std::span(s));
        tmp.push_back(std::vector<std::byte>(v.begin(), v.end()));
        // Samples are potentially very big (tens of MiB),
        // so copying them might require more eviction work than
        // fits within a task quota.
        // Let's add a preemption point here.
        co_await coroutine::maybe_yield();
    }
    co_return co_await container().invoke_on(0, [tmp = std::move(tmp)] (auto& local) mutable {
        if (!local._train_dict) {
            on_internal_error(slogger, "retrain_dict: _train_dict not plugged");
        }
        return local._train_dict(std::move(tmp));
    });
}

future<> dictionary_service::publish_new_sstable_dict(table_id t_id, std::span<const std::byte> dict, service::raft_group0_client& group0_client) {
    co_await container().invoke_on(0, coroutine::lambda([t_id, dict, &group0_client] (dictionary_service& local_ss) -> future<> {
        auto group0_holder = local_ss._group0->hold_group0_gate();
        while (true) {
            try {
                auto name = fmt::format("sstables/{}", t_id);
                slogger.debug("publish_new_sstable_dict: trying to publish the dict as {}", name);
                auto batch = service::group0_batch(co_await group0_client.start_operation(local_ss._group0_as));
                auto write_ts = batch.write_timestamp();
                auto new_dict_ts = db_clock::now();
                auto data = bytes(reinterpret_cast<const bytes::value_type*>(dict.data()), dict.size());
                auto this_host_id = local_ss._db.local().get_token_metadata().get_topology().get_config().this_host_id;
                mutation publish_new_dict = co_await local_ss._sys_ks.local().get_insert_dict_mutation(name, std::move(data), this_host_id, new_dict_ts, write_ts);
                batch.add_mutation(std::move(publish_new_dict), "publish new SSTable compression dictionary");
                slogger.debug("publish_new_sstable_dict: committing");
                co_await std::move(batch).commit(group0_client, local_ss._group0_as, {});
                slogger.debug("publish_new_sstable_dict: finished");
                break;
            } catch (const service::group0_concurrent_modification&) {
                slogger.debug("group0_concurrent_modification in publish_new_sstable_dict, retrying");
            }
        }
    }));
}

void dictionary_service::set_train_dict_callback(decltype(_train_dict) cb) {
    _train_dict = std::move(cb);
}

future<utils::chunked_vector<temporary_buffer<char>>> dictionary_service::do_sample_sstables(table_id t, uint64_t chunk_size, uint64_t n_chunks) {
    uint64_t max_chunks_per_round = 16 * 1024 * 1024 / chunk_size;
    uint64_t chunks_done = 0;
    auto result = utils::chunked_vector<temporary_buffer<char>>();
    result.reserve(n_chunks);
    while (chunks_done < n_chunks) {
        auto chunks_this_round = std::min(max_chunks_per_round, n_chunks - chunks_done);
        auto round_result = co_await do_sample_sstables_oneshot(t, chunk_size, chunks_this_round);
        std::move(round_result.begin(), round_result.end(), std::back_inserter(result));
        if (round_result.size() < chunks_this_round) {
            break;
        }
        chunks_done += chunks_this_round;
    }
    co_return result;
}

future<utils::chunked_vector<temporary_buffer<char>>> dictionary_service::do_sample_sstables_oneshot(table_id t, uint64_t chunk_size, uint64_t n_chunks) {
    slogger.debug("do_sample_sstables(): called with table_id={} chunk_size={} n_chunks={}", t, chunk_size, n_chunks);
    auto& db = _db.local();
    auto& ms = _messaging.local();
    std::unordered_map<locator::host_id, uint64_t> estimated_sizes;
    co_await coroutine::parallel_for_each(
        db.get_token_metadata().get_host_ids(),
        [&] (auto h) -> future<> {
            auto est = co_await ser::sstable_dicts_rpc_verbs::send_estimate_sstable_volume(&ms, h, t);
            if (est) {
                estimated_sizes.emplace(h, est);
            }
        }
    );
    const auto total_size = std::ranges::fold_left(estimated_sizes | std::ranges::views::values, uint64_t(0), std::plus());
    slogger.debug("do_sample_sstables(): estimate_sstable_volume returned {}, total={}", estimated_sizes, total_size);
    std::unordered_map<locator::host_id, uint64_t> chunks_per_host;
    {
        uint64_t partial_sum = 0;
        uint64_t covered_samples = 0;
        for (const auto& [k, v] : estimated_sizes) {
            partial_sum += v;
            uint64_t next_covered = static_cast<double>(partial_sum) / total_size * n_chunks;
            chunks_per_host.emplace(k, next_covered - covered_samples);
            covered_samples = next_covered;
        }

        // Just a sanity check
        auto covered = std::ranges::fold_left(chunks_per_host | std::ranges::views::values, uint64_t(0), std::plus());
        if (total_size > 0 && covered != n_chunks) {
            on_internal_error(slogger, "do_sample_sstables(): something went wrong with the sample distribution algorithm");
        }
    }
    slogger.debug("do_sample_sstables(): sending out send_sample_sstables with proportions {}", chunks_per_host);
    auto samples = co_await seastar::map_reduce(
        chunks_per_host,
        [&] (std::pair<locator::host_id, uint64_t> h_s) -> future<utils::chunked_vector<temporary_buffer<char>>> {
            const auto& [h, sz] = h_s;
            return ser::sstable_dicts_rpc_verbs::send_sample_sstables(&ms, h, t, chunk_size, sz);
        },
        utils::chunked_vector<temporary_buffer<char>>(),
        [] (auto v, auto some_samples) {
            std::ranges::move(some_samples, std::back_inserter(v));
            return v;
        }
    );
    slogger.debug("do_sample_sstables(): returned {} chunks", samples.size());
    co_return samples;
}

} // namespace sstables
