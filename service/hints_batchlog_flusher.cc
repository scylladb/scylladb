/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/hints_batchlog_flusher.hh"

#include <algorithm>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <fmt/ranges.h>

#include "gms/gossiper.hh"
#include "locator/token_metadata.hh"
#include "repair/repair.hh"
#include "tasks/types.hh"
#include "utils/log.hh"
#include "utils/on_internal_error.hh"

namespace service {

static logging::logger flogger("hints_batchlog_flusher");

namespace {

class gossip_cluster : public hints_batchlog_flusher::cluster {
    gms::gossiper& _gossiper;
    netw::messaging_service& _messaging;
    const locator::shared_token_metadata& _shared_tm;
    utils::updateable_value<uint32_t> _flush_timeout_in_seconds;
public:
    gossip_cluster(gms::gossiper& gossiper, netw::messaging_service& messaging, const locator::shared_token_metadata& shared_tm,
            utils::updateable_value<uint32_t> flush_timeout_in_seconds)
        : _gossiper(gossiper), _messaging(messaging), _shared_tm(shared_tm), _flush_timeout_in_seconds(std::move(flush_timeout_in_seconds))
    {
    }

    std::unordered_set<locator::host_id> nodes() const override {
        return _shared_tm.get()->get_topology().get_all_host_ids();
    }

    bool is_alive(locator::host_id node) const override {
        return _gossiper.is_alive(node);
    }

    future<gc_clock::time_point> flush(locator::host_id node, abort_source& as) override {
        auto timeout = std::chrono::seconds(_flush_timeout_in_seconds());
        repair_flush_hints_batchlog_request req{tasks::task_id::create_null_id(), {}, timeout, timeout};
        return flush_hints_batchlog_on_node(_messaging, node, req, as);
    }
};

} // anonymous namespace

hints_batchlog_flusher::hints_batchlog_flusher(std::unique_ptr<cluster> cluster_ptr, utils::updateable_value<uint32_t> flush_cache_time_in_ms, abort_source& as)
    : _cluster(std::move(cluster_ptr))
    , _flush_cache_time_in_ms(std::move(flush_cache_time_in_ms))
    , _as(as)
    , _gate("hints_batchlog_flusher")
{
}

hints_batchlog_flusher::hints_batchlog_flusher(gms::gossiper& gossiper, netw::messaging_service& messaging, const locator::shared_token_metadata& shared_tm,
        utils::updateable_value<uint32_t> flush_cache_time_in_ms, utils::updateable_value<uint32_t> flush_timeout_in_seconds, abort_source& as)
    : hints_batchlog_flusher(std::make_unique<gossip_cluster>(gossiper, messaging, shared_tm, std::move(flush_timeout_in_seconds)), std::move(flush_cache_time_in_ms), as)
{
}

std::chrono::milliseconds hints_batchlog_flusher::cache_time() const {
    return std::chrono::milliseconds(_flush_cache_time_in_ms());
}

bool hints_batchlog_flusher::is_fresh(locator::host_id node, gc_clock::time_point now) const {
    if (cache_time() == std::chrono::milliseconds(0)) {
        return false;
    }
    auto it = _flush_times.find(node);
    return it != _flush_times.end() && now - it->second.sent_at <= cache_time();
}

gc_clock::time_point hints_batchlog_flusher::earliest_flush_time(const std::unordered_set<locator::host_id>& nodes) const {
    if (nodes.empty()) {
        on_internal_error(flogger, "No nodes in topology");
    }
    auto result = gc_clock::time_point::max();
    for (const auto& node : nodes) {
        auto it = _flush_times.find(node);
        if (it == _flush_times.end()) {
            on_internal_error(flogger, fmt::format("No flush time for node {}", node));
        }
        result = std::min(result, it->second.flush_time);
    }
    return result;
}

future<std::optional<gc_clock::time_point>> hints_batchlog_flusher::run_flush() {
    auto holder = _gate.hold();
    auto nodes = _cluster->nodes();
    auto now = gc_clock::now();
    std::erase_if(_flush_times, [&] (const auto& e) { return !nodes.contains(e.first); });

    std::vector<locator::host_id> to_flush;
    std::vector<locator::host_id> down;
    for (const auto& node : nodes) {
        if (!is_fresh(node, now)) {
            (_cluster->is_alive(node) ? to_flush : down).push_back(node);
        }
    }
    // A failed flush fails the repairs waiting for it, and the topology
    // coordinator reschedules them right away, so these can repeat quickly.
    static thread_local logger::rate_limit failure_rate_limit{std::chrono::seconds(10)};
    if (!down.empty()) {
        flogger.log(log_level::warn, failure_rate_limit, "Skipped flushing hints and batchlog, nodes are down: {}", down);
        co_return std::nullopt;
    }

    flogger.info("Flushing hints and batchlog on nodes={}", to_flush);
    bool failed = false;
    co_await coroutine::parallel_for_each(to_flush, [&] (locator::host_id node) -> future<> {
        try {
            auto flush_time = co_await _cluster->flush(node, _as);
            _flush_times[node] = flush_record{now, flush_time};
        } catch (const abort_requested_exception&) {
            throw;
        } catch (...) {
            flogger.log(log_level::warn, failure_rate_limit, "Flushing hints and batchlog failed on node={}: {}", node, std::current_exception());
            failed = true;
        }
    });
    if (failed) {
        co_return std::nullopt;
    }

    std::vector<locator::host_id> joined;
    for (const auto& node : _cluster->nodes()) {
        if (!nodes.contains(node)) {
            joined.push_back(node);
        }
    }
    if (!joined.empty()) {
        flogger.log(log_level::warn, failure_rate_limit, "Flushed hints and batchlog, but nodes joined meanwhile: {}", joined);
        co_return std::nullopt;
    }

    auto result = earliest_flush_time(nodes);
    flogger.info("Flushed hints and batchlog, flush_time={}", result);
    co_return result;
}

future<std::optional<gc_clock::time_point>> hints_batchlog_flusher::flush_time() {
    auto nodes = _cluster->nodes();
    auto now = gc_clock::now();
    if (std::ranges::all_of(nodes, [&] (locator::host_id node) { return is_fresh(node, now); })) {
        co_return earliest_flush_time(nodes);
    }
    if (!_in_progress || _in_progress->available()) {
        _in_progress = run_flush();
    }
    co_return co_await _in_progress->get_future();
}

future<> hints_batchlog_flusher::stop() noexcept {
    return _gate.close();
}

} // namespace service
