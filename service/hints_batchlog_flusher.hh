/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include "gc_clock.hh"
#include "locator/host_id.hh"
#include "utils/updateable_value.hh"
#include "seastarx.hh"

namespace gms { class gossiper; }
namespace netw { class messaging_service; }
namespace locator { class shared_token_metadata; }

namespace service {

// Flushes hints and batchlog on all nodes on behalf of tablet repairs, so
// that the topology coordinator flushes once and hands the flush time to
// the repairs it starts instead of each repair asking every node itself.
class hints_batchlog_flusher {
public:
    // What the flusher asks of the cluster. Tests supply their own.
    class cluster {
    public:
        virtual ~cluster() = default;
        virtual std::unordered_set<locator::host_id> nodes() const = 0;
        virtual bool is_alive(locator::host_id node) const = 0;
        // Fails with abort_requested_exception when as is aborted.
        virtual future<gc_clock::time_point> flush(locator::host_id node, abort_source& as) = 0;
    };

private:
    struct flush_record {
        gc_clock::time_point sent_at;
        gc_clock::time_point flush_time;
    };

    std::unique_ptr<cluster> _cluster;
    utils::updateable_value<uint32_t> _flush_cache_time_in_ms;
    abort_source& _as;

    std::unordered_map<locator::host_id, flush_record> _flush_times;
    std::optional<shared_future<std::optional<gc_clock::time_point>>> _in_progress;
    named_gate _gate;

public:
    hints_batchlog_flusher(std::unique_ptr<cluster> cluster_ptr, utils::updateable_value<uint32_t> flush_cache_time_in_ms, abort_source& as);
    hints_batchlog_flusher(gms::gossiper& gossiper, netw::messaging_service& messaging, const locator::shared_token_metadata& shared_tm,
            utils::updateable_value<uint32_t> flush_cache_time_in_ms, utils::updateable_value<uint32_t> flush_timeout_in_seconds, abort_source& as);

    // Returns the time of a flush recent enough for a repair started now,
    // running one if needed. Concurrent callers share the same flush.
    // nullopt if a node that needs flushing is down or does not respond.
    // Fails with abort_requested_exception once the abort source is
    // aborted.
    future<std::optional<gc_clock::time_point>> flush_time();

    // Waits for the flush in flight. Abort the abort source first, or this
    // waits until every node answers or times out.
    future<> stop() noexcept;

private:
    std::chrono::milliseconds cache_time() const;
    bool is_fresh(locator::host_id node, gc_clock::time_point now) const;
    gc_clock::time_point earliest_flush_time(const std::unordered_set<locator::host_id>& nodes) const;
    // Flushes the nodes whose flush time is not fresh.
    future<std::optional<gc_clock::time_point>> run_flush();
};

} // namespace service
