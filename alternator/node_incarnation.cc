/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/node_incarnation.hh"

#include <algorithm>
#include <charconv>
#include <type_traits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include "gms/gossiper.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "locator/token_metadata.hh"
#include "utils/UUID.hh"
#include "utils/error_injection.hh"

namespace alternator {

sstring live_node_identifier::to_string() const {
    if (!host_id) {
        return sstring();
    }
    return fmt::format("{}:{}", host_id, gossip_generation);
}

live_node_identifier live_node_identifier::from_string(std::string_view str) {
    auto pos = str.rfind(':');
    if (pos == std::string_view::npos || pos == 0) {
        return live_node_identifier{};
    }
    std::uint32_t generation = 0;
    auto gen_str = str.substr(pos + 1);
    auto [ptr, ec] = std::from_chars(gen_str.data(), gen_str.data() + gen_str.size(), generation);
    if (ec != std::errc() || ptr != gen_str.data() + gen_str.size()) {
        return live_node_identifier{};
    }
    try {
        return live_node_identifier{ .host_id = locator::host_id(utils::UUID(str.substr(0, pos))), .gossip_generation = generation };
    } catch (...) {
        return live_node_identifier{};
    }
}

std::optional<live_node_identifier> get_self_node_id(gms::gossiper& gossiper) {
    auto ep_state = gossiper.get_this_endpoint_state_ptr();
    if (!ep_state) {
        return std::nullopt;
    }
    auto generation = ep_state->get_heart_beat_state().get_generation();
    return live_node_identifier{ .host_id = gossiper.my_host_id(), .gossip_generation = std::uint32_t(generation.value()) };
}

bool running_node_ids::may_be_running(const live_node_identifier& node_id) const {
    if (!node_id.host_id) {
        return true;
    }
    auto it = generations.find(node_id.host_id);
    if (it == generations.end()) {
        // The node is unreachable for too long, or it is not a member of the cluster any more
        // (it was removed or replaced).
        return false;
    }
    // Only a higher generation shows that the node restarted, or restarted gossip. After a gossip
    // restart the job may still run, but it is taken anyway: the node cannot tell its jobs from
    // those of an earlier incarnation, and the job's next conditional write does not apply. A
    // lower generation can be old gossip state or the placeholder 0, so the job may still run.
    return it->second <= node_id.gossip_generation;
}

running_node_ids get_running_nodes(gms::gossiper& gossiper, const locator::token_metadata& tm,
        std::chrono::seconds unreachable_timeout) {
    running_node_ids result;
    auto timeout = std::chrono::duration_cast<std::chrono::microseconds>(unreachable_timeout).count();
    // The callback must not yield.
    gossiper.for_each_endpoint_state([&] (const gms::endpoint_state& ep_state) {
        // A node which left the cluster runs no job. The gossiper normally drops its state, but
        // if that failed, the state stays, with no downtime counted.
        if (gossiper.is_left(ep_state)) {
            return;
        }
        auto host_id = ep_state.get_host_id();
        // get_endpoint_downtime() reports microseconds, and 0 for a node which is up.
        if (gossiper.get_endpoint_downtime(host_id) > timeout) {
            return;
        }
        auto generation = ep_state.get_heart_beat_state().get_generation();
        // A valid generation is positive; anything else counts as the unknown generation 0.
        result.generations.emplace(host_id, std::uint32_t(std::max<std::int32_t>(generation.value(), 0)));
    });
    // The gossip view of this node can still be incomplete, for example right after it joined, so
    // a cluster member which the gossiper does not know is not necessarily dead. Such a node has no
    // downtime either, so the timeout would never apply to it: count it as running, with the
    // generation 0 of an unknown incarnation. A node which is not a member any more stays out.
    for (const auto& host_id : tm.get_host_ids()) {
        if (!gossiper.get_endpoint_state_ptr(host_id)) {
            result.generations.emplace(host_id, 0);
        }
    }
    return result;
}

// The requests of one bucket of up to this period ask for one round, not for one round each:
// several nodes going down at about the same time (e.g. a rack, or a fast rolling restart) do not
// cause back-to-back rounds.
static constexpr std::chrono::minutes round_request_merge_period{1};

// The scheduler sleeps in steps of at most this length, and looks at the time of the next round
// after each step, because a node-down event can move the next round earlier.
static constexpr std::chrono::minutes poll_period{1};

gc_round_scheduler::gc_round_scheduler(gms::gossiper& gossiper, config cfg, seastar::logger& logger)
    : _gossiper(gossiper)
    , _cfg(cfg)
    , _logger(logger)
{}

void gc_round_scheduler::request_round(seastar::lowres_clock::duration delay) {
    // Round the time up to a multiple of `bucket`, so that the requests of one bucket are a single
    // element of the set. A bucket of at most a tenth of `delay` delays the round by at most 10%.
    auto at = seastar::lowres_clock::now() + delay;
    auto bucket = std::min<seastar::lowres_clock::duration>(round_request_merge_period, delay / 10);
    if (bucket > seastar::lowres_clock::duration::zero()) {
        if (auto rem = at.time_since_epoch() % bucket; rem != seastar::lowres_clock::duration::zero()) {
            at += bucket - rem;
        }
    }
    _round_requests.insert(at);
}

future<bool> gc_round_scheduler::wait_for_next_round(abort_source& as) {
    auto& injector = utils::get_local_injector();
    for (;;) {
        if (injector.is_enabled(_cfg.wakeup_injection)) {
            co_await injector.inject(_cfg.wakeup_injection, utils::wait_for_message(_cfg.period, &as), false);
            // Disabling the injection also ends the wait, without a message. Such a wakeup must not
            // start a round: the round could end after the next test enabled the injections, and
            // that test would take it for its own round.
            if (!injector.is_enabled(_cfg.wakeup_injection)) {
                continue;
            }
            co_return true;
        }
        auto now = seastar::lowres_clock::now();
        auto round_at = _round_requests.empty() || _retry_pending ? _round_at : std::min(_round_at, *_round_requests.begin());
        if (now >= round_at) {
            // The round which starts now answers all the requests which are due.
            _round_requests.erase(_round_requests.begin(), _round_requests.upper_bound(now));
            co_return false;
        }
        seastar::lowres_clock::duration step = std::min<seastar::lowres_clock::duration>(round_at - now, poll_period);
        // A sleeping scheduler must notice when a test enables the wakeup injection.
        if constexpr (std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
            step = std::min<seastar::lowres_clock::duration>(step, std::chrono::seconds(1));
        }
        co_await seastar::sleep_abortable(step, as);
    }
}

class gc_round_scheduler::node_down_subscriber : public gms::i_endpoint_state_change_subscriber {
    gc_round_scheduler& _scheduler;
    std::chrono::seconds _delay;
public:
    node_down_subscriber(gc_round_scheduler& scheduler, std::chrono::seconds delay) noexcept
        : _scheduler(scheduler), _delay(delay) {}
    future<> on_dead(gms::inet_address, locator::host_id, gms::endpoint_state_ptr, gms::permit_id) override {
        _scheduler.request_round(_delay);
        return make_ready_future<>();
    }
    future<> on_remove(gms::inet_address, locator::host_id, gms::permit_id) override {
        _scheduler.request_round(_delay);
        return make_ready_future<>();
    }
};

future<> gc_round_scheduler::run(abort_source& as, std::chrono::seconds node_down_delay, noncopyable_function<future<>()> round) {
    auto subscriber = seastar::make_shared<node_down_subscriber>(*this, node_down_delay);
    _gossiper.register_(subscriber);
    std::chrono::seconds retry_period = _cfg.retry_period;
    while (!as.abort_requested()) {
        try {
            auto triggered_by_injection = co_await wait_for_next_round(as);
            auto notify_round_finished = defer([this, triggered_by_injection] () noexcept {
                if (triggered_by_injection) {
                    utils::get_local_injector().enter(_cfg.finished_injection);
                }
            });
            co_await round();
            _round_at = seastar::lowres_clock::now() + _cfg.period;
            _retry_pending = false;
            retry_period = _cfg.retry_period;
        } catch (const abort_requested_exception&) {
            break;
        } catch (...) {
            _logger.warn("Garbage collection of {} failed: {}", _cfg.name, std::current_exception());
            _round_at = seastar::lowres_clock::now() + retry_period;
            _retry_pending = true;
            retry_period = std::min(retry_period * 2, _cfg.period);
        }
    }
    co_await _gossiper.unregister_(subscriber);
}

} // namespace alternator
