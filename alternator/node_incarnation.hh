/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>
#include "locator/host_id.hh"
#include "seastarx.hh"

namespace gms { class gossiper; }
namespace locator { class token_metadata; }

// Helpers for the background jobs of Alternator which a single node runs and which have to be
// taken over (marked as failed) when that node stops running them: S3 imports (SCYLLADB-4044) and,
// in the future, S3 exports. A job row records the incarnation of the node which runs the job, and a
// garbage collector, running periodically on every node, takes the rows of incarnations which
// cannot be running any more. So far only imports do this.

namespace alternator {

// Identifies one incarnation of a node: its host_id and gossip generation. The value changes
// after a restart, and after a restart of gossip (`nodetool disablegossip` and `enablegossip`):
// both increase the gossip generation.
// The `node_id` columns of the import tables store it as "{host_id}:{gossip_generation}" (the export
// tables store only a host id, until exports use these helpers).
// A default value (a null host_id) means "no node".
struct live_node_identifier {
    locator::host_id host_id;
    std::uint32_t gossip_generation = 0;

    sstring to_string() const;

    // Parses the value of to_string(). Returns a default value for an empty or incorrect input.
    static live_node_identifier from_string(std::string_view str);

    auto operator<=>(const live_node_identifier&) const = default;
};

// Returns the identifier of this node's incarnation, or nullopt if the gossiper has no state for
// this node yet - before gossip starts.
std::optional<live_node_identifier> get_self_node_id(gms::gossiper& gossiper);

// The node incarnations which can still be running a job.
struct running_node_ids {
    // The gossip generation of each node which can still be running a job: the nodes the gossiper
    // knows, except those unreachable for longer than the timeout given to get_running_nodes(),
    // and the cluster members the gossiper does not know.
    // The generation is 0 if the incarnation is not known: at restart, a node loads its peers from
    // system.peers with generation 0 (see gms::gossiper::add_saved_endpoint()), and a member which
    // the gossiper does not know has no incarnation at all. 0 is not higher than the generation in
    // any row, so the generation of such a node never shows a restart: it counts as running while
    // it is in the map.
    std::unordered_map<locator::host_id, std::uint32_t> generations;

    // Returns true if `node_id` can still be running a job. Also returns true for "no node" (a
    // missing or incorrect `node_id`): nobody can tell whether such a row is orphaned, so the
    // garbage collector leaves it alone until its TTL removes it.
    bool may_be_running(const live_node_identifier& node_id) const;
};

// Returns the node incarnations which can still be running a job, see running_node_ids. A node
// which is DOWN still counts as running until it was unreachable for `unreachable_timeout`: after
// a reactor stall or a network partition, it may continue its jobs after we marked them failed.
running_node_ids get_running_nodes(gms::gossiper& gossiper, const locator::token_metadata& tm,
        std::chrono::seconds unreachable_timeout);

// Runs the rounds of a garbage collector of background jobs: one at start, then one every
// `period`, one `node_down_delay` after each node-down event of the gossiper, and one when
// request_round() asks for it (the requests of one bucket of up to a minute get one round). A
// failed round is retried after `retry_period`, doubling up to `period`.
// When the error injection `wakeup_injection` is enabled, rounds are started only by its messages,
// and the injection `finished_injection` is entered after each such round, so that a test can
// start a round and wait for its end (enable both).
class gc_round_scheduler {
public:
    struct config {
        // Names the job in log messages.
        std::string_view name;
        std::chrono::seconds period;
        std::chrono::seconds retry_period;
        std::string_view wakeup_injection;
        std::string_view finished_injection;
    };
private:
    gms::gossiper& _gossiper;
    config _cfg;
    seastar::logger& _logger;
    // The time of the next periodic round. The first value is in the past, so that the first round
    // does not wait.
    seastar::lowres_clock::time_point _round_at = seastar::lowres_clock::time_point::min();
    // The times of the rounds asked for with request_round(): by node-down events, and by the
    // users of the scheduler.
    std::set<seastar::lowres_clock::time_point> _round_requests;
    class node_down_subscriber;
    // Returns true if error injection started the round.
    future<bool> wait_for_next_round(abort_source& as);
public:
    gc_round_scheduler(gms::gossiper& gossiper, config cfg, seastar::logger& logger);
    // Asks for a round in `delay`, rounded up to the end of a bucket of up to a minute.
    void request_round(seastar::lowres_clock::duration delay);
    // Runs `round` as described above, until `as` is aborted. `node_down_delay` is how long after
    // a node-down event the round starts: a round can take the jobs of a node only once it was
    // unreachable for the timeout given to get_running_nodes().
    future<> run(abort_source& as, std::chrono::seconds node_down_delay, noncopyable_function<future<>()> round);
};

} // namespace alternator
