/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <ranges>
#include <unordered_map>
#include <variant>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "exceptions/coordinator_result.hh"
#include "keys/full_position.hh"
#include "locator/host_id.hh"
#include "mutation_query.hh"
#include "query/query-result.hh"
#include "utils/assert.hh"
#include "utils/small_vector.hh"

// Coordinator decisions which turn replica replies into a page for the client.
//
// A read has up to two kinds of rounds:
// - The first round sends a data request to one replica, or sometimes two,
//   and digest requests to the others. A digest is a hash of the page which
//   the replica would return. If the digests match, the data reply becomes
//   the page.
// - If the digests differ, reconciliation rounds follow. Each replica returns
//   a mutation page: its data together with its tombstones. The coordinator
//   merges the mutation pages and computes the page itself. A round can also
//   decide that the replies are not enough, and ask for another round.
//
// This code does not send requests, choose replicas, or handle errors and
// timeouts. storage_proxy does that and passes the successful replies here.
// Tests can therefore drive these decisions with chosen replies.

namespace service {

// The state of the first round of a read when enough replies arrived to
// satisfy the consistency level.
struct digest_read_result {
    // The first data reply.
    foreign_ptr<lw_shared_ptr<query::result>> result;
    // Whether the digests of the replies received until then match.
    bool digests_match;
};

// Collects the successful data and digest replies of the first round of a
// read, and reports when they reach the consistency level.
//
// storage_proxy decides which replicas count toward the consistency level. It
// also handles errors and timeouts, and calls fail() when the request fails.
class foreground_reply_collector {
    struct digest_and_last_pos {
        query::result_digest digest;
        std::optional<full_position> last_pos;

        digest_and_last_pos(query::result_digest digest, std::optional<full_position> last_pos)
            : digest(std::move(digest)), last_pos(std::move(last_pos))
        { }
    };

    schema_ptr _schema;
    size_t _block_for;
    size_t _targets_count = 0;
    size_t _cl_responses = 0;
    promise<exceptions::coordinator_result<digest_read_result>> _cl_promise;
    bool _cl_reported = false;
    foreign_ptr<lw_shared_ptr<query::result>> _data_result;
    utils::small_vector<digest_and_last_pos, 3> _digest_results;
    api::timestamp_type _last_modified = api::missing_timestamp;

    void got_response(bool counts_for_cl);
public:
    // The consistency level requires `block_for` replies which count toward
    // it, including at least one data reply.
    foreground_reply_collector(schema_ptr schema, size_t block_for);

    // Adds `count` targets to the number of replies to wait for.
    void add_wait_targets(size_t count);

    // Adds a successful reply. `counts_for_cl` tells whether the replica
    // counts toward the consistency level.
    void add_data(bool counts_for_cl, foreign_ptr<lw_shared_ptr<query::result>> result);
    void add_digest(bool counts_for_cl, query::result_digest digest, api::timestamp_type last_modified, std::optional<full_position> last_pos);

    // Drops the collected replies. If the consistency level was not reached,
    // has_cl() resolves with `ex`.
    void fail(exceptions::coordinator_exception_container ex);

    // Resolves when the replies reach the consistency level, or when the
    // request fails before that. The result holds the first data reply.
    //
    // The continuation runs after the reply which reached the consistency
    // level was added. Replies added in between are visible through this
    // object, but not through the result.
    future<exceptions::coordinator_result<digest_read_result>> has_cl();

    // The number of replies, including those after the consistency level.
    size_t response_count() const {
        return _digest_results.size();
    }
    // The number of replies which count toward the consistency level.
    size_t cl_responses() const {
        return _cl_responses;
    }
    size_t block_for() const {
        return _block_for;
    }
    // Whether the collector holds a data reply. The collector hands the first
    // data reply over to has_cl() when the consistency level is reached. It
    // keeps a data reply which arrives after that.
    bool has_data() const {
        return bool(_data_result);
    }
    // Whether all targets replied.
    bool is_completed() const {
        return response_count() == _targets_count;
    }
    api::timestamp_type last_modified() const {
        return _last_modified;
    }

    // Whether the digests of all replies received so far match.
    bool digests_match() const;
    // The earliest last position of all replies received so far. A reply's
    // last position is where its reader was when the page ended; see
    // replica::read_data_page(). A reply without one sorts first.
    const std::optional<full_position>& min_position() const;
};

// The functions of foreground_reply_collector on the path of every read are
// inline, so that storage_proxy can inline them.

inline foreground_reply_collector::foreground_reply_collector(schema_ptr schema, size_t block_for)
    : _schema(std::move(schema))
    , _block_for(block_for)
{}

inline void foreground_reply_collector::add_wait_targets(size_t count) {
    _targets_count += count;
}

inline void foreground_reply_collector::add_data(bool counts_for_cl, foreign_ptr<lw_shared_ptr<query::result>> result) {
    // if only one target was queried digest_check() will be skipped so we can also skip digest calculation
    _digest_results.emplace_back(_targets_count == 1 ? query::result_digest() : *result->digest(), result->last_position());
    _last_modified = std::max(_last_modified, result->last_modified());
    if (!_data_result) {
        _data_result = std::move(result);
    }
    got_response(counts_for_cl);
}

inline void foreground_reply_collector::add_digest(bool counts_for_cl, query::result_digest digest, api::timestamp_type last_modified,
        std::optional<full_position> last_pos) {
    _digest_results.emplace_back(std::move(digest), std::move(last_pos));
    _last_modified = std::max(_last_modified, last_modified);
    got_response(counts_for_cl);
}

inline void foreground_reply_collector::got_response(bool counts_for_cl) {
    if (!_cl_reported) {
        if (counts_for_cl) {
            _cl_responses++;
        }
        if (_cl_responses >= _block_for && _data_result) {
            _cl_reported = true;
            _cl_promise.set_value(digest_read_result{std::move(_data_result), digests_match()});
        }
    }
}

inline future<exceptions::coordinator_result<digest_read_result>> foreground_reply_collector::has_cl() {
    return _cl_promise.get_future();
}

inline bool foreground_reply_collector::digests_match() const {
    SCYLLA_ASSERT(response_count());
    if (response_count() == 1) {
        return true;
    }
    auto it = std::ranges::begin(_digest_results);
    const auto& first_digest = it->digest;
    return std::ranges::all_of(std::ranges::subrange(++it, std::ranges::end(_digest_results)),
                               [&first_digest] (const digest_and_last_pos& digest) {
                                   return digest.digest == first_digest;
                               });
}

// The data reply may be returned to the client.
struct accepted_digest_page {
    foreign_ptr<lw_shared_ptr<query::result>> result;
};

// The digests do not match; the coordinator must reconcile the replicas'
// mutations.
struct digest_page_mismatch {};

using digest_page_decision = std::variant<accepted_digest_page, digest_page_mismatch>;

// Decides what to do when the first round of a read reaches the consistency
// level.
//
// `cl_result` is the value of `replies.has_cl()`. The decision compares the
// digests which `cl_result` saw. When the digests match and
// `empty_replica_pages` is true, it lowers the page's last position to
// replies.min_position(). That covers replies which arrived after the
// consistency level was reached. If a reply has no last position, the page
// loses its own. `empty_replica_pages` tells whether the
// empty_replica_pages cluster feature is enabled.
digest_page_decision decide_digest_page(const schema& s, digest_read_result cl_result, const foreground_reply_collector& replies,
        bool empty_replica_pages);

using mutations_per_partition_key_map =
        std::unordered_map<partition_key, std::unordered_map<locator::host_id, std::optional<mutation>>, partition_key::hashing, partition_key::equality>;

// A mutation page returned by one replica in a reconciliation round.
struct mutation_page_reply {
    locator::host_id from;
    foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
};

// A reconciled page which the coordinator may return to the client.
struct accepted_mutation_page {
    // The client-visible page. The original command limits it.
    query::result result;
    // For each partition and replica, the mutation which the replica lacks
    // compared to the merged replies, if any. The mutations use the table
    // schema, also for reversed queries.
    mutations_per_partition_key_map repair_diffs;
};

// The replies are not enough to build the page, and the coordinator must run
// another round.
struct mutation_page_retry {
    // The command for the next round. Its limits may be larger than the
    // limits of the original command.
    lw_shared_ptr<query::read_command> cmd;
};

using mutation_page_resolution = std::variant<accepted_mutation_page, mutation_page_retry>;

// Sets the options of the command which a reconciliation round sends to the
// replicas. `empty_replica_mutation_pages` tells whether the
// empty_replica_mutation_pages cluster feature is enabled. Its option lets a
// replica end a mutation page without a live row.
void prepare_mutation_read(query::read_command& cmd, bool empty_replica_mutation_pages);

// Reconciles the replies of all targets of one reconciliation round.
//
// `cmd` is the command sent to the replicas in this round. `original_cmd` is
// the client's command, which limits the returned page. In the first round,
// both are the same command.
//
// For reversed queries, `schema` is the reversed schema, and the replies are
// in native reversed format.
future<mutation_page_resolution> resolve_mutation_page(schema_ptr schema, const query::read_command& original_cmd,
        const query::read_command& cmd, std::vector<mutation_page_reply> replies);

} // namespace service
