/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <bitset>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/core.h>

#include "service/pager/paging_state.hh"
#include "test/lib/read_model.hh"

class cql_test_env;

// Runs queries of the read model's table through the production read path,
// with simulated replicas which hold different parts of a history. A history
// is a sequence of writes. Tests compare the pages with the complete answer of
// read_model::evaluate().
//
// Terms:
// - The consistency level decides which replica replies the coordinator
//   needs before it can answer.
// - A digest is a hash of a replica's result. In the first round of a read,
//   some replicas get a data request and the others get a digest request. If
//   all digests match, the coordinator returns the data reply.
// - Reconciliation is what the coordinator does when the digests differ. It
//   reads mutations from the replicas, merges them into a page, and computes
//   a repair mutation for each replica which lacks some of the merged data.
//   It may need several rounds.
// - The cursor of a result is query::result::last_position(): the position of
//   the last fragment which the replica's reader consumed. The reader may
//   consume fragments which the result omits, such as tombstones, so the
//   cursor can lie after the last row of the result.
// - The schedule is the set of random choices of the coordinator. See
//   read_options::schedule_seed.
//
// The CQL layer is the production one. The harness prepares the statement of
// read_model::to_cql() and executes it with
// select_statement::execute_with_query_function(). The statement builds the
// read command, the pager and the result. The harness passes the serialized
// paging state from one page to the next, like a client.
//
// The query function plays the coordinator. For each partition range, it
// runs the production decisions of service/read_page_resolution.hh:
// foreground_reply_collector and decide_digest_page() in the first round,
// and frontier_reconciliation in the reconciliation rounds, or
// prepare_mutation_read() and resolve_mutation_page() without
// read_frontiers. It merges the results of several ranges with
// query::result_merger, like storage_proxy::query_singular() and
// storage_proxy::query_partition_key_range().
//
// Each replica holds the mutations of its part of the history. It reads its
// pages with the production page driver, replica::read_data_page() and
// replica::read_mutation_page(), without tombstone GC.
//
// Compared with storage_proxy, the harness:
// - reads every range from all replicas. The consistency level requires the
//   replies of all replicas except the extra ones;
// - has no topology. It splits scans at the vnode boundaries of the schedule,
//   and all replicas own all ranges;
// - delivers the replies of a read only after all replicas read their pages;
// - reads with fresh readers, or keeps queriers in a querier cache for each
//   replica, with random evictions;
// - checks the frontier of every data, digest and mutation reply, see
//   query::read_frontier. A reply has one only if its command asks for one.
//   A new querier which reads only the replica's contents that the frontier
//   covers, without the row and partition limits, must return the same
//   page. The covered contents of a mutation reply end at its skips; a data
//   reply has none, so its read keeps the per-partition limit. The frontier
//   of a short reply must be a stop, and the stop must be
//   after the start of the page;
// - fails a reconciliation when a round repeats an earlier round: the
//   limits, with each limit clamped to the history's bounds, and for
//   frontier_reconciliation also the range and the slice. storage_proxy
//   would retry until the read times out;
// - reports a short result which has neither a partition nor a cursor as a
//   failed read. The pager would fail an assertion on it;
// - checks that no repair mutation adds data which no replica has, and
//   applies the repair mutations to the replicas iff apply_repairs is set;
// - ignores the preferred replicas and the read repair decision of the paging
//   state.

namespace tests::paged_read {

// The largest number of replicas which a case can have.
inline constexpr size_t max_replicas = 32;

// A set of replicas. Bit i stands for replica i.
using replica_set = std::bitset<max_replicas>;

// A write of a history, and the replicas which hold it.
struct placed_operation {
    read_model::operation op;
    replica_set replicas;
};

using placed_history = std::vector<placed_operation>;

// The writes of `h`, on whichever replicas.
read_model::history complete_history(const placed_history& h);

// Places every write of `h` on the replicas in `replicas`.
placed_history on_replicas(const read_model::history& h, replica_set replicas);

// `h` as C++ code, which a test can use to replay it.
std::string describe(const placed_history& h);

struct read_options {
    // The total number of replicas, including extra replicas (see below).
    // The test harness considers the CL satisfied when and only when
    // all non-extra replicas have responded (and at least one data response has arrived).
    // (Therefore every valid write must be added to at least one non-extra replica.
    // This ensures that if a read satisfies the CL, it sees all writes).
    size_t replica_count = 2;
    // The number of extra replicas. Responses from extra replicas might trigger
    // reconciliation or supply the data reply,
    // but do not count toward meeting the consistency level.
    // (Similarly to a remote-DC replica when LOCAL consistency level is used).
    // In the replica bitset, extra replicas are the ones with the highest bits.
    size_t extra_replicas = 0;
    // The page size of the CQL query, in rows. A query with a page size of 0
    // is unpaged.
    int32_t page_size = 100;
    // The page size in bytes, which the coordinator puts into each read
    // command instead of the configured one. It limits reads which allow
    // short reads. nullopt keeps the configured size.
    std::optional<uint64_t> page_size_in_bytes;
    // The tombstone limit of each read command, instead of the configured
    // one. nullopt keeps the configured limit. Like storage_proxy, the
    // coordinator limits tombstones only when empty_replica_pages is enabled.
    std::optional<uint64_t> tombstone_limit;
    // Whether the cluster features empty_replica_pages and
    // empty_replica_mutation_pages are enabled. A cluster which enables the
    // second one also enables the first one, because its nodes are newer.
    bool empty_replica_pages = true;
    bool empty_replica_mutation_pages = true;
    // Whether the cluster feature native_reverse_queries is enabled. A
    // cluster which enables it also enables empty_replica_mutation_pages.
    // Without it, the coordinator sends reversed reads to the replicas other
    // than its own in the legacy reversed format, and the replicas convert
    // them back, like storage_proxy::handle_read().
    bool native_reverse_queries = true;
    // Whether the cluster feature read_frontiers is enabled. A cluster which
    // enables it also enables native_reverse_queries. With it, the replicas
    // compute digests which cover only the partitions of the result, like
    // storage_proxy's digest_algorithm(), and the coordinator asks the
    // replicas for frontiers and decides pages by them, see
    // service::frontier_reconciliation. Without it, replicas which disagree
    // about a static-only row can have matching digests.
    bool read_frontiers = true;
    // Whether each replica keeps its queriers between pages in a querier
    // cache, like replica::database. Otherwise the replicas read every page
    // with new queriers.
    bool querier_cache = false;
    // Whether the coordinator applies the repair mutations of each accepted
    // reconciled page to the replicas before it reads further. Otherwise it
    // only checks them. A case cannot both apply repairs and keep queriers,
    // because a cached querier keeps reading the contents from before the
    // repair.
    bool apply_repairs = false;
    // Seeds the schedule, which is the set of random choices of the
    // coordinator. For the whole query, the seed chooses:
    // - up to 4 vnode boundaries at which a scan splits;
    // - whether each round of a scan merges its contiguous ranges, as with
    //   vnodes, or reads them apart, as with tablets;
    // - the replica which the coordinator runs on, if any.
    // Before each read which the statement makes, each replica with a
    // querier cache may evict one or all of its cached queriers. In each
    // read of a range, the seed chooses:
    // - the replicas which get a data request: one which counts toward the
    //   consistency level, and sometimes another one;
    // - the order in which the replies arrive;
    // - how many replies arrive after the consistency level is reached, but
    //   before the continuation which decides the page runs;
    // - whether the reconciliation reads from all replicas, from the
    //   replicas which replied before the decision, or from the replicas
    //   which count toward the consistency level;
    // - the order of the mutation replies in each reconciliation round.
    // nullopt makes the canonical choices: scans do not split, the
    // coordinator runs on replica 0, no replica evicts queriers, replica 0
    // gets the only data request, all replies arrive in replica order before
    // the decision, and the reconciliation reads from all replicas.
    std::optional<uint32_t> schedule_seed;
};

// A query over a history, with the options of its run.
struct read_case {
    placed_history history;
    read_model::select_query query;
    read_options options;
};

// `c` as C++ code, which a test can use to replay it.
std::string describe(const read_case& c);

struct page {
    std::vector<read_model::answer_row> rows;
    // The paging state which the page gives to the client. nullptr when the
    // query is exhausted.
    lw_shared_ptr<const service::pager::paging_state> state;
    // What the coordinator did: the read commands, the replies and the
    // decisions.
    std::vector<std::string> trace;
};

struct outcome {
    std::vector<page> pages;
    // The rows of all pages, in order.
    std::vector<read_model::answer_row> rows;
    // Why the client stopped before the query was exhausted: a paging state
    // which repeats an earlier one, too many pages, or a failed page. A page
    // fails also when the statement makes too many reads for it.
    std::optional<std::string> error;
    // The number of repair mutations which the coordinator planned.
    size_t repair_mutations = 0;
    // The properties which the coordinator's reads violated: a repair
    // mutation adds data which no replica has, the result of a range
    // exceeds the limits of its command, a page which a replica read with a
    // cached querier differs from the page which a fresh querier would read,
    // or a reply's frontier is wrong. Each property appears once.
    std::vector<std::string> coordinator_violations;
};

// The properties which `o` violates, compared with the complete answer
// `expected`:
// - after every page, the rows so far are a prefix of the complete answer;
// - the client reaches the end of the query;
// - the rows of all pages equal the complete answer;
// - the properties of outcome::coordinator_violations.
std::vector<std::string> check(const outcome& o, const std::vector<read_model::answer_row>& expected);

// The kind of a failure: its violation messages with all digits removed.
// Shrinking a case keeps the kind of its failure.
std::string violation_kind(const std::vector<std::string>& violations);

// A report of a run which violates `violations`: the case, the expected and
// actual rows, and every page with its trace.
std::string report(const read_case& c, const outcome& o, const std::vector<read_model::answer_row>& expected,
        const std::vector<std::string>& violations);

// A table of the read model, whose queries read from simulated replicas.
//
// All functions which run a case must run in a seastar thread.
class harness {
    cql_test_env& _env;
    std::string _ks;
    std::string _cf;
public:
    // Creates table `ks.cf` of the read model. Keyspace `ks` must exist, and
    // the tombstone_gc extension must be registered.
    harness(cql_test_env& env, std::string_view ks, std::string_view cf);

    schema_ptr schema() const;

    // Runs the query of `c` page by page, until the query is exhausted or the
    // client stops.
    outcome run(const read_case& c);

    // The properties which a run of `c` violates. See check().
    std::vector<std::string> violations(const read_case& c);

    // A smaller case whose run fails like the run of `c`. The run of `c`
    // must fail. Greedily removes writes, replicas, options and clauses of
    // the query, and places writes on fewer replicas, while the kind of the
    // failure stays the same.
    read_case shrink(read_case c);
};

} // namespace tests::paged_read

// Formats the options as C++ code, which a test can use to replay them.
template <>
struct fmt::formatter<tests::paged_read::read_options> : fmt::formatter<string_view> {
    auto format(const tests::paged_read::read_options& o, fmt::format_context& ctx) const -> decltype(ctx.out());
};
