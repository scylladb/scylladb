/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/paged_read.hh"

#include <algorithm>
#include <cctype>
#include <map>
#include <random>
#include <ranges>
#include <set>
#include <stdexcept>
#include <type_traits>

#include <fmt/ranges.h>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "compaction/compaction_garbage_collector.hh"
#include "cql3/query_options.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/untyped_result_set.hh"
#include "locator/token_range_splitter.hh"
#include "query/query-result-writer.hh"
#include "query/query_result_merger.hh"
#include "query_ranges_to_vnodes.hh"
#include "readers/from_mutations.hh"
#include "readers/mutation_source.hh"
#include "replica/database.hh"
#include "replica/querier.hh"
#include "test/lib/read_page.hh"
#include "service/query_state.hh"
#include "service/read_page_resolution.hh"
#include "service/storage_proxy.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "transport/messages/result_message.hh"

namespace tests::paged_read {

namespace {

using read_model::answer_row;
using query_result = exceptions::coordinator_result<service::storage_proxy_coordinator_query_result>;

locator::host_id replica_id(size_t i) {
    return locator::host_id{utils::UUID(0, i + 1)};
}

// Replicas 0 to `count` - 1.
replica_set first_replicas(size_t count) {
    return ~replica_set() >> (max_replicas - count);
}

// `c` without replica `r`. The writes which only `r` holds disappear, and the
// replicas after `r` move down by one.
read_case without_replica(read_case c, size_t r) {
    const size_t block_for = c.options.replica_count - c.options.extra_replicas;
    placed_history h;
    for (const auto& w : c.history) {
        const auto rest = (w.replicas & first_replicas(r)) | ((w.replicas >> (r + 1)) << r);
        if (rest.any()) {
            h.push_back(placed_operation{w.op, rest});
        }
    }
    c.history = std::move(h);
    --c.options.replica_count;
    if (r >= block_for) {
        --c.options.extra_replicas;
    }
    return c;
}

// Splits the ring at fixed tokens, like the splitter of a ring of vnodes.
class fixed_token_splitter : public locator::token_range_splitter {
    // Sorted, without duplicates.
    std::vector<dht::token> _tokens;
    std::vector<dht::token>::const_iterator _next;
public:
    explicit fixed_token_splitter(std::vector<dht::token> tokens)
        : _tokens(std::move(tokens))
        , _next(_tokens.begin())
    { }
    void reset(dht::ring_position_view pos) override {
        _next = std::ranges::lower_bound(_tokens, pos.token());
    }
    std::optional<dht::token> next_token() override {
        if (_next == _tokens.end()) {
            return std::nullopt;
        }
        return *_next++;
    }
};

// Merges consecutive contiguous ranges, like
// storage_proxy::query_partition_key_range_concurrent() does with vnodes.
// storage_proxy merges two ranges only if enough replicas own both. The
// simulated replicas own every range, so this condition always holds. Like
// storage_proxy, the function does not merge a range which ends at the end of
// the ring, nor ranges which may be discontiguous.
dht::partition_range_vector merge_contiguous(const schema& s, dht::partition_range_vector ranges) {
    dht::partition_range_vector merged;
    for (auto& r : ranges) {
        if (!merged.empty()) {
            auto& last = merged.back();
            if (last.end() && r.start() && last.end()->value().equal(s, r.start()->value())
                    && (last.end()->is_inclusive() || r.start()->is_inclusive())) {
                last = dht::partition_range(last.start(), r.end());
                continue;
            }
        }
        merged.push_back(std::move(r));
    }
    return merged;
}

// A mutation source over `contents`. A reader reads the contents at the time
// it is created.
mutation_source make_source(lw_shared_ptr<const utils::chunked_vector<mutation>> contents) {
    return mutation_source([contents = std::move(contents)] (schema_ptr s, reader_permit permit, const dht::partition_range& range,
            const query::partition_slice& slice, tracing::trace_state_ptr, streamed_mutation::forwarding fwd, mutation_reader::forwarding) {
        return make_mutation_reader_from_mutations(std::move(s), std::move(permit), *contents, range, slice, fwd);
    });
}

// `m` without what lies at or after `pos` in the order of `query_schema`,
// which is reversed for a reversed query. `m` uses the table's schema. The
// partition tombstone precedes every position of the partition, and the static
// row precedes every clustering position.
mutation cut_before(const schema& query_schema, const mutation& m, position_in_partition_view pos) {
    const auto& s = *m.schema();
    const bool reversed = query_schema.version() != s.version();
    switch (pos.region()) {
    case partition_region::partition_start:
        return mutation(m.schema(), m.decorated_key());
    case partition_region::static_row: {
        mutation header(m.schema(), m.decorated_key());
        header.partition().apply(m.partition().partition_tombstone());
        return header;
    }
    case partition_region::clustered: {
        if (position_in_partition(pos).is_before_all_clustered_rows(query_schema)) {
            return m.sliced({});
        }
        // The positions before `pos` in the query's order.
        auto before = reversed
                ? position_range(position_in_partition(pos.reversed()), position_in_partition::after_all_clustered_rows())
                : position_range(position_in_partition::before_all_clustered_rows(), position_in_partition(pos));
        auto range = position_range_to_clustering_range(before, s);
        return m.sliced(range ? query::clustering_row_ranges{*range} : query::clustering_row_ranges{});
    }
    case partition_region::partition_end:
        return m;
    }
    std::abort();
}

// `cmd` without the row, partition and tombstone limits. It keeps the
// per-partition limit, and DISTINCT, which is a per-partition limit of one.
query::read_command without_page_limits(const query::read_command& cmd) {
    auto unlimited = cmd;
    unlimited.set_row_limit(query::max_rows);
    unlimited.partition_limit = query::max_partitions;
    unlimited.tombstone_limit = static_cast<uint64_t>(query::tombstone_limit::max);
    return unlimited;
}

// `cmd` without its row, partition, per-partition and tombstone limits. A
// DISTINCT query has a per-partition limit of one, so the command loses the
// distinct option too.
query::read_command without_limits(const query::read_command& cmd) {
    auto unlimited = without_page_limits(cmd);
    unlimited.slice.set_partition_row_limit(query::partition_max_rows);
    unlimited.slice.options.remove<query::partition_slice::option::distinct>();
    return unlimited;
}

int32_t pk_of(const read_model::operation& op) {
    return std::visit([] (const auto& o) { return o.pk; }, op);
}

// Upper bounds of what a read of the history can return: rows, rows of one
// partition, and partitions. They count every written row and static row,
// before deletions and expiry, because a replica may lack a deletion. A
// static row counts even in a partition with clustering rows, because the
// replicas which lack those rows may return a static-only row.
struct history_bounds {
    uint64_t rows = 0;
    uint64_t partition_rows = 0;
    uint64_t partitions = 0;
};

history_bounds bounds_of(const placed_history& h) {
    struct partition_rows {
        std::set<int32_t> clustering_keys;
        bool has_static = false;
    };
    std::map<int32_t, partition_rows> partitions;
    for (const auto& w : h) {
        auto& p = partitions[pk_of(w.op)];
        std::visit([&] (const auto& op) {
            using operation = std::decay_t<decltype(op)>;
            if constexpr (std::is_same_v<operation, read_model::regular_cell_write>
                    || std::is_same_v<operation, read_model::row_marker_write>) {
                p.clustering_keys.insert(op.ck);
            } else if constexpr (std::is_same_v<operation, read_model::static_cell_write>) {
                p.has_static = true;
            }
        }, w.op);
    }
    history_bounds b{.partitions = partitions.size()};
    for (const auto& [pk, rows] : partitions) {
        const uint64_t count = rows.clustering_keys.size() + rows.has_static;
        b.rows += count;
        b.partition_rows = std::max(b.partition_rows, count);
    }
    return b;
}

int32_t int_value(bytes_view b) {
    return value_cast<int32_t>(int32_type->deserialize(b));
}

std::string describe(const schema& s, const partition_key& pk) {
    auto components = pk.explode(s);
    return components.empty() ? "no partition" : fmt::format("pk {}", int_value(components.front()));
}

std::string describe(const schema& s, position_in_partition_view pos) {
    if (!pos.has_key()) {
        return fmt::format("{}", pos);
    }
    auto components = pos.key().explode(s);
    if (components.empty()) {
        return fmt::format("{}", pos);
    }
    return fmt::format("ck {} (weight {})", int_value(components.front()), int(pos.get_bound_weight()));
}

std::string describe(const schema& s, const std::optional<full_position>& pos) {
    if (!pos) {
        return "none";
    }
    return fmt::format("{}, {}", describe(s, pos->partition), describe(s, position_in_partition_view(pos->position)));
}

std::string describe(const schema& s, const std::optional<query::read_frontier>& frontier) {
    if (!frontier) {
        return "none";
    }
    return frontier->stop ? fmt::format("stop at {}", describe(s, frontier->stop)) : std::string("end of range");
}

// The skips of `r`, with the keys of their partitions. See
// reconcilable_result::skips().
std::vector<full_position> skips_of(const reconcilable_result& r) {
    return r.skips() | std::views::transform([&] (const partition_skip& skip) {
        return full_position(r.partitions()[skip.partition].mut().key(), skip.position);
    }) | std::ranges::to<std::vector>();
}

std::string describe(const schema& s, const reconcilable_result& r) {
    auto out = describe(s, r.frontier());
    for (const auto& skip : skips_of(r)) {
        out += fmt::format(", skip at {}", describe(s, std::optional<full_position>(skip)));
    }
    return out;
}

// The cursor of `r`, or its frontier if it holds one.
std::string describe_position(const schema& s, const query::result& r) {
    if (auto frontier = r.frontier()) {
        return fmt::format("frontier {}", describe(s, frontier));
    }
    return fmt::format("cursor {}", describe(s, r.last_position()));
}

std::string describe(const schema& s, const dht::partition_range& range) {
    if (range.is_singular() && range.start()->value().has_key()) {
        return describe(s, *range.start()->value().key());
    }
    return fmt::format("{}", range);
}

// A readable summary of a paging state, for traces and error messages. It
// omits the query id and the replicas, which the harness does not vary.
std::string describe(const schema& s, const service::pager::paging_state& state) {
    return fmt::format("{}, {}, remaining {}, rows fetched for the partition {}", describe(s, state.get_partition_key()),
            describe(s, state.get_position_in_partition()), state.get_remaining(), state.get_rows_fetched_for_last_partition());
}

std::string_view describe(query::short_read sr) {
    return sr ? "short" : "not short";
}

std::vector<answer_row> rows_of(::shared_ptr<cql_transport::messages::result_message> msg) {
    cql3::untyped_result_set rs(msg);
    std::vector<answer_row> rows;
    for (const auto& r : rs) {
        rows.push_back(answer_row{
            .pk = r.get_as<int32_t>("pk"),
            .ck = r.get_opt<int32_t>("ck"),
            .s = r.get_opt<int32_t>("s"),
            .v1 = r.get_opt<int32_t>("v1"),
            .v2 = r.get_opt<int32_t>("v2"),
        });
    }
    return rows;
}

lw_shared_ptr<const service::pager::paging_state> paging_state_of(const ::shared_ptr<cql_transport::messages::result_message>& msg) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    if (!rows) {
        throw std::runtime_error("The statement did not return rows");
    }
    return rows->rs().get_metadata().paging_state();
}

// The coordinator of the simulated reads. Its query() replaces
// storage_proxy::query_result().
class coordinator {
    schema_ptr _schema;
    // The mutations of each replica, sorted in ring order. Applied repairs
    // change them.
    std::vector<lw_shared_ptr<utils::chunked_vector<mutation>>> _contents;
    std::vector<mutation_source> _replicas;
    // The merged contents of all replicas. A repair must not change them.
    utils::chunked_vector<mutation> _merged;
    // The time relative to which the values of the contents expire.
    gc_clock::time_point _query_time;
    const read_options& _opts;
    const history_bounds _bounds;
    tests::reader_concurrency_semaphore_wrapper _semaphore;
    // Replies and results hold memory units of this limiter, so it must
    // outlive them.
    query::result_memory_limiter _limiter{query::result_memory_limiter::maximum_result_size * 100};
    std::vector<std::string> _trace;
    size_t _repair_mutations = 0;
    std::vector<std::string> _violations;
    // The random choices of the reads. Without a schedule seed, every choice
    // is the canonical one.
    std::optional<std::mt19937> _schedule;
    // The vnode boundaries at which a scan splits, sorted. The same for all
    // pages.
    std::vector<dht::token> _split_tokens;
    // Whether a round of a scan merges its contiguous ranges, as with vnodes,
    // or reads them apart, as with tablets.
    bool _merge_ranges = false;
    // The replica which the coordinator runs on, if any. This replica reads
    // reversed queries in the native format even without
    // native_reverse_queries.
    std::optional<size_t> _local_replica;
    // The querier cache of each replica, if the replicas keep queriers. The
    // cached queriers hold permits of `_semaphore`, so the caches must be
    // destroyed before it.
    std::vector<std::unique_ptr<replica::querier_cache>> _caches;

    template <typename... Args>
    void trace(fmt::format_string<Args...> format, Args&&... args) {
        _trace.push_back(fmt::format(format, std::forward<Args>(args)...));
    }

    // A choice of 0 to `count` - 1, or `canonical` without a schedule.
    size_t choose(size_t count, size_t canonical) {
        if (!_schedule) {
            return canonical;
        }
        return std::uniform_int_distribution<size_t>(0, count - 1)(*_schedule);
    }

    // Shuffles `v`, or keeps its order without a schedule.
    template <typename T>
    void shuffle(std::vector<T>& v) {
        if (_schedule) {
            std::ranges::shuffle(v, *_schedule);
        }
    }

    // Records a violated property. Each property appears once, so that the
    // kind of a failure does not depend on how often it occurred.
    void violation(std::string what) {
        trace("  violation: {}", what);
        if (std::ranges::find(_violations, what) == _violations.end()) {
            _violations.push_back(std::move(what));
        }
    }

    // Checks that `result`, which the coordinator returns for a read of one
    // range with `cmd`, stays within the limits of `cmd`. A reconciliation
    // round may retry with larger limits, but the result must still respect
    // the limits of `cmd`.
    void check_limits(const query::read_command& cmd, query::result& result) {
        result.ensure_counts();
        if (*result.row_count() > cmd.get_row_limit()) {
            violation("A result has more rows than the row limit of its command");
        }
        if (*result.partition_count() > cmd.partition_limit) {
            violation("A result has more partitions than the partition limit of its command");
        }
    }

    size_t replica_of(locator::host_id host) const {
        for (size_t i = 0; i < _replicas.size(); ++i) {
            if (replica_id(i) == host) {
                return i;
            }
        }
        throw std::runtime_error(fmt::format("Unknown replica {}", host));
    }

    // `m` with its expired values turned into tombstones and its shadowed
    // data dropped, without tombstone GC. Mutations whose contents are the
    // same at the query time are equal after this.
    mutation normalized(mutation m) const {
        m.partition().compact_for_compaction(*m.schema(), never_gc, m.decorated_key(), _query_time, tombstone_gc_state::no_gc());
        return m;
    }

    // Checks the repair mutation `diff` of `replica`: the merged contents of
    // the replicas must already contain it. With apply_repairs, applies it to
    // the replica.
    void repair(size_t replica, const mutation& diff) {
        auto same_partition = [&] (const mutation& m) { return m.decorated_key().equal(*_schema, diff.decorated_key()); };
        auto merged = std::ranges::find_if(_merged, same_partition);
        if (merged == _merged.end()) {
            trace("  the repair mutation of replica {} is for a partition which no replica has: {}", replica, diff);
            violation("A repair mutation adds data which no replica has");
        } else {
            auto with_diff = *merged;
            with_diff.apply(diff);
            if (normalized(std::move(with_diff)) != normalized(*merged)) {
                trace("  the repair mutation of replica {} adds data which no replica has: {}", replica, diff);
                violation("A repair mutation adds data which no replica has");
            }
        }
        if (_opts.apply_repairs) {
            auto& contents = *_contents[replica];
            if (auto it = std::ranges::find_if(contents, same_partition); it != contents.end()) {
                it->apply(diff);
            } else {
                contents.push_back(diff);
                std::sort(contents.begin(), contents.end(), mutation_decorated_key_less_comparator());
            }
        }
    }

    // The contents of `replica` which `frontier` and `skips` cover. See
    // query::read_frontier and reconcilable_result::skips().
    //
    // With `enter_stop_partition`, the covered contents of the stop's
    // partition get a partition tombstone older than every write. It makes a
    // read enter the partition without changing its rows. See
    // check_data_frontier().
    mutation_source covered_contents(size_t replica, const schema& query_schema, const query::read_frontier& frontier,
            const std::vector<full_position>& skips, bool enter_stop_partition = false) const {
        auto covered = make_lw_shared<utils::chunked_vector<mutation>>();
        for (const auto& m : *_contents[replica]) {
            const auto after = [&] (const full_position& pos) {
                return m.key().ring_order_tri_compare(*_schema, pos.partition);
            };
            if (frontier.stop && after(*frontier.stop) > 0) {
                break;
            }
            const auto skip = std::ranges::find_if(skips, [&] (const full_position& pos) { return after(pos) == 0; });
            if (frontier.stop && after(*frontier.stop) == 0) {
                covered->push_back(cut_before(query_schema, m, frontier.stop->position));
                if (enter_stop_partition) {
                    covered->back().partition().apply(tombstone(api::min_timestamp, gc_clock::time_point()));
                }
            } else if (skip != skips.end()) {
                covered->push_back(cut_before(query_schema, m, skip->position));
            } else {
                covered->push_back(m);
            }
        }
        return make_source(std::move(covered));
    }

    // Checks the properties of the frontier of a reply of `replica` to a read
    // of `range` with `cmd` which a new querier cannot check: the reply has a
    // frontier if and only if the command asked for one, a short reply
    // stopped, the skips are in order before the stop, and the stop is after
    // the page's start. Returns whether the reply has a frontier.
    bool check_frontier_shape(size_t replica, const schema& query_schema, const query::read_command& cmd, const dht::partition_range& range,
            const std::optional<query::read_frontier>& frontier, const std::vector<full_position>& skips, query::short_read short_read) {
        if (!cmd.slice.options.contains<query::partition_slice::option::send_read_frontier>()) {
            if (frontier || !skips.empty()) {
                violation("A reply has a frontier which its command did not ask for");
            }
            return false;
        }
        if (!frontier) {
            violation("A reply has no frontier");
            return false;
        }
        const auto ring_cmp = [&] (const full_position& a, const full_position& b) {
            return a.partition.ring_order_tri_compare(query_schema, b.partition);
        };
        if (short_read && frontier->reached_end()) {
            trace("  replica {} returns a short reply which reached the end of its range", replica);
            violation("A short reply has a frontier at the end of its range");
        }
        for (size_t i = 0; i < skips.size(); ++i) {
            const auto& skip = skips[i];
            if ((i > 0 && ring_cmp(skips[i - 1], skip) >= 0) || (frontier->stop && ring_cmp(skip, *frontier->stop) >= 0)
                    || skip.position.region() != partition_region::clustered) {
                trace("  replica {} returns the frontier {} with a skip at {}", replica, describe(query_schema, frontier),
                        describe(query_schema, std::optional<full_position>(skip)));
                violation("A frontier has a skip out of order or outside the clustering rows");
            }
        }
        // The page starts inside the first partition of the range if the
        // range includes it. Its fragments before the start of its first
        // clustering range are the partition's state, which an earlier page
        // already read.
        if (auto pk = query_result_builder::start_partition_of(range); pk && frontier->stop) {
            const auto& ranges = cmd.slice.row_ranges(query_schema, *pk);
            const full_position start(*pk, ranges.empty() ? position_in_partition::after_all_clustered_rows()
                    : position_in_partition::for_range_start(ranges.front()));
            if (full_position::cmp(query_schema, *frontier->stop, start) <= 0) {
                trace("  replica {} returns the frontier {}, which does not move past the page's start {}", replica, describe(query_schema, frontier),
                        describe(query_schema, std::optional<full_position>(start)));
                violation("A frontier does not move past the start of its page");
            }
        }
        return true;
    }

    // Checks the frontier of `reply`, which `replica` returned for a read of
    // `range` with `cmd` and `opts`: a new querier which reads only what the
    // frontier covers, without the row and partition limits, returns the same
    // page and digest. A data reply does not say where the read left
    // partitions at the per-partition limit, so that read keeps the
    // per-partition limit.
    void check_data_frontier(size_t replica, const schema_ptr& query_schema, const query::read_command& cmd, query::result_options opts,
            const dht::partition_range& range, const query::result& reply) {
        if (!check_frontier_shape(replica, *query_schema, cmd, range, reply.frontier(), {}, reply.is_short_read())) {
            return;
        }
        auto read_covered = [&] (bool enter_stop_partition) {
            auto permit = _semaphore.make_permit();
            permit.set_max_result_size(query::max_result_size(query::result_memory_limiter::unlimited_result_size));
            return tests::read_data_page(covered_contents(replica, *query_schema, *reply.frontier(), {}, enter_stop_partition), query_schema,
                    std::move(permit), without_page_limits(cmd), opts, {range}, {},
                    query::result_memory_accounter{query::result_memory_limiter::unlimited_result_size}, tombstone_gc_state::no_gc(), {}, nullptr).get();
        };
        auto covered = read_covered(false);
        // A page enters the partition which it stops in, because it stops
        // after a fragment of the partition which reached its consumer. The
        // digest covers the key of each partition which the page enters, also
        // when the page has no row of it. The fragment can be a range
        // tombstone change at the stop, which the covered contents lack. The
        // read then enters the partition only with a tombstone which makes it
        // enter.
        if (reply.buf() == covered->buf() && reply.digest() != covered->digest() && reply.frontier()->stop) {
            covered = read_covered(true);
        }
        if (!(reply.buf() == covered->buf()) || reply.digest() != covered->digest()) {
            trace("  replica {} returns {}, with the frontier {}", replica, reply.pretty_printer(query_schema, cmd.slice),
                    describe(*query_schema, reply.frontier()));
            trace("  replica {} holds {} within the frontier", replica, covered->pretty_printer(query_schema, cmd.slice));
            violation("A reply differs from a read of what its frontier covers");
        }
    }

    // Like check_data_frontier(), for a mutation page. The covered contents
    // end at the reply's skips, so the read has no limits.
    void check_mutation_frontier(size_t replica, const schema_ptr& query_schema, const query::read_command& cmd, const dht::partition_range& range,
            const reconcilable_result& reply) {
        const auto skips = skips_of(reply);
        if (!check_frontier_shape(replica, *query_schema, cmd, range, reply.frontier(), skips, reply.is_short_read())) {
            return;
        }
        auto permit = _semaphore.make_permit();
        permit.set_max_result_size(query::max_result_size(query::result_memory_limiter::unlimited_result_size));
        auto covered = tests::read_mutation_page(covered_contents(replica, *query_schema, *reply.frontier(), skips), query_schema, std::move(permit),
                without_limits(cmd), range, {}, query::result_memory_accounter{query::result_memory_limiter::unlimited_result_size},
                tombstone_gc_state::no_gc(), {}, nullptr).get();
        if (!(reply == covered)) {
            trace("  replica {} returns {}", replica, reply.pretty_printer(query_schema));
            trace("  replica {} holds {} within the frontier", replica, covered.pretty_printer(query_schema));
            violation("A reply differs from a read of what its frontier covers");
        }
    }

    // The digest algorithm which the replicas use, like storage_proxy's
    // digest_algorithm().
    query::digest_algorithm digest_algorithm() const {
        return _opts.read_frontiers ? query::digest_algorithm::xxHash_without_empty_partitions : query::digest_algorithm::xxHash;
    }

    // Whether a read of `cmd` from `replica` uses the legacy reversed format.
    bool legacy_format(size_t replica, const query::read_command& cmd) const {
        return cmd.slice.is_reversed() && !_opts.native_reverse_queries && _local_replica != replica;
    }

    // The command which `replica` executes for `cmd`. Without
    // native_reverse_queries, a replica other than the coordinator's own
    // receives a reversed command in the legacy format, like from
    // abstract_read_executor::make_data_request(). It converts the command
    // back, like storage_proxy::handle_read().
    lw_shared_ptr<const query::read_command> command_on_replica(size_t replica, const query::read_command& cmd) {
        if (!legacy_format(replica, cmd)) {
            return make_lw_shared<query::read_command>(cmd);
        }
        trace("  replica {} receives the legacy reversed format", replica);
        auto legacy = reversed(make_lw_shared<query::read_command>(cmd));
        // handle_read() recognizes the legacy format by the table's schema
        // version.
        if (legacy->schema_version != _schema->version()) {
            throw std::runtime_error("A command in the legacy reversed format must have the schema version of the table");
        }
        return reversed(std::move(legacy));
    }

    // The querier cache which a read of `cmd` from `replica` uses, if any.
    // Like replica::database, only a paged query saves its queriers.
    replica::querier_cache* querier_cache_of(size_t replica, const query::read_command& cmd) {
        return _caches.empty() || !cmd.query_uuid ? nullptr : _caches[replica].get();
    }

    void trace_lookup(size_t replica, const std::optional<replica::querier>& saved) {
        trace("  replica {} {}", replica, saved ? "reuses its cached querier" : "has no usable cached querier");
    }

    // Runs `read`, and then closes the querier in `saved`, like
    // replica::database::query().
    template <typename Func>
    static std::invoke_result_t<Func> with_saved_querier(std::optional<replica::querier>& saved, Func read) {
        std::exception_ptr ex;
        std::optional<std::invoke_result_t<Func>> result;
        try {
            result.emplace(read());
        } catch (...) {
            ex = std::current_exception();
        }
        if (saved) {
            saved->close().get();
        }
        if (ex) {
            std::rethrow_exception(std::move(ex));
        }
        return std::move(*result);
    }

    // Reads a data or digest page of `range` from `replica`, like
    // replica::database::query() and replica::table::query(). With a querier
    // cache, the read continues with the querier which the replica saved on
    // an earlier page, if the cache still has it. It then saves its querier
    // for the next page.
    lw_shared_ptr<query::result> read_data(size_t replica, schema_ptr query_schema, const query::read_command& coordinator_cmd, query::result_options opts,
            const dht::partition_range& range) {
        const auto replica_cmd = command_on_replica(replica, coordinator_cmd);
        const auto& cmd = *replica_cmd;
        const auto max_size = cmd.max_result_size.value();
        const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
        auto accounter = (opts.request == query::result_request::only_digest
                ? _limiter.new_digest_read(max_size, short_read_allowed)
                : _limiter.new_data_read(max_size, short_read_allowed)).get();
        auto* cache = querier_cache_of(replica, cmd);
        std::optional<replica::querier> saved;
        if (cache && !cmd.is_first_page) {
            saved = cache->lookup_data_querier(cmd.query_uuid, *query_schema, range, cmd.slice, _semaphore.semaphore(), {}, db::no_timeout);
            trace_lookup(replica, saved);
        }
        const bool reused = bool(saved);
        auto result = with_saved_querier(saved, [&] {
            auto permit = saved ? saved->permit() : _semaphore.make_permit();
            permit.set_max_result_size(max_size);
            auto result = tests::read_data_page(_replicas[replica], query_schema, std::move(permit), cmd, opts, {range}, {}, std::move(accounter),
                    tombstone_gc_state::no_gc(), {}, cache ? &saved : nullptr).get();
            if (cache && saved) {
                cache->insert_data_querier(cmd.query_uuid, std::move(*saved), {});
            }
            return result;
        });
        if (reused) {
            // A cached querier must not change what a page returns.
            auto fresh_accounter = (opts.request == query::result_request::only_digest
                    ? _limiter.new_digest_read(max_size, short_read_allowed)
                    : _limiter.new_data_read(max_size, short_read_allowed)).get();
            auto permit = _semaphore.make_permit();
            permit.set_max_result_size(max_size);
            auto fresh = tests::read_data_page(_replicas[replica], query_schema, std::move(permit), cmd, opts, {range}, {}, std::move(fresh_accounter),
                    tombstone_gc_state::no_gc(), {}, nullptr).get();
            const auto& pos = result->wire_position();
            const auto& fresh_pos = fresh->wire_position();
            const bool same_position = bool(result->frontier()) == bool(fresh->frontier())
                    && bool(pos) == bool(fresh_pos) && (!pos || full_position::cmp(*query_schema, *pos, *fresh_pos) == 0);
            if (!(result->buf() == fresh->buf()) || !same_position || result->is_short_read() != fresh->is_short_read()
                    || result->digest() != fresh->digest()) {
                trace("  replica {} returns with its cached querier: {}, {}, {}", replica, result->pretty_printer(query_schema, cmd.slice),
                        describe(result->is_short_read()), describe_position(*query_schema, *result));
                trace("  replica {} returns with a new querier: {}, {}, {}", replica, fresh->pretty_printer(query_schema, cmd.slice),
                        describe(fresh->is_short_read()), describe_position(*query_schema, *fresh));
                violation("A page read with a cached querier differs from the page read with a new querier");
            }
        }
        check_data_frontier(replica, query_schema, cmd, opts, range, *result);
        return result;
    }

    // Reads a mutation page of `range` from `replica`, like
    // replica::database::query_mutations() and replica::table::mutation_query().
    // See read_data() for the querier cache.
    reconcilable_result read_mutations(size_t replica, schema_ptr query_schema, const query::read_command& coordinator_cmd, const dht::partition_range& range) {
        const auto replica_cmd = command_on_replica(replica, coordinator_cmd);
        const auto& cmd = *replica_cmd;
        const auto max_size = cmd.max_result_size.value();
        const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
        auto accounter = _limiter.new_mutation_read(max_size, short_read_allowed).get();
        auto* cache = querier_cache_of(replica, cmd);
        std::optional<replica::querier> saved;
        if (cache && !cmd.is_first_page) {
            saved = cache->lookup_mutation_querier(cmd.query_uuid, *query_schema, range, cmd.slice, _semaphore.semaphore(), {}, db::no_timeout);
            trace_lookup(replica, saved);
        }
        const bool reused = bool(saved);
        auto reply = with_saved_querier(saved, [&] {
            auto permit = saved ? saved->permit() : _semaphore.make_permit();
            permit.set_max_result_size(max_size);
            auto result = tests::read_mutation_page(_replicas[replica], query_schema, std::move(permit), cmd, range, {}, std::move(accounter),
                    tombstone_gc_state::no_gc(), {}, cache ? &saved : nullptr).get();
            if (cache && saved) {
                cache->insert_mutation_querier(cmd.query_uuid, std::move(*saved), {});
            }
            return result;
        });
        if (reused) {
            // A cached querier must not change what a page returns.
            auto permit = _semaphore.make_permit();
            permit.set_max_result_size(max_size);
            auto fresh = tests::read_mutation_page(_replicas[replica], query_schema, std::move(permit), cmd, range, {},
                    _limiter.new_mutation_read(max_size, short_read_allowed).get(), tombstone_gc_state::no_gc(), {}, nullptr).get();
            const auto frontier = reply.frontier();
            const auto fresh_frontier = fresh.frontier();
            const bool same_frontier = bool(frontier) == bool(fresh_frontier) && (!frontier || frontier->equal(*query_schema, *fresh_frontier));
            const auto skips = skips_of(reply);
            const auto fresh_skips = skips_of(fresh);
            const bool same_skips = std::ranges::equal(skips, fresh_skips, [&] (const full_position& a, const full_position& b) {
                return full_position::cmp(*query_schema, a, b) == 0;
            });
            if (!(reply == fresh) || reply.row_count() != fresh.row_count() || reply.is_short_read() != fresh.is_short_read()
                    || !same_frontier || !same_skips) {
                trace("  replica {} returns with its cached querier: {}", replica, reply.pretty_printer(query_schema));
                trace("  replica {} returns with a new querier: {}", replica, fresh.pretty_printer(query_schema));
                violation("A page read with a cached querier differs from the page read with a new querier");
            }
        }
        check_mutation_frontier(replica, query_schema, cmd, range, reply);
        if (legacy_format(replica, coordinator_cmd)) {
            // Like handle_read(), the replica returns the mutations in the
            // legacy format, and the coordinator converts them back, like
            // make_mutation_data_request().
            auto legacy = reversed(make_foreign(make_lw_shared<reconcilable_result>(std::move(reply)))).get();
            return std::move(*reversed(std::move(legacy)).get());
        }
        return reply;
    }

    // The limits of a reconciliation round, which decide its replies. Each
    // limit is clamped to one above the history's bound: higher limits read
    // the same rows, and one above the bound shows that the read reached the
    // end.
    struct round_limits {
        uint64_t rows;
        uint64_t partition_rows;
        uint64_t partitions;
        bool short_reads;
        auto operator<=>(const round_limits&) const = default;
    };

    round_limits limits_of(const query::read_command& cmd) const {
        return {
            std::min<uint64_t>(cmd.get_row_limit(), _bounds.rows + 1),
            std::min<uint64_t>(cmd.slice.partition_row_limit(), _bounds.partition_rows + 1),
            std::min<uint64_t>(cmd.partition_limit, _bounds.partitions + 1),
            cmd.slice.options.contains<query::partition_slice::option::allow_short_read>(),
        };
    }

    // Checks and counts the repair mutations of an accepted reconciled page,
    // and returns the number of them.
    size_t repair(const service::mutations_per_partition_key_map& repair_diffs) {
        size_t repairs = 0;
        for (const auto& [pk, diffs] : repair_diffs) {
            for (const auto& [host, diff] : diffs) {
                if (diff) {
                    ++repairs;
                    repair(replica_of(host), *diff);
                }
            }
        }
        _repair_mutations += repairs;
        return repairs;
    }

    // Reconciles the mutation pages of `range` from the replicas `targets`,
    // like abstract_read_executor::reconcile_by_frontiers().
    foreign_ptr<lw_shared_ptr<query::result>> reconcile_by_frontiers(schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd,
            const dht::partition_range& range, const std::vector<size_t>& targets) {
        const auto& s = *query_schema;
        trace("  reconciling replicas {} by their frontiers", fmt::join(targets, ", "));
        service::frontier_reconciliation reconciliation(query_schema, cmd, range);
        // The replicas' contents do not change during a reconciliation,
        // because repairs apply only to an accepted page. A round's range,
        // slice and limits decide its replies. So a round which repeats them
        // gets the same replies as an earlier round, and the reconciliation
        // makes no progress.
        std::set<std::pair<std::string, round_limits>> seen;
        for (size_t round = 1;; ++round) {
            const auto& round_cmd = *reconciliation.round_command();
            const auto& round_range = reconciliation.round_range();
            if (!seen.emplace(fmt::format("{}, {}", describe(s, round_range), round_cmd.slice), limits_of(round_cmd)).second) {
                throw std::runtime_error(fmt::format("Reconciliation round {} repeats the range, slice and limits of an earlier round", round));
            }
            if (round > 1) {
                trace("  round {}: range {}, slice {}", round, describe(s, round_range), round_cmd.slice);
            }
            std::vector<service::mutation_page_reply> replies;
            for (auto i : targets) {
                auto reply = read_mutations(i, query_schema, round_cmd, round_range);
                trace("  round {}: replica {} mutations: {} partitions, {} rows, {}, frontier {}", round, i, reply.partitions().size(), reply.row_count(),
                        describe(reply.is_short_read()), describe(s, reply));
                replies.push_back({replica_id(i), make_foreign(make_lw_shared<reconcilable_result>(std::move(reply)))});
            }
            // The reconciliation gets the replies in the order of their
            // arrival.
            shuffle(replies);
            auto page = reconciliation.add_round(std::move(replies)).get();
            if (page) {
                const auto repairs = repair(page->repair_diffs);
                trace("  round {}: accepted {} rows, {}, cursor {}, {} repair mutations", round, page->result.row_count().value_or(0),
                        describe(page->result.is_short_read()), describe(s, page->result.last_position()), repairs);
                return make_foreign(make_lw_shared<query::result>(std::move(page->result)));
            }
        }
    }

    // Reconciles the mutation pages of `range` from the replicas `targets`,
    // like abstract_read_executor::reconcile().
    foreign_ptr<lw_shared_ptr<query::result>> reconcile(schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& range,
            const std::vector<size_t>& targets) {
        if (_opts.read_frontiers) {
            return reconcile_by_frontiers(std::move(query_schema), std::move(cmd), range, targets);
        }
        const auto& s = *query_schema;
        trace("  reconciling replicas {}", fmt::join(targets, ", "));
        // The first round sends the client's command.
        auto round_cmd = cmd;
        // The replicas' contents do not change during a reconciliation,
        // because repairs apply only to an accepted page. So a round whose
        // limits repeat an earlier round's gets the same replies, and the
        // reconciliation makes no progress.
        std::set<round_limits> seen;
        for (size_t round = 1;; ++round) {
            if (!seen.insert(limits_of(*round_cmd)).second) {
                throw std::runtime_error(fmt::format("Reconciliation round {} repeats the limits of an earlier round", round));
            }
            service::prepare_mutation_read(*round_cmd, _opts.empty_replica_mutation_pages);
            std::vector<service::mutation_page_reply> replies;
            for (auto i : targets) {
                auto reply = read_mutations(i, query_schema, *round_cmd, range);
                trace("  round {}: replica {} mutations: {} partitions, {} rows, {}, frontier {}", round, i, reply.partitions().size(), reply.row_count(),
                        describe(reply.is_short_read()), describe(s, reply));
                replies.push_back({replica_id(i), make_foreign(make_lw_shared<reconcilable_result>(std::move(reply)))});
            }
            // The resolution gets the replies in the order of their arrival.
            shuffle(replies);
            auto resolution = service::resolve_mutation_page(query_schema, *cmd, *round_cmd, std::move(replies)).get();
            if (auto* page = std::get_if<service::accepted_mutation_page>(&resolution)) {
                const auto repairs = repair(page->repair_diffs);
                trace("  round {}: accepted {} rows, {}, cursor {}, {} repair mutations", round, page->result.row_count().value_or(0),
                        describe(page->result.is_short_read()), describe(s, page->result.last_position()), repairs);
                return make_foreign(make_lw_shared<query::result>(std::move(page->result)));
            }
            round_cmd = std::get<service::mutation_page_retry>(resolution).cmd;
            trace("  round {}: retry with row limit {}, per-partition limit {}, partition limit {}", round, round_cmd->get_row_limit(),
                    round_cmd->slice.partition_row_limit(), round_cmd->partition_limit);
        }
    }

    // Reads `range` from all replicas, like abstract_read_executor::execute().
    //
    // The replicas which count toward the consistency level are the first
    // ones. The extra replicas are the last ones. All replicas read their
    // pages first. Their replies then arrive in the order of the schedule.
    // Some replies may arrive after the consistency level is reached but
    // before the decision runs.
    foreign_ptr<lw_shared_ptr<query::result>> read_range(schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& range) {
        const auto& s = *query_schema;
        trace("range {}", describe(s, range));
        const size_t targets = _replicas.size();
        const size_t block_for = targets - _opts.extra_replicas;

        // A replica which counts toward the consistency level gets a data
        // request. Another replica may get one too, like with
        // always_speculating_read_executor.
        std::vector<bool> wants_data(targets);
        wants_data[choose(block_for, 0)] = true;
        if (targets > 1 && choose(2, 0)) {
            auto others = std::views::iota(size_t(0), targets)
                    | std::views::filter([&] (size_t i) { return !wants_data[i]; })
                    | std::ranges::to<std::vector>();
            wants_data[others[choose(others.size(), 0)]] = true;
        }

        // The data requests also ask for a digest when there is more than one
        // replica, like abstract_read_executor::make_requests().
        const auto data_opts = targets > 1
                ? query::result_options{query::result_request::result_and_digest, digest_algorithm()}
                : query::result_options{query::result_request::only_result, query::digest_algorithm::none};
        struct first_round_reply {
            size_t replica;
            bool data;
            lw_shared_ptr<query::result> result;
        };
        std::vector<first_round_reply> first_round;
        for (size_t i = 0; i < targets; ++i) {
            if (wants_data[i]) {
                auto data = read_data(i, query_schema, *cmd, data_opts, range);
                trace("  replica {} data: {} rows, {}, {}", i, data->row_count().value_or(0), describe(data->is_short_read()),
                        describe_position(s, *data));
                first_round.push_back({i, true, std::move(data)});
            } else {
                auto digest = read_data(i, query_schema, *cmd, query::result_options::only_digest(digest_algorithm()), range);
                // Like storage_proxy::query_result_local_digest(), the reply
                // omits the short-read flag. Only the trace shows it. A
                // digest result does not count its rows.
                trace("  replica {} digest: {}, {}", i, describe(digest->is_short_read()), describe_position(s, *digest));
                first_round.push_back({i, false, std::move(digest)});
            }
        }
        shuffle(first_round);

        service::foreground_reply_collector replies(query_schema, block_for);
        replies.add_wait_targets(targets);
        auto decision = replies.has_cl().then([&] (exceptions::coordinator_result<service::digest_read_result> cl_result) {
            return service::decide_digest_page(s, *cmd, std::move(cl_result).value(), replies, _opts.empty_replica_pages, _opts.read_frontiers);
        });
        auto deliver = [&] (first_round_reply& r) {
            const bool counts_for_cl = r.replica < block_for;
            if (r.data) {
                replies.add_data(counts_for_cl, make_foreign(std::move(r.result)));
            } else {
                // Like storage_proxy::query_result_local_digest() and
                // abstract_read_executor::make_digest_requests(), the reply
                // carries the position of the wire, which the command tells
                // the meaning of.
                auto pos = r.result->wire_position();
                if (cmd->slice.options.contains<query::partition_slice::option::send_read_frontier>()) {
                    replies.add_digest(counts_for_cl, *r.result->digest(), r.result->last_modified(), std::nullopt, query::read_frontier{std::move(pos)});
                } else {
                    replies.add_digest(counts_for_cl, *r.result->digest(), r.result->last_modified(), std::move(pos), std::nullopt);
                }
            }
        };
        // The replies reach the consistency level when all replicas which
        // count toward it and a replica with a data request have replied.
        size_t delivered = 0;
        size_t cl_replies = 0;
        bool has_data = false;
        while (cl_replies < block_for || !has_data) {
            auto& r = first_round[delivered++];
            cl_replies += r.replica < block_for;
            has_data |= r.data;
            deliver(r);
        }
        const size_t at_cl = delivered;
        // More replies may arrive before the continuation which decides the
        // page runs. The decision does not wait for the others.
        const size_t before_decision = at_cl + choose(targets - at_cl + 1, targets - at_cl);
        for (; delivered < before_decision; ++delivered) {
            deliver(first_round[delivered]);
        }
        trace("  replies from replicas {}; consistency level after {}, decision after {}",
                fmt::join(first_round | std::views::transform(&first_round_reply::replica), ", "), at_cl, delivered);

        auto page = decision.get();
        if (auto* accepted = std::get_if<service::accepted_digest_page>(&page)) {
            trace("  digests match, cursor {}", describe(s, accepted->result->last_position()));
            check_limits(*cmd, *accepted->result);
            return std::move(accepted->result);
        }
        trace("  digests differ");
        // The schedule chooses the targets of the reconciliation:
        // 0. All replicas, like abstract_read_executor::reconcile().
        // 1. The replicas which replied before the decision, like a
        //    speculating executor, which reconciles its used_targets().
        // 2. The replicas which count toward the consistency level, like a
        //    LOCAL consistency level, which may leave out the replicas of
        //    other datacenters.
        std::vector<size_t> reconciliation_targets;
        switch (choose(3, 0)) {
        case 0:
            reconciliation_targets = std::views::iota(size_t(0), targets) | std::ranges::to<std::vector>();
            break;
        case 1:
            reconciliation_targets = first_round | std::views::take(delivered) | std::views::transform(&first_round_reply::replica)
                    | std::ranges::to<std::vector>();
            std::ranges::sort(reconciliation_targets);
            break;
        default:
            reconciliation_targets = std::views::iota(size_t(0), block_for) | std::ranges::to<std::vector>();
        }
        auto result = reconcile(query_schema, cmd, range, reconciliation_targets);
        check_limits(*cmd, *result);
        return result;
    }

    // When a short page has no cursor, the pager computes the position from
    // the page's last partition with
    // query::result_view::calculate_last_position(). If the page has no
    // partition, that function fails an assertion. The harness reports such
    // a page as a failed read instead.
    query_result finish(const schema& s, foreign_ptr<lw_shared_ptr<query::result>> result) {
        result->ensure_counts();
        trace("result: {} partitions, {} rows, {}, cursor {}", *result->partition_count(), *result->row_count(), describe(result->is_short_read()),
                describe(s, result->last_position()));
        if (result->is_short_read() && !result->last_position() && !*result->partition_count()) {
            throw std::runtime_error("The result is short, but has neither a partition nor a cursor to continue from");
        }
        return service::storage_proxy_coordinator_query_result(std::move(result));
    }

    // Like storage_proxy::do_query(), query_singular() and
    // query_partition_key_range(). A scan splits only at the vnode
    // boundaries of the schedule.
    query_result do_query(schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector ranges) {
        // A replica may evict one or all of its cached queriers between
        // pages, as when it needs their memory.
        for (size_t i = 0; i < _caches.size(); ++i) {
            size_t evicted = 0;
            switch (choose(3, 0)) {
            case 1:
                evicted += _caches[i]->evict_one().get();
                break;
            case 2:
                while (_caches[i]->evict_one().get()) {
                    ++evicted;
                }
                break;
            }
            if (evicted) {
                trace("replica {} evicted {} cached queriers", i, evicted);
            }
        }
        const auto& slice = cmd->slice;
        if (ranges.empty() || (slice.default_row_ranges().empty() && !slice.get_specific_ranges())) {
            trace("nothing to read");
            return service::storage_proxy_coordinator_query_result(make_foreign(make_lw_shared<query::result>()));
        }
        if (_opts.page_size_in_bytes) {
            const auto configured = cmd->max_result_size.value();
            cmd->max_result_size = query::max_result_size(configured.soft_limit, configured.hard_limit, *_opts.page_size_in_bytes);
        }
        // Like storage_proxy::do_query(), which asks the replicas for
        // frontiers when read_frontiers is enabled.
        if (_opts.read_frontiers) {
            cmd->slice.options.set<query::partition_slice::option::send_read_frontier>();
        }
        // Like storage_proxy::get_tombstone_limit(), which gives the statement
        // the configured limit only when empty_replica_pages is enabled.
        if (!_opts.empty_replica_pages) {
            cmd->tombstone_limit = static_cast<uint64_t>(query::tombstone_limit::max);
        } else if (_opts.tombstone_limit) {
            cmd->tombstone_limit = *_opts.tombstone_limit;
        }
        trace("command: row limit {}, partition limit {}, {}, slice {}", cmd->get_row_limit(), cmd->partition_limit,
                slice.options.contains<query::partition_slice::option::allow_short_read>() ? "short reads allowed" : "no short reads", slice);

        if (query::is_single_partition(ranges.front())) {
            if (!std::ranges::all_of(ranges, &dht::partition_range::is_singular)) {
                throw std::runtime_error("mixed singular and non singular range are not supported");
            }
            query::result_merger merger(cmd->get_row_limit(), cmd->partition_limit);
            for (const auto& range : ranges) {
                merger(read_range(query_schema, cmd, range));
            }
            return finish(*query_schema, merger.get());
        }

        // query_partition_key_range() splits the ranges at vnode boundaries.
        // query_partition_key_range_concurrent() reads them in rounds of
        // growing concurrency, with the remaining limits.
        if (!_split_tokens.empty()) {
            trace("scan split at tokens {}, {}", fmt::join(_split_tokens | std::views::transform(&dht::token::raw), ", "),
                    _merge_ranges ? "merging contiguous ranges" : "without merging ranges");
        }
        const auto row_limit = cmd->get_row_limit();
        const auto partition_limit = cmd->partition_limit;
        auto remaining_rows = row_limit;
        auto remaining_partitions = partition_limit;
        query_ranges_to_vnodes_generator ranges_to_vnodes(std::make_unique<fixed_token_splitter>(_split_tokens), query_schema, std::move(ranges),
                _split_tokens.empty());
        std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results;
        size_t concurrency = 1;
        for (;;) {
            auto round = ranges_to_vnodes(concurrency);
            concurrency = std::max(size_t(1), round.size());
            if (_merge_ranges) {
                round = merge_contiguous(*query_schema, std::move(round));
            }
            query::result_merger round_merger(cmd->get_row_limit(), cmd->partition_limit);
            for (const auto& range : round) {
                round_merger(read_range(query_schema, cmd, range));
            }
            auto result = round_merger.get();
            result->ensure_counts();
            remaining_rows -= result->row_count().value();
            remaining_partitions -= result->partition_count().value();
            results.push_back(std::move(result));
            if (ranges_to_vnodes.empty() || !remaining_rows || !remaining_partitions) {
                break;
            }
            cmd->set_row_limit(remaining_rows);
            cmd->partition_limit = remaining_partitions;
            concurrency *= 2;
        }
        query::result_merger merger(row_limit, partition_limit);
        for (auto& r : results) {
            merger(std::move(r));
        }
        return finish(*query_schema, merger.get());
    }

public:
    coordinator(schema_ptr s, std::vector<lw_shared_ptr<utils::chunked_vector<mutation>>> contents, utils::chunked_vector<mutation> merged,
            gc_clock::time_point query_time, const read_options& opts, history_bounds bounds)
        : _schema(std::move(s))
        , _contents(std::move(contents))
        , _replicas(_contents
                | std::views::transform([] (const lw_shared_ptr<utils::chunked_vector<mutation>>& c) { return make_source(c); })
                | std::ranges::to<std::vector>())
        , _merged(std::move(merged))
        , _query_time(query_time)
        , _opts(opts)
        , _bounds(bounds)
    {
        if (_opts.schedule_seed) {
            _schedule.emplace(*_opts.schedule_seed);
            // Up to 4 vnode boundaries. Each one is at the token of a
            // partition, right before it, or at a random token. A range ends
            // at its boundary, so a boundary at the token of a partition puts
            // the partition at the end of a range. Without a schedule, scans do
            // not split, as in a keyspace whose replicas do not depend on the
            // token.
            for (size_t n = choose(5, 0); n > 0; --n) {
                const auto kind = _merged.empty() ? 2 : choose(3, 0);
                const auto t = kind == 2 ? dht::token(std::uniform_int_distribution<int64_t>()(*_schedule))
                        : _merged[choose(_merged.size(), 0)].decorated_key().token();
                _split_tokens.push_back(kind == 1 ? dht::token(t.raw() - 1) : t);
            }
            std::ranges::sort(_split_tokens);
            _split_tokens.erase(std::ranges::unique(_split_tokens).begin(), _split_tokens.end());
            _merge_ranges = choose(2, 0);
        }
        // Without a schedule, the coordinator runs on replica 0.
        if (const auto r = choose(_replicas.size() + 1, 0); r < _replicas.size()) {
            _local_replica = r;
        }
        if (_opts.querier_cache) {
            for (size_t i = 0; i < _replicas.size(); ++i) {
                // Entries do not expire during a run.
                _caches.push_back(std::make_unique<replica::querier_cache>([] (const reader_concurrency_semaphore&) { return true; },
                        std::chrono::hours(24)));
            }
        }
    }

    // Closes the cached queriers. Must be called before the coordinator is
    // destroyed.
    void stop() noexcept {
        for (auto& cache : _caches) {
            cache->stop().get();
        }
    }

    future<query_result> query(schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector ranges) {
        return seastar::async([this, query_schema = std::move(query_schema), cmd = std::move(cmd), ranges = std::move(ranges)] () mutable {
            return do_query(std::move(query_schema), std::move(cmd), std::move(ranges));
        });
    }

    std::vector<std::string> take_trace() {
        return std::exchange(_trace, {});
    }

    size_t repair_mutations() const {
        return _repair_mutations;
    }

    std::vector<std::string> take_violations() {
        return std::exchange(_violations, {});
    }
};

} // anonymous namespace

read_model::history complete_history(const placed_history& h) {
    return h | std::views::transform(&placed_operation::op) | std::ranges::to<read_model::history>();
}

placed_history on_replicas(const read_model::history& h, replica_set replicas) {
    return h | std::views::transform([replicas] (const read_model::operation& op) {
        return placed_operation{op, replicas};
    }) | std::ranges::to<placed_history>();
}

std::string describe(const placed_history& h) {
    auto ops = h | std::views::transform([] (const placed_operation& w) {
        return fmt::format("{{{}, {:#b}}}", read_model::describe(w.op), w.replicas.to_ulong());
    });
    return fmt::format("placed_history{{\n    {},\n}}", fmt::join(ops, ",\n    "));
}

std::vector<std::string> check(const outcome& o, const std::vector<answer_row>& expected) {
    std::vector<std::string> violations;
    size_t rows_so_far = 0;
    for (size_t i = 0; i < o.pages.size(); ++i) {
        rows_so_far += o.pages[i].rows.size();
        if (rows_so_far > expected.size() || !std::equal(o.rows.begin(), o.rows.begin() + rows_so_far, expected.begin())) {
            violations.push_back(fmt::format("After page {}, the rows are not a prefix of the complete answer", i));
            break;
        }
    }
    violations.insert(violations.end(), o.coordinator_violations.begin(), o.coordinator_violations.end());
    if (o.error) {
        violations.push_back(*o.error);
    } else if (o.rows != expected) {
        violations.push_back("The rows of all pages differ from the complete answer");
    }
    return violations;
}

std::string describe(const read_case& c) {
    return fmt::format("read_case{{\n{},\n{},\n{},\n}}", describe(c.history), c.query, c.options);
}

std::string violation_kind(const std::vector<std::string>& violations) {
    return fmt::format("{}", fmt::join(violations, "; ")) | std::views::filter([] (char ch) {
        return !std::isdigit(static_cast<unsigned char>(ch));
    }) | std::ranges::to<std::string>();
}

std::string report(const read_case& c, const outcome& o, const std::vector<answer_row>& expected, const std::vector<std::string>& violations) {
    auto out = fmt::format("{}\n{}\n{}\nExpected: {}\nActual:   {}\n", fmt::join(violations, "\n"), describe(c),
            read_model::to_cql(c.query, "ks", "cf"), expected, o.rows);
    for (size_t i = 0; i < o.pages.size(); ++i) {
        out += fmt::format("Page {}: {}\n", i, o.pages[i].rows);
        for (const auto& line : o.pages[i].trace) {
            out += fmt::format("  {}\n", line);
        }
    }
    out += fmt::format("Repair mutations: {}", o.repair_mutations);
    return out;
}

harness::harness(cql_test_env& env, std::string_view ks, std::string_view cf)
    : _env(env)
    , _ks(ks)
    , _cf(cf)
{
    _env.execute_cql(read_model::create_table_statement(ks, cf)).get();
}

schema_ptr harness::schema() const {
    return _env.local_db().find_schema(sstring(_ks), sstring(_cf));
}

outcome harness::run(const read_case& c) {
    const auto& h = c.history;
    const auto& q = c.query;
    const auto& opts = c.options;
    if (opts.replica_count == 0 || opts.replica_count > max_replicas) {
        throw std::invalid_argument(fmt::format("Unsupported number of replicas: {}", opts.replica_count));
    }
    if (opts.empty_replica_mutation_pages && !opts.empty_replica_pages) {
        throw std::invalid_argument("A cluster which enables empty_replica_mutation_pages also enables empty_replica_pages");
    }
    if (opts.native_reverse_queries && !opts.empty_replica_mutation_pages) {
        throw std::invalid_argument("A cluster which enables native_reverse_queries also enables empty_replica_mutation_pages");
    }
    if (opts.read_frontiers && !opts.native_reverse_queries) {
        throw std::invalid_argument("A cluster which enables read_frontiers also enables native_reverse_queries");
    }
    if (opts.apply_repairs && opts.querier_cache) {
        throw std::invalid_argument("A case which applies repairs cannot keep queriers, because a cached querier reads the contents from before a repair");
    }
    if (opts.extra_replicas >= opts.replica_count) {
        throw std::invalid_argument(fmt::format("Some of the {} replicas must count toward the consistency level", opts.replica_count));
    }
    const auto all_replicas = first_replicas(opts.replica_count);
    const auto cl_replicas = first_replicas(opts.replica_count - opts.extra_replicas);
    for (const auto& w : h) {
        if (w.replicas.none() || (w.replicas & ~all_replicas).any()) {
            throw std::invalid_argument(fmt::format("A write must be on some of the {} replicas: {}", opts.replica_count, read_model::describe(w.op)));
        }
        if ((w.replicas & cl_replicas).none()) {
            throw std::invalid_argument(fmt::format("A write on an extra replica must also be on a replica which counts toward the consistency level: {}",
                    read_model::describe(w.op)));
        }
    }
    read_model::validate(complete_history(h));

    auto s = schema();
    // Values with a TTL expire relative to this time. The statement reads a
    // little later.
    const auto query_time = gc_clock::now();
    std::vector<lw_shared_ptr<utils::chunked_vector<mutation>>> contents;
    for (size_t i = 0; i < opts.replica_count; ++i) {
        read_model::history part;
        for (const auto& w : h) {
            if (w.replicas.test(i)) {
                part.push_back(w.op);
            }
        }
        contents.push_back(make_lw_shared(read_model::to_mutations(s, part, query_time)));
    }
    // Each page returns a row or moves past a fragment of the replicas'
    // mutations, so the number of fragments bounds the number of pages.
    // max_pages is a generous upper bound on the number of fragments: each
    // write adds at most two, and each partition at most three.
    std::set<int32_t> pks;
    for (const auto& w : h) {
        pks.insert(pk_of(w.op));
    }
    const size_t max_pages = 2 * (2 * h.size() + 3 * pks.size()) + 10;

    coordinator coord(s, std::move(contents), read_model::to_mutations(s, complete_history(h), query_time), query_time, opts,
            bounds_of(h));
    auto stop_coordinator = defer([&coord] () noexcept { coord.stop(); });
    // The number of reads for the current page. For an unpaged query with a
    // filter, the statement pages internally until the pager is exhausted.
    // One page of the client can then make many reads. max_pages bounds
    // them too.
    size_t reads = 0;
    service::pager::query_function query_function = [&coord, &reads, max_pages] (service::storage_proxy&, schema_ptr query_schema,
            lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& ranges, db::consistency_level, service::storage_proxy_coordinator_query_options,
            std::optional<service::cas_shard>) {
        if (++reads > max_pages) {
            return make_exception_future<query_result>(std::runtime_error(fmt::format("The statement read more than {} pages internally", max_pages)));
        }
        return coord.query(std::move(query_schema), std::move(cmd), std::move(ranges));
    };

    auto id = _env.prepare(read_model::to_cql(q, _ks, _cf)).get();
    auto prepared = _env.local_qp().get_prepared(id);
    if (!prepared) {
        throw std::runtime_error(fmt::format("The statement of {} is not prepared", q));
    }
    auto stmt = dynamic_pointer_cast<cql3::statements::select_statement>(prepared->statement);
    auto bound_names = prepared->bound_names;

    outcome o;
    // The serialized paging states of the pages so far. A client which gets
    // the same state twice would loop forever. The harness compares whole
    // serialized states, not summaries, so that the comparison covers every
    // field which decides the next page. The query id is the same on every
    // page, so it does not hide a repetition.
    std::set<bytes> seen_states;
    lw_shared_ptr<const service::pager::paging_state> state;
    for (;;) {
        if (o.pages.size() == max_pages) {
            o.error = fmt::format("The client stopped after {} pages", max_pages);
            break;
        }
        auto options = std::make_unique<cql3::query_options>(db::consistency_level::ALL, cql3::raw_value_vector_with_unset(),
                cql3::query_options::specific_options{opts.page_size, state, db::consistency_level::SERIAL, api::new_timestamp(),
                        service::node_local_only::no});
        options->prepare(bound_names);
        service::query_state query_state(_env.local_client_state(), empty_service_permit());
        page p;
        reads = 0;
        try {
            auto msg = stmt->execute_with_query_function(_env.local_qp(), query_state, *options, &query_function).get();
            msg = cql_transport::messages::propagate_exception_as_future(std::move(msg)).get();
            p.rows = rows_of(msg);
            if (auto next = paging_state_of(msg)) {
                // A client receives the paging state serialized.
                p.state = service::pager::paging_state::deserialize(next->serialize());
            }
        } catch (...) {
            p.trace = coord.take_trace();
            o.pages.push_back(std::move(p));
            o.error = fmt::format("Page {} failed: {}", o.pages.size() - 1, std::current_exception());
            break;
        }
        p.trace = coord.take_trace();
        p.trace.push_back(p.state ? fmt::format("paging state: {}", describe(*s, *p.state)) : "exhausted");
        o.rows.insert(o.rows.end(), p.rows.begin(), p.rows.end());
        state = p.state;
        o.pages.push_back(std::move(p));
        if (!state) {
            break;
        }
        if (!seen_states.insert(*state->serialize()).second) {
            o.error = fmt::format("Page {} repeats the paging state of an earlier page: {}", o.pages.size() - 1, describe(*s, *state));
            break;
        }
    }
    o.repair_mutations = coord.repair_mutations();
    o.coordinator_violations = coord.take_violations();
    return o;
}

std::vector<std::string> harness::violations(const read_case& c) {
    const auto expected = read_model::evaluate(*schema(), complete_history(c.history), c.query);
    return check(run(c), expected);
}

read_case harness::shrink(read_case c) {
    const auto kind = violation_kind(violations(c));
    if (kind.empty()) {
        throw std::invalid_argument(fmt::format("Only a failing case can be shrunk: {}", describe(c)));
    }
    bool shrunk = true;
    // Replaces `c` with `candidate` if its run fails the same way.
    auto try_candidate = [&] (read_case candidate) {
        try {
            if (violation_kind(violations(candidate)) != kind) {
                return false;
            }
        } catch (const std::invalid_argument&) {
            // The harness or the model rejects the candidate.
            return false;
        }
        c = std::move(candidate);
        shrunk = true;
        return true;
    };
    while (shrunk) {
        shrunk = false;
        for (size_t i = 0; i < c.history.size();) {
            auto candidate = c;
            candidate.history.erase(candidate.history.begin() + i);
            if (!try_candidate(std::move(candidate))) {
                ++i;
            }
        }
        for (size_t i = 0; i < c.history.size(); ++i) {
            for (size_t r = 0; r < c.options.replica_count && c.history[i].replicas.count() > 1; ++r) {
                auto candidate = c;
                candidate.history[i].replicas = replica_set().set(r);
                try_candidate(std::move(candidate));
            }
        }
        for (size_t r = 0; c.options.replica_count > 1 && r < c.options.replica_count;) {
            if (!try_candidate(without_replica(c, r))) {
                ++r;
            }
        }
        if (c.options.extra_replicas) {
            auto candidate = c;
            candidate.options.extra_replicas = 0;
            try_candidate(std::move(candidate));
        }
        if (c.options.schedule_seed) {
            auto candidate = c;
            candidate.options.schedule_seed.reset();
            try_candidate(std::move(candidate));
        }
        if (c.options.querier_cache) {
            auto candidate = c;
            candidate.options.querier_cache = false;
            try_candidate(std::move(candidate));
        }
        if (c.options.apply_repairs) {
            auto candidate = c;
            candidate.options.apply_repairs = false;
            try_candidate(std::move(candidate));
        }
        if (c.options.tombstone_limit) {
            auto candidate = c;
            candidate.options.tombstone_limit.reset();
            try_candidate(std::move(candidate));
        }
        if (c.options.page_size_in_bytes) {
            auto candidate = c;
            candidate.options.page_size_in_bytes.reset();
            try_candidate(std::move(candidate));
        }
        // Enabled features are the default, which the printed case omits.
        if (!c.options.empty_replica_pages) {
            auto candidate = c;
            candidate.options.empty_replica_pages = true;
            try_candidate(std::move(candidate));
        }
        if (!c.options.empty_replica_mutation_pages) {
            auto candidate = c;
            candidate.options.empty_replica_mutation_pages = true;
            try_candidate(std::move(candidate));
        }
        if (!c.options.native_reverse_queries) {
            auto candidate = c;
            candidate.options.native_reverse_queries = true;
            try_candidate(std::move(candidate));
        }
        if (!c.options.read_frontiers) {
            auto candidate = c;
            candidate.options.read_frontiers = true;
            candidate.options.native_reverse_queries = true;
            candidate.options.empty_replica_mutation_pages = true;
            candidate.options.empty_replica_pages = true;
            try_candidate(std::move(candidate));
        }
        if (c.query.limit) {
            auto candidate = c;
            candidate.query.limit.reset();
            try_candidate(std::move(candidate));
        }
        if (c.query.per_partition_limit) {
            auto candidate = c;
            candidate.query.per_partition_limit.reset();
            try_candidate(std::move(candidate));
        }
        for (size_t i = 0; i < c.query.filter.size();) {
            auto candidate = c;
            candidate.query.filter.erase(candidate.query.filter.begin() + i);
            if (!try_candidate(std::move(candidate))) {
                ++i;
            }
        }
        for (size_t i = 0; c.query.partitions && c.query.partitions->size() > 1 && i < c.query.partitions->size();) {
            auto candidate = c;
            candidate.query.partitions->erase(candidate.query.partitions->begin() + i);
            if (!try_candidate(std::move(candidate))) {
                ++i;
            }
        }
        if (c.query.ck_start) {
            auto candidate = c;
            candidate.query.ck_start.reset();
            try_candidate(std::move(candidate));
        }
        if (c.query.ck_end) {
            auto candidate = c;
            candidate.query.ck_end.reset();
            try_candidate(std::move(candidate));
        }
    }
    return c;
}

} // namespace tests::paged_read

auto fmt::formatter<tests::paged_read::read_options>::format(const tests::paged_read::read_options& o, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::vector<std::string> fields{fmt::format(".replica_count = {}", o.replica_count)};
    if (o.extra_replicas) {
        fields.push_back(fmt::format(".extra_replicas = {}", o.extra_replicas));
    }
    fields.push_back(fmt::format(".page_size = {}", o.page_size));
    if (o.page_size_in_bytes) {
        fields.push_back(fmt::format(".page_size_in_bytes = {}", *o.page_size_in_bytes));
    }
    if (o.tombstone_limit) {
        fields.push_back(fmt::format(".tombstone_limit = {}", *o.tombstone_limit));
    }
    if (!o.empty_replica_pages) {
        fields.push_back(".empty_replica_pages = false");
    }
    if (!o.empty_replica_mutation_pages) {
        fields.push_back(".empty_replica_mutation_pages = false");
    }
    if (!o.native_reverse_queries) {
        fields.push_back(".native_reverse_queries = false");
    }
    if (!o.read_frontiers) {
        fields.push_back(".read_frontiers = false");
    }
    if (o.querier_cache) {
        fields.push_back(".querier_cache = true");
    }
    if (o.apply_repairs) {
        fields.push_back(".apply_repairs = true");
    }
    if (o.schedule_seed) {
        fields.push_back(fmt::format(".schedule_seed = {}", *o.schedule_seed));
    }
    return fmt::format_to(ctx.out(), "read_options{{{}}}", fmt::join(fields, ", "));
}
