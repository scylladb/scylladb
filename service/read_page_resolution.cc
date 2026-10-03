/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include <algorithm>
#include <functional>
#include <map>
#include <ranges>

#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "service/read_page_resolution.hh"
#include "mutation/async_utils.hh"
#include "mutation/mutation_partition.hh"
#include "query/query-result-reader.hh"
#include "query/query_result_merger.hh"
#include "utils/assert.hh"
#include "utils/chunked_vector.hh"
#include "utils/log.hh"

namespace service {

static logging::logger rplogger("read_page_resolution");

namespace {

// Merges the mutation pages of one reconciliation round, calculates the
// repair differences, and checks whether the replies are sufficient.
class mutation_page_resolver {
    struct reply {
        locator::host_id from;
        foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
        bool reached_end = false;
        reply(locator::host_id from_, foreign_ptr<lw_shared_ptr<reconcilable_result>> result_) : from(std::move(from_)), result(std::move(result_)) {}
    };
    struct version {
        locator::host_id from;
        std::optional<partition> par;
        bool reached_end;
        bool reached_partition_end;
        version(locator::host_id from_, std::optional<partition> par_, bool reached_end, bool reached_partition_end)
                : from(std::move(from_)), par(std::move(par_)), reached_end(reached_end), reached_partition_end(reached_partition_end) {}
    };
    struct mutation_and_live_row_count {
        mutation mut;
        uint64_t live_row_count;
    };

    struct primary_key {
        dht::decorated_key partition;
        std::optional<clustering_key> clustering;

        class less_compare_clustering {
            bool _is_reversed;
            clustering_key::less_compare _ck_cmp;
        public:
            less_compare_clustering(const schema& s, bool is_reversed)
                : _is_reversed(is_reversed), _ck_cmp(s) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                if (!b.clustering) {
                    return false;
                }
                if (!a.clustering) {
                    return true;
                }
                if (_is_reversed) {
                    return _ck_cmp(*b.clustering, *a.clustering);
                } else {
                    return _ck_cmp(*a.clustering, *b.clustering);
                }
            }
        };

        class less_compare {
            const schema& _schema;
            less_compare_clustering _ck_cmp;
        public:
            less_compare(const schema& s, bool is_reversed)
                : _schema(s), _ck_cmp(s, is_reversed) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                auto pk_result = a.partition.tri_compare(_schema, b.partition);
                if (pk_result != 0) {
                    return pk_result < 0;
                }
                return _ck_cmp(a, b);
            }
        };
    };

    schema_ptr _schema;
    uint64_t _total_live_count = 0;
    uint64_t _max_live_count = 0;
    uint32_t _short_read_diff = 0;
    uint64_t _max_per_partition_live_count = 0;
    uint32_t _partition_count = 0;
    uint32_t _live_partition_count = 0;
    bool _increase_per_partition_limit = false;
    bool _all_reached_end = true;
    query::short_read _is_short_read;
    std::vector<reply> _data_results;
    mutations_per_partition_key_map _diffs;
private:
    void register_live_count(const std::vector<version>& replica_versions, uint64_t reconciled_live_rows, uint64_t limit) {
        bool any_not_at_end = std::ranges::any_of(replica_versions, [] (const version& v) {
            return !v.reached_partition_end;
        });
        if (any_not_at_end && reconciled_live_rows < limit && limit - reconciled_live_rows > _short_read_diff) {
            _short_read_diff = limit - reconciled_live_rows;
            _max_per_partition_live_count = reconciled_live_rows;
        }
    }
    void find_short_partitions(const std::vector<mutation_and_live_row_count>& rp, const utils::chunked_vector<std::vector<version>>& versions,
                               uint64_t per_partition_limit, uint64_t row_limit, uint32_t partition_limit) {
        // Go through the partitions that weren't limited by the total row limit
        // and check whether we got enough rows to satisfy per-partition row
        // limit.
        auto partitions_left = partition_limit;
        auto rows_left = row_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | std::views::reverse) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                register_live_count(*pv, row_count, per_partition_limit);
            } else {
                break;
            }
            ++pv;
        }
    }

    static primary_key get_last_row(const schema& s, const partition& p, bool is_reversed) {
        return {p.mut().decorated_key(s), is_reversed ? p.mut().partition().first_row_key() : p.mut().partition().last_row_key()  };
    }

    // Returns the highest row sent by the specified replica, according to the schema and the direction of
    // the query.
    // versions is a table where rows are partitions in descending order and the columns identify the partition
    // sent by a particular replica.
    static primary_key get_last_row(const schema& s, bool is_reversed, const utils::chunked_vector<std::vector<version>>& versions, uint32_t replica) {
        const partition* last_partition = nullptr;
        // Versions are in the reversed order.
        for (auto&& pv : versions) {
            const std::optional<partition>& p = pv[replica].par;
            if (p) {
                last_partition = &p.value();
                break;
            }
        }
        SCYLLA_ASSERT(last_partition);
        return get_last_row(s, *last_partition, is_reversed);
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, const query::read_command& cmd, uint64_t limit, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        auto mp = mutation_partition(s, m.partition());
        auto&& ranges = cmd.slice.row_ranges(s, m.key());
        bool always_return_static_content = cmd.slice.options.contains<query::partition_slice::option::always_return_static_content>();
        mp.compact_for_query(s, m.decorated_key(), cmd.timestamp, ranges, always_return_static_content, limit);
        return primary_key{m.decorated_key(), get_last_reconciled_row(s, mp, is_reversed)};
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        return primary_key{m.decorated_key(), get_last_reconciled_row(s, m.partition(), is_reversed)};
    }

    static std::optional<clustering_key> get_last_reconciled_row(const schema& s, const mutation_partition& mp, bool is_reversed) {
        std::optional<clustering_key> ck;
        if (!mp.clustered_rows().empty()) {
            if (is_reversed) {
                ck = mp.clustered_rows().begin()->key();
            } else {
                ck = mp.clustered_rows().rbegin()->key();
            }
        }
        return ck;
    }

    static bool got_incomplete_information_in_partition(const schema& s, const primary_key& last_reconciled_row, const std::vector<version>& versions, bool is_reversed) {
        primary_key::less_compare_clustering ck_cmp(s, is_reversed);
        for (auto&& v : versions) {
            if (!v.par || v.reached_partition_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, *v.par, is_reversed);
            if (ck_cmp(replica_last_row, last_reconciled_row)) {
                return true;
            }
        }
        return false;
    }

    bool got_incomplete_information_across_partitions(const schema& s, const query::read_command& cmd,
                                                      const primary_key& last_reconciled_row, std::vector<mutation_and_live_row_count>& rp,
                                                      const utils::chunked_vector<std::vector<version>>& versions, bool is_reversed) {
        bool short_reads_allowed = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        bool always_return_static_content = cmd.slice.options.contains<query::partition_slice::option::always_return_static_content>();
        primary_key::less_compare cmp(s, is_reversed);
        std::optional<primary_key> shortest_read;
        auto num_replicas = versions[0].size();
        for (uint32_t i = 0; i < num_replicas; ++i) {
            if (versions.front()[i].reached_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, is_reversed, versions, i);
            if (cmp(replica_last_row, last_reconciled_row)) {
                if (short_reads_allowed) {
                    if (!shortest_read || cmp(replica_last_row, *shortest_read)) {
                        shortest_read = std::move(replica_last_row);
                    }
                } else {
                    return true;
                }
            }
        }

        // Short reads are allowed, trim the reconciled result.
        if (shortest_read) {
            _is_short_read = query::short_read::yes;

            // Prepare to remove all partitions past shortest_read
            auto it = rp.begin();
            for (; it != rp.end() && shortest_read->partition.less_compare(s, it->mut.decorated_key()); ++it) { }

            // Remove all clustering rows past shortest_read
            if (it != rp.end() && it->mut.decorated_key().equal(s, shortest_read->partition)) {
                if (!shortest_read->clustering) {
                    ++it;
                } else {
                    std::vector<query::clustering_range> ranges;
                    ranges.emplace_back(is_reversed ? query::clustering_range::make_starting_with(std::move(*shortest_read->clustering))
                                                    : query::clustering_range::make_ending_with(std::move(*shortest_read->clustering)));
                    it->live_row_count = it->mut.partition().compact_for_query(s, it->mut.decorated_key(), cmd.timestamp, ranges, always_return_static_content,
                            query::partition_max_rows);
                }
            }

            // Actually remove all partitions past shortest_read
            rp.erase(rp.begin(), it);

            // Update total live count and live partition count
            _live_partition_count = 0;
            _total_live_count = std::ranges::fold_left(rp, uint64_t(0), [this] (uint64_t lc, const mutation_and_live_row_count& m_a_rc) {
                _live_partition_count += !!m_a_rc.live_row_count;
                return lc + m_a_rc.live_row_count;
            });
        }

        return false;
    }

    bool got_incomplete_information(const schema& s, const query::read_command& cmd, uint64_t original_row_limit, uint64_t original_per_partition_limit,
                            uint64_t original_partition_limit, std::vector<mutation_and_live_row_count>& rp, const utils::chunked_vector<std::vector<version>>& versions) {
        // We need to check whether the reconciled result contains all information from all available
        // replicas. It is possible that some of the nodes have returned less rows (because the limit
        // was set and they had some tombstones missing) than the others. In such cases we cannot just
        // merge all results and return that to the client as the replicas that returned less row
        // may have newer data for the rows they did not send than any other node in the cluster.
        //
        // This function is responsible for detecting whether such problem may happen. We get partition
        // and clustering keys of the last row that is going to be returned to the client and check if
        // it is in range of rows returned by each replicas that returned as many rows as they were
        // asked for (if a replica returned less rows it means it returned everything it has).
        auto is_reversed = cmd.slice.is_reversed();

        auto rows_left = original_row_limit;
        auto partitions_left = original_partition_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | std::views::reverse) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left > !!row_count) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                if (original_per_partition_limit < query:: max_rows_if_set) {
                    auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, original_per_partition_limit, is_reversed);
                    if (got_incomplete_information_in_partition(s, last_row, *pv, is_reversed)) {
                        _increase_per_partition_limit = true;
                        return true;
                    }
                }
            } else {
                auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, rows_left, is_reversed);
                return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
            }
            ++pv;
        }
        if (rp.empty()) {
            return false;
        }
        auto&& last_row = get_last_reconciled_row(s, *rp.begin(), is_reversed);
        return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
    }
public:
    mutation_page_resolver(schema_ptr schema, std::vector<mutation_page_reply> replies)
        : _schema(std::move(schema))
        , _diffs(10, partition_key::hashing(*_schema), partition_key::equality(*_schema)) {
        _data_results.reserve(replies.size());
        for (auto& r : replies) {
            _max_live_count = std::max(r.result->row_count(), _max_live_count);
            _data_results.emplace_back(std::move(r.from), std::move(r.result));
        }
    }
    bool any_partition_short_read() const {
        return _short_read_diff > 0;
    }
    bool increase_per_partition_limit() const {
        return _increase_per_partition_limit;
    }
    uint32_t max_per_partition_live_count() const {
        return _max_per_partition_live_count;
    }
    uint32_t partition_count() const {
        return _partition_count;
    }
    uint32_t live_partition_count() const {
        return _live_partition_count;
    }
    bool all_reached_end() const {
        return _all_reached_end;
    }
    future<std::optional<reconcilable_result>> resolve(const query::read_command& cmd, uint64_t original_row_limit, uint64_t original_per_partition_limit,
            uint32_t original_partition_limit) {
        SCYLLA_ASSERT(_data_results.size());

        if (_data_results.size() == 1) {
            // if there is a result only from one node there is nothing to reconcile
            // should happen only for range reads since single key reads will not
            // try to reconcile for CL=ONE
            auto& p = _data_results[0].result;
            co_return reconcilable_result(p->row_count(), p->partitions(), p->is_short_read());
        }

        const auto& schema = *_schema;

        // return true if lh > rh
        auto cmp = [&schema](reply& lh, reply& rh) {
            if (lh.result->partitions().size() == 0) {
                return false; // reply with empty partition array goes to the end of the sorted array
            } else if (rh.result->partitions().size() == 0) {
                return true;
            } else {
                auto lhk = lh.result->partitions().back().mut().key();
                auto rhk = rh.result->partitions().back().mut().key();
                return lhk.ring_order_tri_compare(schema, rhk) > 0;
            }
        };

        // this array will have an entry for each partition which will hold all available versions
        // Use chunked_vector to avoid a single large contiguous reallocation: when partitions are
        // small (e.g. Alternator items), a 1 MB page can carry thousands of partitions.  A plain
        // std::vector would double its buffer on overflow, easily producing a >128 KB allocation
        // (the seastar large-allocation warning threshold).  chunked_vector caps each individual
        // allocation at 128 KB regardless of the total number of partitions or replica divergence.
        utils::chunked_vector<std::vector<version>> versions;
        versions.reserve(_data_results.front().result->partitions().size());

        for (auto& r : _data_results) {
            _is_short_read = _is_short_read || r.result->is_short_read();
            r.reached_end = !r.result->is_short_read() && r.result->row_count() < cmd.get_row_limit()
                            && (cmd.partition_limit == query::max_partitions
                                || std::ranges::count_if(r.result->partitions(), [] (const partition& p) {
                                    return p.row_count();
                                }) < cmd.partition_limit);
            _all_reached_end = _all_reached_end && r.reached_end;
        }

        do {
            // after this sort reply with largest key is at the beginning
            std::ranges::sort(_data_results, cmp);
            if (_data_results.front().result->partitions().empty()) {
                break; // if top of the heap is empty all others are empty too
            }
            const auto& max_key = _data_results.front().result->partitions().back().mut().key();
            versions.emplace_back();
            std::vector<version>& v = versions.back();
            v.reserve(_data_results.size());
            for (reply& r : _data_results) {
                auto pit = r.result->partitions().rbegin();
                if (pit != r.result->partitions().rend() && pit->mut().key().legacy_equal(schema, max_key)) {
                    bool reached_partition_end = pit->row_count() < cmd.slice.partition_row_limit();
                    v.emplace_back(r.from, std::move(*pit), r.reached_end, reached_partition_end);
                    r.result->partitions().pop_back();
                } else {
                    // put empty partition for destination without result
                    v.emplace_back(r.from, std::optional<partition>(), r.reached_end, true);
                }
            }

            std::ranges::sort(v, std::less<locator::host_id>(), std::mem_fn(&version::from));
        } while(true);

        std::vector<mutation_and_live_row_count> reconciled_partitions;
        reconciled_partitions.reserve(versions.size());

        // reconcile all versions
        for (std::vector<version>& v : versions) {
            auto it = std::ranges::find_if(v, [] (auto&& ver) {
                    return bool(ver.par);
            });
            // The first version has nothing to be merged with, so it is unfrozen
            // into the reconciled mutation directly instead of being applied to it.
            auto m = co_await unfreeze_gently(it->par->mut(), _schema);
            for (auto i = std::next(it); i != v.end(); ++i) {
                if (i->par) {
                    mutation_application_stats app_stats;
                    co_await apply_gently(m.partition(), schema, i->par->mut().partition(), schema, app_stats);
                }
            }
            auto live_row_count = m.live_row_count();
            _total_live_count += live_row_count;
            _live_partition_count += !!live_row_count;
            reconciled_partitions.emplace_back(mutation_and_live_row_count{ std::move(m), live_row_count });
            co_await coroutine::maybe_yield();
        }
        _partition_count = reconciled_partitions.size();

        bool has_diff = false;

        // Сalculate differences: iterate over the versions from all the nodes and calculate the difference with the reconciled result.
        for (auto z : std::views::zip(versions, reconciled_partitions)) {
            const mutation& m = std::get<1>(z).mut;
            for (const version& v : std::get<0>(z)) {
                auto diff = v.par
                          ? m.partition().difference(schema, (co_await unfreeze_gently(v.par->mut(), _schema)).partition())
                          : mutation_partition(schema, m.partition());
                std::optional<mutation> mdiff;
                if (!diff.empty()) {
                    has_diff = true;
                    mdiff = mutation(_schema, m.decorated_key(), std::move(diff));
                }
                if (auto [it, added] = _diffs[m.key()].try_emplace(v.from, std::move(mdiff)); !added) {
                    // A collision could happen only in 2 cases:
                    // 1. We have 2 versions for the same node.
                    // 2. `versions` (and or) `reconciled_partitions` are not unique per partition key.
                    // Both cases are not possible unless there is a bug in the reconcilliation code.
                    on_internal_error(rplogger, fmt::format("Partition key conflict, key: {}, node: {}, table: {}.", m.key(), v.from, schema.ks_name()));
                }
                co_await coroutine::maybe_yield();
            }
        }

        if (has_diff) {
            if (got_incomplete_information(schema, cmd, original_row_limit, original_per_partition_limit,
                                           original_partition_limit, reconciled_partitions, versions)) {
                co_return std::nullopt;
            }
            // filter out partitions with empty diffs
            for (auto it = _diffs.begin(); it != _diffs.end();) {
                if (std::ranges::none_of(it->second | std::views::values, std::mem_fn(&std::optional<mutation>::operator bool))) {
                    it = _diffs.erase(it);
                } else {
                    ++it;
                }
            }
        } else {
            _diffs.clear();
        }

        find_short_partitions(reconciled_partitions, versions, original_per_partition_limit, original_row_limit, original_partition_limit);

        bool allow_short_reads = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        if (allow_short_reads && _max_live_count >= original_row_limit && _total_live_count < original_row_limit && _total_live_count) {
            // We ended up with less rows than the client asked for (but at least one),
            // avoid retry and mark as short read instead.
            _is_short_read = query::short_read::yes;
        }

        // build reconcilable_result from reconciled data
        // traverse backwards since large keys are at the start
        utils::chunked_vector<partition> vec;
        vec.reserve(_partition_count);
        for (auto it = reconciled_partitions.rbegin(); it != reconciled_partitions.rend(); it++) {
            const mutation_and_live_row_count& m_a_rc = *it;
            vec.emplace_back(partition(m_a_rc.live_row_count, freeze(m_a_rc.mut)));
            co_await coroutine::maybe_yield();
        }

        co_return reconcilable_result(_total_live_count, std::move(vec), _is_short_read);
    }
    auto total_live_count() const {
        return _total_live_count;
    }
    auto get_diffs_for_repair() {
        return std::move(_diffs);
    }
};

// Whether a read which stopped at `pos` read everything which decides the
// rows of the partition: its static row and all its clustering rows.
bool decides_partition(const schema& s, position_in_partition_view pos) {
    return pos.region() == partition_region::partition_end
            || (pos.region() == partition_region::clustered && pos.is_after_all_clustered_rows(s));
}

// The cursor of a page which ends at the frontier stop `stop`. The pager
// moves past the partition of a cursor outside the clustering rows, so a stop
// before the static row becomes a cursor before the clustering rows. The
// next page then continues the partition from its start.
full_position cursor_of(full_position stop) {
    if (stop.position.region() == partition_region::static_row) {
        stop.position = position_in_partition::before_all_clustered_rows();
    }
    return stop;
}

// Whether `result` ends with a static-only row of the partition of `stop`,
// which `stop` does not decide. Such a row may be spurious, because the
// partition may have a live clustering row after `stop`. A partition without
// a key may be that partition.
bool ends_with_undecided_static_row(const schema& s, const query::result& result, const full_position& stop) {
    if (decides_partition(s, stop.position)) {
        return false;
    }
    auto last = query::result_view::do_with(result, [] (query::result_view v) { return v.last_partition(); });
    return last && last->row_count == 0 && (!last->key || last->key->equal(s, stop.partition));
}

// `m` without what lies at or after `pos`, in the order of `m`'s schema. The
// partition tombstone precedes every position of the partition, and the
// static row precedes every clustering position.
mutation cut_before(const mutation& m, position_in_partition_view pos) {
    const auto& s = *m.schema();
    switch (pos.region()) {
    case partition_region::partition_start:
        return mutation(m.schema(), m.decorated_key());
    case partition_region::static_row: {
        mutation header(m.schema(), m.decorated_key());
        header.partition().apply(m.partition().partition_tombstone());
        return header;
    }
    case partition_region::clustered: {
        if (position_in_partition(pos).is_before_all_clustered_rows(s)) {
            return m.sliced({});
        }
        auto range = position_range_to_clustering_range(position_range(position_in_partition::before_all_clustered_rows(), position_in_partition(pos)), s);
        return m.sliced(range ? query::clustering_row_ranges{*range} : query::clustering_row_ranges{});
    }
    case partition_region::partition_end:
        return m;
    }
    std::abort();
}

// Removes the data of `m` which is dead at every query time: the cells
// which tombstones cover, the rows without live data, and the range
// tombstones, which then cover nothing. Nothing expires at the minimal time,
// like in to_data_query_result(). Keeps the partition tombstone and the
// tombstones of live rows.
void drop_dead_data(mutation& m) {
    static const std::vector<query::clustering_range> all_rows = {query::clustering_range::make_open_ended_both_sides()};
    const auto& s = *m.schema();
    auto& mp = m.partition();
    mp.compact_for_query(s, m.decorated_key(), gc_clock::time_point::min(), all_rows, true, query::partition_max_rows);
    auto& rows = mp.mutable_clustered_rows();
    for (auto it = rows.begin(); it != rows.end();) {
        if (!it->dummy() && !it->row().is_live(s, column_kind::regular_column, tombstone(), gc_clock::time_point::min())) {
            it = rows.erase_and_dispose(it, current_deleter<rows_entry>());
        } else {
            ++it;
        }
    }
    mp.mutable_row_tombstones().clear();
}

// The number of live clustering rows of `m` at `query_time`. Unlike
// mutation_partition::live_row_count(), a live static row does not count.
uint64_t live_clustering_row_count(const mutation& m, gc_clock::time_point query_time) {
    const auto& s = *m.schema();
    const auto& mp = m.partition();
    uint64_t count = 0;
    for (const rows_entry& e : mp.non_dummy_rows()) {
        if (e.row().is_live(s, column_kind::regular_column, mp.range_tombstone_for_row(s, e.key()), query_time)) {
            ++count;
        }
    }
    return count;
}

} // anonymous namespace

void prepare_mutation_read(query::read_command& cmd, bool empty_replica_mutation_pages) {
    if (empty_replica_mutation_pages) {
        cmd.slice.options.set<query::partition_slice::option::allow_mutation_read_page_without_live_row>();
    }
}

future<mutation_page_resolution> resolve_mutation_page(schema_ptr schema, const query::read_command& original_cmd,
        const query::read_command& cmd, std::vector<mutation_page_reply> replies) {
    const uint64_t original_row_limit = original_cmd.get_row_limit();
    const uint64_t original_per_partition_row_limit = original_cmd.slice.partition_row_limit();
    const uint32_t original_partition_limit = original_cmd.partition_limit;

    mutation_page_resolver resolver(schema, std::move(replies));
    auto rr_opt = co_await resolver.resolve(cmd, original_row_limit, original_per_partition_row_limit, original_partition_limit); // reconciliation happens here

    // We generate a retry if at least one node reply with count live columns but after merge we have less
    // than the total number of column we are interested in (which may be < count on a retry).
    // So in particular, if no host returned count live columns, we know it's not a short read due to
    // row or partition limits being exhausted and retry is not needed.
    if (rr_opt && (rr_opt->is_short_read()
                   || resolver.all_reached_end()
                   || rr_opt->row_count() >= original_row_limit
                   || resolver.live_partition_count() >= original_partition_limit)
            && !resolver.any_partition_short_read()) {
        rplogger.trace("reconciled: {}", rr_opt->pretty_printer(schema));

        auto result = co_await to_data_query_result(*rr_opt, schema, original_cmd.slice, original_row_limit, original_partition_limit);

        // Un-reverse mutations for reversed queries. When a mutation comes from a node in mixed-node cluster
        // it is reversed in make_mutation_data_request(). So we always deal here with reversed mutations for
        // reversed queries. No matter what format. Forward mutations are sent to spare replicas from reversing
        // them in the write-path.
        auto diffs = resolver.get_diffs_for_repair();
        if (original_cmd.slice.is_reversed()) {
            for (auto&& [token, diff] : diffs) {
                for (auto&& [address, opt_mut] : diff) {
                    if (opt_mut) {
                        opt_mut = reverse(std::move(opt_mut.value()));
                        co_await coroutine::maybe_yield();
                    }
                }
            }
        }

        co_return accepted_mutation_page{std::move(result), std::move(diffs)};
    }

    auto retry_cmd = make_lw_shared<query::read_command>(cmd);
    // We asked t (= cmd.get_row_limit()) live columns and got l (=resolver.total_live_count) ones.
    // From that, we can estimate that on this row, for x requested
    // columns, only l/t end up live after reconciliation. So for next
    // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
    auto x = [](uint64_t t, uint64_t l) -> uint64_t {
        using uint128_t = unsigned __int128;
        auto ret = std::min<uint128_t>(query::max_rows, l == 0 ? t + 1 : (uint128_t) t * t / l + 1);
        return static_cast<uint64_t>(ret);
    };
    auto all_partitions_x = [](uint64_t x, uint32_t partitions) -> uint64_t {
        using uint128_t = unsigned __int128;
        auto ret = std::min<uint128_t>(query::max_rows, (uint128_t) x * partitions);
        return static_cast<uint64_t>(ret);
    };
    if (resolver.any_partition_short_read() || resolver.increase_per_partition_limit()) {
        // The number of live rows was bounded by the per partition limit.
        auto new_partition_limit = x(cmd.slice.partition_row_limit(), resolver.max_per_partition_live_count());
        retry_cmd->slice.set_partition_row_limit(new_partition_limit);
        auto new_limit = all_partitions_x(new_partition_limit, resolver.partition_count());
        retry_cmd->set_row_limit(std::max(cmd.get_row_limit(), new_limit));
    } else {
        // The number of live rows was bounded by the total row limit or partition limit.
        if (cmd.partition_limit != query::max_partitions) {
            retry_cmd->partition_limit = std::min<uint64_t>(query::max_partitions, x(cmd.partition_limit, resolver.live_partition_count()));
        }
        if (cmd.get_row_limit() != query::max_rows) {
            retry_cmd->set_row_limit(x(cmd.get_row_limit(), resolver.total_live_count()));
        }
    }

    // We may be unable to send a single live row because of replicas bailing out too early.
    // If that is the case disallow short reads so that we can make progress.
    if (!resolver.total_live_count()) {
        retry_cmd->slice.options.remove<query::partition_slice::option::allow_short_read>();
    }

    co_return mutation_page_retry{std::move(retry_cmd)};
}

void foreground_reply_collector::fail(exceptions::coordinator_exception_container ex) {
    if (!_cl_reported) {
        _cl_promise.set_value(std::move(ex));
    }
    // we will not need them any more
    _data_result = foreign_ptr<lw_shared_ptr<query::result>>();
    _digest_results.clear();
}

const std::optional<full_position>& foreground_reply_collector::min_position() const {
    return std::min_element(_digest_results.begin(), _digest_results.end(), [this] (const digest_and_last_pos& a, const digest_and_last_pos& b) {
        // last_pos can be disengaged when there are not results whatsoever
        if (!a.last_pos || !b.last_pos) {
            return bool(a.last_pos) < bool(b.last_pos);
        }
        return full_position::cmp(*_schema, *a.last_pos, *b.last_pos) < 0;
    })->last_pos;
}

namespace detail {

digest_page_decision decide_digest_page_at_stop(const schema& s, const query::read_command& cmd,
        foreign_ptr<lw_shared_ptr<query::result>> result, const full_position& stop) {
    const bool allow_short_read = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
    if (ends_with_undecided_static_row(s, *result, stop)) {
        // The page may not hold the row. Without partition keys, the row
        // may belong to an earlier partition, which the page must keep.
        // Without short reads, the page must go on after E. In both cases
        // the reconciliation decides the page.
        if (!allow_short_read || !cmd.slice.options.contains<query::partition_slice::option::send_partition_key>()) {
            return digest_page_mismatch{};
        }
        // Leave the row out, like the reconciliation, and end the page short
        // at E. The next page reads the rest of the partition, which decides
        // the row.
        result->drop_last_partition();
        result->set_short_read(query::short_read::yes);
        result->set_last_position(cursor_of(stop));
        return accepted_digest_page{std::move(result)};
    }
    result->ensure_counts();
    if (*result->row_count() >= cmd.get_row_limit() || *result->partition_count() >= cmd.partition_limit) {
        result->set_last_position(cursor_of(stop));
        return accepted_digest_page{std::move(result)};
    }
    if (allow_short_read) {
        result->set_short_read(query::short_read::yes);
        result->set_last_position(cursor_of(stop));
        return accepted_digest_page{std::move(result)};
    }
    // The page must go on after E, but the replies do not cover it.
    return digest_page_mismatch{};
}

void lower_to_min_position(const schema& s, query::result& result, const foreground_reply_collector& replies) {
    auto& mp = replies.min_position();
    auto& lp = result.last_position();
    if (!mp || bool(lp) < bool(mp) || full_position::cmp(s, *mp, *lp) < 0) {
        result.set_last_position(mp);
    }
}

} // namespace detail

frontier_reconciliation::frontier_reconciliation(schema_ptr schema, lw_shared_ptr<const query::read_command> cmd, dht::partition_range range)
    : _schema(std::move(schema))
    , _cmd(std::move(cmd))
    , _range(std::move(range))
    , _round_cmd(make_lw_shared<query::read_command>(*_cmd))
    , _round_range(_range)
    , _diffs(10, partition_key::hashing(*_schema), partition_key::equality(*_schema))
{
    if (!_cmd->slice.options.contains<query::partition_slice::option::send_read_frontier>()) {
        on_internal_error(rplogger, "frontier_reconciliation: the command does not ask for frontiers");
    }
    // The feature read_frontiers implies empty_replica_mutation_pages.
    prepare_mutation_read(*_round_cmd, true);
}

future<query::result> frontier_reconciliation::convert(size_t count) {
    utils::chunked_vector<partition> partitions;
    partitions.reserve(count);
    uint64_t row_count = 0;
    for (const auto& m : _reconciled | std::views::take(count)) {
        const auto live_rows = m.live_row_count(_cmd->timestamp);
        row_count += live_rows;
        partitions.emplace_back(live_rows, freeze(m));
        co_await coroutine::maybe_yield();
    }
    const reconcilable_result reconciled(row_count, std::move(partitions), query::short_read::no);
    rplogger.trace("reconciled: {}", reconciled.pretty_printer(_schema));
    // A per-partition limit applies within one partition, so a conversion
    // which starts at a partition boundary needs only the row and partition
    // limits which remain.
    co_return co_await to_data_query_result(reconciled, _schema, _cmd->slice, _cmd->get_row_limit() - _converted_rows,
            _cmd->partition_limit - _converted_partitions);
}

void frontier_reconciliation::keep(query::result result) {
    result.set_frontier(std::nullopt);
    result.ensure_counts();
    _converted_rows += *result.row_count();
    _converted_partitions += *result.partition_count();
    _converted.push_back(make_foreign(make_lw_shared<query::result>(std::move(result))));
}

query::result frontier_reconciliation::converted_page() {
    query::result_merger merger(_cmd->get_row_limit(), _cmd->partition_limit);
    merger.reserve(_converted.size());
    for (auto& r : _converted) {
        merger(std::move(r));
    }
    _converted.clear();
    return std::move(*merger.get());
}

future<std::optional<accepted_mutation_page>> frontier_reconciliation::add_round(std::vector<mutation_page_reply> replies) {
    const schema& s = *_schema;
    const position_in_partition::tri_compare pos_cmp(s);

    // The common frontier E, before E moves back to an incomplete skip: the
    // earliest stop, and the earliest skip of each partition.
    std::optional<full_position> stop;
    std::map<dht::decorated_key, position_in_partition, dht::decorated_key::less_comparator> skips{dht::decorated_key::less_comparator(_schema)};
    for (const auto& r : replies) {
        const auto& frontier = r.result->frontier();
        if (!frontier) {
            on_internal_error(rplogger, fmt::format("The mutation reply of {} for {}.{} has no frontier", r.from, s.ks_name(), s.cf_name()));
        }
        if (frontier->stop && (!stop || full_position::cmp(s, *frontier->stop, *stop) < 0)) {
            stop = frontier->stop;
        }
        for (const auto& skip : r.result->skips()) {
            // The wire names a skip's partition by its index in the reply.
            if (skip.partition >= r.result->partitions().size()) {
                on_internal_error(rplogger, fmt::format("The mutation reply of {} for {}.{} has a skip of partition {}, but only {} partitions",
                        r.from, s.ks_name(), s.cf_name(), skip.partition, r.result->partitions().size()));
            }
            auto [it, added] = skips.emplace(r.result->partitions()[skip.partition].mut().decorated_key(s), skip.position);
            if (!added && pos_cmp(skip.position, it->second) < 0) {
                it->second = skip.position;
            }
        }
    }
    const std::optional<dht::decorated_key> stop_key = stop ? std::optional(dht::decorate_key(s, stop->partition)) : std::nullopt;

    // Where the merged data of a partition ends: at its skip, or at the stop
    // in the stop's partition. nullopt for a partition after the stop.
    auto cut_of = [&] (const dht::decorated_key& dk) -> std::optional<position_in_partition> {
        auto cut = position_in_partition::for_partition_end();
        if (stop_key) {
            const auto c = dk.tri_compare(s, *stop_key);
            if (c > 0) {
                return std::nullopt;
            }
            if (c == 0) {
                cut = stop->position;
            }
        }
        if (auto it = skips.find(dk); it != skips.end() && pos_cmp(it->second, cut) < 0) {
            cut = it->second;
        }
        return cut;
    };

    // The versions of each partition, cut, in ring order.
    struct version {
        locator::host_id from;
        mutation mut;
    };
    std::map<dht::decorated_key, std::vector<version>, dht::decorated_key::less_comparator> versions{dht::decorated_key::less_comparator(_schema)};
    const auto hosts = replies | std::views::transform(&mutation_page_reply::from) | std::ranges::to<std::vector>();
    for (const auto& r : replies) {
        for (const partition& p : r.result->partitions()) {
            auto dk = p.mut().decorated_key(s);
            auto cut = cut_of(dk);
            if (!cut) {
                break;
            }
            auto m = cut_before(co_await unfreeze_gently(p.mut(), _schema), *cut);
            versions[std::move(dk)].push_back(version{r.from, std::move(m)});
        }
    }
    replies.clear();

    // Merge.
    std::vector<mutation> merged;
    merged.reserve(versions.size());
    for (const auto& [dk, vs] : versions) {
        mutation m = vs.front().mut;
        for (const auto& v : vs | std::views::drop(1)) {
            co_await apply_gently(m, v.mut);
        }
        merged.push_back(std::move(m));
    }

    // A skip is incomplete if the merged partition, with the part of it which
    // earlier rounds reconciled, holds fewer live rows than the limit before
    // the skip. The first incomplete skip moves E back to it. A complete skip
    // in the stop's partition decides that partition, and moves E to its
    // end.
    std::optional<full_position> end = stop;
    const uint64_t partition_row_limit = query::effective_partition_row_limit(_cmd->slice);
    for (const auto& [dk, skip] : skips) {
        auto cut = cut_of(dk);
        if (!cut || pos_cmp(*cut, skip) != 0) {
            // The skip lies after the stop.
            break;
        }
        auto m = std::ranges::find_if(merged, [&] (const mutation& m) { return m.decorated_key().equal(s, dk); });
        uint64_t live_rows = 0;
        if (m != merged.end()) {
            if (!_reconciled.empty() && _reconciled.back().decorated_key().equal(s, dk)) {
                auto whole = _reconciled.back();
                whole.apply(*m);
                live_rows = live_clustering_row_count(whole, _cmd->timestamp);
            } else {
                live_rows = live_clustering_row_count(*m, _cmd->timestamp);
            }
        }
        if (live_rows < partition_row_limit) {
            end = full_position(dk.key(), skip);
            break;
        }
        if (stop_key && dk.equal(s, *stop_key)) {
            end = full_position(dk.key(), position_in_partition::for_partition_end());
            break;
        }
    }
    const std::optional<dht::decorated_key> end_key = end ? std::optional(dht::decorate_key(s, end->partition)) : std::nullopt;
    const auto after_end = [&] (const dht::decorated_key& dk) {
        return end_key && dk.tri_compare(s, *end_key) > 0;
    };

    // Repair diffs, from the data before E. Every target gets an entry for
    // every partition, so that the repair writes count the replies of all
    // targets (for their CL).
    size_t i = 0;
    for (auto& [dk, vs] : versions) {
        const mutation& m = merged[i++];
        if (after_end(dk)) {
            break;
        }
        auto& diffs = _diffs[dk.key()];
        for (const auto& host : hosts) {
            auto v = std::ranges::find(vs, host, &version::from);
            auto diff = v != vs.end() ? m.partition().difference(s, v->mut.partition()) : mutation_partition(s, m.partition());
            auto& d = diffs[host];
            if (!diff.empty()) {
                auto mdiff = mutation(_schema, dk, std::move(diff));
                if (d) {
                    co_await apply_gently(*d, std::move(mdiff));
                } else {
                    d = std::move(mdiff);
                }
            }
            co_await coroutine::maybe_yield();
        }
    }

    // Append the data before E to the data of the earlier rounds. The first
    // partition may continue the last one of the previous round.
    for (auto& m : merged) {
        if (after_end(m.decorated_key())) {
            break;
        }
        if (m.partition().empty()) {
            continue;
        }
        if (!_reconciled.empty() && _reconciled.back().decorated_key().equal(s, m.decorated_key())) {
            co_await apply_gently(_reconciled.back(), std::move(m));
        } else {
            _reconciled.push_back(std::move(m));
        }
    }
    merged.clear();
    versions.clear();

    // A partition which E cuts, and which has no live clustering row before
    // E, may yield a static-only row. Whether it has one depends on the
    // rest of the partition, so the conversion leaves the partition out. A
    // skip cuts only partitions with live clustering rows.
    const bool convert_last = !_reconciled.empty() && !(end && !decides_partition(s, end->position)
            && _reconciled.back().decorated_key().equal(s, *end_key)
            && live_clustering_row_count(_reconciled.back(), _cmd->timestamp) == 0);

    // All partitions but the last one are final. Convert them once, and keep
    // only their conversion. Then convert the last partition, unless the
    // final partitions reached the limits. Its conversion counts only if the
    // page ends, because the next round converts it again.
    std::optional<full_position> conversion_stop;
    if (_reconciled.size() > 1) {
        auto conversion = co_await convert(_reconciled.size() - 1);
        conversion_stop = conversion.frontier().value().stop;
        keep(std::move(conversion));
        auto last = std::move(_reconciled.back());
        _reconciled.clear();
        _reconciled.push_back(std::move(last));
    }
    std::optional<query::result> last_conversion;
    if (!conversion_stop && convert_last) {
        last_conversion = co_await convert(1);
        conversion_stop = last_conversion->frontier().value().stop;
    }

    // _reconciled ends at E. If E lies inside a partition, _reconciled holds
    // only the part of that partition before E. to_data_query_result() cannot
    // tell that part from a whole partition, so it ends the partition after
    // that part. If the partition limit runs out there, it reports a stop at
    // the end of the partition. That stop is false, because the partition's
    // rows after E may still belong on the page. Ignore it, so that the page
    // ends at E instead.
    if (conversion_stop && end && conversion_stop->position.region() == partition_region::partition_end
            && end->position.region() != partition_region::partition_end && conversion_stop->partition.equal(s, end->partition)) {
        conversion_stop.reset();
    }

    bool short_page = false;
    std::optional<full_position> page_cursor;
    if (conversion_stop) {
        page_cursor = cursor_of(std::move(*conversion_stop));
    } else if (end) {
        // The next round starts at E. With a cursor inside a partition, it
        // reads the rest of the partition's clustering ranges.
        const auto& ek = *end_key;
        std::optional<query::clustering_row_ranges> ranges;
        if (!decides_partition(s, end->position)) {
            auto rest = _cmd->slice.row_ranges(s, end->partition);
            query::trim_clustering_row_ranges_to(s, rest, cursor_of(*end).position);
            if (!rest.empty()) {
                ranges = std::move(rest);
            }
        }
        std::optional<dht::partition_range> rest_of_range;
        if (ranges) {
            rest_of_range = _range.is_singular() ? dht::partition_range::make_singular(ek)
                    : dht::partition_range(dht::partition_range::bound(dht::ring_position(ek), true), _range.end());
        } else if (!_range.is_singular() && (!_range.end() || ek.tri_compare(s, _range.end()->value()) < 0)) {
            rest_of_range = dht::partition_range(dht::partition_range::bound(dht::ring_position(ek), false), _range.end());
        }
        if (!rest_of_range) {
            // Nothing of the range remains after E, so the page reached the
            // end of the range.
        } else if (_cmd->slice.options.contains<query::partition_slice::option::allow_short_read>()) {
            short_page = true;
            page_cursor = cursor_of(*end);
        } else {
            if (_round_start && full_position::cmp(s, *end, *_round_start) <= 0) {
                on_internal_error(rplogger, fmt::format("A reconciliation round of {}.{} did not move past its start {}",
                        s.ks_name(), s.cf_name(), _round_start->position));
            }
            _round_start = end;
            _round_range = std::move(*rest_of_range);
            _round_cmd = make_lw_shared<query::read_command>(*_cmd);
            _round_cmd->slice.clear_ranges();
            if (ranges) {
                _round_cmd->slice.set_range(s, end->partition, std::move(*ranges));
            }
            prepare_mutation_read(*_round_cmd, true);
            // The next round converts the last partition again. Keep only its
            // live data, so that it does not grow with the rounds.
            if (!_reconciled.empty()) {
                drop_dead_data(_reconciled.back());
            }
            co_return std::nullopt;
        }
    }

    if (last_conversion) {
        keep(std::move(*last_conversion));
    }
    auto result = converted_page();
    if (short_page) {
        result.set_short_read(query::short_read::yes);
    }
    if (page_cursor) {
        result.set_last_position(std::move(*page_cursor));
    }

    // Drop the partitions without diffs. Un-reverse the diffs of reversed
    // queries, like resolve_mutation_page().
    // (There are no "reverse writes", so the repair diffs have to be
    // converted to non-reverse diffs).
    for (auto it = _diffs.begin(); it != _diffs.end();) {
        if (std::ranges::none_of(it->second | std::views::values, std::mem_fn(&std::optional<mutation>::operator bool))) {
            it = _diffs.erase(it);
        } else {
            ++it;
        }
    }
    if (_cmd->slice.is_reversed()) {
        for (auto&& [key, diff] : _diffs) {
            for (auto&& [host, opt_mut] : diff) {
                if (opt_mut) {
                    opt_mut = reverse(std::move(opt_mut.value()));
                    co_await coroutine::maybe_yield();
                }
            }
        }
    }
    co_return accepted_mutation_page{std::move(result), std::move(_diffs)};
}

} // namespace service
