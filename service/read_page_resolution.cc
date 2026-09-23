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
#include <ranges>

#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "service/read_page_resolution.hh"
#include "mutation/async_utils.hh"
#include "mutation/mutation_partition.hh"
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

digest_page_decision decide_digest_page(const schema& s, digest_read_result cl_result, const foreground_reply_collector& replies,
        bool empty_replica_pages) {
    if (!cl_result.digests_match) {
        return digest_page_mismatch{};
    }
    auto& result = cl_result.result;
    if (empty_replica_pages && replies.response_count() > 1) {
        auto& mp = replies.min_position();
        auto& lp = result->last_position();
        if (!mp || bool(lp) < bool(mp) || full_position::cmp(s, *mp, *lp) < 0) {
            result->set_last_position(mp);
        }
    }
    return accepted_digest_page{std::move(result)};
}

} // namespace service
