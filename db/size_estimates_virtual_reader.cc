/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <cmath>
#include <unordered_map>

#include "utils/assert.hh"

#include "keys/clustering_bounds_comparator.hh"
#include "replica/database_fwd.hh"
#include "db/system_keyspace.hh"
#include "dht/i_partitioner.hh"
#include "partition_range_compat.hh"
#include "utils/interval.hh"
#include "mutation/mutation_fragment.hh"
#include "sstables/hyperloglog.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "replica/database.hh"

#include "db/size_estimates_virtual_reader.hh"
#include "readers/from_mutations.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/tablets.hh"
#include "utils/chunked_vector.hh"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/log.hh>

namespace db {

namespace size_estimates {

static logging::logger selogger("size_estimates");

struct virtual_row {
    const bytes& cf_name;
    const token_range& tokens;
    clustering_key_prefix as_key() const {
        return clustering_key_prefix::from_exploded(std::vector<bytes_view>{cf_name, tokens.start, tokens.end});
    }
};

/**
 * Returns the keyspaces, ordered by name, as selected by the partition_range.
 */
static std::vector<sstring> get_keyspaces(const schema& s, const replica::database& db, dht::partition_range range) {
    struct keyspace_less_comparator {
        const schema& _s;
        keyspace_less_comparator(const schema& s) : _s(s) { }
        dht::ring_position as_ring_position(const sstring& ks) {
            auto pkey = partition_key::from_single_value(_s, utf8_type->decompose(ks));
            return dht::decorate_key(_s, std::move(pkey));
        }
        bool operator()(const sstring& ks1, const sstring& ks2) {
            return as_ring_position(ks1).less_compare(_s, as_ring_position(ks2));
        }
        bool operator()(const sstring& ks, const dht::ring_position& rp) {
            return as_ring_position(ks).less_compare(_s, rp);
        }
        bool operator()(const dht::ring_position& rp, const sstring& ks) {
            return rp.less_compare(_s, as_ring_position(ks));
        }
        bool operator()(const dht::ring_position& rp1, const dht::ring_position& rp2) {
            return rp1.less_compare(_s, rp2);
        }
    };
    auto keyspaces = db.get_non_system_keyspaces();
    auto cmp = keyspace_less_comparator(s);
    std::ranges::sort(keyspaces, cmp);
    return
        range.slice(keyspaces, std::move(cmp)) | std::views::filter([&s] (const auto& ks) {
            // If this is a range query, results are divided between shards by the partition key (keyspace_name).
            return dht::static_shard_of(s, dht::get_token(s,
                        partition_key::from_single_value(s, utf8_type->decompose(ks))))
                == this_shard_id();
        })
        | std::ranges::to<std::vector<sstring>>();
}

/**
 * Makes a wrapping range of ring_position from a nonwrapping range of token, used to select sstables.
 */
static dht::partition_range as_ring_position_range(const dht::token_range& r) {
    std::optional<wrapping_interval<dht::ring_position>::bound> start_bound, end_bound;
    if (r.start()) {
        start_bound = {{ dht::ring_position(r.start()->value(), dht::ring_position::token_bound::start), r.start()->is_inclusive() }};
    }
    if (r.end()) {
        end_bound = {{ dht::ring_position(r.end()->value(), dht::ring_position::token_bound::end), r.end()->is_inclusive() }};
    }
    return dht::partition_range(std::move(start_bound), std::move(end_bound), r.is_singular());
}

/**
 * Estimates for the specified ranges of the table identified by `cf_id`, considering its sstables across all shards.
 */
static future<std::vector<system_keyspace::range_estimates>> estimate(replica::database& db, table_id cf_id, schema_ptr schema, std::vector<token_range> trs) {
    struct range_estimate {
        int64_t count = 0;
        utils::estimated_histogram hist{0};
    };
    using shard_estimate = std::vector<range_estimate>;
    auto from_bytes = [] (auto& b) {
        return dht::token::from_sstring(utf8_type->to_string(b));
    };
    std::vector<dht::token_range_vector> ranges;
    for (const auto& tr : trs) {
        auto& unwrapped = ranges.emplace_back();
        ::compat::unwrap_into(
            wrapping_interval<dht::token>({{ from_bytes(tr.start), false }}, {{ from_bytes(tr.end) }}),
            dht::token_comparator(),
            [&] (auto&& rng) { unwrapped.push_back(std::move(rng)); });
    }

    // Estimate partition count and size distribution of each range from sstables on a single shard.
    auto estimate_on_shard = [cf_id, ranges] (replica::database& local_db) -> future<shard_estimate> {
        shard_estimate result(ranges.size());
        auto table_ptr = local_db.get_tables_metadata().get_table_if_exists(cf_id);
        if (!table_ptr) {
            co_return result;
        }
        auto& cf = *table_ptr;
        // Sketches live on disk; cache them by generation (not holding the sstables) so vnode ranges
        // spanning an sstable don't re-read it. Once full, further ranges keep the plain sum.
        std::unordered_map<sstables::generation_type, std::optional<hll::HyperLogLog>> sketches;
        constexpr size_t max_cached_sketches = 4096;
        constexpr uint32_t sketch_registers = uint32_t(1) << hll::cardinality_precision;
        static thread_local logger::rate_limit sketch_warn_limit{std::chrono::minutes(1)};
        for (size_t i = 0; i < ranges.size(); ++i) {
            co_await coroutine::maybe_yield();
            for (auto&& r : ranges[i]) {
                auto sstables = cf.select_sstables(as_ring_position_range(r));
                int64_t count = 0;
                uint64_t total_keys = 0;
                std::optional<hll::HyperLogLog> merged;
                bool use_sketches = sstables.size() > 1;
                for (auto&& sstable : sstables) {
                    count += co_await sstable->estimated_keys_for_range(r);
                    result[i].hist.merge(sstable->get_stats_metadata().estimated_partition_size);
                    if (use_sketches) {
                        auto it = sketches.find(sstable->generation());
                        if (it == sketches.end()) {
                            std::optional<hll::HyperLogLog> sketch;
                            // A compacted-away sstable's files may be gone already.
                            if (sketches.size() < max_cached_sketches && !sstable->marked_for_deletion() && !sstable->unlinked_at()) {
                                try {
                                    sketch = co_await sstable->read_cardinality_estimator();
                                } catch (const sstables::malformed_sstable_exception& e) {
                                    selogger.log(log_level::warn, sketch_warn_limit, "Cannot read cardinality sketch of {}: {}", sstable->get_filename(), e.what());
                                } catch (const std::system_error& e) {
                                    selogger.log(log_level::warn, sketch_warn_limit, "Cannot read cardinality sketch of {}: {}", sstable->get_filename(), e.what());
                                } catch (const storage_io_error& e) {
                                    selogger.log(log_level::warn, sketch_warn_limit, "Cannot read cardinality sketch of {}: {}", sstable->get_filename(), e.what());
                                }
                                it = sketches.emplace(sstable->generation(), std::move(sketch)).first;
                            }
                        }
                        static const std::optional<hll::HyperLogLog> no_sketch;
                        const auto& sketch = it != sketches.end() ? it->second : no_sketch;
                        // Legacy b=4 sketches are too coarse, and merge() rejects mixed precisions.
                        if (!sketch || sketch->registerSize() != sketch_registers) {
                            use_sketches = false;
                        } else if (merged) {
                            merged->merge(*sketch);
                        } else {
                            merged = sketch;
                        }
                        total_keys += sstable->get_estimated_key_count();
                    }
                }
                // The per-sstable sum counts keys present in several overlapping sstables more than once.
                // Ratios within 3 standard errors (1.04/sqrt(m)) of 1 are sketch noise, not overlap.
                if (use_sketches && total_keys > 0) {
                    const double ratio = merged->estimate() / total_keys;
                    if (count > 0 && ratio < 1.0 - 3 * 1.04 / std::sqrt(double(merged->registerSize()))) {
                        // Never round a non-empty range down to zero; Spark plans splits from these counts.
                        count = std::max<int64_t>(1, std::llround(count * ratio));
                    }
                }
                result[i].count += count;
            }
        }
        co_return result;
    };

    // Combine partial results from two shards.
    auto reduce = [] (shard_estimate a, const shard_estimate& b) {
        for (size_t i = 0; i < a.size(); ++i) {
            a[i].count += b[i].count;
            a[i].hist.merge(b[i].hist);
        }
        return a;
    };

    auto aggregate = co_await db.container().map_reduce0(std::move(estimate_on_shard), shard_estimate(ranges.size()), std::move(reduce));
    std::vector<system_keyspace::range_estimates> estimates;
    estimates.reserve(trs.size());
    for (size_t i = 0; i < trs.size(); ++i) {
        int64_t mean_size = aggregate[i].count > 0 ? aggregate[i].hist.mean() : 0;
        estimates.push_back(system_keyspace::range_estimates{schema, std::move(trs[i].start), std::move(trs[i].end), aggregate[i].count, mean_size});
    }
    co_return estimates;
}

/**
 * Returns the primary ranges for the local node.
 */
static future<std::vector<token_range>> get_local_ranges(replica::database& db, db::system_keyspace& sys_ks) {
    return sys_ks.get_local_tokens().then([&db] (auto&& tokens) {
        auto ranges = db.get_token_metadata().get_primary_ranges_for(std::move(tokens));
        std::vector<token_range> local_ranges;
        auto to_bytes = [](const std::optional<dht::token_range::bound>& b) {
            SCYLLA_ASSERT(b);
            return utf8_type->decompose(b->value().to_sstring());
        };
        // We merge the ranges to be compatible with how Cassandra shows it's size estimates table.
        // All queries will be on that table, where all entries are text and there's no notion of
        // token ranges form the CQL point of view.
        auto left_inf = std::ranges::find_if(ranges, [] (auto&& r) {
            return r.end() && (!r.start() || r.start()->value().is_minimum());
        });
        auto right_inf = std::ranges::find_if(ranges, [] (auto&& r) {
            return r.start() && (!r.end() || r.end()->value().is_maximum());
        });
        if (left_inf != right_inf && left_inf != ranges.end() && right_inf != ranges.end()) {
            local_ranges.push_back(token_range{to_bytes(right_inf->start()), to_bytes(left_inf->end())});
            ranges.erase(left_inf);
            ranges.erase(right_inf);
        }
        for (auto&& r : ranges) {
            local_ranges.push_back(token_range{to_bytes(r.start()), to_bytes(r.end())});
        }
        std::ranges::sort(local_ranges, [] (auto&& tr1, auto&& tr2) {
            return utf8_type->less(tr1.start, tr2.start);
        });
        return local_ranges;
    });
}

future<std::vector<token_range>> test_get_local_ranges(replica::database& db, db::system_keyspace& sys_ks) {
    return get_local_ranges(db, sys_ks);
}

size_estimates_mutation_reader::size_estimates_mutation_reader(replica::database& db, db::system_keyspace& sys_ks, schema_ptr schema, reader_permit permit, const dht::partition_range& prange,
        const query::partition_slice& slice, streamed_mutation::forwarding fwd)
            : impl(std::move(schema), std::move(permit))
            , _db(db)
            , _sys_ks(sys_ks)
            , _prange(&prange)
            , _slice(slice)
            , _fwd(fwd)
    { }

future<> size_estimates_mutation_reader::get_next_partition() {
    if (!_keyspaces) {
        _keyspaces = get_keyspaces(*_schema, _db, *_prange);
        _current_partition = _keyspaces->begin();
    }
    if (_current_partition == _keyspaces->end()) {
        _end_of_stream = true;
        co_return;
    }
    auto guard = reader_permit::awaits_guard(_permit);
    auto estimates = co_await this->estimates_for_current_keyspace();
    auto mutations = db::system_keyspace::make_size_estimates_mutation(*_current_partition, std::move(estimates));
    ++_current_partition;
    // Rows already match the slice; the reader only emits them in reverse for reversed reads.
    auto reader = make_mutation_reader_from_mutations(_schema, _permit, std::move(mutations), _fwd, _slice.is_reversed());
    if (_partition_reader) {
        co_await close_partition_reader();
    }
    _partition_reader = std::move(reader);
}

future<> size_estimates_mutation_reader::close_partition_reader() noexcept {
    return _partition_reader ? _partition_reader->close() : make_ready_future<>();
}

future<> size_estimates_mutation_reader::fill_buffer() {
    return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
        if (!_partition_reader) {
            return get_next_partition();
        }
        return _partition_reader->consume_pausable([this] (mutation_fragment_v2 mf) {
            push_mutation_fragment(std::move(mf));
            return stop_iteration(is_buffer_full());
        }).then([this] {
            if (_partition_reader->is_end_of_stream() && _partition_reader->is_buffer_empty()) {
                return _partition_reader->close();
            }
            return make_ready_future<>();
        });
    });
}

future<> size_estimates_mutation_reader::next_partition() {
    clear_buffer_to_next_partition();
    if (is_buffer_empty()) {
        return close_partition_reader();
    }
    return make_ready_future<>();
}

future<> size_estimates_mutation_reader::fast_forward_to(const dht::partition_range& pr) {
    clear_buffer();
    _prange = &pr;
    _keyspaces = std::nullopt;
    _end_of_stream = false;
    return close_partition_reader();
}

future<> size_estimates_mutation_reader::fast_forward_to(position_range pr) {
    clear_buffer();
    _end_of_stream = false;
    if (_partition_reader) {
        return _partition_reader->fast_forward_to(std::move(pr));
    }
    return make_ready_future<>();
}

future<> size_estimates_mutation_reader::close() noexcept {
    return close_partition_reader();
}

future<std::vector<db::system_keyspace::range_estimates>>
size_estimates_mutation_reader::estimates_for_current_keyspace() const {
    // Rows are produced in query order; the partition reader emits them reversed for reversed reads.
    const bool reversed = _slice.is_reversed();
    // For reversed reads _schema is the reversed query schema; table_schema is always forward.
    auto table_schema = reversed ? _schema->get_reversed() : _schema;
    std::optional<query::partition_slice> fwd_slice;
    if (reversed) {
        fwd_slice.emplace(query::reverse_slice(*_schema, _slice));
    }
    const auto& slice = fwd_slice ? *fwd_slice : _slice;
    // For each specified range, estimate (crudely) mean partition size and partitions count.
    auto pkey = partition_key::from_single_value(*table_schema, utf8_type->decompose(*_current_partition));
    auto cfs = _db.find_keyspace(*_current_partition).metadata()->cf_meta_data();
    auto cf_names = cfs | std::views::transform([] (auto&& cf) {
        return utf8_type->decompose(cf.first);
    }) | std::ranges::to<std::vector<bytes>>();
    std::ranges::sort(cf_names, [] (auto&& n1, auto&& n2) {
        return utf8_type->less(n1, n2);
    });
    std::vector<db::system_keyspace::range_estimates> estimates;
    // Vnode ranges are only needed once a vnode table is reached.
    std::optional<utils::chunked_vector<token_range>> local_ranges;
    const auto& row_ranges = slice.row_ranges(*table_schema, pkey);
    auto pranges = row_ranges | std::views::transform([] (const auto& r) { return position_range(r); })
            | std::ranges::to<std::vector<position_range>>();
    position_in_partition::less_compare less(*table_schema);
    // Rows are produced in query order so the row limit can stop early.
    if (reversed) {
        std::ranges::reverse(cf_names);
        std::ranges::reverse(pranges);
    }
    for (const auto& cf_name : cf_names) {
        auto cf_key = clustering_key_prefix::from_exploded(std::vector<bytes_view>{cf_name});
        auto cf_name_str = utf8_type->to_string(cf_name);
        // Candidate ranges of this table, resolved on first use (s is null until then).
        utils::chunked_vector<token_range> tablet_ranges;
        const utils::chunked_vector<token_range>* table_ranges = nullptr;
        schema_ptr s;
        // Estimated in batches, so a sketch is read once per batch rather than once per range.
        constexpr size_t max_ranges_per_estimate = 1000;
        std::vector<token_range> wanted;
        bool limit_reached = false;
        for (const auto& pr : pranges) {
            if (limit_reached) {
                break;
            }
            // All rows of a table share its name as first component, so a miss on it skips the table.
            // Rows of the table lie strictly between before_key(cf_key) and after_all_prefixed(cf_key).
            if (!(less(pr.start(), position_in_partition_view::after_all_prefixed(cf_key))
                    && less(position_in_partition_view::before_key(cf_key), pr.end()))) {
                continue;
            }
            if (!s) {
                // The tablet loop yields, so a table may be dropped mid-query.
                if (!_db.has_schema(*_current_partition, cf_name_str)) {
                    break;
                }
                auto& cf = _db.find_column_family(*_current_partition, cf_name_str);
                s = cf.schema();
                if (cf.uses_tablets()) {
                    // Only the primary replica reports a tablet, so summing over all nodes counts each tablet once.
                    auto erm = cf.get_effective_replication_map();
                    const auto& topo = erm->get_topology();
                    const auto& tmap = erm->get_token_metadata().tablets().get_tablet_map(s->id());
                    for (auto tid : tmap.tablet_ids()) {
                        // get_primary_replica() copies and sorts the replica set; skip tablets we don't host.
                        const auto& replicas = tmap.get_tablet_info(tid).replicas;
                        if (std::ranges::any_of(replicas, [&] (const auto& r) { return r.host == topo.my_host_id(); })
                                && tmap.get_primary_replica(tid, topo).host == topo.my_host_id()) {
                            auto r = tmap.get_token_range(tid);
                            tablet_ranges.push_back(
                                    token_range{utf8_type->decompose(r.start()->value().to_sstring()), utf8_type->decompose(r.end()->value().to_sstring())});
                        }
                        co_await coroutine::maybe_yield();
                    }
                    // Tablet order is token order, but rows are ordered by range_start as text.
                    std::ranges::sort(tablet_ranges, [] (auto&& tr1, auto&& tr2) {
                        return utf8_type->less(tr1.start, tr2.start);
                    });
                    table_ranges = &tablet_ranges;
                } else {
                    if (!local_ranges) {
                        auto v = co_await get_local_ranges(_db, _sys_ks);
                        local_ranges.emplace();
                        std::ranges::move(v, std::back_inserter(*local_ranges));
                    }
                    // local_ranges is already sorted by range_start.
                    table_ranges = &*local_ranges;
                }
            }
            auto row_pos_less = [&] (const token_range& r, position_in_partition_view bound) {
                auto key = virtual_row{cf_name, r}.as_key();
                return less(position_in_partition_view::for_key(key), bound);
            };
            auto first = std::ranges::partition_point(*table_ranges, [&] (const token_range& r) { return row_pos_less(r, pr.start()); });
            auto last = std::partition_point(first, table_ranges->end(), [&] (const token_range& r) { return row_pos_less(r, pr.end()); });
            for (ssize_t i = 0, n = last - first; i < n; ++i) {
                wanted.push_back(reversed ? *(last - 1 - i) : *(first + i));
                if (estimates.size() + wanted.size() >= slice.partition_row_limit()) {
                    limit_reached = true;
                    break;
                }
                // Bound the per-call vector (it is also copied to every shard).
                if (wanted.size() >= max_ranges_per_estimate) {
                    std::ranges::move(co_await estimate(_db, s->id(), s, std::exchange(wanted, {})), std::back_inserter(estimates));
                }
            }
        }
        if (!wanted.empty()) {
            std::ranges::move(co_await estimate(_db, s->id(), s, std::move(wanted)), std::back_inserter(estimates));
        }
        if (limit_reached) {
            co_return estimates;
        }
    }
    co_return estimates;
}

} // namespace size_estimates

} // namespace db
