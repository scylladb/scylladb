/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <bit>
#include <map>

#include "dht/ring_position.hh"
#include "sstable_set.hh"
#include "readers/clustering_combined.hh"
#include "sstables/types_fwd.hh"

namespace sstables {

// Indexes sstables by the token range they span, so that a query can select only
// the sstables overlapping the range it asks for.
//
// An sstable spanning [first, last] overlaps a query range [s, e] iff
// first <= e && last >= s. Ordering the sstables by first token turns the first
// half of that test into a bound lookup, and the second half is checked on each
// candidate. On its own that leaves the walk unbounded from below, because an
// sstable overlapping the query may begin arbitrarily early, so a query near the
// middle of the range walks about half the index.
//
// To bound it from below as well, the sstables are partitioned into tiers by the
// width of their token range: tier k holds those whose width has bit_width() == k,
// so every member is narrower than 2^k. A member of tier k reaching s must
// therefore begin at or after s - 2^k, which closes the open end and makes the
// walk proportional to the number of sstables the tier actually contributes to
// the answer, up to the factor of two by which widths vary inside a tier.
//
// The bound comes from the tier's own definition rather than from a measured
// maximum or from the range the owning compaction group spans, so nothing outside
// the tier can make it too small -- and a bound that was too small would silently
// drop sstables that do overlap. Only non-empty tiers are materialized, and an
// sstable's tier never changes, since its bounds are fixed once its keys are known.
//
// Every sstable appears in exactly one tier, so the footprint is linear in the
// number of sstables regardless of how deeply their ranges overlap. Keying on
// tokens rather than on ring positions means a query can select an sstable that
// does not in fact hold the key, which is harmless -- it costs a bloom filter
// check -- whereas failing to select one would lose data.
class partitioned_sstable_set : public sstable_set_impl {
    using token_map = std::multimap<dht::token, shared_sstable>;
    // Keyed by width exponent, so iteration visits the tiers in width order and
    // an empty tier costs nothing.
    using tier_map = std::map<uint8_t, token_map>;
private:
    schema_ptr _schema;
    tier_map _tiers;
    lw_shared_ptr<sstable_list> _all;
    std::unordered_map<run_id, shared_sstable_run> _all_runs;
    // Bumped on every change, so that an incremental selector can tell that the
    // sweep state it accumulated no longer reflects the set.
    uint64_t _change_cnt = 0;
private:
    // Token range covering `range`. Unbounded sides become the minimum and
    // maximum token, and exclusive bounds are widened to inclusive ones, since
    // over-approximating is safe.
    static dht::token_range to_token_range(const dht::partition_range& range);
    // The tier an sstable belongs to: bit_width() of the number of tokens it
    // spans, so a member of tier k spans fewer than 2^k of them. An sstable whose
    // first or last key is unknown reports a non-key sentinel, which spans
    // everything, and lands in the widest tier.
    static uint8_t tier_of(const sstable& sst);
    // The earliest first token from which a member of tier `exponent` can still
    // reach `start`.
    static dht::token tier_window_start(uint8_t exponent, const dht::token& start);
public:
    partitioned_sstable_set(const partitioned_sstable_set&) = delete;
    // `token_range` is the range spanned by the owning compaction group. It is
    // no longer needed to index the sstables and is accepted only so that the
    // callers of make_partitioned_sstable_set() stay unchanged; removing it is
    // left to a separate cleanup.
    explicit partitioned_sstable_set(schema_ptr schema, dht::token_range token_range);
    // For cloning the partitioned_sstable_set (makes a deep copy, including *_all)
    explicit partitioned_sstable_set(
        schema_ptr schema,
        const tier_map& tiers,
        const lw_shared_ptr<sstable_list>& all,
        const std::unordered_map<run_id, shared_sstable_run>& all_runs,
        file_size_stats bytes_on_disk);

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const override;
    virtual std::vector<frozen_sstable_run> all_sstable_runs() const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const override;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const override;
    virtual bool insert(shared_sstable sst) override;
    virtual bool erase(shared_sstable sst) override;
    virtual size_t size() const noexcept override;
    virtual sstable_set_impl::selector_and_schema_t make_incremental_selector() const override;
    class incremental_selector;
};

class time_series_sstable_set : public sstable_set_impl {
private:
    using container_t = std::multimap<position_in_partition, shared_sstable, position_in_partition::less_compare>;

    schema_ptr _schema;
    schema_ptr _reversed_schema; // == _schema->make_reversed();
    bool _enable_optimized_twcs_queries;
    // s.min_position() -> s, ordered using _schema
    lw_shared_ptr<container_t> _sstables;
    // s.max_position().reversed() -> s, ordered using _reversed_schema; the set of values is the same as in _sstables
    lw_shared_ptr<container_t> _sstables_reversed;

public:
    time_series_sstable_set(schema_ptr schema, bool enable_optimized_twcs_queries);
    time_series_sstable_set(const time_series_sstable_set& s);

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const override;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const override;
    virtual bool insert(shared_sstable sst) override;
    virtual bool erase(shared_sstable sst) override;
    virtual size_t size() const noexcept override;
    virtual sstable_set_impl::selector_and_schema_t make_incremental_selector() const override;

    std::unique_ptr<position_reader_queue> make_position_reader_queue(
        std::function<mutation_reader(sstable&)> create_reader,
        std::function<bool(const sstable&)> filter,
        partition_key pk, schema_ptr schema, reader_permit permit,
        streamed_mutation::forwarding fwd_sm,
        bool reversed) const;

    virtual mutation_reader create_single_key_sstable_reader(
        replica::column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&,
        const query::partition_slice&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        const sstable_predicate&,
        sstables::integrity_check integrity = sstables::integrity_check::no) const override;

    friend class sstable_position_reader_queue;
};

// this compound set holds reference to N sstable sets and allow their operations to be combined.
// the managed sets cannot be modified through compound_sstable_set, but only jointly read from, so insert() and erase() are disabled.
class compound_sstable_set : public sstable_set_impl {
    schema_ptr _schema;
    std::vector<lw_shared_ptr<sstable_set>> _sets;
public:
    compound_sstable_set(schema_ptr schema, std::vector<lw_shared_ptr<sstable_set>> sets);

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual std::vector<frozen_sstable_run> all_sstable_runs() const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const override;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const override;
    virtual bool insert(shared_sstable sst) override;
    virtual bool erase(shared_sstable sst) override;
    virtual size_t size() const noexcept override;
    virtual file_size_stats get_file_size_stats() const noexcept override;
    virtual sstable_set_impl::selector_and_schema_t make_incremental_selector() const override;

    virtual mutation_reader create_single_key_sstable_reader(
            replica::column_family*,
            schema_ptr,
            reader_permit,
            utils::estimated_histogram&,
            const dht::partition_range&,
            const query::partition_slice&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding,
            const sstable_predicate&,
            sstables::integrity_check integrity = sstables::integrity_check::no) const override;

    class incremental_selector;
};

} // namespace sstables
