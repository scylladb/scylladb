/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "dht/ring_position.hh"
#include "sstable_set.hh"
#include "readers/clustering_combined.hh"
#include "sstables/types_fwd.hh"
#include "utils/interval_index.hh"

namespace sstables {

// specialized when sstables are partitioned in the token range space
// e.g. leveled compaction strategy
class partitioned_sstable_set : public sstable_set_impl {
    // The intervals are keyed by biased tokens, i.e. tokens mapped into the
    // uint64_t domain (see dht::token::unbias()), rather than by ring
    // positions. That keeps the keys plain integers: comparisons don't need
    // the schema, and no partition key is copied into the index. The loss of
    // precision (a partition key is ignored when a token is shared by
    // sstable bounds) only makes selection return a superset, which callers
    // must tolerate anyway.
    using biased_token = uint64_t;
    using interval_index_type = utils::interval_index<biased_token, shared_sstable>;
    struct token_interval {
        biased_token start;
        biased_token end;
    };
private:
    schema_ptr _schema;
    interval_index_type _sstables;
    lw_shared_ptr<sstable_list> _all;
    std::unordered_map<run_id, shared_sstable_run> _all_runs;
    // Change counter on the interval index, which is used by the incremental
    // selector to determine whether or not to reposition its cursor.
    uint64_t _change_cnt = 0;
private:
    static token_interval make_interval(const dht::partition_range& range);
    static token_interval make_interval(const sstable& sst);
    // The range over which a selection made at `pos` holds, given the position
    // at which the set of sstables covering the position next changes, if any.
    static dht::partition_range to_partition_range(const dht::ring_position_view& pos, std::optional<biased_token> change);
    static dht::ring_position_ext to_next_position(std::optional<biased_token> change);
public:

    partitioned_sstable_set(const partitioned_sstable_set&) = delete;
    explicit partitioned_sstable_set(schema_ptr schema);
    // For cloning the partitioned_sstable_set (makes a deep copy, including *_all)
    explicit partitioned_sstable_set(
        schema_ptr schema,
        const interval_index_type& sstables,
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
