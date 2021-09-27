/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <boost/icl/interval_map.hpp>

#include "compatible_ring_position.hh"
#include "sstable_set.hh"
#include "mutation_reader.hh"

namespace sstables {

class incremental_selector_impl {
public:
    virtual ~incremental_selector_impl() {}
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const dht::ring_position_view&) = 0;
};

class sstable_set_impl {
public:
    virtual ~sstable_set_impl() {}
    virtual std::unique_ptr<sstable_set_impl> clone() const = 0;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const = 0;
    virtual std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const;
    virtual lw_shared_ptr<sstable_list> all() const = 0;
    virtual void for_each_sstable(std::function<void(const shared_sstable&)> func) const = 0;
    virtual void insert(shared_sstable sst) = 0;
    virtual void erase(shared_sstable sst) = 0;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const = 0;

    virtual flat_mutation_reader create_single_key_sstable_reader(
        column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding) const;
};

// specialized when sstables are partitioned in the token range space
// e.g. leveled compaction strategy
class partitioned_sstable_set : public sstable_set_impl {
    using value_set = std::unordered_set<shared_sstable>;
    using interval_map_type = boost::icl::interval_map<compatible_ring_position_or_view, value_set>;
    using interval_type = interval_map_type::interval_type;
    using map_iterator = interval_map_type::const_iterator;
private:
    schema_ptr _schema;
    std::vector<shared_sstable> _unleveled_sstables;
    interval_map_type _leveled_sstables;
    lw_shared_ptr<sstable_list> _all;
    std::unordered_map<utils::UUID, sstable_run> _all_runs;
    // Change counter on interval map for leveled sstables which is used by
    // incremental selector to determine whether or not to invalidate iterators.
    uint64_t _leveled_sstables_change_cnt = 0;
    bool _use_level_metadata = false;
private:
    static interval_type make_interval(const schema& s, const dht::partition_range& range);
    interval_type make_interval(const dht::partition_range& range) const;
    static interval_type make_interval(const schema_ptr& s, const sstable& sst);
    interval_type make_interval(const sstable& sst);
    interval_type singular(const dht::ring_position& rp) const;
    std::pair<map_iterator, map_iterator> query(const dht::partition_range& range) const;
    // SSTables are stored separately to avoid interval map's fragmentation issue when level 0 falls behind.
    bool store_as_unleveled(const shared_sstable& sst) const;
public:
    static dht::ring_position to_ring_position(const compatible_ring_position_or_view& crp);
    static dht::partition_range to_partition_range(const interval_type& i);
    static dht::partition_range to_partition_range(const dht::ring_position_view& pos, const interval_type& i);

    partitioned_sstable_set(const partitioned_sstable_set&) = delete;
    explicit partitioned_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all, bool use_level_metadata = true);
    // For cloning the partitioned_sstable_set (makes a deep copy, including *_all)
    explicit partitioned_sstable_set(
        schema_ptr schema,
        const std::vector<shared_sstable>& unleveled_sstables,
        const interval_map_type& leveled_sstables,
        const lw_shared_ptr<sstable_list>& all,
        const std::unordered_map<utils::UUID, sstable_run>& all_runs,
        bool use_level_metadata);

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const override;
    virtual std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const override;
    virtual lw_shared_ptr<sstable_list> all() const override;
    virtual void for_each_sstable(std::function<void(const shared_sstable&)> func) const override;
    virtual void insert(shared_sstable sst) override;
    virtual void erase(shared_sstable sst) override;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
};

class time_series_sstable_set : public sstable_set_impl {
private:
    using container_t = std::multimap<position_in_partition, shared_sstable, position_in_partition::less_compare>;

    schema_ptr _schema;
    schema_ptr _reversed_schema; // == _schema->make_reversed();
    // s.min_position() -> s, ordered using _schema
    lw_shared_ptr<container_t> _sstables;
    // s.max_position().reversed() -> s, ordered using _reversed_schema; the set of values is the same as in _sstables
    lw_shared_ptr<container_t> _sstables_reversed;

public:
    time_series_sstable_set(schema_ptr schema);
    time_series_sstable_set(const time_series_sstable_set& s);

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual lw_shared_ptr<sstable_list> all() const override;
    virtual void for_each_sstable(std::function<void(const shared_sstable&)> func) const override;
    virtual void insert(shared_sstable sst) override;
    virtual void erase(shared_sstable sst) override;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;

    std::unique_ptr<position_reader_queue> make_position_reader_queue(
        std::function<flat_mutation_reader(sstable&)> create_reader,
        std::function<bool(const sstable&)> filter,
        partition_key pk, schema_ptr schema, reader_permit permit,
        streamed_mutation::forwarding fwd_sm,
        bool reversed) const;

    virtual flat_mutation_reader create_single_key_sstable_reader(
        column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding) const override;

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
    virtual std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const override;
    virtual lw_shared_ptr<sstable_list> all() const override;
    virtual void for_each_sstable(std::function<void(const shared_sstable&)> func) const override;
    virtual void insert(shared_sstable sst) override;
    virtual void erase(shared_sstable sst) override;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;

    virtual flat_mutation_reader create_single_key_sstable_reader(
            column_family*,
            schema_ptr,
            reader_permit,
            utils::estimated_histogram&,
            const dht::partition_range&,
            const query::partition_slice&,
            const io_priority_class&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding) const override;

    class incremental_selector;
};

} // namespace sstables
