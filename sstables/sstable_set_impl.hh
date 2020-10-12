/*
 * Copyright (C) 2020 ScyllaDB
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

namespace sstables {

class incremental_selector_impl {
public:
    virtual ~incremental_selector_impl() {}
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view> select(const dht::ring_position_view&) = 0;
};

class sstable_set_impl {
public:
    virtual ~sstable_set_impl() {}
    virtual std::unique_ptr<sstable_set_impl> clone() const = 0;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const = 0;
    virtual void insert(shared_sstable sst) = 0;
    virtual void erase(shared_sstable sst) = 0;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const = 0;

    virtual flat_mutation_reader create_single_key_sstable_reader(
        column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::ring_position&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding) const;
};

// default sstable_set, not specialized for anything
class bag_sstable_set : public sstable_set_impl {
    // erasing is slow, but select() is fast
    std::vector<shared_sstable> _sstables;
public:
    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual void insert(shared_sstable sst) override;
    virtual void erase(shared_sstable sst) override;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
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
    explicit partitioned_sstable_set(schema_ptr schema, bool use_level_metadata = true);

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const override;
    virtual void insert(shared_sstable sst) override;
    virtual void erase(shared_sstable sst) override;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
};

class time_series_sstable_set : public sstable_set_impl {
public:
    // s.min_position() -> s
    using container_t = std::multimap<position_in_partition, shared_sstable, position_in_partition::less_compare>;

private:
    schema_ptr _schema;
    lw_shared_ptr<container_t> _sstables;

public:
    time_series_sstable_set(schema_ptr schema);
    time_series_sstable_set(const time_series_sstable_set& s);

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual void insert(shared_sstable sst) override;
    virtual void erase(shared_sstable sst) override;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;

    std::unique_ptr<position_reader_queue> make_min_position_reader_queue(
        std::function<flat_mutation_reader(sstable&)> create_reader,
        std::function<bool(const sstable&)> filter) const;
};

} // namespace sstables
