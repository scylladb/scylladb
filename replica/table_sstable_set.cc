/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replica/database.hh"
#include "replica/compaction_group.hh"
#include "dht/i_partitioner.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstable_set_impl.hh"
#include "log.hh"

namespace replica {

extern logging::logger tlogger;

// this sstable set incrementally consumes unserlying sstable sets
// the managed sets cannot be modified through table_sstable_set, but only jointly read from, so insert() and erase() are disabled.
class table_sstable_set : public sstables::sstable_set_impl {
    table& _table;
    schema_ptr _schema;
public:
    table_sstable_set(table& t) noexcept
        : _table(t)
        , _schema(_table.schema())
    {}

    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<table_sstable_set>(_table);
    }

    static sstables::sstable_set make(table& t) {
        return sstables::sstable_set(std::make_unique<table_sstable_set>(t), t.schema());
    }

    virtual std::vector<sstables::shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual std::vector<sstables::sstable_run> select_sstable_runs(const std::vector<sstables::shared_sstable>& sstables) const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const sstables::shared_sstable&)> func) const override;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const sstables::shared_sstable&)> func) const override;
    virtual bool insert(sstables::shared_sstable sst) override;
    virtual bool erase(sstables::shared_sstable sst) override;
    virtual size_t size() const noexcept override;
    virtual uint64_t bytes_on_disk() const noexcept override;
    virtual std::unique_ptr<sstables::incremental_selector_impl> make_incremental_selector() const override;

    virtual flat_mutation_reader_v2 create_single_key_sstable_reader(
            replica::column_family*,
            schema_ptr,
            reader_permit,
            utils::estimated_histogram&,
            const dht::partition_range&,
            const query::partition_slice&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding,
            const sstables::sstable_predicate&) const override;

    class incremental_selector;
private:
    // The for_each_sstable_set_* helpers guarantee atomicity
    // only for each compaction group' compound_sstable_set,
    // but not across compaction groups.
    stop_iteration for_each_sstable_set_until(const dht::partition_range&, std::function<stop_iteration(lw_shared_ptr<sstables::sstable_set>)>) const;
    future<stop_iteration> for_each_sstable_set_gently_until(const dht::partition_range&, std::function<future<stop_iteration>(lw_shared_ptr<sstables::sstable_set>)>) const;
};

stop_iteration table_sstable_set::for_each_sstable_set_until(const dht::partition_range& pr, std::function<stop_iteration(lw_shared_ptr<sstables::sstable_set>)> func) const {
    if (auto scg = _table.single_compaction_group_if_available()) {
        auto set = scg->make_compound_sstable_set();
        return func(set);
    }
    if (pr.is_full()) {
        for (const auto& cg : _table._compaction_groups) {
            auto set = cg->make_compound_sstable_set();
            if (func(set) == stop_iteration::yes) {
                return stop_iteration::yes;
            }
        }
    } else {
        auto token = pr.start() ? pr.start()->value().token() : dht::minimum_token();
        auto id = _table.compaction_group_for_token(token).group_id();
        token = pr.end() ? pr.end()->value().token() : dht::maximum_token();
        auto end = _table.compaction_group_for_token(token).group_id() + 1;
        for (; id < end; ++id) {
            auto set = _table._compaction_groups[id]->make_compound_sstable_set();
            if (func(set) == stop_iteration::yes) {
                return stop_iteration::yes;
            }
        }
    }
    return stop_iteration::no;
}

future<stop_iteration> table_sstable_set::for_each_sstable_set_gently_until(const dht::partition_range& pr, std::function<future<stop_iteration>(lw_shared_ptr<sstables::sstable_set>)> func) const {
    if (auto scg = _table.single_compaction_group_if_available()) {
        auto set = scg->make_compound_sstable_set();
        co_return co_await func(set);
    }
    if (pr.is_full()) {
        for (const auto& cg : _table._compaction_groups) {
            auto set = cg->make_compound_sstable_set();
            if (co_await func(set) == stop_iteration::yes) {
                co_return stop_iteration::yes;
            }
        }
    } else {
        auto token = pr.start() ? pr.start()->value().token() : dht::minimum_token();
        auto id = _table.compaction_group_for_token(token).group_id();
        token = pr.end() ? pr.end()->value().token() : dht::maximum_token();
        auto end = _table.compaction_group_for_token(token).group_id() + 1;
        for (; id < end; ++id) {
            auto set = _table._compaction_groups[id]->make_compound_sstable_set();
            if (co_await func(set) == stop_iteration::yes) {
                co_return stop_iteration::yes;
            }
        }
    }
    co_return stop_iteration::no;
}

std::vector<sstables::shared_sstable> table_sstable_set::select(const dht::partition_range& range) const {
    std::vector<shared_sstable> ret;
    for_each_sstable_set_until(range, [&] (lw_shared_ptr<sstables::sstable_set> set) {
        auto ssts = set->select(range);
        if (ret.empty()) {
            ret = std::move(ssts);
        } else {
            if (ssts.size() > 1) {
                ret.reserve(ret.size() + ssts.size());
            }
            std::move(ssts.begin(), ssts.end(), std::back_inserter(ret));
        }
        return stop_iteration::no;
    });
    return ret;
}

std::vector<sstable_run> table_sstable_set::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
    std::vector<sstable_run> ret;
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstables::sstable_set> set) {
        auto runs = set->select_sstable_runs(sstables);
        if (ret.empty()) {
            ret = std::move(runs);
        } else {
            ret.reserve(ret.size() + runs.size());
            std::move(runs.begin(), runs.end(), std::back_inserter(ret));
        }
        return stop_iteration::no;
    });
    return ret;
}

lw_shared_ptr<const sstable_list> table_sstable_set::all() const {
    auto ret = make_lw_shared<sstable_list>();
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstables::sstable_set> set) {
        set->for_each_sstable([&] (const shared_sstable& sst) {
            ret->insert(sst);
        });
        return stop_iteration::no;
    });
    return ret;
}

stop_iteration table_sstable_set::for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const {
    return for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstables::sstable_set> set) {
        return set->for_each_sstable_until([&] (const shared_sstable& sst) {
            return func(sst);
        });
    });
}

future<stop_iteration> table_sstable_set::for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const {
    co_return co_await for_each_sstable_set_gently_until(query::full_partition_range, [&] (lw_shared_ptr<sstables::sstable_set> set) {
        return set->for_each_sstable_gently_until(func);
    });
}

bool table_sstable_set::insert(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}
bool table_sstable_set::erase(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}

size_t
table_sstable_set::size() const noexcept {
    size_t ret = 0;
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstables::sstable_set> set) {
        ret += set->size();
        return stop_iteration::no;
    });
    return ret;
}

uint64_t
table_sstable_set::bytes_on_disk() const noexcept {
    uint64_t ret = 0;
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstables::sstable_set> set) {
        ret += set->bytes_on_disk();
        return stop_iteration::no;
    });
    return ret;
}

class table_incremental_selector : public sstables::incremental_selector_impl {
private:
    table& _table;
    schema_ptr _schema;
    replica::compaction_group* _cur_cg;
    lw_shared_ptr<sstable_set> _cur_set;
    std::optional<sstable_set::incremental_selector> _cur_selector;
public:
    table_incremental_selector(table& t)
            : _table(t)
            , _schema(t.schema())
            , _cur_cg(_table.single_compaction_group_if_available())
    {
        if (_cur_cg) {
            _cur_set = _cur_cg->make_compound_sstable_set();
            _cur_selector.emplace(_cur_set->make_incremental_selector());
        }
    }

    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const dht::ring_position_view& pos) override {
        // Always return minimum singular range, such that incremental_selector::select() will always call this function,
        // which in turn will find the next sstable set to select sstables from.
        const dht::partition_range current_range = dht::partition_range::make_singular(dht::ring_position::min());

        // Return all sstables selected on the requested position from the first matching sstable set.
        // This assumes that the underlying sstable sets are disjoint in their token ranges so
        // only one of them contain any given token.
        std::vector<shared_sstable> sstables;

        if (!_cur_selector) {
            _cur_cg = &_table.compaction_group_for_token(pos.token());
            _cur_set = _cur_cg->make_compound_sstable_set();
            _cur_selector.emplace(_cur_set->make_incremental_selector());
        }

        auto res = _cur_selector->select(pos);
        sstables = res.sstables;
        // Return the lowest next position, such that this function will be called again to select the
        // lowest next position from the selector which previously returned it.
        dht::ring_position_ext lowest_next_position = res.next_position;
        // Jump to the next sstable set when this one is exhausted
        if (lowest_next_position.is_max()) {
            _cur_selector.reset();
            _cur_set = {};
            auto id = _cur_cg->group_id();
            _cur_cg = nullptr;
            if (++id < _table._compaction_groups.size()) {
                auto& cg = *_table._compaction_groups[id];
                // Next compaction group start bound must be engaged
                auto start = *cg.token_range().start();
                dht::token token = start.value();
                if (!start.is_inclusive()) {
                    token = token.next();
                }
                lowest_next_position = dht::ring_position_view::starting_at(token);
            }
        }

        tlogger.trace("table_incremental_selector: select pos={}: returning {} sstables, next_pos={}", pos, sstables.size(), lowest_next_position);
        return std::make_tuple(std::move(current_range), std::move(sstables), std::move(lowest_next_position));
    }
};

std::unique_ptr<incremental_selector_impl> table_sstable_set::make_incremental_selector() const {
    return std::make_unique<table_incremental_selector>(_table);
}

flat_mutation_reader_v2
table_sstable_set::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstable_predicate& predicate) const {
    // The singular partition_range start bound must be engaged.
    auto token = pr.start()->value().token();
    auto& cg = _table.compaction_group_for_token(token);
    auto set = cg.make_compound_sstable_set();
    return set->create_single_key_sstable_reader(cf, std::move(schema), std::move(permit), sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate);
}

lw_shared_ptr<sstables::sstable_set> table::make_table_sstable_set() {
    return make_lw_shared(table_sstable_set::make(*this));
}

} // namespace replica
