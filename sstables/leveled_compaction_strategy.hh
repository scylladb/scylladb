/*
 * Copyright (C) 2017-present ScyllaDB
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

#include <optional>
#include <vector>
#include <map>
#include <memory>

#include <seastar/core/sstring.hh>

#include "compaction_strategy_type.hh"
#include "database_fwd.hh"
#include "sstables/size_tiered_compaction_strategy.hh"
#include "sstables/compaction_strategy_impl.hh"
#include "sstables/compaction_backlog_manager.hh"
#include "sstables/shared_sstable.hh"

class leveled_manifest;

namespace sstables {

class sstable_set_impl;

class leveled_compaction_strategy : public compaction_strategy_impl {
    static constexpr int32_t DEFAULT_MAX_SSTABLE_SIZE_IN_MB = 160;
    const sstring SSTABLE_SIZE_OPTION = "sstable_size_in_mb";

    int32_t _max_sstable_size_in_mb = DEFAULT_MAX_SSTABLE_SIZE_IN_MB;
    std::optional<std::vector<std::optional<dht::decorated_key>>> _last_compacted_keys;
    std::vector<int> _compaction_counter;
    size_tiered_compaction_strategy_options _stcs_options;
    compaction_backlog_tracker _backlog_tracker;
    int32_t calculate_max_sstable_size_in_mb(std::optional<sstring> option_value) const;
public:
    leveled_compaction_strategy(const std::map<sstring, sstring>& options);
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    virtual compaction_descriptor get_major_compaction_job(column_family& cf, std::vector<sstables::shared_sstable> candidates) override;

    virtual void notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) override;

    // for each level > 0, get newest sstable and use its last key as last
    // compacted key for the previous level.
    void generate_last_compacted_keys(leveled_manifest& manifest);

    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    virtual bool parallel_compaction() const override {
        return false;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::leveled;
    }
    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const override;

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode) override;
};

}
