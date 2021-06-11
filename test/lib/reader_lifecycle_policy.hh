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

#include "mutation_reader.hh"
#include <seastar/core/gate.hh>

class test_reader_lifecycle_policy
        : public reader_lifecycle_policy
        , public enable_shared_from_this<test_reader_lifecycle_policy> {
    using factory_function = std::function<flat_mutation_reader(
            schema_ptr,
            const dht::partition_range&,
            const query::partition_slice&,
            const io_priority_class&,
            tracing::trace_state_ptr,
            mutation_reader::forwarding)>;

    struct reader_context {
        std::optional<reader_concurrency_semaphore> semaphore;
        std::optional<const dht::partition_range> range;
        std::optional<const query::partition_slice> slice;

        reader_context() = default;
        reader_context(dht::partition_range range, query::partition_slice slice) : range(std::move(range)), slice(std::move(slice)) {
        }
    };

    factory_function _factory_function;
    std::vector<foreign_ptr<std::unique_ptr<reader_context>>> _contexts;
    std::vector<future<>> _destroy_futures;
    bool _evict_paused_readers = false;

public:
    explicit test_reader_lifecycle_policy(factory_function f, bool evict_paused_readers = false)
        : _factory_function(std::move(f))
        , _contexts(smp::count)
        , _evict_paused_readers(evict_paused_readers) {
    }
    virtual flat_mutation_reader create_reader(
            schema_ptr schema,
            reader_permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override {
        const auto shard = this_shard_id();
        if (_contexts[shard]) {
            _contexts[shard]->range.emplace(range);
            _contexts[shard]->slice.emplace(slice);
        } else {
            _contexts[shard] = make_foreign(std::make_unique<reader_context>(range, slice));
        }
        return _factory_function(std::move(schema), *_contexts[shard]->range, *_contexts[shard]->slice, pc, std::move(trace_state), fwd_mr);
    }
    virtual future<> destroy_reader(stopped_reader reader) noexcept override {
        auto& ctx = _contexts[this_shard_id()];
        auto reader_opt = ctx->semaphore->unregister_inactive_read(std::move(reader.handle));
        auto ret = reader_opt ? reader_opt->close() : make_ready_future<>();
        return ret.finally([&ctx] {
            return ctx->semaphore->stop().finally([&ctx] {
                ctx.release();
            });
        });
    }
    virtual reader_concurrency_semaphore& semaphore() override {
        const auto shard = this_shard_id();
        if (!_contexts[shard]) {
            _contexts[shard] = make_foreign(std::make_unique<reader_context>());
        } else if (_contexts[shard]->semaphore) {
            return *_contexts[shard]->semaphore;
        }
        // To support multiple reader life-cycle instances alive at the same
        // time, incorporate `this` into the name, to make their names unique.
        auto name = format("tests::reader_lifecycle_policy@{}@shard_id={}", fmt::ptr(this), shard);
        if (_evict_paused_readers) {
            // Create with no memory, so all inactive reads are immediately evicted.
            _contexts[shard]->semaphore.emplace(1, 0, std::move(name));
        } else {
            _contexts[shard]->semaphore.emplace(reader_concurrency_semaphore::no_limits{}, std::move(name));
        }
        return *_contexts[shard]->semaphore;
    }
};

