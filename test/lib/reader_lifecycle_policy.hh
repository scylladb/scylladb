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

#include "mutation_reader.hh"

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

    struct reader_params {
        const dht::partition_range range;
        const query::partition_slice slice;
    };
    struct reader_context {
        foreign_ptr<std::unique_ptr<reader_concurrency_semaphore>> semaphore;
        foreign_ptr<std::unique_ptr<const reader_params>> params;
    };

    factory_function _factory_function;
    std::vector<reader_context> _contexts;
    bool _evict_paused_readers = false;

public:
    explicit test_reader_lifecycle_policy(factory_function f, bool evict_paused_readers = false)
        : _factory_function(std::move(f))
        , _contexts(smp::count)
        , _evict_paused_readers(evict_paused_readers) {
    }
    virtual flat_mutation_reader create_reader(
            schema_ptr schema,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override {
        const auto shard = engine().cpu_id();
        _contexts[shard].params = make_foreign(std::make_unique<const reader_params>(reader_params{range, slice}));
        return _factory_function(std::move(schema), _contexts[shard].params->range, _contexts[shard].params->slice, pc,
                std::move(trace_state), fwd_mr);
    }
    virtual void destroy_reader(shard_id shard, future<stopped_reader> reader) noexcept override {
        // Move to the background.
        (void)reader.then([shard, this] (stopped_reader&& reader) {
            return smp::submit_to(shard, [handle = std::move(reader.handle), ctx = std::move(_contexts[shard])] () mutable {
                ctx.semaphore->unregister_inactive_read(std::move(*handle));
            });
        }).finally([zis = shared_from_this()] {});
    }
    virtual reader_concurrency_semaphore& semaphore() override {
        const auto shard = engine().cpu_id();
        if (!_contexts[shard].semaphore) {
            if (_evict_paused_readers) {
                _contexts[shard].semaphore = make_foreign(std::make_unique<reader_concurrency_semaphore>(0, std::numeric_limits<ssize_t>::max(),
                        format("reader_concurrency_semaphore @shard_id={}", shard)));
                 // Add a waiter, so that all registered inactive reads are
                 // immediately evicted.
                 // We don't care about the returned future.
                (void)_contexts[shard].semaphore->wait_admission(1, db::no_timeout);
            } else {
                _contexts[shard].semaphore = make_foreign(std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}));
            }
        }
        return *_contexts[shard].semaphore;
    }
};

