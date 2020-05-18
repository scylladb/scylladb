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
public:
    class operations_gate {
    public:
        class operation {
            gate* _g = nullptr;

        private:
            void leave() {
                if (_g) {
                    _g->leave();
                }
            }

        public:
            operation() = default;
            explicit operation(gate& g) : _g(&g) { _g->enter(); }
            operation(const operation&) = delete;
            operation(operation&& o) : _g(std::exchange(o._g, nullptr)) { }
            ~operation() { leave(); }
            operation& operator=(const operation&) = delete;
            operation& operator=(operation&& o) {
                leave();
                _g = std::exchange(o._g, nullptr);
                return *this;
            }
        };

    private:
        std::vector<gate> _gates;

    public:
        operations_gate()
            : _gates(smp::count) {
        }

        operation enter() {
            return operation(_gates[this_shard_id()]);
        }

        future<> close() {
            return parallel_for_each(boost::irange(smp::count), [this] (shard_id shard) {
                return smp::submit_to(shard, [this, shard] {
                    return _gates[shard].close();
                });
            });
        }
    };

private:
    using factory_function = std::function<flat_mutation_reader(
            schema_ptr,
            const dht::partition_range&,
            const query::partition_slice&,
            const io_priority_class&,
            tracing::trace_state_ptr,
            mutation_reader::forwarding)>;

    struct reader_context {
        std::unique_ptr<reader_concurrency_semaphore> semaphore;
        operations_gate::operation op;
        std::optional<reader_permit> permit;
        std::optional<future<reader_permit::resource_units>> wait_future;
        std::optional<const dht::partition_range> range;
        std::optional<const query::partition_slice> slice;

        reader_context(dht::partition_range range, query::partition_slice slice) : range(std::move(range)), slice(std::move(slice)) {
        }
    };

    factory_function _factory_function;
    operations_gate& _operation_gate;
    std::vector<foreign_ptr<std::unique_ptr<reader_context>>> _contexts;
    std::vector<future<>> _destroy_futures;
    bool _evict_paused_readers = false;

public:
    explicit test_reader_lifecycle_policy(factory_function f, operations_gate& g, bool evict_paused_readers = false)
        : _factory_function(std::move(f))
        , _operation_gate(g)
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
        const auto shard = this_shard_id();
        if (_contexts[shard]) {
            _contexts[shard]->range.emplace(range);
            _contexts[shard]->slice.emplace(slice);
        } else {
            _contexts[shard] = make_foreign(std::make_unique<reader_context>(range, slice));
        }
        _contexts[shard]->op = _operation_gate.enter();
        return _factory_function(std::move(schema), *_contexts[shard]->range, *_contexts[shard]->slice, pc, std::move(trace_state), fwd_mr);
    }
    virtual void destroy_reader(shard_id shard, future<stopped_reader> reader) noexcept override {
        // Move to the background, waited via _operation_gate
        (void)reader.then([shard, this] (stopped_reader&& reader) {
            return smp::submit_to(shard, [handle = std::move(reader.handle), ctx = std::move(_contexts[shard])] () mutable {
                ctx->semaphore->unregister_inactive_read(std::move(*handle));
                ctx->semaphore->broken(std::make_exception_ptr(broken_semaphore{}));
                if (ctx->wait_future) {
                    return ctx->wait_future->then_wrapped([ctx = std::move(ctx)] (future<reader_permit::resource_units> f) mutable {
                        f.ignore_ready_future();
                        ctx->permit.reset(); // make sure it's destroyed before the semaphore
                    });
                }
                return make_ready_future<>();
            });
        }).finally([zis = shared_from_this()] {});
    }
    virtual reader_concurrency_semaphore& semaphore() override {
        const auto shard = this_shard_id();
        if (!_contexts[shard]->semaphore) {
            if (_evict_paused_readers) {
                _contexts[shard]->semaphore = std::make_unique<reader_concurrency_semaphore>(0, std::numeric_limits<ssize_t>::max(),
                        format("reader_concurrency_semaphore @shard_id={}", shard));
                _contexts[shard]->permit = _contexts[shard]->semaphore->make_permit();
                // Add a waiter, so that all registered inactive reads are
                // immediately evicted.
                // We don't care about the returned future.
                _contexts[shard]->wait_future = _contexts[shard]->permit->wait_admission(1, db::no_timeout);
            } else {
                _contexts[shard]->semaphore = std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{});
            }
        }
        return *_contexts[shard]->semaphore;
    }
};

