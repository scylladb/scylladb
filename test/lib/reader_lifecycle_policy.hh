/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/assert.hh"
#include "readers/multishard.hh"
#include <seastar/core/gate.hh>

class test_reader_lifecycle_policy
        : public reader_lifecycle_policy_v2
        , public enable_shared_from_this<test_reader_lifecycle_policy> {
public:
    using reader_factory_function = std::function<mutation_reader(
            schema_ptr,
            reader_permit,
            const dht::partition_range&,
            const query::partition_slice&,
            tracing::trace_state_ptr,
            mutation_reader::forwarding)>;

    class semaphore_factory {
    public:
        virtual ~semaphore_factory() = default;
        virtual lw_shared_ptr<reader_concurrency_semaphore> create(sstring name) {
            return make_lw_shared<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, std::move(name), reader_concurrency_semaphore::register_metrics::no);
        }
        virtual future<> stop(reader_concurrency_semaphore& semaphore) {
            return semaphore.stop();
        }
    };

private:
    struct reader_context {
        lw_shared_ptr<reader_concurrency_semaphore> semaphore;
        lw_shared_ptr<const dht::partition_range> range;
        std::optional<const query::partition_slice> slice;

        reader_context() = default;
        reader_context(dht::partition_range range, query::partition_slice slice)
            : range(make_lw_shared<const dht::partition_range>(std::move(range))), slice(std::move(slice)) {
        }
    };

    reader_factory_function _reader_factory_function;
    std::unique_ptr<semaphore_factory> _semaphore_factory;
    std::vector<foreign_ptr<std::unique_ptr<reader_context>>> _contexts;
    std::vector<future<>> _destroy_futures;

public:
    explicit test_reader_lifecycle_policy(reader_factory_function reader_factory, std::unique_ptr<semaphore_factory> semaphore_factory_object = std::make_unique<semaphore_factory>())
        : _reader_factory_function(std::move(reader_factory))
        , _semaphore_factory(std::move(semaphore_factory_object))
        , _contexts(smp::count) {
    }
    virtual mutation_reader create_reader(
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override {
        const auto shard = this_shard_id();
        if (_contexts[shard]) {
            _contexts[shard]->range = make_lw_shared<const dht::partition_range>(range);
            _contexts[shard]->slice.emplace(slice);
        } else {
            _contexts[shard] = make_foreign(std::make_unique<reader_context>(range, slice));
        }
        return _reader_factory_function(std::move(schema), std::move(permit), *_contexts[shard]->range, *_contexts[shard]->slice, std::move(trace_state), fwd_mr);
    }
    virtual const dht::partition_range* get_read_range() const override {
        const auto shard = this_shard_id();
        SCYLLA_ASSERT(_contexts[shard]);
        return _contexts[shard]->range.get();
    }
    void update_read_range(lw_shared_ptr<const dht::partition_range> range) override {
        const auto shard = this_shard_id();
        SCYLLA_ASSERT(_contexts[shard]);
        _contexts[shard]->range = std::move(range);
    }
    virtual future<> destroy_reader(stopped_reader reader) noexcept override {
        auto& ctx = _contexts[this_shard_id()];
        auto reader_opt = ctx->semaphore->unregister_inactive_read(std::move(reader.handle));
        auto ret = reader_opt ? reader_opt->close() : make_ready_future<>();
        return ret.finally([this, &ctx] {
            return _semaphore_factory->stop(*ctx->semaphore).finally([&ctx] {
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
        _contexts[shard]->semaphore = _semaphore_factory->create(std::move(name));
        return *_contexts[shard]->semaphore;
    }
    virtual future<reader_permit> obtain_reader_permit(schema_ptr schema, const char* const description, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) override {
        return semaphore().obtain_permit(schema, description, 128 * 1024, timeout, std::move(trace_ptr));
    }
};

