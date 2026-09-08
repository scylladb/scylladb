/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>
#include <vector>
#include <seastar/core/sharded.hh>
#include "gc_clock.hh"
#include "schema/schema_fwd.hh"
#include "utils/phased_barrier.hh"
#include "readers/multishard.hh"

namespace replica { class database; }


class streaming_reader_lifecycle_policy
    : public reader_lifecycle_policy
        , public enable_shared_from_this<streaming_reader_lifecycle_policy> {

    template <typename T>
    using foreign_unique_ptr = foreign_ptr<std::unique_ptr<T>>;

    struct reader_context {
        foreign_ptr<lw_shared_ptr<const dht::partition_range>> range;
        foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
        reader_concurrency_semaphore* semaphore;
    };
    sharded<replica::database>& _db;
    table_id _table_id;
    gc_clock::time_point _compaction_time;
    std::vector<reader_context> _contexts;
public:
    streaming_reader_lifecycle_policy(sharded<replica::database>& db, table_id table_id, gc_clock::time_point compaction_time);
    virtual mutation_reader create_reader(
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr,
        mutation_reader::forwarding fwd_mr) override;
    virtual const dht::partition_range* get_read_range() const override;
    virtual void update_read_range(lw_shared_ptr<const dht::partition_range> range) override;
    virtual future<> destroy_reader(stopped_reader reader) noexcept override;
    virtual reader_concurrency_semaphore& semaphore() override;
    virtual future<reader_permit> obtain_reader_permit(schema_ptr schema, const char* const description, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) override;
};
