/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "mutation/timestamp.hh"
#include "transport/messages/result_message_base.hh"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace service {
class query_state;
}

namespace cql3 {

class query_processor;
class query_options;

namespace statements {

class batch_statement;

/*
 * How a BATCH reaches storage.
 *
 * The same idea as modification_executor, and separate from it because a batch
 * never commits its modifications one at a time. It builds their mutations,
 * merges them and commits the result, so the executors of the modifications it
 * holds are never used: the batch's own commits, once.
 */
class batch_executor {
public:
    virtual ~batch_executor() = default;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    commit(const batch_statement& batch, query_processor& qp, service::query_state& qs,
            const query_options& options, bool local, api::timestamp_type now) const = 0;

    // What this batch may and may not contain. Asked from the batch's
    // constructor, because the rules differ by backend: a strongly consistent
    // batch is stricter about counters, timestamps and how many tables it spans.
    virtual void validate(const batch_statement& batch) const = 0;
};

}

}
