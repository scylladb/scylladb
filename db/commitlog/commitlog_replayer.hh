/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <functional>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace replica {
class database;
}

namespace cql3 {
class query_processor;
}

namespace db {

class commitlog;
class system_keyspace;
class raft_commitlog_replay_buffer;

class commitlog_replayer {
public:
    // Restricts a replay to the tables the predicate accepts.  A restricted
    // replay also leaves raft batches alone: the full replay that follows
    // owns the raft replay buffer, and add_batch() decides each batch as it
    // arrives, so each batch must reach add_batch() once.  Unset replays
    // everything, which is what recovery on startup does.  The predicate is
    // called from every shard.
    using table_filter = std::function<bool(table_id)>;

    // Names the pass in its log lines.  A boot runs several of them over
    // different sets of tables, and without this their output is identical.
    static constexpr auto default_context = "user tables";

    commitlog_replayer(commitlog_replayer&&) noexcept;
    ~commitlog_replayer();

    // `qp` is needed only for raft replay, which reads and writes each group's
    // row; pass `qp` whenever `raft_buffer` is passed.
    static future<commitlog_replayer> create_replayer(seastar::sharded<replica::database>&, seastar::sharded<db::system_keyspace>&,
            seastar::sharded<raft_commitlog_replay_buffer>* raft_buffer = nullptr,
            seastar::sharded<cql3::query_processor>* qp = nullptr, table_filter filter = {},
            sstring context = default_context);

    future<> recover(std::vector<sstring> files, sstring fname_prefix);
    future<> recover(sstring file, sstring fname_prefix);

private:
    commitlog_replayer(seastar::sharded<replica::database>&, seastar::sharded<db::system_keyspace>&,
            seastar::sharded<raft_commitlog_replay_buffer>* raft_buffer,
            seastar::sharded<cql3::query_processor>* qp, table_filter filter, sstring context);

    class impl;
    std::unique_ptr<impl> _impl;
};

}
