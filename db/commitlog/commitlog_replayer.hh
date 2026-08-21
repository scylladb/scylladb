/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "seastarx.hh"

namespace replica {
class database;
}

namespace db {

class commitlog;
class system_keyspace;
class raft_commitlog_replay_buffer;

class commitlog_replayer {
public:
    commitlog_replayer(commitlog_replayer&&) noexcept;
    ~commitlog_replayer();

    static future<commitlog_replayer> create_replayer(seastar::sharded<replica::database>&, seastar::sharded<db::system_keyspace>&,
            seastar::sharded<raft_commitlog_replay_buffer>* raft_buffer = nullptr);

    future<> recover(std::vector<sstring> files, sstring fname_prefix);
    future<> recover(sstring file, sstring fname_prefix);

private:
    commitlog_replayer(seastar::sharded<replica::database>&, seastar::sharded<db::system_keyspace>&,
            seastar::sharded<raft_commitlog_replay_buffer>* raft_buffer);

    class impl;
    std::unique_ptr<impl> _impl;
};

}
