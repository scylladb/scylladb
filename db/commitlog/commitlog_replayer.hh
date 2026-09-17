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
#include <seastar/util/bool_class.hh>

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
    // Replays only the tables whose writes wait for the commitlog sync - the
    // ones that have to be intact before the rest of the database is loaded,
    // which is the same reason they are synced in the first place.  Such a pass
    // also leaves raft entries alone: the full replay that follows owns the
    // raft replay buffer, and adding an entry to it twice would duplicate it.
    using synced_tables_only = bool_class<class synced_tables_only_tag>;

    commitlog_replayer(commitlog_replayer&&) noexcept;
    ~commitlog_replayer();

    static future<commitlog_replayer> create_replayer(seastar::sharded<replica::database>&, seastar::sharded<db::system_keyspace>&,
            seastar::sharded<raft_commitlog_replay_buffer>* raft_buffer = nullptr,
            synced_tables_only synced_only = synced_tables_only::no);

    future<> recover(std::vector<sstring> files, sstring fname_prefix);
    future<> recover(sstring file, sstring fname_prefix);

private:
    commitlog_replayer(seastar::sharded<replica::database>&, seastar::sharded<db::system_keyspace>&,
            seastar::sharded<raft_commitlog_replay_buffer>* raft_buffer, synced_tables_only synced_only);

    class impl;
    std::unique_ptr<impl> _impl;
};

}
