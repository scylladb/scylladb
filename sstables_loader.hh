/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "schema/schema_fwd.hh"
#include "sstables/shared_sstable.hh"

using namespace seastar;

namespace replica {
class database;
}

namespace netw { class messaging_service; }
namespace db {
namespace view {
class view_builder;
}
}

// The handler of the 'storage_service/load_new_ss_tables' endpoint which, in
// turn, is the target of the 'nodetool refresh' command.
// Gets sstables from the upload directory and makes them available in the
// system. Built on top of the distributed_loader functionality.
class sstables_loader : public seastar::peering_sharded_service<sstables_loader> {
    sharded<replica::database>& _db;
    netw::messaging_service& _messaging;
    sharded<db::view::view_builder>& _view_builder;
    seastar::scheduling_group _sched_group;

    // Note that this is obviously only valid for the current shard. Users of
    // this facility should elect a shard to be the coordinator based on any
    // given objective criteria
    //
    // It shouldn't be impossible to actively serialize two callers if the need
    // ever arise.
    bool _loading_new_sstables = false;

    future<> load_and_stream(sstring ks_name, sstring cf_name,
            table_id, std::vector<sstables::shared_sstable> sstables,
            bool primary_replica_only);

public:
    sstables_loader(sharded<replica::database>& db,
            netw::messaging_service& messaging,
            sharded<db::view::view_builder>& vb,
            seastar::scheduling_group sg)
        : _db(db)
        , _messaging(messaging)
        , _view_builder(vb)
        , _sched_group(std::move(sg))
    {
    }

    /**
     * Load new SSTables not currently tracked by the system
     *
     * This can be called, for instance, after copying a batch of SSTables to a CF directory.
     *
     * This should not be called in parallel for the same keyspace / column family, and doing
     * so will throw an std::runtime_exception.
     *
     * @param ks_name the keyspace in which to search for new SSTables.
     * @param cf_name the column family in which to search for new SSTables.
     * @return a future<> when the operation finishes.
     */
    future<> load_new_sstables(sstring ks_name, sstring cf_name,
            bool load_and_stream, bool primary_replica_only);
};
