/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "message/messaging_service.hh"
#include "utils/chunked_vector.hh"
#include "db/snapshot/cluster_backup.hh"

#include "idl/uuid.idl.hh"
#include "idl/sstables.idl.hh"

namespace db {
    namespace snapshot {
        // silly little wrapper type to handle
        // actual byte load of a manifest being more
        // than we'd like sending as a string
        class manifest_data final {
            bytes data();
        };
    }
}

// RPC verb for cluster level backup. Sent by coordinator to sstable owning nodes.
// "endpoint" is an object storage endpoint reference. Needs to be equivalent across
// all participating nodes.
// first_token and last_token represent the range covered by the sstables requested
// for backing up.   
verb [[]] backup_snapshot_sstables(table_id table_id, sstring tag, sstring endpoint, sstring bucket, sstring prefix, dht::token first_token, dht::token last_token, utils::chunked_vector<sstables::sstable_id> sstable_ids, bool use_move);

// Fallback RPC to have node in other dc (or whatnot) store manifest data
// for those cases where we cannot access the object storage endpoint in question
// from coordinator
verb [[]] store_backup_snapshot_manifest(sstring tag, sstring endpoint, sstring bucket, sstring prefix, db::snapshot::manifest_data manifest);
