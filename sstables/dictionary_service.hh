/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/bool_class.hh>

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "utils/chunked_vector.hh"

namespace db {
class system_keyspace;
}

namespace gms {
class feature_service;
}

namespace netw {
class messaging_service;
}

namespace replica {
class database;
}

class sstable_compressor_factory;

namespace service {
class raft_group0;
class raft_group0_client;
}

namespace utils {
class alien_worker;
}

namespace sstables {

// The interface between the users of SSTable compression dictionaries (the REST
// API and the automatic training loop) and the dict machinery: it collects
// SSTable samples from the whole cluster, trains dictionaries on them and
// publishes the results to system.dicts (via Raft group 0), from where they
// reach every node's sstable_compressor_factory.
//
// This is the SSTable counterpart of the RPC compression dictionary_service.
class dictionary_service : public peering_sharded_service<dictionary_service> {
    sharded<replica::database>& _db;
    sharded<netw::messaging_service>& _messaging;
    sharded<db::system_keyspace>& _sys_ks;
    sharded<gms::feature_service>& _feature_service;
    // Training is nonpreemptible 3rd party code, so it runs on a thread of its own.
    utils::alien_worker& _trainer;
    service::raft_group0* _group0 = nullptr;
    // We need to be able to abort all group0 operations during shutdown,
    // so we need a special abort source for that.
    abort_source _group0_as;
    // Chains _group0_as onto the abort source of whoever shuts group 0 down.
    // Must be declared after _group0_as, so that it is unsubscribed first.
    optimized_optional<abort_source::subscription> _group0_as_sub;
    // Callers of `do_sample_sstables` should hold this semaphore until they are
    // done with the sample, to avoid several samples being gathered at once.
    semaphore _do_sample_sstables_concurrency_limiter{1};
    // Held for the duration of async REST handlers, so that stop() drains
    // in-flight requests before teardown.
    named_gate _async_gate{"sstables::dictionary_service"};

    // To avoid overly-large RPC messages, `do_sample_sstables` is broken up into several rounds.
    future<utils::chunked_vector<temporary_buffer<char>>> do_sample_sstables_oneshot(table_id, uint64_t chunk_size, uint64_t n_chunks);

public:
    struct ignore_errors_tag;
    // if ignore_errors set to yes, the function will not throw an exception - any error will be ignored
    // and node for which an error happened will be considered having size 0.
    using ignore_errors = bool_class<ignore_errors_tag>;

    dictionary_service(sharded<replica::database>&, sharded<netw::messaging_service>&, sharded<db::system_keyspace>&, sharded<gms::feature_service>&, utils::alien_worker&);

    future<> stop();

    replica::database& get_database() const noexcept;
    db::system_keyspace& get_system_keyspace() const noexcept;
    const gms::feature_service& get_feature_service() const noexcept;
    bool is_raft_leader() const noexcept;
    // The factory the published dictionaries eventually land in.
    sstable_compressor_factory& get_compressor_factory() const noexcept;
    gate::holder hold_async_gate() { return _async_gate.hold(); }

    // `group0_abort` is fired before group 0 is drained. Publishing holds the
    // group 0 gate, so it must be aborted by then -- otherwise the drain would
    // wait for a publish which has nothing to wake it up.
    void set_group0(service::raft_group0&, abort_source& group0_abort);

    semaphore& get_do_sample_sstables_concurrency_limiter();
    // Gathers a combined sample of the table's SSTable data from the whole cluster.
    future<utils::chunked_vector<temporary_buffer<char>>> do_sample_sstables(table_id, uint64_t chunk_size, uint64_t n_chunks);
    future<uint64_t> estimate_total_sstable_volume(table_id, ignore_errors = ignore_errors::no);
    future<std::vector<std::byte>> train_dict(utils::chunked_vector<temporary_buffer<char>> sample);
    future<> publish_new_sstable_dict(table_id, std::span<const std::byte>, service::raft_group0_client&);
};

} // namespace sstables
