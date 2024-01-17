/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_map>
#include <exception>
#include <absl/container/btree_set.h>
#include <fmt/core.h>
#include <boost/range/adaptors.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

#include "locator/abstract_replication_strategy.hh"
#include "replica/database_fwd.hh"
#include "mutation/frozen_mutation.hh"
#include "utils/hash.hh"
#include "repair/hash.hh"
#include "repair/sync_boundary.hh"
#include "tasks/types.hh"
#include "schema/schema.hh"

namespace tasks {
namespace repair {
class task_manager_module;
}
}

namespace replica {
class database;
}

class repair_service;
namespace db {
    namespace view {
        class view_builder;
    }
}
namespace netw { class messaging_service; }
namespace service {
class migration_manager;
}
namespace gms { class gossiper; }

class repair_exception : public std::exception {
private:
    sstring _what;
public:
    repair_exception(sstring msg) : _what(std::move(msg)) { }
    virtual const char* what() const noexcept override { return _what.c_str(); }
};

class repair_stopped_exception : public repair_exception {
public:
    repair_stopped_exception() : repair_exception("Repair stopped") { }
};

struct repair_uniq_id {
    // The integer ID used to identify a repair job. It is currently used by nodetool and http API.
    int id;
    // Task info containing a UUID to identify a repair job, and a shard of the job.
    // We will transit to use UUID over the integer ID.
    tasks::task_info task_info;

    tasks::task_id uuid() const noexcept {
        return task_info.id;
    }

    unsigned shard() const noexcept {
        return task_info.shard;
    }
};
std::ostream& operator<<(std::ostream& os, const repair_uniq_id& x);

// If the repair master sets the dst_cpu_id to repair_unspecified_shard. It
// means the repair master does not choose shard id for the repair follower.
// The repair follower should choose the shard id itself.
constexpr shard_id repair_unspecified_shard = shard_id(-1);

// NOTE: repair_start() can be run on any node, but starts a node-global
// operation.
// repair_start() starts the requested repair on this node. It returns an
// integer id which can be used to query the repair's status with
// repair_get_status(). The returned future<int> becomes available quickly,
// as soon as repair_get_status() can be used - it doesn't wait for the
// repair to complete.
future<int> repair_start(seastar::sharded<repair_service>& repair,
        sstring keyspace, std::unordered_map<sstring, sstring> options);

// TODO: Have repair_progress contains a percentage progress estimator
// instead of just "RUNNING".
enum class repair_status { RUNNING, SUCCESSFUL, FAILED };

enum class repair_checksum {
    legacy = 0,
    streamed = 1,
};

class repair_stats {
public:
    uint64_t round_nr = 0;
    uint64_t round_nr_fast_path_already_synced = 0;
    uint64_t round_nr_fast_path_same_combined_hashes= 0;
    uint64_t round_nr_slow_path = 0;

    uint64_t rpc_call_nr = 0;

    uint64_t tx_hashes_nr = 0;
    uint64_t rx_hashes_nr = 0;

    uint64_t tx_row_nr = 0;
    uint64_t rx_row_nr = 0;

    uint64_t tx_row_bytes = 0;
    uint64_t rx_row_bytes = 0;

    std::map<gms::inet_address, uint64_t> row_from_disk_bytes;
    std::map<gms::inet_address, uint64_t> row_from_disk_nr;

    std::map<gms::inet_address, uint64_t> tx_row_nr_peer;
    std::map<gms::inet_address, uint64_t> rx_row_nr_peer;

    lowres_clock::time_point start_time = lowres_clock::now();

public:
    void add(const repair_stats& o);
    sstring get_stats();
};

class repair_neighbors {
public:
    std::vector<gms::inet_address> all;
    std::vector<gms::inet_address> mandatory;
    std::unordered_map<gms::inet_address, shard_id> shard_map;
    repair_neighbors() = default;
    explicit repair_neighbors(std::vector<gms::inet_address> a)
        : all(std::move(a)) {
    }
    explicit repair_neighbors(const std::unordered_map<locator::host_id, gms::inet_address>& a)
        : all(boost::copy_range<std::vector<gms::inet_address>>(a | boost::adaptors::map_values)) {
    }
    repair_neighbors(std::vector<gms::inet_address> a, std::vector<gms::inet_address> m)
        : all(std::move(a))
        , mandatory(std::move(m)) {
    }
    repair_neighbors(std::vector<gms::inet_address> nodes, std::vector<shard_id> shards);
};

future<uint64_t> estimate_partitions(seastar::sharded<replica::database>& db, const sstring& keyspace,
        const sstring& cf, const dht::token_range& range);


enum class repair_row_level_start_status: uint8_t {
    ok,
    no_such_column_family,
};

struct repair_row_level_start_response {
    repair_row_level_start_status status;
};

// Return value of the REPAIR_GET_SYNC_BOUNDARY RPC verb
struct get_sync_boundary_response {
    std::optional<repair_sync_boundary> boundary;
    repair_hash row_buf_combined_csum;
    // The current size of the row buf
    uint64_t row_buf_size;
    // The number of bytes this verb read from disk
    uint64_t new_rows_size;
    // The number of rows this verb read from disk
    uint64_t new_rows_nr;
};

// Return value of the REPAIR_GET_COMBINED_ROW_HASH RPC verb
using get_combined_row_hash_response = repair_hash;

struct node_repair_meta_id {
    gms::inet_address ip;
    uint32_t repair_meta_id;
    bool operator==(const node_repair_meta_id& x) const {
        return x.ip == ip && x.repair_meta_id == repair_meta_id;
    }
};

// Represent a partition_key and frozen_mutation_fragments within the partition_key.
class partition_key_and_mutation_fragments {
    partition_key _key;
    std::list<frozen_mutation_fragment> _mfs;
public:
    partition_key_and_mutation_fragments()
        : _key(std::vector<bytes>() ) {
    }
    partition_key_and_mutation_fragments(partition_key key, std::list<frozen_mutation_fragment> mfs)
        : _key(std::move(key))
        , _mfs(std::move(mfs)) {
    }
    const partition_key& get_key() const { return _key; }
    const std::list<frozen_mutation_fragment>& get_mutation_fragments() const { return _mfs; }
    partition_key& get_key() { return _key; }
    std::list<frozen_mutation_fragment>& get_mutation_fragments() { return _mfs; }
    void push_mutation_fragment(frozen_mutation_fragment mf) { _mfs.push_back(std::move(mf)); }
};

using repair_row_on_wire = partition_key_and_mutation_fragments;
using repair_rows_on_wire = std::list<partition_key_and_mutation_fragments>;

enum class repair_stream_cmd : uint8_t {
    error,
    hash_data,
    row_data,
    end_of_current_hash_set,
    needs_all_rows,
    end_of_current_rows,
    get_full_row_hashes,
    put_rows_done,
};

struct repair_hash_with_cmd {
    repair_stream_cmd cmd;
    repair_hash hash;
};

struct repair_row_on_wire_with_cmd {
    repair_stream_cmd cmd;
    repair_row_on_wire row;
};

enum class row_level_diff_detect_algorithm : uint8_t {
    send_full_set,
    send_full_set_rpc_stream,
};

std::string_view format_as(row_level_diff_detect_algorithm);

struct repair_update_system_table_request {
    tasks::task_id repair_uuid;
    table_id table_uuid;
    sstring keyspace_name;
    sstring table_name;
    dht::token_range range;
    gc_clock::time_point repair_time;
};

struct repair_update_system_table_response {
};

struct repair_flush_hints_batchlog_request {
    tasks::task_id repair_uuid;
    std::list<gms::inet_address> target_nodes;
    std::chrono::seconds hints_timeout;
    std::chrono::seconds batchlog_timeout;
};

struct repair_flush_hints_batchlog_response {
};

struct tablet_repair_task_meta {
    sstring keyspace_name;
    sstring table_name;
    table_id tid;
    shard_id master_shard_id;
    dht::token_range range;
    repair_neighbors neighbors;
    locator::tablet_replica_set replicas;
    locator::effective_replication_map_ptr erm;
};

namespace std {

template<>
struct hash<repair_hash> {
    size_t operator()(repair_hash h) const { return h.hash; }
};

template<>
struct hash<node_repair_meta_id> {
    size_t operator()(node_repair_meta_id id) const { return utils::tuple_hash()(id.ip, id.repair_meta_id); }
};

}

template <> struct fmt::formatter<row_level_diff_detect_algorithm> : fmt::formatter<string_view> {
    auto format(row_level_diff_detect_algorithm algo, fmt::format_context& ctx) const {
        return formatter<string_view>::format(format_as(algo), ctx);
    }
};
