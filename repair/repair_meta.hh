/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/distributed.hh>
#include "repair/reader.hh"
#include "repair/repair.hh"
#include "repair/row.hh"
#include "repair/row_level_repair.hh"
#include "repair/sink_source_for_repair.hh"
#include "repair/writer.hh"

class repair_meta_tracker;

struct shard_config {
    unsigned shard;
    unsigned shard_count;
    unsigned ignore_msb;
};

enum class repair_state : uint16_t {
    unknown,
    row_level_start_started,
    row_level_start_finished,
    get_estimated_partitions_started,
    get_estimated_partitions_finished,
    set_estimated_partitions_started,
    set_estimated_partitions_finished,
    get_sync_boundary_started,
    get_sync_boundary_finished,
    get_combined_row_hash_started,
    get_combined_row_hash_finished,
    get_row_diff_with_rpc_stream_started,
    get_row_diff_with_rpc_stream_finished,
    get_row_diff_and_update_peer_row_hash_sets_started,
    get_row_diff_and_update_peer_row_hash_sets_finished,
    get_full_row_hashes_with_rpc_stream_started,
    get_full_row_hashes_with_rpc_stream_finished,
    get_full_row_hashes_started,
    get_full_row_hashes_finished,
    get_row_diff_started,
    get_row_diff_finished,
    put_row_diff_with_rpc_stream_started,
    put_row_diff_with_rpc_stream_finished,
    put_row_diff_started,
    put_row_diff_finished,
    row_level_stop_started,
    row_level_stop_finished,
};

struct repair_node_state {
    gms::inet_address node;
    repair_state state = repair_state::unknown;
    explicit repair_node_state(gms::inet_address n) : node(n) { }
};

struct row_level_repair_metrics {
    seastar::metrics::metric_groups _metrics;
    uint64_t tx_row_nr{0};
    uint64_t rx_row_nr{0};
    uint64_t tx_row_bytes{0};
    uint64_t rx_row_bytes{0};
    uint64_t row_from_disk_nr{0};
    uint64_t row_from_disk_bytes{0};
    uint64_t tx_hashes_nr{0};
    uint64_t rx_hashes_nr{0};
    row_level_repair_metrics();
};

class repair_meta {
    friend repair_meta_tracker;
    friend future<uint64_t> repair_get_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id);
    friend future<> repair_set_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id, uint64_t estimated_partitions);
public:
    using repair_master = bool_class<class repair_master_tag>;
    using update_working_row_buf = bool_class<class update_working_row_buf_tag>;
    using update_peer_row_hash_sets = bool_class<class update_peer_row_hash_sets_tag>;
    using needs_all_rows_t = bool_class<class needs_all_rows_tag>;
    using msg_addr = netw::messaging_service::msg_addr;
    using tracker_link_type = boost::intrusive::list_member_hook<bi::link_mode<boost::intrusive::auto_unlink>>;
private:
    seastar::sharded<database>& _db;
    seastar::sharded<netw::messaging_service>& _messaging;
    column_family& _cf;
    schema_ptr _schema;
    reader_permit _permit;
    dht::token_range _range;
    repair_sync_boundary::tri_compare _cmp;
    // The algorithm used to find the row difference
    row_level_diff_detect_algorithm _algo;
    // Max rows size can be stored in _row_buf
    size_t _max_row_buf_size;
    uint64_t _seed = 0;
    repair_master _repair_master;
    gms::inet_address _myip;
    uint32_t _repair_meta_id;
    streaming::stream_reason _reason;
    // Repair master's sharding configuration
    shard_config _master_node_shard_config;
    // sharding info of repair master
    dht::sharder _remote_sharder;
    bool _same_sharding_config = false;
    uint64_t _estimated_partitions = 0;
    // For repair master nr peers is the number of repair followers, for repair
    // follower nr peers is always one because repair master is the only peer.
    size_t _nr_peer_nodes= 1;
    repair_stats _stats;
    repair_reader _repair_reader;
    lw_shared_ptr<repair_writer> _repair_writer;
    // Contains rows read from disk
    std::list<repair_row> _row_buf;
    // Contains rows we are working on to sync between peers
    std::list<repair_row> _working_row_buf;
    // Combines all the repair_hash in _working_row_buf
    repair_hash _working_row_buf_combined_hash;
    // Tracks the last sync boundary
    std::optional<repair_sync_boundary> _last_sync_boundary;
    // Tracks current sync boundary
    std::optional<repair_sync_boundary> _current_sync_boundary;
    // Contains the hashes of rows in the _working_row_buffor for all peer nodes
    std::vector<repair_hash_set> _peer_row_hash_sets;
    // Gate used to make sure pending operation of meta data is done
    seastar::gate _gate;
    sink_source_for_get_full_row_hashes _sink_source_for_get_full_row_hashes;
    sink_source_for_get_row_diff _sink_source_for_get_row_diff;
    sink_source_for_put_row_diff _sink_source_for_put_row_diff;
    tracker_link_type _tracker_link;
    row_level_repair* _row_level_repair_ptr;
    std::vector<repair_node_state> _all_node_states;
    is_dirty_on_master _dirty_on_master = is_dirty_on_master::no;
public:
    std::vector<repair_node_state>& all_nodes() {
        return _all_node_states;
    }
    void set_repair_state(repair_state state, gms::inet_address node) {
        for (auto& ns : all_nodes()) {
            if (ns.node == node) {
                ns.state = state;
            }
        }
    }
    void set_repair_state_for_local_node(repair_state state) {
        // The first node is the local node
        all_nodes().front().state = state;
    }
    repair_stats& stats() {
        return _stats;
    }
    gms::inet_address myip() const {
        return _myip;
    }
    uint32_t repair_meta_id() const {
        return _repair_meta_id;
    }
    const std::optional<repair_sync_boundary>& current_sync_boundary() const {
        return _current_sync_boundary;
    }
    const std::optional<repair_sync_boundary>& last_sync_boundary() const {
        return _last_sync_boundary;
    };
    const repair_hash& working_row_buf_combined_hash() const {
        return _working_row_buf_combined_hash;
    }
    bool use_rpc_stream() const {
        return is_rpc_stream_supported(_algo);
    }

public:
    repair_meta(
        seastar::sharded<database>& db,
        seastar::sharded<netw::messaging_service>& ms,
        column_family& cf,
        schema_ptr s,
        dht::token_range range,
        row_level_diff_detect_algorithm algo,
        size_t max_row_buf_size,
        uint64_t seed,
        repair_master master,
        uint32_t repair_meta_id,
        streaming::stream_reason reason,
        shard_config master_node_shard_config,
        std::vector<gms::inet_address> all_live_peer_nodes,
        size_t nr_peer_nodes = 1,
        row_level_repair* row_level_repair_ptr = nullptr);

public:
    future<> stop();

    void reset_peer_row_hash_sets();

    repair_hash_set& peer_row_hash_sets(unsigned node_idx);

    // Get a list of row hashes in _working_row_buf
    future<repair_hash_set>
    working_row_hashes();

    std::pair<std::optional<repair_sync_boundary>, bool>
    get_common_sync_boundary(bool zero_rows,
                             std::vector<repair_sync_boundary>& sync_boundaries,
                             std::vector<repair_hash>& combined_hashes);

private:
    future<uint64_t> do_estimate_partitions_on_all_shards();

    future<uint64_t> do_estimate_partitions_on_local_shard();

    future<uint64_t> get_estimated_partitions();

    future<> set_estimated_partitions(uint64_t estimated_partitions);

    dht::sharder make_remote_sharder();

    bool is_same_sharding_config();

    future<size_t> get_repair_rows_size(const std::list<repair_row>& rows) const;

    // Get the size of rows in _row_buf
    future<size_t> row_buf_size() const {
        return get_repair_rows_size(_row_buf);
    }

    // return the combined checksum of rows in _row_buf
    future<repair_hash> row_buf_csum();

    repair_hash do_hash_for_mf(const decorated_key_with_hash& dk_with_hash, const mutation_fragment& mf);

    stop_iteration handle_mutation_fragment(mutation_fragment& mf, size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows);

    // Read rows from sstable until the size of rows exceeds _max_row_buf_size  - current_size
    // This reads rows from where the reader left last time into _row_buf
    // _current_sync_boundary or _last_sync_boundary have no effect on the reader neither.
    future<std::tuple<std::list<repair_row>, size_t>>
    read_rows_from_disk(size_t cur_size);

    future<> clear_row_buf();

    future<> clear_working_row_buf();

    // Read rows from disk until _max_row_buf_size of rows are filled into _row_buf.
    // Calculate the combined checksum of the rows
    // Calculate the total size of the rows in _row_buf
    future<get_sync_boundary_response>
    get_sync_boundary(std::optional<repair_sync_boundary> skipped_sync_boundary);

    future<> move_row_buf_to_working_row_buf();

    // Move rows from <_row_buf> to <_working_row_buf> according to
    // _last_sync_boundary and common_sync_boundary. That is rows within the
    // (_last_sync_boundary, _current_sync_boundary] in <_row_buf> are moved
    // into the <_working_row_buf>
    future<get_combined_row_hash_response>
    request_row_hashes(const std::optional<repair_sync_boundary>& common_sync_boundary);

    future<std::list<repair_row>>
    copy_rows_from_working_row_buf();

    future<std::list<repair_row>>
    copy_rows_from_working_row_buf_within_set_diff(repair_hash_set set_diff);

    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Give a set of row hashes, return the corresponding rows
    // If needs_all_rows is set, return all the rows in _working_row_buf, ignore the set_diff
    future<std::list<repair_row>>
    get_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows = needs_all_rows_t::no);

    future<> do_apply_rows(std::list<repair_row>&& row_diff, update_working_row_buf update_buf);

    // Give a list of rows, apply the rows to disk and update the _working_row_buf and _peer_row_hash_sets if requested
    // Must run inside a seastar thread
    void apply_rows_on_master_in_thread(repair_rows_on_wire rows, gms::inet_address from, update_working_row_buf update_buf,
                                        update_peer_row_hash_sets update_hash_set, unsigned node_idx);

public:
    void flush_rows_in_working_row_buf();

private:
    future<>
    apply_rows_on_follower(repair_rows_on_wire rows);

    future<repair_rows_on_wire> to_repair_rows_on_wire(std::list<repair_row> row_list);

    future<std::list<repair_row>> to_repair_rows_list(repair_rows_on_wire rows);

public:
    // RPC API
    // Return the hashes of the rows in _working_row_buf
    future<repair_hash_set>
    get_full_row_hashes(gms::inet_address remote_node);

private:
    future<> get_full_row_hashes_source_op(
        lw_shared_ptr<repair_hash_set> current_hashes,
        gms::inet_address remote_node,
        unsigned node_idx,
        rpc::source<repair_hash_with_cmd>& source);

    future<> get_full_row_hashes_sink_op(rpc::sink<repair_stream_cmd>& sink);

public:
    future<repair_hash_set>
    get_full_row_hashes_with_rpc_stream(gms::inet_address remote_node, unsigned node_idx);

    // RPC handler
    future<repair_hash_set>
    get_full_row_hashes_handler();

    // RPC API
    // Return the combined hashes of the current working row buf
    future<get_combined_row_hash_response>
    get_combined_row_hash(std::optional<repair_sync_boundary> common_sync_boundary, gms::inet_address remote_node);

    // RPC handler
    future<get_combined_row_hash_response>
    get_combined_row_hash_handler(std::optional<repair_sync_boundary> common_sync_boundary);

    // RPC API
    future<>
    repair_row_level_start(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range, table_schema_version schema_version, streaming::stream_reason reason);

    // RPC API
    future<> repair_row_level_stop(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range);

    // RPC API
    future<uint64_t> repair_get_estimated_partitions(gms::inet_address remote_node);

    // RPC API
    future<> repair_set_estimated_partitions(gms::inet_address remote_node, uint64_t estimated_partitions);

    // RPC API
    // Return the largest sync point contained in the _row_buf , current _row_buf checksum, and the _row_buf size
    future<get_sync_boundary_response>
    get_sync_boundary(gms::inet_address remote_node, std::optional<repair_sync_boundary> skipped_sync_boundary);

    // RPC handler
    future<get_sync_boundary_response>
    get_sync_boundary_handler(std::optional<repair_sync_boundary> skipped_sync_boundary);

    // RPC API
    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Must run inside a seastar thread
    void get_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node, unsigned node_idx);

    // Must run inside a seastar thread
    void get_row_diff_and_update_peer_row_hash_sets(gms::inet_address remote_node, unsigned node_idx);

private:
    // Must run inside a seastar thread
    void get_row_diff_source_op(
        update_peer_row_hash_sets update_hash_set,
        gms::inet_address remote_node,
        unsigned node_idx,
        rpc::sink<repair_hash_with_cmd>& sink,
        rpc::source<repair_row_on_wire_with_cmd>& source);

    future<> get_row_diff_sink_op(
        repair_hash_set set_diff,
        needs_all_rows_t needs_all_rows,
        rpc::sink<repair_hash_with_cmd>& sink,
        gms::inet_address remote_node);

public:
    // Must run inside a seastar thread
    void get_row_diff_with_rpc_stream(
        repair_hash_set set_diff,
        needs_all_rows_t needs_all_rows,
        update_peer_row_hash_sets update_hash_set,
        gms::inet_address remote_node,
        unsigned node_idx);

    // RPC handler
    future<repair_rows_on_wire> get_row_diff_handler(repair_hash_set set_diff, needs_all_rows_t needs_all_rows);

    // RPC API
    // Send rows in the _working_row_buf with hash within the given sef_diff
    future<> put_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node);

private:
    future<> put_row_diff_source_op(
        gms::inet_address remote_node,
        unsigned node_idx,
        rpc::source<repair_stream_cmd>& source);

    future<> put_row_diff_sink_op(
        repair_rows_on_wire rows,
        rpc::sink<repair_row_on_wire_with_cmd>& sink,
        gms::inet_address remote_node);

public:
    future<> put_row_diff_with_rpc_stream(
        repair_hash_set set_diff,
        needs_all_rows_t needs_all_rows,
        gms::inet_address remote_node, unsigned node_idx);

    // RPC handler
    future<> put_row_diff_handler(repair_rows_on_wire rows, gms::inet_address from);
};
