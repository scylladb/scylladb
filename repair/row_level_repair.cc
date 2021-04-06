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

#include "repair/row_level_repair.hh"
#include "repair/repair_meta.hh"

extern logging::logger rlogger;

static uint64_t get_random_seed() {
    static thread_local std::default_random_engine random_engine{std::random_device{}()};
    static thread_local std::uniform_int_distribution<uint64_t> random_dist{};
    return random_dist(random_engine);
}

const std::vector<row_level_diff_detect_algorithm>& suportted_diff_detect_algorithms() {
    static std::vector<row_level_diff_detect_algorithm> _algorithms = {
        row_level_diff_detect_algorithm::send_full_set,
        row_level_diff_detect_algorithm::send_full_set_rpc_stream,
    };
    return _algorithms;
};

static row_level_diff_detect_algorithm get_common_diff_detect_algorithm(netw::messaging_service& ms, const std::vector<gms::inet_address>& nodes) {
    std::vector<std::vector<row_level_diff_detect_algorithm>> nodes_algorithms(nodes.size());
    parallel_for_each(boost::irange(size_t(0), nodes.size()), [&ms, &nodes_algorithms, &nodes] (size_t idx) {
        return ms.send_repair_get_diff_algorithms(netw::messaging_service::msg_addr(nodes[idx])).then(
            [&nodes_algorithms, &nodes, idx] (std::vector<row_level_diff_detect_algorithm> algorithms) {
                std::sort(algorithms.begin(), algorithms.end());
                nodes_algorithms[idx] = std::move(algorithms);
                rlogger.trace("Got node_algorithms={}, from node={}", nodes_algorithms[idx], nodes[idx]);
            });
    }).get();

    auto common_algorithms = suportted_diff_detect_algorithms();
    for (auto& algorithms : nodes_algorithms) {
        std::sort(common_algorithms.begin(), common_algorithms.end());
        std::vector<row_level_diff_detect_algorithm> results;
        std::set_intersection(algorithms.begin(), algorithms.end(),
                              common_algorithms.begin(), common_algorithms.end(),
                              std::back_inserter(results));
        common_algorithms = std::move(results);
    }
    rlogger.trace("peer_algorithms={}, local_algorithms={}, common_diff_detect_algorithms={}",
                  nodes_algorithms, suportted_diff_detect_algorithms(), common_algorithms);
    if (common_algorithms.empty()) {
        throw std::runtime_error("Can not find row level repair diff detect algorithm");
    }
    return common_algorithms.back();
}

static future<uint32_t> get_next_repair_meta_id() {
    return smp::submit_to(0, [] {
        static uint32_t next_id = 0;
        return next_id++;
    });
}

// Must run inside a seastar thread
static repair_hash_set
get_set_diff(const repair_hash_set& x, const repair_hash_set& y) {
    repair_hash_set set_diff;
    // Note std::set_difference needs x and y are sorted.
    std::copy_if(x.begin(), x.end(), std::inserter(set_diff, set_diff.end()),
                 [&y] (auto& item) { thread::maybe_yield(); return !y.contains(item); });
    return set_diff;
}

row_level_repair::row_level_repair(repair_info& ri,
                     sstring cf_name,
                     utils::UUID table_id,
                     dht::token_range range,
                     std::vector<gms::inet_address> all_live_peer_nodes)
        : _ri(ri)
        , _cf_name(std::move(cf_name))
        , _table_id(std::move(table_id))
        , _range(std::move(range))
        , _all_live_peer_nodes(std::move(all_live_peer_nodes))
        , _cf(_ri.db.local().find_column_family(_table_id))
        , _seed(get_random_seed()) {
    }

size_t row_level_repair::get_max_row_buf_size(row_level_diff_detect_algorithm algo) {
    // Max buffer size per repair round
    return is_rpc_stream_supported(algo) ?  tracker::max_repair_memory_per_range() : 256 * 1024;
}

// Step A: Negotiate sync boundary to use
row_level_repair::op_status row_level_repair::negotiate_sync_boundary(repair_meta& master) {
    check_in_shutdown();
    _ri.check_in_abort();
    _sync_boundaries.clear();
    _combined_hashes.clear();
    _zero_rows = false;
    rlogger.debug("ROUND {}, _last_sync_boundary={}, _current_sync_boundary={}, _skipped_sync_boundary={}",
                  master.stats().round_nr, master.last_sync_boundary(), master.current_sync_boundary(), _skipped_sync_boundary);
    master.stats().round_nr++;
    parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
        const auto& node = ns.node;
        // By calling `get_sync_boundary`, the `_last_sync_boundary`
        // is moved to the `_current_sync_boundary` or
        // `_skipped_sync_boundary` if it is not std::nullopt.
        ns.state = repair_state::get_sync_boundary_started;
        return master.get_sync_boundary(node, _skipped_sync_boundary).then([&, this] (get_sync_boundary_response res) {
            ns.state = repair_state::get_sync_boundary_finished;
            master.stats().row_from_disk_bytes[node] += res.new_rows_size;
            master.stats().row_from_disk_nr[node] += res.new_rows_nr;
            if (res.boundary && res.row_buf_size > 0) {
                _sync_boundaries.push_back(*res.boundary);
                _combined_hashes.push_back(res.row_buf_combined_csum);
            } else {
                // row_size equals 0 means there is no data from on
                // that node, so we ignore the sync boundary of
                // this node when calculating common sync boundary
                _zero_rows = true;
            }
            rlogger.debug("Called master.get_sync_boundary for node {} sb={}, combined_csum={}, row_size={}, zero_rows={}, skipped_sync_boundary={}",
                          node, res.boundary, res.row_buf_combined_csum, res.row_buf_size, _zero_rows, _skipped_sync_boundary);
        });
    }).get();
    rlogger.debug("sync_boundaries nr={}, combined_hashes nr={}",
                  _sync_boundaries.size(), _combined_hashes.size());
    if (!_sync_boundaries.empty()) {
        // We have data to sync between (_last_sync_boundary, _current_sync_boundary]
        auto res = master.get_common_sync_boundary(_zero_rows, _sync_boundaries, _combined_hashes);
        _common_sync_boundary = res.first;
        bool already_synced = res.second;
        rlogger.debug("Calling master._get_common_sync_boundary: common_sync_boundary={}, already_synced={}",
                      _common_sync_boundary, already_synced);
        // If rows between (_last_sync_boundary, _current_sync_boundary] are synced, goto first step
        // This is the first fast path.
        if (already_synced) {
            _skipped_sync_boundary = _common_sync_boundary;
            rlogger.debug("Skip set skipped_sync_boundary={}", _skipped_sync_boundary);
            master.stats().round_nr_fast_path_already_synced++;
            return op_status::next_round;
        } else {
            _skipped_sync_boundary = std::nullopt;
        }
    } else {
        master.stats().round_nr_fast_path_already_synced++;
        // We are done with this range because all the nodes have no more data.
        return op_status::all_done;
    }
    return op_status::next_step;
}

// Step B: Get missing rows from peer nodes so that local node contains all the rows
row_level_repair::op_status row_level_repair::get_missing_rows_from_follower_nodes(repair_meta& master) {
    check_in_shutdown();
    _ri.check_in_abort();
    // `combined_hashes` contains the combined hashes for the
    // `_working_row_buf`. Like `_row_buf`, `_working_row_buf` contains
    // rows which are within the (_last_sync_boundary, _current_sync_boundary]
    // By calling `get_combined_row_hash(_common_sync_boundary)`,
    // all the nodes move the `_current_sync_boundary` to `_common_sync_boundary`,
    // Rows within the (_last_sync_boundary, _current_sync_boundary] are
    // moved from the `_row_buf` to `_working_row_buf`.
    std::vector<repair_hash> combined_hashes;
    combined_hashes.resize(master.all_nodes().size());
    parallel_for_each(boost::irange(size_t(0), master.all_nodes().size()), [&, this] (size_t idx) {
        // Request combined hashes from all nodes between (_last_sync_boundary, _current_sync_boundary]
        // Each node will
        // - Set `_current_sync_boundary` to `_common_sync_boundary`
        // - Move rows from `_row_buf` to `_working_row_buf`
        // But the full hashes (each and every hashes for the rows in
        // the `_working_row_buf`) are not returned until repair master
        // explicitly requests with get_full_row_hashes() below as
        // an optimization. Because if the combined_hashes from all
        // peers are identical, we think rows in the `_working_row_buff`
        // are identical, there is no need to transfer each and every
        // row hashes to the repair master.
        master.all_nodes()[idx].state = repair_state::get_combined_row_hash_started;
        return master.get_combined_row_hash(_common_sync_boundary, master.all_nodes()[idx].node).then([&, this, idx] (get_combined_row_hash_response resp) {
            master.all_nodes()[idx].state = repair_state::get_combined_row_hash_finished;
            rlogger.debug("Calling master.get_combined_row_hash for node {}, got combined_hash={}", master.all_nodes()[idx].node, resp);
            combined_hashes[idx]= std::move(resp);
        });
    }).get();

    // If all the peers has the same combined_hashes. This means they contain
    // the identical rows. So there is no need to sync for this sync boundary.
    bool same_combined_hashes = std::adjacent_find(combined_hashes.begin(), combined_hashes.end(),
                                                   std::not_equal_to<repair_hash>()) == combined_hashes.end();
    if (same_combined_hashes) {
        // `_working_row_buf` on all the nodes are the same
        // This is the second fast path.
        master.stats().round_nr_fast_path_same_combined_hashes++;
        return op_status::next_round;
    }

    master.reset_peer_row_hash_sets();
    // Note: We can not work on _all_live_peer_nodes in parallel,
    // because syncing with _all_live_peer_nodes in serial avoids
    // getting the same rows from more than one peers.
    for (unsigned node_idx = 0; node_idx < _all_live_peer_nodes.size(); node_idx++) {
        auto& ns = master.all_nodes()[node_idx + 1];
        auto& node = _all_live_peer_nodes[node_idx];
        // Here is an optimization to avoid transferring full rows hashes,
        // if remote and local node, has the same combined_hashes.
        // For example:
        // node1: 1 2 3
        // node2: 1 2 3 4
        // node3: 1 2 3 4
        // After node1 get the row 4 from node2, node1 update it is
        // combined_hashes, so we can avoid fetching the full row hashes from node3.
        if (combined_hashes[node_idx + 1] == master.working_row_buf_combined_hash()) {
            // local node and peer node have the same combined hash. This
            // means we can set peer_row_hash_sets[n] to local row hashes
            // without fetching it from peers to save network traffic.
            master.peer_row_hash_sets(node_idx) = master.working_row_hashes().get0();
            rlogger.debug("Calling optimize master.working_row_hashes for node {}, hash_sets={}",
                          node, master.peer_row_hash_sets(node_idx).size());
            continue;
        }

        // Fast path: if local has zero row and remote has rows, request them all.
        if (master.working_row_buf_combined_hash() == repair_hash() && combined_hashes[node_idx + 1] != repair_hash()) {
            master.peer_row_hash_sets(node_idx).clear();
            if (master.use_rpc_stream()) {
                rlogger.debug("FastPath: get_row_diff with needs_all_rows_t::yes rpc stream");
                ns.state = repair_state::get_row_diff_with_rpc_stream_started;
                master.get_row_diff_with_rpc_stream({}, repair_meta::needs_all_rows_t::yes, repair_meta::update_peer_row_hash_sets::yes, node, node_idx);
                ns.state = repair_state::get_row_diff_with_rpc_stream_finished;
            } else {
                rlogger.debug("FastPath: get_row_diff with needs_all_rows_t::yes rpc verb");
                ns.state = repair_state::get_row_diff_and_update_peer_row_hash_sets_started;
                master.get_row_diff_and_update_peer_row_hash_sets(node, node_idx);
                ns.state = repair_state::get_row_diff_and_update_peer_row_hash_sets_finished;
            }
            continue;
        }

        rlogger.debug("Before master.get_full_row_hashes for node {}, hash_sets={}",
                      node, master.peer_row_hash_sets(node_idx).size());
        // Ask the peer to send the full list hashes in the working row buf.
        if (master.use_rpc_stream()) {
            ns.state = repair_state::get_full_row_hashes_with_rpc_stream_started;
            master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes_with_rpc_stream(node, node_idx).get0();
            ns.state = repair_state::get_full_row_hashes_with_rpc_stream_finished;
        } else {
            ns.state = repair_state::get_full_row_hashes_started;
            master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes(node).get0();
            ns.state = repair_state::get_full_row_hashes_finished;
        }
        rlogger.debug("After master.get_full_row_hashes for node {}, hash_sets={}",
                      node, master.peer_row_hash_sets(node_idx).size());

        // With hashes of rows from peer node, we can figure out
        // what rows repair master is missing. Note we get missing
        // data from repair follower 1, apply the rows, then get
        // missing data from repair follower 2 and so on. We do it
        // sequentially because the rows from repair follower 1 to
        // repair master might reduce the amount of missing data
        // between repair master and repair follower 2.
        repair_hash_set set_diff = get_set_diff(master.peer_row_hash_sets(node_idx), master.working_row_hashes().get0());
        // Request missing sets from peer node
        rlogger.debug("Before get_row_diff to node {}, local={}, peer={}, set_diff={}",
                      node, master.working_row_hashes().get0().size(), master.peer_row_hash_sets(node_idx).size(), set_diff.size());
        // If we need to pull all rows from the peer. We can avoid
        // sending the row hashes on wire by setting needs_all_rows flag.
        auto needs_all_rows = repair_meta::needs_all_rows_t(set_diff.size() == master.peer_row_hash_sets(node_idx).size());
        if (master.use_rpc_stream()) {
            ns.state = repair_state::get_row_diff_with_rpc_stream_started;
            master.get_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, repair_meta::update_peer_row_hash_sets::no, node, node_idx);
            ns.state = repair_state::get_row_diff_with_rpc_stream_finished;
        } else {
            ns.state = repair_state::get_row_diff_started;
            master.get_row_diff(std::move(set_diff), needs_all_rows, node, node_idx);
            ns.state = repair_state::get_row_diff_finished;
        }
        rlogger.debug("After get_row_diff node {}, hash_sets={}", master.myip(), master.working_row_hashes().get0().size());
    }
    master.flush_rows_in_working_row_buf();
    return op_status::next_step;
}

// Step C: Send missing rows to the peer nodes
void row_level_repair::send_missing_rows_to_follower_nodes(repair_meta& master) {
    // At this time, repair master contains all the rows between (_last_sync_boundary, _current_sync_boundary]
    // So we can figure out which rows peer node are missing and send the missing rows to them
    check_in_shutdown();
    _ri.check_in_abort();
    repair_hash_set local_row_hash_sets = master.working_row_hashes().get0();
    auto sz = _all_live_peer_nodes.size();
    std::vector<repair_hash_set> set_diffs(sz);
    for (size_t idx : boost::irange(size_t(0), sz)) {
        set_diffs[idx] = get_set_diff(local_row_hash_sets, master.peer_row_hash_sets(idx));
    }
    parallel_for_each(boost::irange(size_t(0), sz), [&, this] (size_t idx) {
        auto& ns = master.all_nodes()[idx + 1];
        auto needs_all_rows = repair_meta::needs_all_rows_t(master.peer_row_hash_sets(idx).empty());
        auto& set_diff = set_diffs[idx];
        rlogger.debug("Calling master.put_row_diff to node {}, set_diff={}, needs_all_rows={}", _all_live_peer_nodes[idx], set_diff.size(), needs_all_rows);
        if (master.use_rpc_stream()) {
            ns.state = repair_state::put_row_diff_with_rpc_stream_started;
            return master.put_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx], idx).then([&ns] {
                ns.state = repair_state::put_row_diff_with_rpc_stream_finished;
            });
        } else {
            ns.state = repair_state::put_row_diff_finished;
            return master.put_row_diff(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx]).then([&ns] {
                ns.state = repair_state::put_row_diff_finished;
            });
        }
    }).get();
    master.stats().round_nr_slow_path++;
}

future<> row_level_repair::run() {
    return seastar::async([this] {
        check_in_shutdown();
        _ri.check_in_abort();
        auto repair_meta_id = get_next_repair_meta_id().get0();
        auto algorithm = get_common_diff_detect_algorithm(_ri.messaging.local(), _all_live_peer_nodes);
        auto max_row_buf_size = get_max_row_buf_size(algorithm);
        auto master_node_shard_config = shard_config {
            this_shard_id(),
            _ri.sharder.shard_count(),
            _ri.sharder.sharding_ignore_msb()
        };
        auto s = _cf.schema();
        auto schema_version = s->version();
        bool table_dropped = false;

        repair_meta master(_ri.db,
                           _ri.messaging,
                           _cf,
                           s,
                           _range,
                           algorithm,
                           max_row_buf_size,
                           _seed,
                           repair_meta::repair_master::yes,
                           repair_meta_id,
                           _ri.reason,
                           std::move(master_node_shard_config),
                           _all_live_peer_nodes,
                           _all_live_peer_nodes.size(),
                           this);

        rlogger.debug(">>> Started Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_size={}",
                      master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _ri.keyspace, _cf_name, schema_version, _range, _seed, max_row_buf_size);


        std::vector<gms::inet_address> nodes_to_stop;
        nodes_to_stop.reserve(master.all_nodes().size());
        try {
            parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
                const auto& node = ns.node;
                ns.state = repair_state::row_level_start_started;
                return master.repair_row_level_start(node, _ri.keyspace, _cf_name, _range, schema_version, _ri.reason).then([&] () {
                    ns.state = repair_state::row_level_start_finished;
                    nodes_to_stop.push_back(node);
                    ns.state = repair_state::get_estimated_partitions_started;
                    return master.repair_get_estimated_partitions(node).then([this, node, &ns] (uint64_t partitions) {
                        ns.state = repair_state::get_estimated_partitions_finished;
                        rlogger.trace("Get repair_get_estimated_partitions for node={}, estimated_partitions={}", node, partitions);
                        _estimated_partitions += partitions;
                    });
                });
            }).get();

            parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
                const auto& node = ns.node;
                rlogger.trace("Get repair_set_estimated_partitions for node={}, estimated_partitions={}", node, _estimated_partitions);
                ns.state = repair_state::set_estimated_partitions_started;
                return master.repair_set_estimated_partitions(node, _estimated_partitions).then([&ns] {
                    ns.state = repair_state::set_estimated_partitions_finished;
                });
            }).get();

            while (true) {
                auto status = negotiate_sync_boundary(master);
                if (status == op_status::next_round) {
                    continue;
                } else if (status == op_status::all_done) {
                    break;
                }
                status = get_missing_rows_from_follower_nodes(master);
                if (status == op_status::next_round) {
                    continue;
                }
                send_missing_rows_to_follower_nodes(master);
            }
        } catch (no_such_column_family& e) {
            table_dropped = true;
            rlogger.warn("repair id {} on shard {}, keyspace={}, cf={}, range={}, got error in row level repair: {}",
                         _ri.id, this_shard_id(), _ri.keyspace, _cf_name, _range, e);
            _failed = true;
        } catch (std::exception& e) {
            rlogger.warn("repair id {} on shard {}, keyspace={}, cf={}, range={}, got error in row level repair: {}",
                         _ri.id, this_shard_id(), _ri.keyspace, _cf_name, _range, e);
            // In case the repair process fail, we need to call repair_row_level_stop to clean up repair followers
            _failed = true;
        }

        parallel_for_each(nodes_to_stop, [&] (const gms::inet_address& node) {
            master.set_repair_state(repair_state::row_level_stop_finished, node);
            return master.repair_row_level_stop(node, _ri.keyspace, _cf_name, _range).then([node, &master] {
                master.set_repair_state(repair_state::row_level_stop_finished, node);
            });
        }).get();

        _ri.update_statistics(master.stats());
        if (_failed) {
            if (table_dropped) {
                throw no_such_column_family(_ri.keyspace,  _cf_name);
            } else {
                throw std::runtime_error(format("Failed to repair for keyspace={}, cf={}, range={}", _ri.keyspace, _cf_name, _range));
            }
        }
        rlogger.debug("<<< Finished Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}, tx_hashes_nr={}, rx_hashes_nr={}, tx_row_nr={}, rx_row_nr={}, row_from_disk_bytes={}, row_from_disk_nr={}",
                      master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _ri.keyspace, _cf_name, _range, master.stats().tx_hashes_nr, master.stats().rx_hashes_nr, master.stats().tx_row_nr, master.stats().rx_row_nr, master.stats().row_from_disk_bytes, master.stats().row_from_disk_nr);
    });
}

