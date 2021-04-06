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

#include <boost/range/adaptors.hpp>
#include "gms/gossiper.hh"
#include "service/storage_proxy.hh"
#include "sstables/sstables.hh"
#include "utils/stall_free.hh"
#include "service/migration_manager.hh"
#include "repair/hash.hh"
#include "repair/sink_source_for_repair.hh"
#include "repair/row.hh"
#include "repair/reader.hh"
#include "repair/writer.hh"
#include "repair/row_level.hh"
#include "repair/repair_meta.hh"

extern logging::logger rlogger;

namespace repair {
    inline bool inject_rpc_stream_error = false;

    inline distributed<db::system_distributed_keyspace> *_sys_dist_ks;
    inline distributed<db::view::view_update_generator> *_view_update_generator;
    inline sharded<netw::messaging_service> *_messaging;
}

using namespace repair;

row_level_repair_metrics::row_level_repair_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("repair", {
        sm::make_derive("tx_row_nr", tx_row_nr,
                        sm::description("Total number of rows sent on this shard.")),
        sm::make_derive("rx_row_nr", rx_row_nr,
                        sm::description("Total number of rows received on this shard.")),
        sm::make_derive("tx_row_bytes", tx_row_bytes,
                        sm::description("Total bytes of rows sent on this shard.")),
        sm::make_derive("rx_row_bytes", rx_row_bytes,
                        sm::description("Total bytes of rows received on this shard.")),
        sm::make_derive("tx_hashes_nr", tx_hashes_nr,
                        sm::description("Total number of row hashes sent on this shard.")),
        sm::make_derive("rx_hashes_nr", rx_hashes_nr,
                        sm::description("Total number of row hashes received on this shard.")),
        sm::make_derive("row_from_disk_nr", row_from_disk_nr,
                        sm::description("Total number of rows read from disk on this shard.")),
        sm::make_derive("row_from_disk_bytes", row_from_disk_bytes,
                        sm::description("Total bytes of rows read from disk on this shard.")),
    });
}

namespace repair {
    inline thread_local row_level_repair_metrics _metrics;
}

static void add_to_repair_meta_for_masters(repair_meta& rm);
static void add_to_repair_meta_for_followers(repair_meta& rm);

    repair_meta::repair_meta(
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
            size_t nr_peer_nodes,
            row_level_repair* row_level_repair_ptr)
            : _db(db)
            , _messaging(ms)
            , _cf(cf)
            , _schema(s)
            , _permit(_cf.streaming_read_concurrency_semaphore().make_permit(_schema.get(), "repair-meta"))
            , _range(range)
            , _cmp(repair_sync_boundary::tri_compare(*_schema))
            , _algo(algo)
            , _max_row_buf_size(max_row_buf_size)
            , _seed(seed)
            , _repair_master(master)
            , _myip(utils::fb_utilities::get_broadcast_address())
            , _repair_meta_id(repair_meta_id)
            , _reason(reason)
            , _master_node_shard_config(std::move(master_node_shard_config))
            , _remote_sharder(make_remote_sharder())
            , _same_sharding_config(is_same_sharding_config())
            , _nr_peer_nodes(nr_peer_nodes)
            , _repair_reader(
                    _db,
                    _cf,
                    _schema,
                    _permit,
                    _range,
                    _remote_sharder,
                    _master_node_shard_config.shard,
                    _seed,
                    repair_reader::is_local_reader(_repair_master || _same_sharding_config)
              )
            , _repair_writer(make_lw_shared<repair_writer>(_schema, _permit, _estimated_partitions, _nr_peer_nodes, _reason))
            , _sink_source_for_get_full_row_hashes(_repair_meta_id, _nr_peer_nodes,
                    [&ms] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return ms.local().make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(repair_meta_id, addr);
                })
            , _sink_source_for_get_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [&ms] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return ms.local().make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(repair_meta_id, addr);
                })
            , _sink_source_for_put_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [&ms] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return ms.local().make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(repair_meta_id, addr);
                })
            , _row_level_repair_ptr(row_level_repair_ptr)
            {
            if (master) {
                add_to_repair_meta_for_masters(*this);
            } else {
                add_to_repair_meta_for_followers(*this);
            }
            _all_node_states.push_back(repair_node_state(utils::fb_utilities::get_broadcast_address()));
            for (auto& node : all_live_peer_nodes) {
                _all_node_states.push_back(repair_node_state(node));
            }
    }

    future<> repair_meta::stop() {
        auto gate_future = _gate.close();
        auto f1 = _sink_source_for_get_full_row_hashes.close();
        auto f2 = _sink_source_for_get_row_diff.close();
        auto f3 = _sink_source_for_put_row_diff.close();
        return when_all_succeed(std::move(gate_future), std::move(f1), std::move(f2), std::move(f3)).discard_result().finally([this] {
            return _repair_writer->wait_for_writer_done();
        });
    }

    static std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>>& repair_meta_map();

    static lw_shared_ptr<repair_meta> get_repair_meta(gms::inet_address from, uint32_t repair_meta_id) {
        node_repair_meta_id id{from, repair_meta_id};
        auto it = repair_meta_map().find(id);
        if (it == repair_meta_map().end()) {
            throw std::runtime_error(format("get_repair_meta: repair_meta_id {} for node {} does not exist", id.repair_meta_id, id.ip));
        } else {
            return it->second;
        }
    }

    static future<>
    insert_repair_meta(const gms::inet_address& from,
            uint32_t src_cpu_id,
            uint32_t repair_meta_id,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            uint64_t max_row_buf_size,
            uint64_t seed,
            shard_config master_node_shard_config,
            table_schema_version schema_version,
            streaming::stream_reason reason) {
        return service::get_schema_for_write(schema_version, {from, src_cpu_id}, ::_messaging->local()).then([from,
                repair_meta_id,
                range,
                algo,
                max_row_buf_size,
                seed,
                master_node_shard_config,
                schema_version,
                reason] (schema_ptr s) {
            sharded<netw::messaging_service>& ms = *::_messaging;
            auto& db = service::get_local_storage_proxy().get_db();
            auto& cf = db.local().find_column_family(s->id());
            node_repair_meta_id id{from, repair_meta_id};
            auto rm = make_lw_shared<repair_meta>(db,
                    ms,
                    cf,
                    s,
                    range,
                    algo,
                    max_row_buf_size,
                    seed,
                    repair_meta::repair_master::no,
                    repair_meta_id,
                    reason,
                    std::move(master_node_shard_config),
                    std::vector<gms::inet_address>{from});
            rm->set_repair_state_for_local_node(repair_state::row_level_start_started);
            bool insertion = repair_meta_map().emplace(id, rm).second;
            if (!insertion) {
                rlogger.warn("insert_repair_meta: repair_meta_id {} for node {} already exists, replace existing one", id.repair_meta_id, id.ip);
                repair_meta_map()[id] = rm;
                rm->set_repair_state_for_local_node(repair_state::row_level_start_finished);
            } else {
                rlogger.debug("insert_repair_meta: Inserted repair_meta_id {} for node {}", id.repair_meta_id, id.ip);
            }
        });
    }

    static future<>
    remove_repair_meta(const gms::inet_address& from,
            uint32_t repair_meta_id,
            sstring ks_name,
            sstring cf_name,
            dht::token_range range) {
        node_repair_meta_id id{from, repair_meta_id};
        auto it = repair_meta_map().find(id);
        if (it == repair_meta_map().end()) {
            rlogger.warn("remove_repair_meta: repair_meta_id {} for node {} does not exist", id.repair_meta_id, id.ip);
            return make_ready_future<>();
        } else {
            auto rm = it->second;
            repair_meta_map().erase(it);
            rlogger.debug("remove_repair_meta: Stop repair_meta_id {} for node {} started", id.repair_meta_id, id.ip);
            return rm->stop().then([rm, id] {
                rlogger.debug("remove_repair_meta: Stop repair_meta_id {} for node {} finished", id.repair_meta_id, id.ip);
            });
        }
    }

    static future<>
    remove_repair_meta(gms::inet_address from) {
        rlogger.debug("Remove all repair_meta for single node {}", from);
        auto repair_metas = make_lw_shared<utils::chunked_vector<lw_shared_ptr<repair_meta>>>();
        for (auto it = repair_meta_map().begin(); it != repair_meta_map().end();) {
            if (it->first.ip == from) {
                repair_metas->push_back(it->second);
                it = repair_meta_map().erase(it);
            } else {
                it++;
            }
        }
        return parallel_for_each(*repair_metas, [repair_metas] (auto& rm) {
            return rm->stop().then([&rm] {
                rm = {};
            });
        }).then([repair_metas, from] {
            rlogger.debug("Removed all repair_meta for single node {}", from);
        });
    }

    static future<>
    remove_repair_meta() {
        rlogger.debug("Remove all repair_meta for all nodes");
        auto repair_metas = make_lw_shared<utils::chunked_vector<lw_shared_ptr<repair_meta>>>(
                boost::copy_range<utils::chunked_vector<lw_shared_ptr<repair_meta>>>(repair_meta_map()
                | boost::adaptors::map_values));
        repair_meta_map().clear();
        return parallel_for_each(*repair_metas, [repair_metas] (auto& rm) {
            return rm->stop().then([&rm] {
                rm = {};
            });
        }).then([repair_metas] {
            rlogger.debug("Removed all repair_meta for all nodes");
        });
    }

    void repair_meta::reset_peer_row_hash_sets() {
        if (_peer_row_hash_sets.size() != _nr_peer_nodes) {
            _peer_row_hash_sets.resize(_nr_peer_nodes);
        } else {
            for (auto& x : _peer_row_hash_sets) {
                x.clear();
            }
        }

    }

    repair_hash_set& repair_meta::peer_row_hash_sets(unsigned node_idx) {
        return _peer_row_hash_sets[node_idx];
    }

    // Get a list of row hashes in _working_row_buf
    future<repair_hash_set>
    repair_meta::working_row_hashes() {
        return do_with(repair_hash_set(), [this] (repair_hash_set& hashes) {
            return do_for_each(_working_row_buf, [&hashes] (repair_row& r) {
                hashes.emplace(r.hash());
            }).then([&hashes] {
                return std::move(hashes);
            });
        });
    }

    std::pair<std::optional<repair_sync_boundary>, bool>
    repair_meta::get_common_sync_boundary(bool zero_rows,
            std::vector<repair_sync_boundary>& sync_boundaries,
            std::vector<repair_hash>& combined_hashes) {
        if (sync_boundaries.empty()) {
            throw std::runtime_error("sync_boundaries is empty");
        }
        if(combined_hashes.empty()) {
            throw std::runtime_error("combined_hashes is empty");
        }
        // Get the smallest sync boundary in the list as the common sync boundary
        std::sort(sync_boundaries.begin(), sync_boundaries.end(),
                [this] (const auto& a, const auto& b) { return this->_cmp(a, b) < 0; });
        repair_sync_boundary sync_boundary_min = sync_boundaries.front();
        // Check if peers have identical combined hashes and sync boundary
        bool same_hashes = std::adjacent_find(combined_hashes.begin(), combined_hashes.end(),
                std::not_equal_to<repair_hash>()) == combined_hashes.end();
        bool same_boundary = std::adjacent_find(sync_boundaries.begin(), sync_boundaries.end(),
                [this] (const repair_sync_boundary& a, const repair_sync_boundary& b) { return this->_cmp(a, b) != 0; }) == sync_boundaries.end();
        rlogger.debug("get_common_sync_boundary: zero_rows={}, same_hashes={}, same_boundary={}, combined_hashes={}, sync_boundaries={}",
            zero_rows, same_hashes, same_boundary, combined_hashes, sync_boundaries);
        bool already_synced = same_hashes && same_boundary && !zero_rows;
        return std::pair<std::optional<repair_sync_boundary>, bool>(sync_boundary_min, already_synced);
    }

    future<uint64_t> repair_meta::do_estimate_partitions_on_all_shards() {
        return estimate_partitions(_db, _schema->ks_name(), _schema->cf_name(), _range);
    }

    future<uint64_t> repair_meta::do_estimate_partitions_on_local_shard() {
        return do_with(_cf.get_sstables(), uint64_t(0), [this] (lw_shared_ptr<const sstable_list>& sstables, uint64_t& partition_count) {
            return do_for_each(*sstables, [this, &partition_count] (const sstables::shared_sstable& sst) mutable {
                partition_count += sst->estimated_keys_for_range(_range);
            }).then([&partition_count] {
                return partition_count;
            });
        });
    }

    future<uint64_t> repair_meta::get_estimated_partitions() {
      return with_gate(_gate, [this] {
        if (_repair_master || _same_sharding_config) {
            return do_estimate_partitions_on_local_shard();
        } else {
            return do_with(dht::selective_token_range_sharder(_remote_sharder, _range, _master_node_shard_config.shard), uint64_t(0), [this] (auto& sharder, auto& partitions_sum) mutable {
                return repeat([this, &sharder, &partitions_sum] () mutable {
                    auto shard_range = sharder.next();
                    if (shard_range) {
                        return do_estimate_partitions_on_all_shards().then([this, &partitions_sum] (uint64_t partitions) mutable {
                            partitions_sum += partitions;
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    } else {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                }).then([&partitions_sum] {
                    return partitions_sum;
                });
            });
        }
      });
    }

    future<> repair_meta::set_estimated_partitions(uint64_t estimated_partitions) {
        return with_gate(_gate, [this, estimated_partitions] {
            _estimated_partitions = estimated_partitions;
        });
    }

    dht::sharder repair_meta::make_remote_sharder() {
        return dht::sharder(_master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
    }

    bool repair_meta::is_same_sharding_config() {
        rlogger.debug("is_same_sharding_config: remote_shard={}, remote_shard_count={}, remote_ignore_msb={}",
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
        return _schema->get_sharder().shard_count() == _master_node_shard_config.shard_count
               && _schema->get_sharder().sharding_ignore_msb() == _master_node_shard_config.ignore_msb
               && this_shard_id() == _master_node_shard_config.shard;
    }

    future<size_t> repair_meta::get_repair_rows_size(const std::list<repair_row>& rows) const {
        return do_with(size_t(0), [&rows] (size_t& sz) {
            return do_for_each(rows, [&sz] (const repair_row& r) mutable {
                sz += r.size();
            }).then([&sz] {
                return sz;
            });
        });
    }

    // return the combined checksum of rows in _row_buf
    future<repair_hash> repair_meta::row_buf_csum() {
        return do_with(repair_hash(), [this] (repair_hash& combined) {
            return do_for_each(_row_buf, [&combined] (repair_row& r) mutable {
                combined.add(r.hash());
            }).then([&combined] {
                return combined;
            });
        });
    }

    repair_hash repair_meta::do_hash_for_mf(const decorated_key_with_hash& dk_with_hash, const mutation_fragment& mf) {
        xx_hasher h(_seed);
        fragment_hasher fh(*_schema, h);
        fh.hash(mf);
        feed_hash(h, dk_with_hash.hash.hash);
        return repair_hash(h.finalize_uint64());
    }

    stop_iteration repair_meta::handle_mutation_fragment(mutation_fragment& mf, size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows) {
        if (mf.is_partition_start()) {
            auto& start = mf.as_partition_start();
            _repair_reader.set_current_dk(start.key());
            if (!start.partition_tombstone()) {
                // Ignore partition_start with empty partition tombstone
                return stop_iteration::no;
            }
        } else if (mf.is_end_of_partition()) {
            _repair_reader.clear_current_dk();
            return stop_iteration::no;
        }
        auto hash = do_hash_for_mf(*_repair_reader.get_current_dk(), mf);
        repair_row r(freeze(*_schema, mf), position_in_partition(mf.position()), _repair_reader.get_current_dk(), hash, is_dirty_on_master::no);
        rlogger.trace("Reading: r.boundary={}, r.hash={}", r.boundary(), r.hash());
        _metrics.row_from_disk_nr++;
        _metrics.row_from_disk_bytes += r.size();
        cur_size += r.size();
        new_rows_size += r.size();
        cur_rows.push_back(std::move(r));
        return stop_iteration::no;
    }

    // Read rows from sstable until the size of rows exceeds _max_row_buf_size  - current_size
    // This reads rows from where the reader left last time into _row_buf
    // _current_sync_boundary or _last_sync_boundary have no effect on the reader neither.
    future<std::tuple<std::list<repair_row>, size_t>>
    repair_meta::read_rows_from_disk(size_t cur_size) {
        using value_type = std::tuple<std::list<repair_row>, size_t>;
        return do_with(cur_size, size_t(0), std::list<repair_row>(), [this] (size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows) {
            return repeat([this, &cur_size, &cur_rows, &new_rows_size] () mutable {
                if (cur_size >= _max_row_buf_size) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                _gate.check();
                return _repair_reader.read_mutation_fragment().then([this, &cur_size, &new_rows_size, &cur_rows] (mutation_fragment_opt mfopt) mutable {
                    if (!mfopt) {
                        _repair_reader.on_end_of_stream();
                        return stop_iteration::yes;
                    }
                    return handle_mutation_fragment(*mfopt, cur_size, new_rows_size, cur_rows);
                });
            }).then_wrapped([this, &cur_rows, &new_rows_size] (future<> fut) mutable {
                if (fut.failed()) {
                    _repair_reader.on_end_of_stream();
                    return make_exception_future<value_type>(fut.get_exception());
                }
                _repair_reader.pause();
                return make_ready_future<value_type>(value_type(std::move(cur_rows), new_rows_size));
            });
        });
    }

    future<> repair_meta::clear_row_buf() {
        return utils::clear_gently(_row_buf);
    }

    future<> repair_meta::clear_working_row_buf() {
        return utils::clear_gently(_working_row_buf).then([this] {
            _working_row_buf_combined_hash.clear();
        });
    }

    // Read rows from disk until _max_row_buf_size of rows are filled into _row_buf.
    // Calculate the combined checksum of the rows
    // Calculate the total size of the rows in _row_buf
    future<get_sync_boundary_response>
    repair_meta::get_sync_boundary(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        auto f = make_ready_future<>();
        if (skipped_sync_boundary) {
            _current_sync_boundary = skipped_sync_boundary;
            f = clear_row_buf();
        }
        // Here is the place we update _last_sync_boundary
        rlogger.trace("SET _last_sync_boundary from {} to {}", _last_sync_boundary, _current_sync_boundary);
        _last_sync_boundary = _current_sync_boundary;
      return f.then([this, sb = std::move(skipped_sync_boundary)] () mutable {
       return clear_working_row_buf().then([this, sb = sb] () mutable {
        return row_buf_size().then([this, sb = std::move(sb)] (size_t cur_size) {
            return read_rows_from_disk(cur_size).then_unpack([this, sb = std::move(sb)] (std::list<repair_row> new_rows, size_t new_rows_size) mutable {
                size_t new_rows_nr = new_rows.size();
                _row_buf.splice(_row_buf.end(), new_rows);
                return row_buf_csum().then([this, new_rows_size, new_rows_nr, sb = std::move(sb)] (repair_hash row_buf_combined_hash) {
                    return row_buf_size().then([this, new_rows_size, new_rows_nr, row_buf_combined_hash, sb = std::move(sb)] (size_t row_buf_bytes) {
                        std::optional<repair_sync_boundary> sb_max;
                        if (!_row_buf.empty()) {
                            sb_max = _row_buf.back().boundary();
                        }
                        rlogger.debug("get_sync_boundary: Got nr={} rows, sb_max={}, row_buf_size={}, repair_hash={}, skipped_sync_boundary={}",
                                new_rows_nr, sb_max, row_buf_bytes, row_buf_combined_hash, sb);
                        return get_sync_boundary_response{sb_max, row_buf_combined_hash, row_buf_bytes, new_rows_size, new_rows_nr};
                    });
                });
            });
        });
       });
      });
    }

    future<> repair_meta::move_row_buf_to_working_row_buf() {
        if (_cmp(_row_buf.back().boundary(), *_current_sync_boundary) <= 0) {
            // Fast path
            _working_row_buf.swap(_row_buf);
            return make_ready_future<>();
        }
        return do_with(_row_buf.rbegin(), [this, sz = _row_buf.size()] (auto& it) {
            // Move the rows > _current_sync_boundary to _working_row_buf
            // Delete the rows > _current_sync_boundary from _row_buf
            // Swap _working_row_buf and _row_buf so that _working_row_buf
            // contains rows within (_last_sync_boundary,
            // _current_sync_boundary], _row_buf contains rows wthin
            // (_current_sync_boundary, ...]
            return repeat([this, &it, sz] () {
                if (it == _row_buf.rend()) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                repair_row& r = *(it++);
                if (_cmp(r.boundary(), *_current_sync_boundary) > 0) {
                    _working_row_buf.push_front(std::move(r));
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                }
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }).then([this, sz] {
                _row_buf.resize(_row_buf.size() - _working_row_buf.size());
                _row_buf.swap(_working_row_buf);
                if (sz != _working_row_buf.size() + _row_buf.size()) {
                    throw std::runtime_error(format("incorrect row_buf and working_row_buf size, before={}, after={} + {}",
                            sz, _working_row_buf.size(), _row_buf.size()));
                }
            });
        });
    }

    // Move rows from <_row_buf> to <_working_row_buf> according to
    // _last_sync_boundary and common_sync_boundary. That is rows within the
    // (_last_sync_boundary, _current_sync_boundary] in <_row_buf> are moved
    // into the <_working_row_buf>
    future<get_combined_row_hash_response>
    repair_meta::request_row_hashes(const std::optional<repair_sync_boundary>& common_sync_boundary) {
        if (!common_sync_boundary) {
            throw std::runtime_error("common_sync_boundary is empty");
        }
        _current_sync_boundary = common_sync_boundary;
        rlogger.trace("SET _current_sync_boundary to {}, common_sync_boundary={}", _current_sync_boundary, common_sync_boundary);
        _working_row_buf.clear();
        _working_row_buf_combined_hash.clear();

        if (_row_buf.empty()) {
            return make_ready_future<get_combined_row_hash_response>(get_combined_row_hash_response());
        }
        return move_row_buf_to_working_row_buf().then([this] {
            return do_for_each(_working_row_buf, [this] (repair_row& r) {
                _working_row_buf_combined_hash.add(r.hash());
                return make_ready_future<>();
            }).then([this] {
                return get_combined_row_hash_response{_working_row_buf_combined_hash};
            });
        });
    }

    future<std::list<repair_row>>
    repair_meta::copy_rows_from_working_row_buf() {
        return do_with(std::list<repair_row>(), [this] (std::list<repair_row>& rows) {
            return do_for_each(_working_row_buf, [this, &rows] (const repair_row& r) {
                rows.push_back(r);
            }).then([&rows] {
                return std::move(rows);
            });
        });
    }

    future<std::list<repair_row>>
    repair_meta::copy_rows_from_working_row_buf_within_set_diff(repair_hash_set set_diff) {
        return do_with(std::list<repair_row>(), std::move(set_diff),
                [this] (std::list<repair_row>& rows, repair_hash_set& set_diff) {
            return do_for_each(_working_row_buf, [this, &set_diff, &rows] (const repair_row& r) {
                if (set_diff.contains(r.hash())) {
                    rows.push_back(r);
                }
            }).then([&rows] {
                return std::move(rows);
            });
        });
    }

    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Give a set of row hashes, return the corresponding rows
    // If needs_all_rows is set, return all the rows in _working_row_buf, ignore the set_diff
    future<std::list<repair_row>>
    repair_meta::get_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows) {
        if (needs_all_rows) {
            if (!_repair_master || _nr_peer_nodes == 1) {
                return make_ready_future<std::list<repair_row>>(std::move(_working_row_buf));
            }
            return copy_rows_from_working_row_buf();
        } else {
            return copy_rows_from_working_row_buf_within_set_diff(std::move(set_diff));
        }
    }

    future<> repair_meta::do_apply_rows(std::list<repair_row>&& row_diff, update_working_row_buf update_buf) {
        return do_with(std::move(row_diff), [this, update_buf] (std::list<repair_row>& row_diff) {
            return with_semaphore(_repair_writer->sem(), 1, [this, update_buf, &row_diff] {
                _repair_writer->create_writer(_db);
                return repeat([this, update_buf, &row_diff] () mutable {
                    if (row_diff.empty()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    repair_row& r = row_diff.front();
                    if (update_buf) {
                        _working_row_buf_combined_hash.add(r.hash());
                    }
                    // The repair_row here is supposed to have
                    // mutation_fragment attached because we have stored it in
                    // to_repair_rows_list above where the repair_row is created.
                    mutation_fragment mf = std::move(r.get_mutation_fragment());
                    auto dk_with_hash = r.get_dk_with_hash();
                    return _repair_writer->do_write(std::move(dk_with_hash), std::move(mf)).then([&row_diff] {
                        row_diff.pop_front();
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                });
            });
        });
    }

    // Give a list of rows, apply the rows to disk and update the _working_row_buf and _peer_row_hash_sets if requested
    // Must run inside a seastar thread
    void repair_meta::apply_rows_on_master_in_thread(repair_rows_on_wire rows, gms::inet_address from, update_working_row_buf update_buf,
            update_peer_row_hash_sets update_hash_set, unsigned node_idx) {
        if (rows.empty()) {
            return;
        }
        auto row_diff = to_repair_rows_list(rows).get0();
        auto sz = get_repair_rows_size(row_diff).get0();
        stats().rx_row_bytes += sz;
        stats().rx_row_nr += row_diff.size();
        stats().rx_row_nr_peer[from] += row_diff.size();
        if (update_buf) {
            // Both row_diff and _working_row_buf and are ordered, merging
            // two sored list to make sure the combination of row_diff
            // and _working_row_buf are ordered.
            utils::merge_to_gently(_working_row_buf, row_diff,
                 [this] (const repair_row& x, const repair_row& y) { return _cmp(x.boundary(), y.boundary()) < 0; });
            for (auto& r : row_diff) {
                thread::maybe_yield();
                _working_row_buf_combined_hash.add(r.hash());
            }
        }
        if (update_hash_set) {
            _peer_row_hash_sets[node_idx] = boost::copy_range<repair_hash_set>(row_diff |
                    boost::adaptors::transformed([] (repair_row& r) { thread::maybe_yield(); return r.hash(); }));
        }
        // Repair rows in row_diff will be flushed to disk by flush_rows_in_working_row_buf,
        // so we skip calling do_apply_rows here.
        _dirty_on_master = is_dirty_on_master::yes;
    }

    // Must run inside a seastar thread
    void repair_meta::flush_rows_in_working_row_buf() {
        if (_dirty_on_master) {
            _dirty_on_master = is_dirty_on_master::no;
        } else {
            return;
        }
        auto cmp = position_in_partition::tri_compare(*_schema);
        lw_shared_ptr<mutation_fragment> last_mf;
        lw_shared_ptr<const decorated_key_with_hash> last_dk;
        for (auto& r : _working_row_buf) {
            thread::maybe_yield();
            if (!r.dirty_on_master()) {
                continue;
            }
            _repair_writer->create_writer(_db);
            auto mf = r.get_mutation_fragment_ptr();
            const auto& dk = r.get_dk_with_hash()->dk;
            if (last_mf && last_dk &&
                    cmp(last_mf->position(), mf->position()) == 0 &&
                    dk.tri_compare(*_schema, last_dk->dk) == 0 &&
                    last_mf->mergeable_with(*mf)) {
                last_mf->apply(*_schema, std::move(*mf));
            } else {
                if (last_mf && last_dk) {
                    _repair_writer->do_write(std::move(last_dk), std::move(*last_mf)).get();
                }
                last_mf = mf;
                last_dk = r.get_dk_with_hash();
            }
        }
        if (last_mf && last_dk) {
            _repair_writer->do_write(std::move(last_dk), std::move(*last_mf)).get();
        }
    }

    future<>
    repair_meta::apply_rows_on_follower(repair_rows_on_wire rows) {
        if (rows.empty()) {
            return make_ready_future<>();
        }
        return to_repair_rows_list(std::move(rows)).then([this] (std::list<repair_row> row_diff) {
            return do_apply_rows(std::move(row_diff), update_working_row_buf::no);
        });
    }

    future<repair_rows_on_wire> repair_meta::to_repair_rows_on_wire(std::list<repair_row> row_list) {
        lw_shared_ptr<const decorated_key_with_hash> last_dk_with_hash;
        return do_with(repair_rows_on_wire(), std::move(row_list), std::move(last_dk_with_hash),
                [this] (repair_rows_on_wire& rows, std::list<repair_row>& row_list, lw_shared_ptr<const decorated_key_with_hash>& last_dk_with_hash) {
            return get_repair_rows_size(row_list).then([this, &rows, &row_list, &last_dk_with_hash] (size_t row_bytes) {
                _metrics.tx_row_nr += row_list.size();
                _metrics.tx_row_bytes += row_bytes;
                return do_for_each(row_list, [this, &rows, &last_dk_with_hash] (repair_row& r) {
                    const auto& dk_with_hash = r.get_dk_with_hash();
                    // No need to search from the beginning of the rows. Look at the end of repair_rows_on_wire is enough.
                    if (rows.empty()) {
                        auto pk = dk_with_hash->dk.key();
                        last_dk_with_hash = dk_with_hash;
                        rows.push_back(repair_row_on_wire(std::move(pk), {std::move(r.get_frozen_mutation())}));
                    } else {
                        auto& row = rows.back();
                        if (last_dk_with_hash && dk_with_hash->dk.tri_compare(*_schema, last_dk_with_hash->dk) == 0) {
                            row.push_mutation_fragment(std::move(r.get_frozen_mutation()));
                        } else {
                            auto pk = dk_with_hash->dk.key();
                            last_dk_with_hash = dk_with_hash;
                            rows.push_back(repair_row_on_wire(std::move(pk), {std::move(r.get_frozen_mutation())}));
                        }
                    }
                }).then([&rows] {
                    return std::move(rows);
                });
            });
        });
    };

    future<std::list<repair_row>> repair_meta::to_repair_rows_list(repair_rows_on_wire rows) {
        return do_with(std::move(rows), std::list<repair_row>(), lw_shared_ptr<const decorated_key_with_hash>(), lw_shared_ptr<mutation_fragment>(), position_in_partition::tri_compare(*_schema),
          [this] (repair_rows_on_wire& rows, std::list<repair_row>& row_list, lw_shared_ptr<const decorated_key_with_hash>& dk_ptr, lw_shared_ptr<mutation_fragment>& last_mf, position_in_partition::tri_compare& cmp) mutable {
            return do_for_each(rows, [this, &dk_ptr, &row_list, &last_mf, &cmp] (partition_key_and_mutation_fragments& x) mutable {
                dht::decorated_key dk = dht::decorate_key(*_schema, x.get_key());
                if (!(dk_ptr && dk_ptr->dk.equal(*_schema, dk))) {
                    dk_ptr = make_lw_shared<const decorated_key_with_hash>(*_schema, dk, _seed);
                }
                if (_repair_master) {
                    return do_for_each(x.get_mutation_fragments(), [this, &dk_ptr, &row_list] (frozen_mutation_fragment& fmf) mutable {
                        _metrics.rx_row_nr += 1;
                        _metrics.rx_row_bytes += fmf.representation().size();
                        // Keep the mutation_fragment in repair_row as an
                        // optimization to avoid unfreeze again when
                        // mutation_fragment is needed by _repair_writer.do_write()
                        // to apply the repair_row to disk
                        auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*_schema, _permit));
                        auto hash = do_hash_for_mf(*dk_ptr, *mf);
                        position_in_partition pos(mf->position());
                        row_list.push_back(repair_row(std::move(fmf), std::move(pos), dk_ptr, std::move(hash), is_dirty_on_master::yes, std::move(mf)));
                    });
                } else {
                    last_mf = {};
                    return do_for_each(x.get_mutation_fragments(), [this, &dk_ptr, &row_list, &last_mf, &cmp] (frozen_mutation_fragment& fmf) mutable {
                        _metrics.rx_row_nr += 1;
                        _metrics.rx_row_bytes += fmf.representation().size();
                        auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*_schema, _permit));
                        // If the mutation_fragment has the same position as
                        // the last mutation_fragment, it means they are the
                        // same row with different contents. We can not feed
                        // such rows into the sstable writer. Instead we apply
                        // the mutation_fragment into the previous one.
                        if (last_mf && cmp(last_mf->position(), mf->position()) == 0 && last_mf->mergeable_with(*mf)) {
                            last_mf->apply(*_schema, std::move(*mf));
                        } else {
                            last_mf = mf;
                            // On repair follower node, only decorated_key_with_hash and the mutation_fragment inside repair_row are used.
                            row_list.push_back(repair_row({}, {}, dk_ptr, {}, is_dirty_on_master::no, std::move(mf)));
                        }
                    });
                }
            }).then([&row_list] {
                return std::move(row_list);
            });
        });
    }

    // RPC API
    // Return the hashes of the rows in _working_row_buf
    future<repair_hash_set>
    repair_meta::get_full_row_hashes(gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_full_row_hashes_handler();
        }
        return _messaging.local().send_repair_get_full_row_hashes(msg_addr(remote_node),
                _repair_meta_id).then([this, remote_node] (repair_hash_set hashes) {
            rlogger.debug("Got full hashes from peer={}, nr_hashes={}", remote_node, hashes.size());
            _metrics.rx_hashes_nr += hashes.size();
            stats().rx_hashes_nr += hashes.size();
            stats().rpc_call_nr++;
            return hashes;
        });
    }

    future<> repair_meta::get_full_row_hashes_source_op(
            lw_shared_ptr<repair_hash_set> current_hashes,
            gms::inet_address remote_node,
            unsigned node_idx,
            rpc::source<repair_hash_with_cmd>& source) {
        return repeat([this, current_hashes, remote_node, node_idx, &source] () mutable {
            return source().then([this, current_hashes, remote_node, node_idx] (std::optional<std::tuple<repair_hash_with_cmd>> hash_cmd_opt) mutable {
                if (hash_cmd_opt) {
                    repair_hash_with_cmd hash_cmd = std::get<0>(hash_cmd_opt.value());
                    rlogger.trace("get_full_row_hashes: Got repair_hash_with_cmd from peer={}, hash={}, cmd={}", remote_node, hash_cmd.hash, int(hash_cmd.cmd));
                    if (hash_cmd.cmd == repair_stream_cmd::hash_data) {
                        current_hashes->insert(hash_cmd.hash);
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    } else if (hash_cmd.cmd == repair_stream_cmd::end_of_current_hash_set) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    } else if (hash_cmd.cmd == repair_stream_cmd::error) {
                        throw std::runtime_error("get_full_row_hashes: Peer failed to process");
                    } else {
                        throw std::runtime_error("get_full_row_hashes: Got unexpected repair_stream_cmd");
                    }
                } else {
                    _sink_source_for_get_full_row_hashes.mark_source_closed(node_idx);
                    throw std::runtime_error("get_full_row_hashes: Got unexpected end of stream");
                }
            });
        });
    }

    future<> repair_meta::get_full_row_hashes_sink_op(rpc::sink<repair_stream_cmd>& sink) {
        return sink(repair_stream_cmd::get_full_row_hashes).then([&sink] {
            return sink.flush();
        }).handle_exception([&sink] (std::exception_ptr ep) {
            return sink.close().then([ep = std::move(ep)] () mutable {
                return make_exception_future<>(std::move(ep));
            });
        });
    }

    future<repair_hash_set>
    repair_meta::get_full_row_hashes_with_rpc_stream(gms::inet_address remote_node, unsigned node_idx) {
        if (remote_node == _myip) {
            return get_full_row_hashes_handler();
        }
        auto current_hashes = make_lw_shared<repair_hash_set>();
        return _sink_source_for_get_full_row_hashes.get_sink_source(remote_node, node_idx).then_unpack(
                [this, current_hashes, remote_node, node_idx]
                (rpc::sink<repair_stream_cmd>& sink, rpc::source<repair_hash_with_cmd>& source) mutable {
            auto source_op = get_full_row_hashes_source_op(current_hashes, remote_node, node_idx, source);
            auto sink_op = get_full_row_hashes_sink_op(sink);
            return when_all_succeed(std::move(source_op), std::move(sink_op)).discard_result();
        }).then([this, current_hashes] () mutable {
            stats().rx_hashes_nr += current_hashes->size();
            _metrics.rx_hashes_nr += current_hashes->size();
            return std::move(*current_hashes);
        });
    }

    // RPC handler
    future<repair_hash_set>
    repair_meta::get_full_row_hashes_handler() {
        return with_gate(_gate, [this] {
            return working_row_hashes();
        });
    }

    // RPC API
    // Return the combined hashes of the current working row buf
    future<get_combined_row_hash_response>
    repair_meta::get_combined_row_hash(std::optional<repair_sync_boundary> common_sync_boundary, gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_combined_row_hash_handler(common_sync_boundary);
        }
        return _messaging.local().send_repair_get_combined_row_hash(msg_addr(remote_node),
                _repair_meta_id, common_sync_boundary).then([this] (get_combined_row_hash_response resp) {
            stats().rpc_call_nr++;
            stats().rx_hashes_nr++;
            _metrics.rx_hashes_nr++;
            return resp;
        });
    }

    // RPC handler
    future<get_combined_row_hash_response>
    repair_meta::get_combined_row_hash_handler(std::optional<repair_sync_boundary> common_sync_boundary) {
        // We can not call this function twice. The good thing is we do not use
        // retransmission at messaging_service level, so no message will be retransmited.
        rlogger.trace("Calling get_combined_row_hash_handler");
        return with_gate(_gate, [this, common_sync_boundary = std::move(common_sync_boundary)] () mutable {
            return request_row_hashes(common_sync_boundary);
        });
    }

    // RPC API
    future<>
    repair_meta::repair_row_level_start(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range, table_schema_version schema_version, streaming::stream_reason reason) {
        if (remote_node == _myip) {
            return make_ready_future<>();
        }
        stats().rpc_call_nr++;
        // Even though remote partitioner name is ignored in the current version of
        // repair, we still have to send something to keep compatibility with nodes
        // that run older versions. This will make it possible to run mixed cluster.
        // Murmur3 is appropriate because that's the only supported partitioner at
        // the time this change is introduced.
        sstring remote_partitioner_name = "org.apache.cassandra.dht.Murmur3Partitioner";
        return _messaging.local().send_repair_row_level_start(msg_addr(remote_node),
                _repair_meta_id, ks_name, cf_name, std::move(range), _algo, _max_row_buf_size, _seed,
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb,
                remote_partitioner_name, std::move(schema_version), reason).then([ks_name, cf_name] (rpc::optional<repair_row_level_start_response> resp) {
            if (resp && resp->status == repair_row_level_start_status::no_such_column_family) {
                return make_exception_future<>(no_such_column_family(ks_name, cf_name));
            } else {
                return make_ready_future<>();
            }
        });
    }

    // RPC handler
    static future<repair_row_level_start_response>
    repair_row_level_start_handler(gms::inet_address from, uint32_t src_cpu_id, uint32_t repair_meta_id, sstring ks_name, sstring cf_name,
            dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size,
            uint64_t seed, shard_config master_node_shard_config, table_schema_version schema_version, streaming::stream_reason reason) {
        if (!_sys_dist_ks->local_is_initialized() || !_view_update_generator->local_is_initialized()) {
            return make_exception_future<repair_row_level_start_response>(std::runtime_error(format("Node {} is not fully initialized for repair, try again later",
                    utils::fb_utilities::get_broadcast_address())));
        }
        rlogger.debug(">>> Started Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_siz={}",
            utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, schema_version, range, seed, max_row_buf_size);
        return insert_repair_meta(from, src_cpu_id, repair_meta_id, std::move(range), algo, max_row_buf_size, seed, std::move(master_node_shard_config), std::move(schema_version), reason).then([] {
            return repair_row_level_start_response{repair_row_level_start_status::ok};
        }).handle_exception_type([] (no_such_column_family&) {
            return repair_row_level_start_response{repair_row_level_start_status::no_such_column_family};
        });
    }

    // RPC API
    future<> repair_meta::repair_row_level_stop(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range) {
        if (remote_node == _myip) {
            return stop();
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_row_level_stop(msg_addr(remote_node),
                _repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range));
    }

    // RPC handler
    static future<>
    repair_row_level_stop_handler(gms::inet_address from, uint32_t repair_meta_id, sstring ks_name, sstring cf_name, dht::token_range range) {
        rlogger.debug("<<< Finished Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}",
            utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, range);
        auto rm = get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::row_level_stop_started);
        return remove_repair_meta(from, repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range)).then([rm] {
            rm->set_repair_state_for_local_node(repair_state::row_level_stop_finished);
        });
    }

    // RPC API
    future<uint64_t> repair_meta::repair_get_estimated_partitions(gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_estimated_partitions();
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_get_estimated_partitions(msg_addr(remote_node), _repair_meta_id);
    }


    // RPC handler
    future<uint64_t> repair_get_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id) {
        auto rm = get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_started);
        return rm->get_estimated_partitions().then([rm] (uint64_t partitions) {
            rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_finished);
            return partitions;
        });
    }

    // RPC API
    future<> repair_meta::repair_set_estimated_partitions(gms::inet_address remote_node, uint64_t estimated_partitions) {
        if (remote_node == _myip) {
            return set_estimated_partitions(estimated_partitions);
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_set_estimated_partitions(msg_addr(remote_node), _repair_meta_id, estimated_partitions);
    }


    // RPC handler
    future<> repair_set_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id, uint64_t estimated_partitions) {
        auto rm = get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_started);
        return rm->set_estimated_partitions(estimated_partitions).then([rm] {
            rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_finished);
        });
    }

    // RPC API
    // Return the largest sync point contained in the _row_buf , current _row_buf checksum, and the _row_buf size
    future<get_sync_boundary_response>
    repair_meta::get_sync_boundary(gms::inet_address remote_node, std::optional<repair_sync_boundary> skipped_sync_boundary) {
        if (remote_node == _myip) {
            return get_sync_boundary_handler(skipped_sync_boundary);
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_get_sync_boundary(msg_addr(remote_node), _repair_meta_id, skipped_sync_boundary);
    }

    // RPC handler
    future<get_sync_boundary_response>
    repair_meta::get_sync_boundary_handler(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        return with_gate(_gate, [this, skipped_sync_boundary = std::move(skipped_sync_boundary)] () mutable {
            return get_sync_boundary(std::move(skipped_sync_boundary));
        });
    }

    // RPC API
    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Must run inside a seastar thread
    void repair_meta::get_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node, unsigned node_idx) {
        if (needs_all_rows || !set_diff.empty()) {
            if (remote_node == _myip) {
                return;
            }
            if (needs_all_rows) {
                set_diff.clear();
            } else {
                stats().tx_hashes_nr += set_diff.size();
                _metrics.tx_hashes_nr += set_diff.size();
            }
            stats().rpc_call_nr++;
            repair_rows_on_wire rows = _messaging.local().send_repair_get_row_diff(msg_addr(remote_node),
                    _repair_meta_id, std::move(set_diff), bool(needs_all_rows)).get0();
            if (!rows.empty()) {
                apply_rows_on_master_in_thread(std::move(rows), remote_node, update_working_row_buf::yes, update_peer_row_hash_sets::no, node_idx);
            }
        }
    }

    // Must run inside a seastar thread
    void repair_meta::get_row_diff_and_update_peer_row_hash_sets(gms::inet_address remote_node, unsigned node_idx) {
        if (remote_node == _myip) {
            return;
        }
        stats().rpc_call_nr++;
        repair_rows_on_wire rows = _messaging.local().send_repair_get_row_diff(msg_addr(remote_node),
                _repair_meta_id, {}, bool(needs_all_rows_t::yes)).get0();
        if (!rows.empty()) {
            apply_rows_on_master_in_thread(std::move(rows), remote_node, update_working_row_buf::yes, update_peer_row_hash_sets::yes, node_idx);
        }
    }

    // Must run inside a seastar thread
    void repair_meta::get_row_diff_source_op(
            update_peer_row_hash_sets update_hash_set,
            gms::inet_address remote_node,
            unsigned node_idx,
            rpc::sink<repair_hash_with_cmd>& sink,
            rpc::source<repair_row_on_wire_with_cmd>& source) {
        repair_rows_on_wire current_rows;
        for (;;) {
            std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt = source().get0();
            if (row_opt) {
                if (inject_rpc_stream_error) {
                    throw std::runtime_error("get_row_diff: Inject sender error in source loop");
                }
                auto row = std::move(std::get<0>(row_opt.value()));
                if (row.cmd == repair_stream_cmd::row_data) {
                    rlogger.trace("get_row_diff: Got repair_row_on_wire with data");
                    current_rows.push_back(std::move(row.row));
                } else if (row.cmd == repair_stream_cmd::end_of_current_rows) {
                    rlogger.trace("get_row_diff: Got repair_row_on_wire with nullopt");
                    apply_rows_on_master_in_thread(std::move(current_rows), remote_node, update_working_row_buf::yes, update_hash_set, node_idx);
                    break;
                } else if (row.cmd == repair_stream_cmd::error) {
                    throw std::runtime_error("get_row_diff: Peer failed to process");
                } else {
                    throw std::runtime_error("get_row_diff: Got unexpected repair_stream_cmd");
                }
            } else {
                _sink_source_for_get_row_diff.mark_source_closed(node_idx);
                throw std::runtime_error("get_row_diff: Got unexpected end of stream");
            }
        }
    }

    future<> repair_meta::get_row_diff_sink_op(
            repair_hash_set set_diff,
            needs_all_rows_t needs_all_rows,
            rpc::sink<repair_hash_with_cmd>& sink,
            gms::inet_address remote_node) {
        return do_with(std::move(set_diff), [needs_all_rows, remote_node, &sink] (repair_hash_set& set_diff) mutable {
            if (inject_rpc_stream_error) {
                return make_exception_future<>(std::runtime_error("get_row_diff: Inject sender error in sink loop"));
            }
            if (needs_all_rows) {
                rlogger.trace("get_row_diff: request with repair_stream_cmd::needs_all_rows");
                return sink(repair_hash_with_cmd{repair_stream_cmd::needs_all_rows, repair_hash()}).then([&sink] () mutable {
                    return sink.flush();
                });
            }
            return do_for_each(set_diff, [&sink] (const repair_hash& hash) mutable {
                return sink(repair_hash_with_cmd{repair_stream_cmd::hash_data, hash});
            }).then([&sink] () mutable {
                return sink(repair_hash_with_cmd{repair_stream_cmd::end_of_current_hash_set, repair_hash()});
            }).then([&sink] () mutable {
                return sink.flush();
            });
        }).handle_exception([&sink] (std::exception_ptr ep) {
            return sink.close().then([ep = std::move(ep)] () mutable {
                return make_exception_future<>(std::move(ep));
            });
        });
    }

    // Must run inside a seastar thread
    void repair_meta::get_row_diff_with_rpc_stream(
            repair_hash_set set_diff,
            needs_all_rows_t needs_all_rows,
            update_peer_row_hash_sets update_hash_set,
            gms::inet_address remote_node,
            unsigned node_idx) {
        if (needs_all_rows || !set_diff.empty()) {
            if (remote_node == _myip) {
                return;
            }
            if (needs_all_rows) {
                set_diff.clear();
            } else {
                stats().tx_hashes_nr += set_diff.size();
                _metrics.tx_hashes_nr += set_diff.size();
            }
            stats().rpc_call_nr++;
            auto f = _sink_source_for_get_row_diff.get_sink_source(remote_node, node_idx).get0();
            rpc::sink<repair_hash_with_cmd>& sink = std::get<0>(f);
            rpc::source<repair_row_on_wire_with_cmd>& source = std::get<1>(f);
            auto sink_op = get_row_diff_sink_op(std::move(set_diff), needs_all_rows, sink, remote_node);
            get_row_diff_source_op(update_hash_set, remote_node, node_idx, sink, source);
            sink_op.get();
        }
    }

    // RPC handler
    future<repair_rows_on_wire> repair_meta::get_row_diff_handler(repair_hash_set set_diff, needs_all_rows_t needs_all_rows) {
        return with_gate(_gate, [this, set_diff = std::move(set_diff), needs_all_rows] () mutable {
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this] (std::list<repair_row> row_diff) {
                return to_repair_rows_on_wire(std::move(row_diff));
            });
        });
    }

    // RPC API
    // Send rows in the _working_row_buf with hash within the given sef_diff
    future<> repair_meta::put_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node) {
        if (!set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            size_t sz = set_diff.size();
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this, remote_node, sz] (std::list<repair_row> row_diff) {
                if (row_diff.size() != sz) {
                    rlogger.warn("Hash conflict detected, keyspace={}, table={}, range={}, row_diff.size={}, set_diff.size={}. It is recommended to compact the table and rerun repair for the range.",
                            _schema->ks_name(), _schema->cf_name(), _range, row_diff.size(), sz);
                }
                return do_with(std::move(row_diff), [this, remote_node] (std::list<repair_row>& row_diff) {
                    return get_repair_rows_size(row_diff).then([this, remote_node, &row_diff] (size_t row_bytes) mutable {
                        stats().tx_row_nr += row_diff.size();
                        stats().tx_row_nr_peer[remote_node] += row_diff.size();
                        stats().tx_row_bytes += row_bytes;
                        stats().rpc_call_nr++;
                        return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node] (repair_rows_on_wire rows)  {
                            return _messaging.local().send_repair_put_row_diff(msg_addr(remote_node), _repair_meta_id, std::move(rows));
                        });
                    });
                });
            });
        }
        return make_ready_future<>();
    }

    future<> repair_meta::put_row_diff_source_op(
            gms::inet_address remote_node,
            unsigned node_idx,
            rpc::source<repair_stream_cmd>& source) {
        return repeat([this, remote_node, node_idx, &source] () mutable {
            return source().then([this, remote_node, node_idx] (std::optional<std::tuple<repair_stream_cmd>> status_opt) mutable {
                if (status_opt) {
                    repair_stream_cmd status = std::move(std::get<0>(status_opt.value()));
                    rlogger.trace("put_row_diff: Got status code from follower={} for put_row_diff, status={}", remote_node, int(status));
                    if (status == repair_stream_cmd::put_rows_done) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    } else if (status == repair_stream_cmd::error) {
                        throw std::runtime_error(format("put_row_diff: Repair follower={} failed in put_row_diff hanlder, status={}", remote_node, int(status)));
                    } else {
                        throw std::runtime_error("put_row_diff: Got unexpected repair_stream_cmd");
                    }
                } else {
                    _sink_source_for_put_row_diff.mark_source_closed(node_idx);
                    throw std::runtime_error("put_row_diff: Got unexpected end of stream");
                }
            });
        });
    }

    future<> repair_meta::put_row_diff_sink_op(
            repair_rows_on_wire rows,
            rpc::sink<repair_row_on_wire_with_cmd>& sink,
            gms::inet_address remote_node) {
        return do_with(std::move(rows), [&sink, remote_node] (repair_rows_on_wire& rows) mutable {
            return do_for_each(rows, [&sink] (repair_row_on_wire& row) mutable {
                rlogger.trace("put_row_diff: send row");
                return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::row_data, std::move(row)});
            }).then([&sink] () mutable {
                rlogger.trace("put_row_diff: send empty row");
                return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::end_of_current_rows, repair_row_on_wire()}).then([&sink] () mutable {
                    rlogger.trace("put_row_diff: send done");
                    return sink.flush();
                });
            });
        }).handle_exception([&sink] (std::exception_ptr ep) {
            return sink.close().then([ep = std::move(ep)] () mutable {
                return make_exception_future<>(std::move(ep));
            });
        });
    }

    future<> repair_meta::put_row_diff_with_rpc_stream(
            repair_hash_set set_diff,
            needs_all_rows_t needs_all_rows,
            gms::inet_address remote_node, unsigned node_idx) {
        if (!set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            size_t sz = set_diff.size();
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this, remote_node, node_idx, sz] (std::list<repair_row> row_diff) {
                if (row_diff.size() != sz) {
                    rlogger.warn("Hash conflict detected, keyspace={}, table={}, range={}, row_diff.size={}, set_diff.size={}. It is recommended to compact the table and rerun repair for the range.",
                            _schema->ks_name(), _schema->cf_name(), _range, row_diff.size(), sz);
                }
                return do_with(std::move(row_diff), [this, remote_node, node_idx] (std::list<repair_row>& row_diff) {
                    return get_repair_rows_size(row_diff).then([this, remote_node, node_idx, &row_diff] (size_t row_bytes) mutable {
                        stats().tx_row_nr += row_diff.size();
                        stats().tx_row_nr_peer[remote_node] += row_diff.size();
                        stats().tx_row_bytes += row_bytes;
                        stats().rpc_call_nr++;
                        return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node, node_idx] (repair_rows_on_wire rows)  {
                            return  _sink_source_for_put_row_diff.get_sink_source(remote_node, node_idx).then_unpack(
                                    [this, rows = std::move(rows), remote_node, node_idx]
                                    (rpc::sink<repair_row_on_wire_with_cmd>& sink, rpc::source<repair_stream_cmd>& source) mutable {
                                auto source_op = put_row_diff_source_op(remote_node, node_idx, source);
                                auto sink_op = put_row_diff_sink_op(std::move(rows), sink, remote_node);
                                return when_all_succeed(std::move(source_op), std::move(sink_op)).discard_result();
                            });
                        });
                    });
                });
            });
        }
        return make_ready_future<>();
    }

    // RPC handler
    future<> repair_meta::put_row_diff_handler(repair_rows_on_wire rows, gms::inet_address from) {
        return with_gate(_gate, [this, rows = std::move(rows)] () mutable {
            return apply_rows_on_follower(std::move(rows));
        });
    }

// The repair_metas created passively, i.e., local node is the repair follower.
static thread_local std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>> _repair_metas;
std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>>& repair_meta_map() {
    return _repair_metas;
}

static future<stop_iteration> repair_get_row_diff_with_rpc_stream_process_op(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_row_on_wire_with_cmd> sink,
        rpc::source<repair_hash_with_cmd> source,
        bool &error,
        repair_hash_set& current_set_diff,
        std::optional<std::tuple<repair_hash_with_cmd>> hash_cmd_opt) {
    repair_hash_with_cmd hash_cmd = std::get<0>(hash_cmd_opt.value());
    rlogger.trace("Got repair_hash_with_cmd from peer={}, hash={}, cmd={}", from, hash_cmd.hash, int(hash_cmd.cmd));
    if (hash_cmd.cmd == repair_stream_cmd::hash_data) {
        current_set_diff.insert(hash_cmd.hash);
        return make_ready_future<stop_iteration>(stop_iteration::no);
    } else if (hash_cmd.cmd == repair_stream_cmd::end_of_current_hash_set || hash_cmd.cmd == repair_stream_cmd::needs_all_rows) {
        if (inject_rpc_stream_error) {
            return make_exception_future<stop_iteration>(std::runtime_error("get_row_diff_with_rpc_stream: Inject error in handler loop"));
        }
        bool needs_all_rows = hash_cmd.cmd == repair_stream_cmd::needs_all_rows;
        _metrics.rx_hashes_nr += current_set_diff.size();
        auto fp = make_foreign(std::make_unique<repair_hash_set>(std::move(current_set_diff)));
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, needs_all_rows, fp = std::move(fp)] {
            auto rm = get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::get_row_diff_with_rpc_stream_started);
            if (fp.get_owner_shard() == this_shard_id()) {
                return rm->get_row_diff_handler(std::move(*fp), repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                    rm->set_repair_state_for_local_node(repair_state::get_row_diff_with_rpc_stream_finished);
                    return rows;
                });
            } else {
                return rm->get_row_diff_handler(*fp, repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                    rm->set_repair_state_for_local_node(repair_state::get_row_diff_with_rpc_stream_finished);
                    return rows;
                });
            }
        }).then([sink] (repair_rows_on_wire rows_on_wire) mutable {
            if (rows_on_wire.empty()) {
                return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::end_of_current_rows, repair_row_on_wire()});
            }
            return do_with(std::move(rows_on_wire), [sink] (repair_rows_on_wire& rows_on_wire) mutable {
                return do_for_each(rows_on_wire, [sink] (repair_row_on_wire& row) mutable {
                    return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::row_data, std::move(row)});
                }).then([sink] () mutable {
                    return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::end_of_current_rows, repair_row_on_wire()});
                });
            });
        }).then([sink] () mutable {
            return sink.flush();
        }).then([sink] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    } else {
        return make_exception_future<stop_iteration>(std::runtime_error("Got unexpected repair_stream_cmd"));
    }
}

static future<stop_iteration> repair_put_row_diff_with_rpc_stream_process_op(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_stream_cmd> sink,
        rpc::source<repair_row_on_wire_with_cmd> source,
        bool& error,
        repair_rows_on_wire& current_rows,
        std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt) {
    auto row = std::move(std::get<0>(row_opt.value()));
    if (row.cmd == repair_stream_cmd::row_data) {
        rlogger.trace("Got repair_rows_on_wire from peer={}, got row_data", from);
        current_rows.push_back(std::move(row.row));
        return make_ready_future<stop_iteration>(stop_iteration::no);
    } else if (row.cmd == repair_stream_cmd::end_of_current_rows) {
        rlogger.trace("Got repair_rows_on_wire from peer={}, got end_of_current_rows", from);
        auto fp = make_foreign(std::make_unique<repair_rows_on_wire>(std::move(current_rows)));
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, fp = std::move(fp)] () mutable {
            auto rm = get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::put_row_diff_with_rpc_stream_started);
            if (fp.get_owner_shard() == this_shard_id()) {
                return rm->put_row_diff_handler(std::move(*fp), from).then([rm] {
                    rm->set_repair_state_for_local_node(repair_state::put_row_diff_with_rpc_stream_finished);
                });
            } else {
                return rm->put_row_diff_handler(*fp, from).then([rm] {
                    rm->set_repair_state_for_local_node(repair_state::put_row_diff_with_rpc_stream_finished);
                });
            }
        }).then([sink] () mutable {
            return sink(repair_stream_cmd::put_rows_done);
        }).then([sink] () mutable {
            return sink.flush();
        }).then([sink] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    } else {
        return make_exception_future<stop_iteration>(std::runtime_error("Got unexpected repair_stream_cmd"));
    }
}

static future<stop_iteration> repair_get_full_row_hashes_with_rpc_stream_process_op(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_hash_with_cmd> sink,
        rpc::source<repair_stream_cmd> source,
        bool &error,
        std::optional<std::tuple<repair_stream_cmd>> status_opt) {
    repair_stream_cmd status = std::get<0>(status_opt.value());
    rlogger.trace("Got register_repair_get_full_row_hashes_with_rpc_stream from peer={}, status={}", from, int(status));
    if (status == repair_stream_cmd::get_full_row_hashes) {
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] {
            auto rm = get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_started);
            return rm->get_full_row_hashes_handler().then([rm] (repair_hash_set hashes) {
                rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_started);
                _metrics.tx_hashes_nr += hashes.size();
                return hashes;
            });
        }).then([sink] (repair_hash_set hashes) mutable {
            return do_with(std::move(hashes), [sink] (repair_hash_set& hashes) mutable {
                return do_for_each(hashes, [sink] (const repair_hash& hash) mutable {
                    return sink(repair_hash_with_cmd{repair_stream_cmd::hash_data, hash});
                }).then([sink] () mutable {
                    return sink(repair_hash_with_cmd{repair_stream_cmd::end_of_current_hash_set, repair_hash()});
                });
            });
        }).then([sink] () mutable {
            return sink.flush();
        }).then([sink] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    } else {
        return make_exception_future<stop_iteration>(std::runtime_error("Got unexpected repair_stream_cmd"));
    }
}

static future<> repair_get_row_diff_with_rpc_stream_handler(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_row_on_wire_with_cmd> sink,
        rpc::source<repair_hash_with_cmd> source) {
    return do_with(false, repair_hash_set(), [from, src_cpu_id, repair_meta_id, sink, source] (bool& error, repair_hash_set& current_set_diff) mutable {
        return repeat([from, src_cpu_id, repair_meta_id, sink, source, &error, &current_set_diff] () mutable {
            return source().then([from, src_cpu_id, repair_meta_id, sink, source, &error, &current_set_diff] (std::optional<std::tuple<repair_hash_with_cmd>> hash_cmd_opt) mutable {
                if (hash_cmd_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_get_row_diff_with_rpc_stream_process_op(from,
                            src_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            current_set_diff,
                            std::move(hash_cmd_opt)).handle_exception([sink, &error] (std::exception_ptr ep) mutable {
                        error = true;
                        return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::error, repair_row_on_wire()}).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
            });
        });
    }).finally([sink] () mutable {
        return sink.close().finally([sink] { });
    });
}

static future<> repair_put_row_diff_with_rpc_stream_handler(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_stream_cmd> sink,
        rpc::source<repair_row_on_wire_with_cmd> source) {
    return do_with(false, repair_rows_on_wire(), [from, src_cpu_id, repair_meta_id, sink, source] (bool& error, repair_rows_on_wire& current_rows) mutable {
        return repeat([from, src_cpu_id, repair_meta_id, sink, source, &current_rows, &error] () mutable {
            return source().then([from, src_cpu_id, repair_meta_id, sink, source, &current_rows, &error] (std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt) mutable {
                if (row_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_put_row_diff_with_rpc_stream_process_op(from,
                            src_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            current_rows,
                            std::move(row_opt)).handle_exception([sink, &error] (std::exception_ptr ep) mutable {
                        error = true;
                        return sink(repair_stream_cmd::error).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
            });
        });
    }).finally([sink] () mutable {
        return sink.close().finally([sink] { });
    });
}

static future<> repair_get_full_row_hashes_with_rpc_stream_handler(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_hash_with_cmd> sink,
        rpc::source<repair_stream_cmd> source) {
    return repeat([from, src_cpu_id, repair_meta_id, sink, source] () mutable {
        return do_with(false, [from, src_cpu_id, repair_meta_id, sink, source] (bool& error) mutable {
            return source().then([from, src_cpu_id, repair_meta_id, sink, source, &error] (std::optional<std::tuple<repair_stream_cmd>> status_opt) mutable {
                if (status_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_get_full_row_hashes_with_rpc_stream_process_op(from,
                            src_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            std::move(status_opt)).handle_exception([sink, &error] (std::exception_ptr ep) mutable {
                        error = true;
                        return sink(repair_hash_with_cmd{repair_stream_cmd::error, repair_hash()}).then([] () {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
            });
        });
    }).finally([sink] () mutable {
        return sink.close().finally([sink] { });
    });
}

future<> row_level_repair_init_messaging_service_handler(repair_service& rs, distributed<db::system_distributed_keyspace>& sys_dist_ks,
        distributed<db::view::view_update_generator>& view_update_generator, sharded<netw::messaging_service>& ms) {
    _sys_dist_ks = &sys_dist_ks;
    _view_update_generator = &view_update_generator;
    _messaging = &ms;
    return ms.invoke_on_all([] (auto& ms) {
        ms.register_repair_get_row_diff_with_rpc_stream([&ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_hash_with_cmd> source) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto sink = ms.make_sink_for_repair_get_row_diff_with_rpc_stream(source);
            // Start a new fiber.
            (void)repair_get_row_diff_with_rpc_stream_handler(from, src_cpu_id, repair_meta_id, sink, source).handle_exception(
                    [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
                rlogger.warn("Failed to process get_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
            });
            return make_ready_future<rpc::sink<repair_row_on_wire_with_cmd>>(sink);
        });
        ms.register_repair_put_row_diff_with_rpc_stream([&ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_row_on_wire_with_cmd> source) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto sink = ms.make_sink_for_repair_put_row_diff_with_rpc_stream(source);
            // Start a new fiber.
            (void)repair_put_row_diff_with_rpc_stream_handler(from, src_cpu_id, repair_meta_id, sink, source).handle_exception(
                    [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
                rlogger.warn("Failed to process put_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
            });
            return make_ready_future<rpc::sink<repair_stream_cmd>>(sink);
        });
        ms.register_repair_get_full_row_hashes_with_rpc_stream([&ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_stream_cmd> source) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto sink = ms.make_sink_for_repair_get_full_row_hashes_with_rpc_stream(source);
            // Start a new fiber.
            (void)repair_get_full_row_hashes_with_rpc_stream_handler(from, src_cpu_id, repair_meta_id, sink, source).handle_exception(
                    [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
                rlogger.warn("Failed to process get_full_row_hashes_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
            });
            return make_ready_future<rpc::sink<repair_hash_with_cmd>>(sink);
        });
        ms.register_repair_get_full_row_hashes([] (const rpc::client_info& cinfo, uint32_t repair_meta_id) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_started);
                return rm->get_full_row_hashes_handler().then([rm] (repair_hash_set hashes) {
                    rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_finished);
                    _metrics.tx_hashes_nr += hashes.size();
                    return hashes;
                });
            }) ;
        });
        ms.register_repair_get_combined_row_hash([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                std::optional<repair_sync_boundary> common_sync_boundary) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id,
                    common_sync_boundary = std::move(common_sync_boundary)] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                _metrics.tx_hashes_nr++;
                rm->set_repair_state_for_local_node(repair_state::get_combined_row_hash_started);
                return rm->get_combined_row_hash_handler(std::move(common_sync_boundary)).then([rm] (get_combined_row_hash_response resp) {
                    rm->set_repair_state_for_local_node(repair_state::get_combined_row_hash_finished);
                    return resp;
                });
            });
        });
        ms.register_repair_get_sync_boundary([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                std::optional<repair_sync_boundary> skipped_sync_boundary) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id,
                    skipped_sync_boundary = std::move(skipped_sync_boundary)] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::get_sync_boundary_started);
                return rm->get_sync_boundary_handler(std::move(skipped_sync_boundary)).then([rm] (get_sync_boundary_response resp) {
                    rm->set_repair_state_for_local_node(repair_state::get_sync_boundary_finished);
                    return resp;
                });
            });
        });
        ms.register_repair_get_row_diff([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                repair_hash_set set_diff, bool needs_all_rows) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            _metrics.rx_hashes_nr += set_diff.size();
            auto fp = make_foreign(std::make_unique<repair_hash_set>(std::move(set_diff)));
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, fp = std::move(fp), needs_all_rows] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::get_row_diff_started);
                if (fp.get_owner_shard() == this_shard_id()) {
                    return rm->get_row_diff_handler(std::move(*fp), repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                        rm->set_repair_state_for_local_node(repair_state::get_row_diff_finished);
                        return rows;
                    });
                } else {
                    return rm->get_row_diff_handler(*fp, repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                        rm->set_repair_state_for_local_node(repair_state::get_row_diff_finished);
                        return rows;
                    });
                }
            });
        });
        ms.register_repair_put_row_diff([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                repair_rows_on_wire row_diff) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto fp = make_foreign(std::make_unique<repair_rows_on_wire>(std::move(row_diff)));
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, fp = std::move(fp)] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::put_row_diff_started);
                if (fp.get_owner_shard() == this_shard_id()) {
                    return rm->put_row_diff_handler(std::move(*fp), from).then([rm] {
                        rm->set_repair_state_for_local_node(repair_state::put_row_diff_finished);
                    });
                } else {
                    return rm->put_row_diff_handler(*fp, from).then([rm] {
                        rm->set_repair_state_for_local_node(repair_state::put_row_diff_finished);
                    });
                }
            });
        });
        ms.register_repair_row_level_start([] (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring ks_name,
                sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed,
                unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, rpc::optional<streaming::stream_reason> reason) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, src_cpu_id, repair_meta_id, ks_name, cf_name,
                    range, algo, max_row_buf_size, seed, remote_shard, remote_shard_count, remote_ignore_msb, schema_version, reason] () mutable {
                streaming::stream_reason r = reason ? *reason : streaming::stream_reason::repair;
                return repair_row_level_start_handler(from, src_cpu_id, repair_meta_id, std::move(ks_name),
                        std::move(cf_name), std::move(range), algo, max_row_buf_size, seed,
                        shard_config{remote_shard, remote_shard_count, remote_ignore_msb},
                        schema_version, r);
            });
        });
        ms.register_repair_row_level_stop([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                sstring ks_name, sstring cf_name, dht::token_range range) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, ks_name, cf_name, range] () mutable {
                return repair_row_level_stop_handler(from, repair_meta_id,
                        std::move(ks_name), std::move(cf_name), std::move(range));
            });
        });
        ms.register_repair_get_estimated_partitions([] (const rpc::client_info& cinfo, uint32_t repair_meta_id) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] () mutable {
                return repair_get_estimated_partitions_handler(from, repair_meta_id);
            });
        });
        ms.register_repair_set_estimated_partitions([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                uint64_t estimated_partitions) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, estimated_partitions] () mutable {
                return repair_set_estimated_partitions_handler(from, repair_meta_id, estimated_partitions);
            });
        });
        ms.register_repair_get_diff_algorithms([] (const rpc::client_info& cinfo) {
            return make_ready_future<std::vector<row_level_diff_detect_algorithm>>(suportted_diff_detect_algorithms());
        });
    });
}

future<> row_level_repair_uninit_messaging_service_handler() {
    return _messaging->invoke_on_all([] (auto& ms) {
        return when_all_succeed(
            ms.unregister_repair_get_row_diff_with_rpc_stream(),
            ms.unregister_repair_put_row_diff_with_rpc_stream(),
            ms.unregister_repair_get_full_row_hashes_with_rpc_stream(),
            ms.unregister_repair_get_full_row_hashes(),
            ms.unregister_repair_get_combined_row_hash(),
            ms.unregister_repair_get_sync_boundary(),
            ms.unregister_repair_get_row_diff(),
            ms.unregister_repair_put_row_diff(),
            ms.unregister_repair_row_level_start(),
            ms.unregister_repair_row_level_stop(),
            ms.unregister_repair_get_estimated_partitions(),
            ms.unregister_repair_set_estimated_partitions(),
            ms.unregister_repair_get_diff_algorithms()).discard_result();
    });
}

class repair_meta_tracker {
    boost::intrusive::list<repair_meta,
        boost::intrusive::member_hook<repair_meta, repair_meta::tracker_link_type, &repair_meta::_tracker_link>,
        boost::intrusive::constant_time_size<false>> _repair_metas;
public:
    void add(repair_meta& rm) {
        _repair_metas.push_back(rm);
    }
};

namespace debug {
    static thread_local repair_meta_tracker repair_meta_for_masters;
    static thread_local repair_meta_tracker repair_meta_for_followers;
}

static void add_to_repair_meta_for_masters(repair_meta& rm) {
    debug::repair_meta_for_masters.add(rm);
}
static void add_to_repair_meta_for_followers(repair_meta& rm) {
    debug::repair_meta_for_followers.add(rm);
}

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, utils::UUID table_id, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes) {
    return seastar::futurize_invoke([&ri, cf_name = std::move(cf_name), table_id = std::move(table_id), range = std::move(range), &all_peer_nodes] () mutable {
        auto repair = row_level_repair(ri, std::move(cf_name), std::move(table_id), std::move(range), all_peer_nodes);
        return do_with(std::move(repair), [] (row_level_repair& repair) {
            return repair.run();
        });
    });
}

future<> shutdown_all_row_level_repair() {
    return smp::invoke_on_all([] {
        return remove_repair_meta();
    });
}

class row_level_repair_gossip_helper : public gms::i_endpoint_state_change_subscriber {
    void remove_row_level_repair(gms::inet_address node) {
        rlogger.debug("Started to remove row level repair on all shards for node {}", node);
        smp::invoke_on_all([node] {
            return remove_repair_meta(node);
        }).then([node] {
            rlogger.debug("Finished to remove row level repair on all shards for node {}", node);
        }).handle_exception([node] (std::exception_ptr ep) {
            rlogger.warn("Failed to remove row level repair for node {}: {}", node, ep);
        }).get();
    }
    virtual void on_join(
            gms::inet_address endpoint,
            gms::endpoint_state ep_state) override {
    }
    virtual void before_change(
            gms::inet_address endpoint,
            gms::endpoint_state current_state,
            gms::application_state new_state_key,
            const gms::versioned_value& new_value) override {
    }
    virtual void on_change(
            gms::inet_address endpoint,
            gms::application_state state,
            const gms::versioned_value& value) override {
    }
    virtual void on_alive(
            gms::inet_address endpoint,
            gms::endpoint_state state) override {
    }
    virtual void on_dead(
            gms::inet_address endpoint,
            gms::endpoint_state state) override {
        remove_row_level_repair(endpoint);
    }
    virtual void on_remove(
            gms::inet_address endpoint) override {
        remove_row_level_repair(endpoint);
    }
    virtual void on_restart(
            gms::inet_address endpoint,
            gms::endpoint_state ep_state) override {
        remove_row_level_repair(endpoint);
    }
};

repair_service::repair_service(distributed<gms::gossiper>& gossiper, size_t max_repair_memory)
    : _gossiper(gossiper)
    , _gossip_helper(make_shared<row_level_repair_gossip_helper>())
    , _tracker(smp::count, max_repair_memory) {
    _gossiper.local().register_(_gossip_helper);
}

future<> repair_service::stop() {
    return _gossiper.local().unregister_(_gossip_helper).then([this] {
        _stopped = true;
    });
}

repair_service::~repair_service() {
    assert(_stopped);
}
