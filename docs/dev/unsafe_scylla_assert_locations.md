# Unsafe SCYLLA_ASSERT Locations

This document lists specific locations where SCYLLA_ASSERT cannot be safely converted to scylla_assert().

## Summary

- Files with noexcept SCYLLA_ASSERT: 50
- Files with destructor SCYLLA_ASSERT: 25
- Total unsafe SCYLLA_ASSERT in noexcept: 187
- Total unsafe SCYLLA_ASSERT in destructors: 36

## SCYLLA_ASSERT in noexcept Functions

### auth/cache.cc

- Line 118: `SCYLLA_ASSERT(this_shard_id() == 0);`

Total: 1 usages

### db/cache_mutation_reader.hh

- Line 309: `SCYLLA_ASSERT(sr->is_static_row());`

Total: 1 usages

### db/commitlog/commitlog.cc

- Line 531: `SCYLLA_ASSERT(!*this);`
- Line 544: `SCYLLA_ASSERT(!*this);`
- Line 662: `SCYLLA_ASSERT(_iter != _end);`
- Line 1462: `SCYLLA_ASSERT(i->second >= count);`

Total: 4 usages

### db/hints/manager.hh

- Line 167: `SCYLLA_ASSERT(_ep_managers.empty());`

Total: 1 usages

### db/partition_snapshot_row_cursor.hh

- Line 384: `SCYLLA_ASSERT(_latest_it);`

Total: 1 usages

### db/row_cache.cc

- Line 1365: `SCYLLA_ASSERT(it->is_last_dummy());`

Total: 1 usages

### db/schema_tables.cc

- Line 774: `SCYLLA_ASSERT(this_shard_id() == 0);`

Total: 1 usages

### db/view/view.cc

- Line 3623: `SCYLLA_ASSERT(thread::running_in_thread());`

Total: 1 usages

### gms/gossiper.cc

- Line 876: `SCYLLA_ASSERT(ptr->pid == _permit_id);`

Total: 1 usages

### locator/production_snitch_base.hh

- Line 77: `SCYLLA_ASSERT(_backreference != nullptr);`
- Line 82: `SCYLLA_ASSERT(_backreference != nullptr);`
- Line 87: `SCYLLA_ASSERT(_backreference != nullptr);`

Total: 3 usages

### locator/topology.cc

- Line 135: `SCYLLA_ASSERT(_shard == this_shard_id());`

Total: 1 usages

### mutation/counters.hh

- Line 314: `SCYLLA_ASSERT(_cell.is_live());`
- Line 315: `SCYLLA_ASSERT(!_cell.is_counter_update());`

Total: 2 usages

### mutation/mutation_partition_v2.hh

- Line 271: `SCYLLA_ASSERT(s.version() == _schema_version);`

Total: 1 usages

### mutation/partition_version.cc

- Line 364: `SCYLLA_ASSERT(!_snapshot->is_locked());`
- Line 701: `SCYLLA_ASSERT(!rows.empty());`
- Line 703: `SCYLLA_ASSERT(last_dummy.is_last_dummy());`
- Line 746: `SCYLLA_ASSERT(!_snapshot->is_locked());`
- Line 770: `SCYLLA_ASSERT(at_latest_version());`
- Line 777: `SCYLLA_ASSERT(at_latest_version());`

Total: 6 usages

### mutation/partition_version.hh

- Line 211: `SCYLLA_ASSERT(_schema);`
- Line 217: `SCYLLA_ASSERT(_schema);`
- Line 254: `SCYLLA_ASSERT(!_version->_backref);`
- Line 282: `SCYLLA_ASSERT(_version);`
- Line 286: `SCYLLA_ASSERT(_version);`
- Line 290: `SCYLLA_ASSERT(_version);`
- Line 294: `SCYLLA_ASSERT(_version);`

Total: 7 usages

### mutation/partition_version_list.hh

- Line 36: `SCYLLA_ASSERT(!_head->is_referenced_from_entry());`
- Line 42: `SCYLLA_ASSERT(!_tail->is_referenced_from_entry());`
- Line 70: `SCYLLA_ASSERT(!_head->is_referenced_from_entry());`

Total: 3 usages

### mutation/range_tombstone_list.cc

- Line 412: `SCYLLA_ASSERT (it != rt_list.end());`
- Line 422: `SCYLLA_ASSERT (it != rt_list.end());`

Total: 2 usages

### raft/server.cc

- Line 1720: `SCYLLA_ASSERT(_non_joint_conf_commit_promise);`

Total: 1 usages

### reader_concurrency_semaphore.cc

- Line 109: `SCYLLA_ASSERT(_permit == o._permit);`
- Line 432: `SCYLLA_ASSERT(_need_cpu_branches);`
- Line 455: `SCYLLA_ASSERT(_awaits_branches);`
- Line 1257: `SCYLLA_ASSERT(!_stopped);`
- Line 1585: `SCYLLA_ASSERT(_stats.need_cpu_permits);`
- Line 1587: `SCYLLA_ASSERT(_stats.need_cpu_permits >= _stats.awaits_permits);`
- Line 1593: `SCYLLA_ASSERT(_stats.need_cpu_permits >= _stats.awaits_permits);`
- Line 1598: `SCYLLA_ASSERT(_stats.awaits_permits);`

Total: 8 usages

### readers/multishard.cc

- Line 296: `SCYLLA_ASSERT(!_irh);`

Total: 1 usages

### repair/repair.cc

- Line 1073: `SCYLLA_ASSERT(table_names().size() == table_ids.size());`

Total: 1 usages

### replica/database.cc

- Line 3299: `SCYLLA_ASSERT(!_cf_lock.try_write_lock()); // lock should be acquired before the`
- Line 3304: `SCYLLA_ASSERT(!_cf_lock.try_write_lock()); // lock should be acquired before the`

Total: 2 usages

### replica/database.hh

- Line 1971: `SCYLLA_ASSERT(_user_sstables_manager);`
- Line 1976: `SCYLLA_ASSERT(_system_sstables_manager);`

Total: 2 usages

### replica/dirty_memory_manager.cc

- Line 67: `SCYLLA_ASSERT(!child->_heap_handle);`

Total: 1 usages

### replica/dirty_memory_manager.hh

- Line 261: `SCYLLA_ASSERT(_shutdown_requested);`

Total: 1 usages

### replica/memtable.cc

- Line 563: `SCYLLA_ASSERT(_mt._flushed_memory <= static_cast<int64_t>(_mt.occupancy().total_`
- Line 860: `SCYLLA_ASSERT(!reclaiming_enabled());`

Total: 2 usages

### replica/table.cc

- Line 2829: `SCYLLA_ASSERT(!trange.start()->is_inclusive() && trange.end()->is_inclusive());`

Total: 1 usages

### schema/schema.hh

- Line 1022: `SCYLLA_ASSERT(_schema->is_view());`

Total: 1 usages

### schema/schema_registry.cc

- Line 257: `SCYLLA_ASSERT(_state >= state::LOADED);`
- Line 262: `SCYLLA_ASSERT(_state >= state::LOADED);`
- Line 329: `SCYLLA_ASSERT(o._cpu_of_origin == current);`

Total: 3 usages

### service/direct_failure_detector/failure_detector.cc

- Line 628: `SCYLLA_ASSERT(alive != endpoint_liveness.marked_alive);`

Total: 1 usages

### service/storage_service.cc

- Line 3398: `SCYLLA_ASSERT(this_shard_id() == 0);`
- Line 5760: `SCYLLA_ASSERT(this_shard_id() == 0);`
- Line 5775: `SCYLLA_ASSERT(this_shard_id() == 0);`
- Line 5787: `SCYLLA_ASSERT(this_shard_id() == 0);`

Total: 4 usages

### sstables/generation_type.hh

- Line 132: `SCYLLA_ASSERT(bool(gen));`

Total: 1 usages

### sstables/partition_index_cache.hh

- Line 62: `SCYLLA_ASSERT(!ready());`

Total: 1 usages

### sstables/sstables_manager.hh

- Line 244: `SCYLLA_ASSERT(_sstables_registry && "sstables_registry is not plugged");`

Total: 1 usages

### sstables/storage.hh

- Line 86: `SCYLLA_ASSERT(false && "Changing directory not implemented");`
- Line 89: `SCYLLA_ASSERT(false && "Direct links creation not implemented");`
- Line 92: `SCYLLA_ASSERT(false && "Direct move not implemented");`

Total: 3 usages

### sstables_loader.cc

- Line 735: `SCYLLA_ASSERT(p);`

Total: 1 usages

### tasks/task_manager.cc

- Line 56: `SCYLLA_ASSERT(inserted);`
- Line 76: `SCYLLA_ASSERT(child->get_status().progress_units == progress_units);`
- Line 454: `SCYLLA_ASSERT(this_shard_id() == 0);`

Total: 3 usages

### tools/schema_loader.cc

- Line 281: `SCYLLA_ASSERT(p);`

Total: 1 usages

### utils/UUID.hh

- Line 59: `SCYLLA_ASSERT(is_timestamp());`

Total: 1 usages

### utils/bptree.hh

- Line 289: `SCYLLA_ASSERT(n.is_leftmost());`
- Line 301: `SCYLLA_ASSERT(n.is_rightmost());`
- Line 343: `SCYLLA_ASSERT(leaf->is_leaf());`
- Line 434: `SCYLLA_ASSERT(d->attached());`
- Line 453: `SCYLLA_ASSERT(n._num_keys > 0);`
- Line 505: `SCYLLA_ASSERT(n->is_leftmost());`
- Line 511: `SCYLLA_ASSERT(n->is_rightmost());`
- Line 517: `SCYLLA_ASSERT(n->is_root());`
- Line 557: `SCYLLA_ASSERT(!is_end());`
- Line 566: `SCYLLA_ASSERT(!is_end());`
- Line 613: `SCYLLA_ASSERT(n->_num_keys > 0);`
- Line 833: `SCYLLA_ASSERT(_left->_num_keys > 0);`
- Line 926: `SCYLLA_ASSERT(rl == rb);`
- Line 927: `SCYLLA_ASSERT(rl <= nr);`
- Line 1037: `SCYLLA_ASSERT(is_leaf());`
- Line 1042: `SCYLLA_ASSERT(is_leaf());`
- Line 1047: `SCYLLA_ASSERT(is_leaf());`
- Line 1052: `SCYLLA_ASSERT(is_leaf());`
- Line 1062: `SCYLLA_ASSERT(t->_right == this);`
- Line 1083: `SCYLLA_ASSERT(t->_left == this);`
- Line 1091: `SCYLLA_ASSERT(t->_right == this);`
- Line 1103: `SCYLLA_ASSERT(false);`
- Line 1153: `SCYLLA_ASSERT(i <= _num_keys);`
- Line 1212: `SCYLLA_ASSERT(off <= _num_keys);`
- Line 1236: `SCYLLA_ASSERT(from._num_keys > 0);`
- Line 1389: `SCYLLA_ASSERT(!is_root());`
- Line 1450: `SCYLLA_ASSERT(_num_keys == NodeSize);`
- Line 1563: `SCYLLA_ASSERT(_num_keys < NodeSize);`
- Line 1577: `SCYLLA_ASSERT(i != 0 || left_kid_sorted(k, less));`
- Line 1647: `SCYLLA_ASSERT(nodes.empty());`
- Line 1684: `SCYLLA_ASSERT(_num_keys > 0);`
- Line 1686: `SCYLLA_ASSERT(p._kids[i].n == this);`
- Line 1788: `SCYLLA_ASSERT(_num_keys == 0);`
- Line 1789: `SCYLLA_ASSERT(is_root() || !is_leaf() || (get_prev() == this && get_next() == th`
- Line 1821: `SCYLLA_ASSERT(_parent->_kids[i].n == &other);`
- Line 1841: `SCYLLA_ASSERT(i <= _num_keys);`
- Line 1856: `SCYLLA_ASSERT(!_nodes.empty());`
- Line 1938: `SCYLLA_ASSERT(!attached());`
- Line 1943: `SCYLLA_ASSERT(attached());`

Total: 39 usages

### utils/cached_file.hh

- Line 104: `SCYLLA_ASSERT(!_use_count);`

Total: 1 usages

### utils/compact-radix-tree.hh

- Line 1026: `SCYLLA_ASSERT(check_capacity(head, ni));`
- Line 1027: `SCYLLA_ASSERT(!_data.has(ni));`
- Line 1083: `SCYLLA_ASSERT(next_cap > head._capacity);`
- Line 1149: `SCYLLA_ASSERT(capacity != 0);`
- Line 1239: `SCYLLA_ASSERT(i < Size);`
- Line 1240: `SCYLLA_ASSERT(_idx[i] == unused_node_index);`
- Line 1470: `SCYLLA_ASSERT(kid != nullptr);`
- Line 1541: `SCYLLA_ASSERT(ret.first != nullptr);`
- Line 1555: `SCYLLA_ASSERT(leaf_depth >= depth);`
- Line 1614: `SCYLLA_ASSERT(n->check_prefix(key, depth));`
- Line 1850: `SCYLLA_ASSERT(_root.is(nil_root));`

Total: 11 usages

### utils/cross-shard-barrier.hh

- Line 134: `SCYLLA_ASSERT(w.has_value());`

Total: 1 usages

### utils/double-decker.hh

- Line 200: `SCYLLA_ASSERT(!hint.match);`
- Line 366: `SCYLLA_ASSERT(nb == end._bucket);`

Total: 2 usages

### utils/intrusive-array.hh

- Line 217: `SCYLLA_ASSERT(!is_single_element());`
- Line 218: `SCYLLA_ASSERT(pos < max_len);`
- Line 225: `SCYLLA_ASSERT(pos > 0);`
- Line 238: `SCYLLA_ASSERT(train_len < max_len);`
- Line 329: `SCYLLA_ASSERT(idx < max_len); // may the force be with us...`

Total: 5 usages

### utils/intrusive_btree.hh

- Line 148: `SCYLLA_ASSERT(to.num_keys == 0);`
- Line 157: `SCYLLA_ASSERT(!attached());`
- Line 227: `SCYLLA_ASSERT(n->is_inline());`
- Line 232: `SCYLLA_ASSERT(n->is_inline());`
- Line 288: `SCYLLA_ASSERT(n.is_root());`
- Line 294: `SCYLLA_ASSERT(n.is_leftmost());`
- Line 302: `SCYLLA_ASSERT(n.is_rightmost());`
- Line 368: `SCYLLA_ASSERT(_root->is_leaf());`
- Line 371: `SCYLLA_ASSERT(_inline.empty());`
- Line 601: `SCYLLA_ASSERT(n->is_leaf());`
- Line 673: `SCYLLA_ASSERT(!is_end());`
- Line 674: `SCYLLA_ASSERT(h->attached());`
- Line 677: `SCYLLA_ASSERT(_idx < cur.n->_base.num_keys);`
- Line 679: `SCYLLA_ASSERT(_hook->attached());`
- Line 690: `SCYLLA_ASSERT(!is_end());`
- Line 764: `SCYLLA_ASSERT(n->num_keys > 0);`
- Line 994: `SCYLLA_ASSERT(!_it.is_end());`
- Line 1178: `SCYLLA_ASSERT(is_leaf());`
- Line 1183: `SCYLLA_ASSERT(is_root());`
- Line 1261: `SCYLLA_ASSERT(!is_root());`
- Line 1268: `SCYLLA_ASSERT(p->_base.num_keys > 0 && p->_kids[0] == this);`
- Line 1275: `SCYLLA_ASSERT(p->_base.num_keys > 0 && p->_kids[p->_base.num_keys] == this);`
- Line 1286: `SCYLLA_ASSERT(false);`
- Line 1291: `SCYLLA_ASSERT(!nb->is_inline());`
- Line 1296: `SCYLLA_ASSERT(!nb->is_inline());`
- Line 1338: `SCYLLA_ASSERT(_base.num_keys == 0);`
- Line 1373: `SCYLLA_ASSERT(!(is_leftmost() || is_rightmost()));`
- Line 1378: `SCYLLA_ASSERT(p->_kids[i] != this);`
- Line 1396: `SCYLLA_ASSERT(!is_leaf());`
- Line 1537: `SCYLLA_ASSERT(src != _base.num_keys); // need more keys for the next leaf`
- Line 1995: `SCYLLA_ASSERT(_parent.n->_base.num_keys > 0);`
- Line 2135: `SCYLLA_ASSERT(is_leaf());`
- Line 2144: `SCYLLA_ASSERT(_base.num_keys != 0);`
- Line 2160: `SCYLLA_ASSERT(_base.num_keys != 0);`
- Line 2172: `SCYLLA_ASSERT(!empty());`
- Line 2198: `SCYLLA_ASSERT(leaf == ret->is_leaf());`

Total: 36 usages

### utils/loading_shared_values.hh

- Line 203: `SCYLLA_ASSERT(!_set.size());`

Total: 1 usages

### utils/logalloc.cc

- Line 544: `SCYLLA_ASSERT(!_background_reclaimer);`
- Line 926: `SCYLLA_ASSERT(idx < _segments.size());`
- Line 933: `SCYLLA_ASSERT(idx < _segments.size());`
- Line 957: `SCYLLA_ASSERT(i != _segments.end());`
- Line 1323: `SCYLLA_ASSERT(_lsa_owned_segments_bitmap.test(idx_from_segment(seg)));`
- Line 1366: `SCYLLA_ASSERT(desc._region);`
- Line 1885: `SCYLLA_ASSERT(desc._buf_pointers.empty());`
- Line 1911: `SCYLLA_ASSERT(&desc == old_ptr->_desc);`
- Line 2105: `SCYLLA_ASSERT(seg);`
- Line 2116: `SCYLLA_ASSERT(seg);`
- Line 2341: `SCYLLA_ASSERT(pool.current_emergency_reserve_goal() >= n_segments);`

Total: 11 usages

### utils/logalloc.hh

- Line 307: `SCYLLA_ASSERT(this_shard_id() == _cpu);`

Total: 1 usages

### utils/reusable_buffer.hh

- Line 60: `SCYLLA_ASSERT(_refcount == 0);`

Total: 1 usages


## SCYLLA_ASSERT in Destructors

### api/column_family.cc

- Line 102: `SCYLLA_ASSERT(this_shard_id() == 0);`

Total: 1 usages

### cdc/generation.cc

- Line 846: `SCYLLA_ASSERT(_stopped);`

Total: 1 usages

### cdc/log.cc

- Line 173: `SCYLLA_ASSERT(_stopped);`

Total: 1 usages

### compaction/compaction_manager.cc

- Line 1074: `SCYLLA_ASSERT(_state == state::none || _state == state::stopped);`

Total: 1 usages

### db/hints/internal/hint_endpoint_manager.cc

- Line 188: `SCYLLA_ASSERT(stopped());`

Total: 1 usages

### mutation/partition_version.cc

- Line 347: `SCYLLA_ASSERT(!_snapshot->is_locked());`

Total: 1 usages

### reader_concurrency_semaphore.cc

- Line 1116: `SCYLLA_ASSERT(!_stats.waiters);`
- Line 1125: `SCYLLA_ASSERT(_inactive_reads.empty() && !_close_readers_gate.get_count() && !_p`

Total: 2 usages

### repair/row_level.cc

- Line 3647: `SCYLLA_ASSERT(_state == state::none || _state == state::stopped);`

Total: 1 usages

### replica/cell_locking.hh

- Line 371: `SCYLLA_ASSERT(_partitions.empty());`

Total: 1 usages

### replica/distributed_loader.cc

- Line 305: `SCYLLA_ASSERT(_sstable_directories.empty());`

Total: 1 usages

### schema/schema_registry.cc

- Line 45: `SCYLLA_ASSERT(!_schema);`

Total: 1 usages

### service/direct_failure_detector/failure_detector.cc

- Line 378: `SCYLLA_ASSERT(_ping_fiber.available());`
- Line 379: `SCYLLA_ASSERT(_notify_fiber.available());`
- Line 701: `SCYLLA_ASSERT(_shard_workers.empty());`
- Line 702: `SCYLLA_ASSERT(_destroy_subscriptions.available());`
- Line 703: `SCYLLA_ASSERT(_update_endpoint_fiber.available());`
- Line 707: `SCYLLA_ASSERT(!_impl);`

Total: 6 usages

### service/load_broadcaster.hh

- Line 37: `SCYLLA_ASSERT(_stopped);`

Total: 1 usages

### service/paxos/paxos_state.cc

- Line 323: `SCYLLA_ASSERT(_stopped);`

Total: 1 usages

### service/storage_proxy.cc

- Line 281: `SCYLLA_ASSERT(_stopped);`
- Line 3207: `SCYLLA_ASSERT(!_remote);`

Total: 2 usages

### service/tablet_allocator.cc

- Line 3288: `SCYLLA_ASSERT(_stopped);`

Total: 1 usages

### sstables/compressor.cc

- Line 1271: `SCYLLA_ASSERT(thread::running_in_thread());`

Total: 1 usages

### sstables/sstables_manager.cc

- Line 58: `SCYLLA_ASSERT(_closing);`
- Line 59: `SCYLLA_ASSERT(_active.empty());`
- Line 60: `SCYLLA_ASSERT(_undergoing_close.empty());`

Total: 3 usages

### sstables/sstables_manager.hh

- Line 188: `SCYLLA_ASSERT(_storage != nullptr);`

Total: 1 usages

### utils/cached_file.hh

- Line 477: `SCYLLA_ASSERT(_cache.empty());`

Total: 1 usages

### utils/disk_space_monitor.cc

- Line 66: `SCYLLA_ASSERT(_poller_fut.available());`

Total: 1 usages

### utils/file_lock.cc

- Line 34: `SCYLLA_ASSERT(_fd.get() != -1);`
- Line 36: `SCYLLA_ASSERT(r == 0);`

Total: 2 usages

### utils/logalloc.cc

- Line 1991: `SCYLLA_ASSERT(desc.is_empty());`
- Line 1996: `SCYLLA_ASSERT(segment_pool().descriptor(_active).is_empty());`

Total: 2 usages

### utils/lru.hh

- Line 41: `SCYLLA_ASSERT(!_lru_link.is_linked());`

Total: 1 usages

### utils/replicator.hh

- Line 221: `SCYLLA_ASSERT(_stopped);`

Total: 1 usages

