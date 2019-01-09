/*
 * Copyright (C) 2015 ScyllaDB
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

#include <map>
#include <memory>
#include <iosfwd>
#include "database_fwd.hh"
#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "encoding_stats.hh"
#include "mutation_reader.hh"
#include "db/commitlog/replay_position.hh"
#include "db/commitlog/rp_set.hh"
#include "utils/extremum_tracking.hh"
#include "utils/logalloc.hh"
#include "partition_version.hh"
#include "flat_mutation_reader.hh"
#include "mutation_cleaner.hh"

class frozen_mutation;


namespace bi = boost::intrusive;

class memtable_entry {
    bi::set_member_hook<> _link;
    schema_ptr _schema;
    dht::decorated_key _key;
    partition_entry _pe;
public:
    friend class memtable;

    memtable_entry(schema_ptr s, dht::decorated_key key, mutation_partition p)
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(p))
    { }

    memtable_entry(memtable_entry&& o) noexcept;
    // Frees elements of the entry in batches.
    // Returns stop_iteration::yes iff there are no more elements to free.
    stop_iteration clear_gently() noexcept;
    const dht::decorated_key& key() const { return _key; }
    dht::decorated_key& key() { return _key; }
    const partition_entry& partition() const { return _pe; }
    partition_entry& partition() { return _pe; }
    const schema_ptr& schema() const { return _schema; }
    schema_ptr& schema() { return _schema; }
    partition_snapshot_ptr snapshot(memtable& mtbl);

    size_t external_memory_usage_without_rows() const {
        return _key.key().external_memory_usage();
    }

    size_t size_in_allocator_without_rows(allocation_strategy& allocator) {
        return allocator.object_memory_size_in_allocator(this) + external_memory_usage_without_rows();
    }

    size_t size_in_allocator(allocation_strategy& allocator) {
        auto size = size_in_allocator_without_rows(allocator);
        for (auto&& v : _pe.versions()) {
            size += v.size_in_allocator(*_schema, allocator);
        }
        return size;
    }

    struct compare {
        dht::decorated_key::less_comparator _c;

        compare(schema_ptr s)
            : _c(std::move(s))
        {}

        bool operator()(const dht::decorated_key& k1, const memtable_entry& k2) const {
            return _c(k1, k2._key);
        }

        bool operator()(const memtable_entry& k1, const memtable_entry& k2) const {
            return _c(k1._key, k2._key);
        }

        bool operator()(const memtable_entry& k1, const dht::decorated_key& k2) const {
            return _c(k1._key, k2);
        }

        bool operator()(const memtable_entry& k1, const dht::ring_position& k2) const {
            return _c(k1._key, k2);
        }

        bool operator()(const dht::ring_position& k1, const memtable_entry& k2) const {
            return _c(k1, k2._key);
        }
    };

    friend std::ostream& operator<<(std::ostream&, const memtable_entry&);
};

class dirty_memory_manager;

// Managed by lw_shared_ptr<>.
class memtable final : public enable_lw_shared_from_this<memtable>, private logalloc::region {
public:
    using partitions_type = bi::set<memtable_entry,
        bi::member_hook<memtable_entry, bi::set_member_hook<>, &memtable_entry::_link>,
        bi::compare<memtable_entry::compare>>;
private:
    dirty_memory_manager& _dirty_mgr;
    mutation_cleaner _cleaner;
    memtable_list *_memtable_list;
    schema_ptr _schema;
    logalloc::allocating_section _read_section;
    logalloc::allocating_section _allocating_section;
    partitions_type partitions;
    db::replay_position _replay_position;
    db::rp_set _rp_set;
    // mutation source to which reads fall-back after mark_flushed()
    // so that memtable contents can be moved away while there are
    // still active readers. This is needed for this mutation_source
    // to be monotonic (not loose writes). Monotonicity of each
    // mutation_source is necessary for the combined mutation source to be
    // monotonic. That combined source in this case is cache + memtable.
    mutation_source_opt _underlying;
    uint64_t _flushed_memory = 0;

    class encoding_stats_collector {
    private:
        min_max_tracker<api::timestamp_type> timestamp;
        min_tracker<int32_t> min_local_deletion_time;
        min_tracker<int32_t> min_ttl;

        void update_timestamp(api::timestamp_type ts) {
            if (ts != api::missing_timestamp) {
                timestamp.update(ts);
            }
        }

    public:
        encoding_stats_collector()
            : timestamp(encoding_stats::timestamp_epoch, 0)
            , min_local_deletion_time(encoding_stats::deletion_time_epoch)
            , min_ttl(encoding_stats::ttl_epoch)
        {}

        void update(atomic_cell_view cell) {
            update_timestamp(cell.timestamp());
            if (cell.is_live_and_has_ttl()) {
                min_ttl.update(cell.ttl().count());
                min_local_deletion_time.update(cell.expiry().time_since_epoch().count());
            } else if (!cell.is_live()) {
                min_local_deletion_time.update(cell.deletion_time().time_since_epoch().count());
            }
        }

        void update(tombstone tomb) {
            if (tomb) {
                update_timestamp(tomb.timestamp);
                min_local_deletion_time.update(tomb.deletion_time.time_since_epoch().count());
            }
        }

        void update(const schema& s, const row& r, column_kind kind) {
            r.for_each_cell([this, &s, kind](column_id id, const atomic_cell_or_collection& item) {
                auto& col = s.column_at(kind, id);
                if (col.is_atomic()) {
                    update(item.as_atomic_cell(col));
                } else {
                    auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
                  item.as_collection_mutation().data.with_linearized([&] (bytes_view bv) {
                    auto mview = ctype->deserialize_mutation_form(bv);
                    update(mview.tomb);
                    for (auto& entry : mview.cells) {
                        update(entry.second);
                    }
                  });
                }
            });
        }

        void update(const range_tombstone& rt) {
            update(rt.tomb);
        }

        void update(const row_marker& marker) {
            update_timestamp(marker.timestamp());
            if (!marker.is_missing()) {
                if (!marker.is_live()) {
                    min_local_deletion_time.update(marker.deletion_time().time_since_epoch().count());
                } else if (marker.is_expiring()) {
                    min_ttl.update(marker.ttl().count());
                    min_local_deletion_time.update(marker.expiry().time_since_epoch().count());
                }
            }
        }

        void update(const schema& s, const deletable_row& dr) {
            update(dr.marker());
            row_tombstone row_tomb = dr.deleted_at();
            update(row_tomb.regular());
            update(row_tomb.tomb());
            update(s, dr.cells(), column_kind::regular_column);
        }

        void update(const schema& s, const mutation_partition& mp) {
            update(mp.partition_tombstone());
            update(s, mp.static_row(), column_kind::static_column);
            for (auto&& row_entry : mp.clustered_rows()) {
                update(s, row_entry.row());
            }
            for (auto&& rt : mp.row_tombstones()) {
                update(rt);
            }
        }

        encoding_stats get() const {
            return { timestamp.min(), min_local_deletion_time.get(), min_ttl.get() };
        }

        api::timestamp_type max_timestamp() const {
            return timestamp.max();
        }
    } _stats_collector;

    void update(db::rp_handle&&);
    friend class row_cache;
    friend class memtable_entry;
    friend class flush_reader;
    friend class flush_memory_accounter;
private:
    boost::iterator_range<partitions_type::const_iterator> slice(const dht::partition_range& r) const;
    partition_entry& find_or_create_partition(const dht::decorated_key& key);
    partition_entry& find_or_create_partition_slow(partition_key_view key);
    void upgrade_entry(memtable_entry&);
    void add_flushed_memory(uint64_t);
    void remove_flushed_memory(uint64_t);
    void clear() noexcept;
    uint64_t dirty_size() const;
public:
    explicit memtable(schema_ptr schema, dirty_memory_manager&, memtable_list *memtable_list = nullptr,
        seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group());
    // Used for testing that want to control the flush process.
    explicit memtable(schema_ptr schema);
    ~memtable();
    // Clears this memtable gradually without consuming the whole CPU.
    // Never resolves with a failed future.
    future<> clear_gently() noexcept;
    schema_ptr schema() const { return _schema; }
    void set_schema(schema_ptr) noexcept;
    future<> apply(memtable&);
    // Applies mutation to this memtable.
    // The mutation is upgraded to current schema.
    void apply(const mutation& m, db::rp_handle&& = {});
    // The mutation is upgraded to current schema.
    void apply(const frozen_mutation& m, const schema_ptr& m_schema, db::rp_handle&& = {});

    static memtable& from_region(logalloc::region& r) {
        return static_cast<memtable&>(r);
    }

    const logalloc::region& region() const {
        return *this;
    }

    logalloc::region& region() {
        return *this;
    }

    logalloc::region_group* region_group() {
        return group();
    }
    encoding_stats get_stats() const {
        return _stats_collector.get();
    }

    api::timestamp_type get_max_timestamp() const {
        return _stats_collector.max_timestamp();
    }

    mutation_cleaner& cleaner() {
        return _cleaner;
    }
public:
    memtable_list* get_memtable_list() {
        return _memtable_list;
    }

    size_t partition_count() const;
    logalloc::occupancy_stats occupancy() const;

    // Creates a reader of data in this memtable for given partition range.
    //
    // Live readers share ownership of the memtable instance, so caller
    // doesn't need to ensure that memtable remains live.
    //
    // The 'range' parameter must be live as long as the reader is being used
    //
    // Mutations returned by the reader will all have given schema.
    flat_mutation_reader make_flat_reader(schema_ptr,
                                          const dht::partition_range& range,
                                          const query::partition_slice& slice,
                                          const io_priority_class& pc = default_priority_class(),
                                          tracing::trace_state_ptr trace_state_ptr = nullptr,
                                          streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                          mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

    flat_mutation_reader make_flat_reader(schema_ptr s,
                                          const dht::partition_range& range = query::full_partition_range) {
        auto& full_slice = s->full_slice();
        return make_flat_reader(s, range, full_slice);
    }

    flat_mutation_reader make_flush_reader(schema_ptr, const io_priority_class& pc);

    mutation_source as_data_source();

    bool empty() const { return partitions.empty(); }
    void mark_flushed(mutation_source) noexcept;
    bool is_flushed() const;
    void on_detach_from_region_group() noexcept;
    void revert_flushed_memory() noexcept;

    const db::replay_position& replay_position() const {
        return _replay_position;
    }
    const db::rp_set& rp_set() const {
        return _rp_set;
    }
    friend class iterator_reader;

    dirty_memory_manager& get_dirty_memory_manager() {
        return _dirty_mgr;
    }

    friend std::ostream& operator<<(std::ostream&, memtable&);
};
