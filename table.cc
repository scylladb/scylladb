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

#include "database.hh"
#include "sstables/sstables.hh"
#include "service/priority_manager.hh"
#include "db/view/view_updating_consumer.hh"

static logging::logger tlogger("table");

flat_mutation_reader
table::make_reader_excluding_sstable(schema_ptr s,
        sstables::shared_sstable sst,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    std::vector<flat_mutation_reader> readers;
    readers.reserve(_memtables->size() + 1);

    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_flat_reader(s, range, slice, pc, trace_state, fwd, fwd_mr));
    }

    auto effective_sstables = ::make_lw_shared<sstables::sstable_set>(*_sstables);
    effective_sstables->erase(sst);

    readers.emplace_back(make_sstable_reader(s, std::move(effective_sstables), range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    return make_combined_reader(s, std::move(readers), fwd, fwd_mr);
}

void table::move_sstable_from_staging_in_thread(sstables::shared_sstable sst) {
    try {
        sst->move_to_new_dir_in_thread(dir(), sst->generation());
    } catch (...) {
        tlogger.warn("Failed to move sstable {} from staging: {}", sst->get_filename(), std::current_exception());
        return;
    }
    _sstables_staging.erase(sst->generation());
    _compaction_strategy.get_backlog_tracker().add_sstable(sst);
}

/**
 * Given an update for the base table, calculates the set of potentially affected views,
 * generates the relevant updates, and sends them to the paired view replicas.
 */
future<row_locker::lock_holder> table::push_view_replica_updates(const schema_ptr& s, const frozen_mutation& fm, db::timeout_clock::time_point timeout) const {
    //FIXME: Avoid unfreezing here.
    auto m = fm.unfreeze(s);
    return push_view_replica_updates(s, std::move(m), timeout);
}

future<row_locker::lock_holder> table::do_push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout, mutation_source&& source) const {
    if (!_config.view_update_concurrency_semaphore->current()) {
        // We don't have resources to generate view updates for this write. If we reached this point, we failed to
        // throttle the client. The memory queue is already full, waiting on the semaphore would cause this node to
        // run out of memory, and generating hints would ultimately result in the disk queue being full too. We don't
        // drop the base write, which could create inconsistencies between base replicas. So we dolefully continue,
        // and note the fact we dropped a view update.
        ++_config.cf_stats->dropped_view_updates;
        return make_ready_future<row_locker::lock_holder>();
    }
    auto& base = schema();
    m.upgrade(base);
    auto views = affected_views(base, m);
    if (views.empty()) {
        return make_ready_future<row_locker::lock_holder>();
    }
    auto cr_ranges = db::view::calculate_affected_clustering_ranges(*base, m.decorated_key(), m.partition(), views);
    if (cr_ranges.empty()) {
        return generate_and_propagate_view_updates(base, std::move(views), std::move(m), { }).then([] {
                // In this case we are not doing a read-before-write, just a
                // write, so no lock is needed.
                return make_ready_future<row_locker::lock_holder>();
        });
    }
    // We read the whole set of regular columns in case the update now causes a base row to pass
    // a view's filters, and a view happens to include columns that have no value in this update.
    // Also, one of those columns can determine the lifetime of the base row, if it has a TTL.
    auto columns = boost::copy_range<std::vector<column_id>>(
            base->regular_columns() | boost::adaptors::transformed(std::mem_fn(&column_definition::id)));
    query::partition_slice::option_set opts;
    opts.set(query::partition_slice::option::send_partition_key);
    opts.set(query::partition_slice::option::send_clustering_key);
    opts.set(query::partition_slice::option::send_timestamp);
    opts.set(query::partition_slice::option::send_ttl);
    auto slice = query::partition_slice(
            std::move(cr_ranges), { }, std::move(columns), std::move(opts), { }, cql_serialization_format::internal(), query::max_rows);
    // Take the shard-local lock on the base-table row or partition as needed.
    // We'll return this lock to the caller, which will release it after
    // writing the base-table update.
    future<row_locker::lock_holder> lockf = local_base_lock(base, m.decorated_key(), slice.default_row_ranges(), timeout);
    return lockf.then([m = std::move(m), slice = std::move(slice), views = std::move(views), base, this, timeout, source = std::move(source)] (row_locker::lock_holder lock) {
      return do_with(
        dht::partition_range::make_singular(m.decorated_key()),
        std::move(slice),
        std::move(m),
        [base, views = std::move(views), lock = std::move(lock), this, timeout, source = std::move(source)] (auto& pk, auto& slice, auto& m) mutable {
            auto reader = source.make_reader(base, pk, slice, service::get_local_sstable_query_read_priority());
            return this->generate_and_propagate_view_updates(base, std::move(views), std::move(m), std::move(reader)).then([lock = std::move(lock)] () mutable {
                // return the local partition/row lock we have taken so it
                // remains locked until the caller is done modifying this
                // partition/row and destroys the lock object.
                return std::move(lock);
            });
      });
    });
}

future<row_locker::lock_holder> table::push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout) const {
    return do_push_view_replica_updates(s, std::move(m), timeout, as_mutation_source());
}

future<row_locker::lock_holder> table::stream_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout, sstables::shared_sstable excluded_sstable) const {
    return do_push_view_replica_updates(s, std::move(m), timeout, as_mutation_source_excluding(std::move(excluded_sstable)));
}

mutation_source
table::as_mutation_source_excluding(sstables::shared_sstable sst) const {
    return mutation_source([this, sst = std::move(sst)] (schema_ptr s,
                                   const dht::partition_range& range,
                                   const query::partition_slice& slice,
                                   const io_priority_class& pc,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) {
        return this->make_reader_excluding_sstable(std::move(s), std::move(sst), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
    });
}

stop_iteration db::view::view_updating_consumer::consume_end_of_partition() {
    if (_as.abort_requested()) {
        return stop_iteration::yes;
    }
    try {
        auto lock_holder = _table->stream_view_replica_updates(_schema, std::move(*_m), db::no_timeout, _excluded_sstable).get();
    } catch (...) {
        tlogger.warn("Failed to push replica updates for table {}.{}: {}", _schema->ks_name(), _schema->cf_name(), std::current_exception());
    }
    _m.reset();
    return stop_iteration::no;
}
