/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "row_locking.hh"
#include "log.hh"

static logging::logger mylog("row_locking");

row_locker::row_locker(schema_ptr s)
    : _schema(s)
    , _two_level_locks(1, decorated_key_hash(), decorated_key_equals_comparator(this))
{
}

void row_locker::upgrade(schema_ptr new_schema) {
    if (new_schema == _schema) {
        return;
    }
    mylog.debug("row_locker::upgrade from {} to {}", fmt::ptr(_schema.get()), fmt::ptr(new_schema.get()));
    _schema = new_schema;
}

future<> row_locker::lock_holder::lock_partition(const dht::decorated_key* pk, lock_type& partition_lock, bool exclusive, db::timeout_clock::time_point timeout) {
    auto f = exclusive ? partition_lock.hold_write_lock(timeout) : partition_lock.hold_read_lock(timeout);
    return f.then([this, pk, exclusive] (lock_type::holder holder) {
        _partition = pk;
        _partition_lock_holder = std::move(holder);
        _partition_exclusive = exclusive;
    });
}

future<> row_locker::lock_holder::lock_row(const clustering_key_prefix* cpk, lock_type& row_lock, bool exclusive, db::timeout_clock::time_point timeout) {
    auto f = exclusive ? row_lock.hold_write_lock(timeout) : row_lock.hold_read_lock(timeout);
    return f.then([this, cpk, exclusive] (lock_type::holder holder) {
        _row = cpk;
        _row_lock_holder = std::move(holder);
        _row_exclusive = exclusive;
    });
}

row_locker::latency_stats_tracker::latency_stats_tracker(row_locker::single_lock_stats& stats)
    : lock_stats(stats)
{
    waiting_latency.start();
    lock_stats.operations_currently_waiting_for_lock++;
}

row_locker::latency_stats_tracker::~latency_stats_tracker() {
    lock_stats.operations_currently_waiting_for_lock--;
    waiting_latency.stop();
    lock_stats.estimated_waiting_for_lock.add(waiting_latency.latency());
}

void row_locker::latency_stats_tracker::lock_acquired() {
    lock_stats.lock_acquisitions++;
}

future<row_locker::lock_holder>
row_locker::lock_pk(const dht::decorated_key& pk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking {} lock on entire partition {}", (exclusive ? "exclusive" : "shared"), pk);
    auto tracker = latency_stats_tracker(exclusive ? stats.exclusive_partition : stats.shared_partition);
    auto holder = lock_holder(this);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    // Note: we rely on the fact that &i->first and &i->second._partition_lock,
    // the pointers to a key and respectively, its lock, never
    // become invalid (as long as the item is actually in the hash table),
    // even in the case of rehashing.
    co_await holder.lock_partition(&i->first, i->second._partition_lock, exclusive, timeout);
    tracker.lock_acquired();
    co_return holder;
}

future<row_locker::lock_holder>
row_locker::lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& cpk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking shared lock on partition {}, and {} lock on row {} in it", pk, (exclusive ? "exclusive" : "shared"), cpk);
    auto tracker = latency_stats_tracker(exclusive ? stats.exclusive_row : stats.shared_row);
    auto holder = lock_holder(this);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    // Note: we rely on the fact that &i->first and &i->second._partition_lock,
    // the pointers to a key and respectively, its lock, never
    // become invalid (as long as the item is actually in the hash table),
    // even in the case of rehashing.
    co_await holder.lock_partition(&i->first, i->second._partition_lock, false, timeout);
    auto j = i->second._row_locks.find(cpk);
    if (j == i->second._row_locks.end()) {
        // Not yet locked, need to create the lock. This makes a copy of cpk.
            // FIXME: indentation
            j = i->second._row_locks.emplace(cpk, lock_type()).first;
    }
    // Note: we rely on the fact that &j->first and &j->second,
    // the pointers to a clustering key and respectively, its lock, never
    // become invalid (as long as the item is actually in the hash table),
    // even in the case of rehashing.
    co_await holder.lock_row(&j->first, j->second, exclusive, timeout);
    tracker.lock_acquired();
    co_return holder;
}

// We need to zero old's _partition and _row, so when destructed
// the destructor will do nothing and further moves will not create
// duplicates.
row_locker::lock_holder::lock_holder(row_locker::lock_holder&& old) noexcept
        : _locker(std::exchange(old._locker, nullptr))
        , _partition(std::exchange(old._partition, nullptr))
        , _partition_lock_holder(std::move(old._partition_lock_holder))
        , _row(std::exchange(old._row, nullptr))
        , _row_lock_holder(std::move(old._row_lock_holder))
{
}

void
row_locker::cleanup(const dht::decorated_key* pk, const clustering_key_prefix* cpk) {
    // Look for the partition and/or row locks given keys,
    // and if nobody is using one of lock objects any more, delete it:
    if (pk) {
        auto pli = _two_level_locks.find(*pk);
        if (pli == _two_level_locks.end()) {
            // This shouldn't happen... We can't unlock this lock if we can't find it...
            mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for partition", *pk);
            return;
        }
        assert(&pli->first == pk);
        if (cpk) {
            auto rli = pli->second._row_locks.find(*cpk);
            if (rli == pli->second._row_locks.end()) {
                mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for row", *cpk);
                return;
            }
            assert(&rli->first == cpk);
            auto& lock = rli->second;
            if (!lock.locked()) {
                mylog.debug("Erasing lock object for row {} in partition {}", *cpk, *pk);
                pli->second._row_locks.erase(rli);
            }
        }
        auto& lock = pli->second._partition_lock;
        if (!lock.locked()) {
            auto& row_locks = pli->second._row_locks;
            // We don't expect any locked rows since the lock_holder is supposed
            // to clean up any locked row by calling this function in its dtor
            // and lock_ck row lock is serialized after the partition lock.
            for (auto it = row_locks.begin(); it != row_locks.end(); ++it) {
                if (it->second.locked()) {
                    on_internal_error(mylog, format("Encounered a locked row {} when unlocking partition {}", it->first, *pk));
                }
            }
            mylog.debug("Erasing lock object for partition {}", *pk);
            _two_level_locks.erase(pli);
        }
     }
}

row_locker::lock_holder::~lock_holder() {
    if (!_locker) {
        return;
    }
    if (_row_lock_holder) {
        mylog.debug("releasing {} lock for row {} in partition {}", (_row_exclusive ? "exclusive" : "shared"), *_row, *_partition);
        _row_lock_holder.return_all();
    }
    if (_partition_lock_holder) {
        mylog.debug("releasing {} lock for entire partition {}", (_partition_exclusive ? "exclusive" : "shared"), *_partition);
        _partition_lock_holder.return_all();
    }
    _locker->cleanup(_partition, _row);
}
