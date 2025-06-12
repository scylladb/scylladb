/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/assert.hh"
#include "row_locking.hh"
#include "utils/log.hh"
#include "utils/composite_abort_source.hh"

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

row_locker::lock_holder::lock_holder()
    : _locker(nullptr)
    , _partition(nullptr)
    , _partition_exclusive(true)
    , _row(nullptr)
    , _row_exclusive(true) {
}

row_locker::lock_holder::lock_holder(row_locker* locker, const dht::decorated_key* pk, bool exclusive)
    : _locker(locker)
    , _partition(pk)
    , _partition_exclusive(exclusive)
    , _row(nullptr)
    , _row_exclusive(true) {
}

row_locker::lock_holder::lock_holder(row_locker* locker, const dht::decorated_key* pk, const clustering_key_prefix* cpk, bool exclusive)
    : _locker(locker)
    , _partition(pk)
    , _partition_exclusive(false)
    , _row(cpk)
    , _row_exclusive(exclusive) {
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
row_locker::lock_pk(const dht::decorated_key& pk, bool exclusive, db::timeout_clock::time_point timeout, abort_source& abort, stats& stats) {
    mylog.debug("lock_pk: taking {} lock on entire partition {}", (exclusive ? "exclusive" : "shared"), pk);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    auto* pk_ptr = &i->first;
    auto could_lock = exclusive ? i->second._partition_lock.try_write_lock() : i->second._partition_lock.try_read_lock();
    if (could_lock) {
        mylog.trace("lock_ck: acquired {} lock on partition {}", (exclusive ? "exclusive" : "shared"), pk);
        auto& s = exclusive ? stats.exclusive_partition : stats.shared_partition;
        s.lock_acquisitions++;
        return make_ready_future<lock_holder>(this, pk_ptr, exclusive);
    }
    return wait_and_lock_pk(pk, exclusive, timeout, abort, stats);
}

future<row_locker::lock_holder>
row_locker::wait_and_lock_pk(const dht::decorated_key& pk, bool exclusive, db::timeout_clock::time_point timeout, abort_source& abort, stats& stats) {
    auto i = _two_level_locks.try_emplace(pk, this).first;
    auto* pk_ptr = &i->first;
    mylog.trace("wait_and_lock_pk: waiting for {} lock on entire partition {}", (exclusive ? "exclusive" : "shared"), pk);
    auto tracker = latency_stats_tracker(exclusive ? stats.exclusive_partition : stats.shared_partition);
    utils::composite_abort_source composite_as;
    abort_on_expiry<> expiry{timeout};
    composite_as.add(expiry.abort_source());
    composite_as.add(abort);
    co_await (exclusive ? i->second._partition_lock.write_lock(composite_as.abort_source()) : i->second._partition_lock.read_lock(composite_as.abort_source()));
    mylog.trace("wait_and_lock_pk: acquired {} lock on partition {}", (exclusive ? "exclusive" : "shared"), pk);
    // Note: we rely on the fact that &i->first, the pointer to a key, never
    // becomes invalid (as long as the item is actually in the hash table),
    // even in the case of rehashing.
    tracker.lock_acquired();
    co_return lock_holder(this, pk_ptr, exclusive);
}

future<row_locker::lock_holder>
row_locker::lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& cpk, bool exclusive, db::timeout_clock::time_point timeout, abort_source& abort, stats& stats) {
    mylog.debug("lock_ck: taking shared lock on partition {}, and {} lock on row {} in it", pk, (exclusive ? "exclusive" : "shared"), cpk);
    auto ck = cpk;
    // Create a two-level lock entry for the partition if it doesn't exist already.
    auto i = _two_level_locks.try_emplace(pk, this).first;
    // The two-level lock entry we've just created is guaranteed to be kept alive as long as it's locked.
    auto pk_holder = i->second._partition_lock.try_hold_read_lock();
    if (pk_holder) {
        mylog.trace("lock_ck: acquired shared lock on partition {}", pk);
        auto *pk_ptr = &i->first;
        auto j = i->second._row_locks.try_emplace(std::move(ck), lock_type()).first;
        auto* cpk_ptr = &j->first;
        auto& row_lock = j->second;
        // Like to the two-level lock entry above, the row_lock entry we've just created
        // is guaranteed to be kept alive as long as it's locked.
        // Initiating read/write locking in the background below ensures that.
        auto could_lock = exclusive ? row_lock.try_write_lock() : row_lock.try_read_lock();
        if (could_lock) {
            mylog.debug("lock_ck: acquired {} lock on row {}", pk, (exclusive ? "exclusive" : "shared"), cpk);
            pk_holder->release();
            auto& s = exclusive ? stats.exclusive_partition : stats.shared_partition;
            s.lock_acquisitions++;
            return make_ready_future<lock_holder>(this, pk_ptr, cpk_ptr, exclusive);
        }
    }
    return wait_and_lock_ck(pk, cpk, exclusive, timeout, abort, stats);
}

future<row_locker::lock_holder>
row_locker::wait_and_lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& cpk, bool exclusive, db::timeout_clock::time_point timeout, abort_source& abort, stats& stats) {
    mylog.debug("wait_and_lock_ck: wait for shared lock on partition {}, and {} lock on row {} in it", pk, (exclusive ? "exclusive" : "shared"), cpk);
    auto tracker = latency_stats_tracker(exclusive ? stats.exclusive_row : stats.shared_row);
    auto ck = cpk;
    // Create a two-level lock entry for the partition if it doesn't exist already.
    auto i = _two_level_locks.try_emplace(pk, this).first;
    utils::composite_abort_source composite_as;
    abort_on_expiry<> expiry{timeout};
    composite_as.add(expiry.abort_source());
    composite_as.add(abort);
    // The two-level lock entry we've just created is guaranteed to be kept alive as long as it's locked.
    auto pk_holder = co_await i->second._partition_lock.hold_read_lock(composite_as.abort_source());
    mylog.trace("wait_and_lock_ck: acquired shared lock on partition {}", pk);
    // Create a row_lock entry if it doesn't exist already.
    auto *pk_ptr = &i->first;
    auto j = i->second._row_locks.try_emplace(std::move(ck), lock_type()).first;
    auto* cpk_ptr = &j->first;
    auto& row_lock = j->second;
    // Like to the two-level lock entry above, the row_lock entry we've just created
    // is guaranteed to be kept alive as long as it's locked.
    // Initiating read/write locking in the background below ensures that.
    co_await (exclusive ? row_lock.write_lock(composite_as.abort_source()) : row_lock.read_lock(composite_as.abort_source()));
    mylog.debug("wait_and_lock_ck: acquired {} lock on row {}", pk, (exclusive ? "exclusive" : "shared"), cpk);
    pk_holder.release();
    tracker.lock_acquired();
    co_return lock_holder(this, pk_ptr, cpk_ptr, exclusive);
}

row_locker::lock_holder::lock_holder(row_locker::lock_holder&& old) noexcept
        : _locker(old._locker)
        , _partition(old._partition)
        , _partition_exclusive(old._partition_exclusive)
        , _row(old._row)
        , _row_exclusive(old._row_exclusive)
{
    // We also need to zero old's _partition and _row, so when destructed
    // the destructor will do nothing and further moves will not create
    // duplicates.
    old._partition = nullptr;
    old._row = nullptr;
}

row_locker::lock_holder& row_locker::lock_holder::operator=(row_locker::lock_holder&& old) noexcept {
    if (this != &old) {
        this->~lock_holder();
        _locker = old._locker;
        _partition = old._partition;
        _partition_exclusive = old._partition_exclusive;
        _row = old._row;
        _row_exclusive = old._row_exclusive;
        // As above, need to also zero other's data
        old._partition = nullptr;
        old._row = nullptr;
    }
    return *this;
}

void
row_locker::unlock(const dht::decorated_key* pk, bool partition_exclusive,
                    const clustering_key_prefix* cpk, bool row_exclusive) {
    // Look for the partition and/or row locks given keys, release the locks,
    // and if nobody is using one of lock objects any more, delete it:
    if (pk) {
        auto pli = _two_level_locks.find(*pk);
        if (pli == _two_level_locks.end()) {
            // This shouldn't happen... We can't unlock this lock if we can't find it...
            mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for partition", *pk);
            return;
        }
        SCYLLA_ASSERT(&pli->first == pk);
        if (cpk) {
            auto rli = pli->second._row_locks.find(*cpk);
            if (rli == pli->second._row_locks.end()) {
                mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for row", *cpk);
                return;
            }
            SCYLLA_ASSERT(&rli->first == cpk);
            mylog.debug("unlock: releasing {} lock for row {} in partition {}", (row_exclusive ? "exclusive" : "shared"), *cpk, *pk);
            auto& lock = rli->second;
            if (row_exclusive) {
                lock.write_unlock();
            } else {
                lock.read_unlock();
            }
            if (!lock.locked()) {
                mylog.debug("unlock: erasing lock object for row {} in partition {}", *cpk, *pk);
                pli->second._row_locks.erase(rli);
            }
        }
        mylog.debug("unlock: releasing {} lock for entire partition {}", (partition_exclusive ? "exclusive" : "shared"), *pk);
        auto& lock = pli->second._partition_lock;
        if (partition_exclusive) {
            lock.write_unlock();
        } else {
            lock.read_unlock();
        }
        if (!lock.locked()) {
            mylog.debug("unlock: erasing lock object for partition {}", *pk);
            _two_level_locks.erase(pli);
        }
     }
}

row_locker::lock_holder::~lock_holder() {
    if (_locker) {
        _locker->unlock(_partition,  _partition_exclusive, _row, _row_exclusive);
    }
}
