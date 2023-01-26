/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "row_locking.hh"
#include "log.hh"
#include "utils/latency.hh"

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

future<row_locker::lock_holder>
row_locker::lock_pk(const dht::decorated_key& pk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking {} lock on entire partition {}", (exclusive ? "exclusive" : "shared"), pk);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    single_lock_stats &single_lock_stats = exclusive ? stats.exclusive_partition : stats.shared_partition;
    single_lock_stats.operations_currently_waiting_for_lock++;
    utils::latency_counter waiting_latency;
    waiting_latency.start();
    auto f = exclusive ? i->second._partition_lock.write_lock(timeout) : i->second._partition_lock.read_lock(timeout);
    // Note: we rely on the fact that &i->first, the pointer to a key, never
    // becomes invalid (as long as the item is actually in the hash table),
    // even in the case of rehashing.
    return f.then([this, pk = &i->first, exclusive, &single_lock_stats, waiting_latency = std::move(waiting_latency)] () mutable {
        waiting_latency.stop();
        single_lock_stats.estimated_waiting_for_lock.add(waiting_latency.latency());
        single_lock_stats.lock_acquisitions++;
        single_lock_stats.operations_currently_waiting_for_lock--;
        return lock_holder(this, pk, exclusive);
    });
}

future<row_locker::lock_holder>
row_locker::lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& cpk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking shared lock on partition {}, and {} lock on row {} in it", pk, (exclusive ? "exclusive" : "shared"), cpk);
    auto ck = cpk;
    // Create a two-level lock entry for the partition if it doesn't exist already.
    auto i = _two_level_locks.try_emplace(pk, this).first;
    // The two-level lock entry we've just created is guaranteed to be kept alive as long as it's locked.
    // Initiating read locking in the background below ensures that even if the two-level lock is currently
    // write-locked, releasing the write-lock will synchronously engage any waiting
    // locks and will keep the entry alive.
    future<lock_type::holder> lock_partition = i->second._partition_lock.hold_read_lock(timeout);
    single_lock_stats &single_lock_stats = exclusive ? stats.exclusive_row : stats.shared_row;
    single_lock_stats.operations_currently_waiting_for_lock++;
    utils::latency_counter waiting_latency;
    waiting_latency.start();
    return lock_partition.then([this, pk = &i->first, row_locks = &i->second._row_locks, ck = std::move(ck), exclusive, &single_lock_stats, waiting_latency = std::move(waiting_latency), timeout] (auto lock1) mutable {
        auto j = row_locks->find(ck);
        if (j == row_locks->end()) {
            // Not yet locked, need to create the lock.
            j = row_locks->emplace(std::move(ck), lock_type()).first;
        }
        auto* cpk = &j->first;
        auto& row_lock = j->second;
        // Like to the two-level lock entry above, the row_lock entry we've just created
        // is guaranteed to be kept alive as long as it's locked.
        // Initiating read/write locking in the background below ensures that.
        auto lock_row = exclusive ? row_lock.hold_write_lock(timeout) : row_lock.hold_read_lock(timeout);
        return lock_row.then([this, pk, cpk, exclusive, &single_lock_stats, waiting_latency = std::move(waiting_latency), lock1 = std::move(lock1)] (auto lock2) mutable {
        // FIXME: indentation
        lock1.release();
        lock2.release();
        waiting_latency.stop();
        single_lock_stats.estimated_waiting_for_lock.add(waiting_latency.latency());
        single_lock_stats.lock_acquisitions++;
        single_lock_stats.operations_currently_waiting_for_lock--;
        return lock_holder(this, pk, cpk, exclusive);
        });
    });
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
        assert(&pli->first == pk);
        if (cpk) {
            auto rli = pli->second._row_locks.find(*cpk);
            if (rli == pli->second._row_locks.end()) {
                mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for row", *cpk);
                return;
            }
            assert(&rli->first == cpk);
            mylog.debug("releasing {} lock for row {} in partition {}", (row_exclusive ? "exclusive" : "shared"), *cpk, *pk);
            auto& lock = rli->second;
            if (row_exclusive) {
                lock.write_unlock();
            } else {
                lock.read_unlock();
            }
            if (!lock.locked()) {
                mylog.debug("Erasing lock object for row {} in partition {}", *cpk, *pk);
                pli->second._row_locks.erase(rli);
            }
        }
        mylog.debug("releasing {} lock for entire partition {}", (partition_exclusive ? "exclusive" : "shared"), *pk);
        auto& lock = pli->second._partition_lock;
        if (partition_exclusive) {
            lock.write_unlock();
        } else {
            lock.read_unlock();
        }
        if (!lock.locked()) {
            mylog.debug("Erasing lock object for partition {}", *pk);
            _two_level_locks.erase(pli);
        }
     }
}

row_locker::lock_holder::~lock_holder() {
    if (_locker) {
        _locker->unlock(_partition,  _partition_exclusive, _row, _row_exclusive);
    }
}
