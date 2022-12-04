/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "row_locking.hh"
#include "log.hh"
#include "utils/latency.hh"

#include <seastar/core/when_all.hh>

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
    , _partition_state(lock_state::unlocked)
    , _row(nullptr)
    , _row_state(lock_state::unlocked)
{
}

row_locker::lock_holder::lock_holder(row_locker* locker, const dht::decorated_key* pk, lock_state lock_state)
    : _locker(locker)
    , _partition(pk)
    , _partition_state(lock_state)
    , _row(nullptr)
    , _row_state(lock_state::unlocked)
{
}

row_locker::lock_holder::lock_holder(row_locker* locker, const dht::decorated_key* pk, const clustering_key_prefix* cpk, lock_state lock_state)
    : _locker(locker)
    , _partition(pk)
    , _partition_state(lock_state == lock_state::unlocked ? lock_state::unlocked : lock_state::shared)
    , _row(cpk)
    , _row_state(lock_state)
{
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
        return lock_holder(this, pk, exclusive ? lock_state::exclusive : lock_state::shared);
    });
}

future<row_locker::lock_holder>
row_locker::lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& cpk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking shared lock on partition {}, and {} lock on row {} in it", pk, (exclusive ? "exclusive" : "shared"), cpk);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    future<lock_type::holder> lock_partition = i->second._partition_lock.hold_read_lock(timeout);
    auto j = i->second._row_locks.find(cpk);
    if (j == i->second._row_locks.end()) {
        // Not yet locked, need to create the lock. This makes a copy of cpk.
        try {
            j = i->second._row_locks.emplace(cpk, lock_type()).first;
        } catch(...) {
            // If this emplace() failed, e.g., out of memory, we fail. We
            // could do nothing - the partition lock we already started
            // taking will be unlocked automatically after being locked.
            // But it's better form to wait for the work we started, and it
            // will also allow us to remove the hash-table row we added.
            return lock_partition.then([ex = std::current_exception()] (auto lock) {
                // The lock is automatically released when "lock" goes out of scope.
                // TODO: unlock (lock = {}) now, search for the partition in the
                // hash table (we know it's still there, because we held the lock until
                // now) and remove the unused lock from the hash table if still unused.
                return make_exception_future<row_locker::lock_holder>(std::current_exception());
            });
        }
    }
    single_lock_stats &single_lock_stats = exclusive ? stats.exclusive_row : stats.shared_row;
    single_lock_stats.operations_currently_waiting_for_lock++;
    utils::latency_counter waiting_latency;
    waiting_latency.start();
    future<lock_type::holder> lock_row = exclusive ? j->second.hold_write_lock(timeout) : j->second.hold_read_lock(timeout);
    return when_all_succeed(std::move(lock_partition), std::move(lock_row))
    .then_unpack([this, pk = &i->first, cpk = &j->first, exclusive, &single_lock_stats, waiting_latency = std::move(waiting_latency)] (auto lock1, auto lock2) mutable {
        lock1.release();
        lock2.release();
        waiting_latency.stop();
        single_lock_stats.estimated_waiting_for_lock.add(waiting_latency.latency());
        single_lock_stats.lock_acquisitions++;
        single_lock_stats.operations_currently_waiting_for_lock--;
        return lock_holder(this, pk, cpk, exclusive ? lock_state::exclusive : lock_state::shared);
    });
}

row_locker::lock_holder::lock_holder(row_locker::lock_holder&& old) noexcept
        : _locker(old._locker)
        , _partition(old._partition)
        , _partition_state(std::exchange(old._partition_state, lock_state::unlocked))
        , _row(old._row)
        , _row_state(std::exchange(old._row_state, lock_state::unlocked))
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
        _partition_state = std::exchange(old._partition_state, lock_state::unlocked);
        _row = old._row;
        _row_state = std::exchange(old._row_state, lock_state::unlocked);
        // As above, need to also zero other's data
        old._partition = nullptr;
        old._row = nullptr;
    }
    return *this;
}

void
row_locker::unlock(const dht::decorated_key* pk, lock_state partition_state,
                    const clustering_key_prefix* cpk, lock_state row_state) {
    // Look for the partition and/or row locks given keys, release the locks,
    // and if nobody is using one of lock objects any more, delete it:
    if (pk && partition_state != lock_state::unlocked) {
        auto pli = _two_level_locks.find(*pk);
        if (pli == _two_level_locks.end()) {
            // This shouldn't happen... We can't unlock this lock if we can't find it...
            mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for partition", *pk);
            return;
        }
        assert(&pli->first == pk);
        if (cpk && row_state != lock_state::unlocked) {
            auto rli = pli->second._row_locks.find(*cpk);
            if (rli == pli->second._row_locks.end()) {
                mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for row", *cpk);
                return;
            }
            assert(&rli->first == cpk);
            auto row_exclusive = (row_state == lock_state::exclusive);
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
        auto partition_exclusive = (partition_state == lock_state::exclusive);
        mylog.debug("releasing {} lock for entire partition {}", (partition_exclusive ? "exclusive" : "shared"), *pk);
        auto& lock = pli->second._partition_lock;
        if (partition_exclusive) {
            lock.write_unlock();
        } else {
            lock.read_unlock();
        }
        if (!lock.locked()) {
            auto& row_locks = pli->second._row_locks;
            // We can erase the partition entry only if all its rows are unlocked.
            // If _row_locks isn't empty it may contain rows in one of two states:
            // - A locked row.  This is an indication that lock_ck `lock_partition`
            //   failed on timeout while holding the row lock (https://github.com/scylladb/scylladb/issues/12168)
            //   In this case we must not erase the partition element that contains
            //   the row_locks map.
            // - An unlocked row.  This could also occur in if and of lock_ck locks fail.
            //   In this case, since it doesn't get to create the lock_holder, and it
            //   relies only on the semantics of the destroyed rwlock::holder's, it
            //   may leave behind an unlocked row which is not cleaned up by this function.
            //   I this case, it can be cleaned up now.  Better late than never :)
            if (!row_locks.empty()) {
                for (auto it = row_locks.begin(); it != row_locks.end(); ) {
                    if (it->second.locked()) {
                        ++it;
                    } else {
                        it = row_locks.erase(it);
                    }
                }
            }
            if (row_locks.empty()) {
                mylog.debug("Erasing lock object for partition {}", *pk);
                _two_level_locks.erase(pli);
            }
        }
     }
}

row_locker::lock_holder::~lock_holder() {
    if (_locker) {
        _locker->unlock(_partition, _partition_state, _row, _row_state);
    }
}
