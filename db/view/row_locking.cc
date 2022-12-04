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

future<> row_locker::lock_holder::lock_partition(lock_type& partition_lock, bool exclusive) noexcept {
    return exclusive ?
            partition_lock.write_lock().then([this] {
                _partition_state = lock_state::exclusive;
            }) :
            partition_lock.read_lock().then([this] {
                _partition_state = lock_state::shared;
            });
}

future<> row_locker::lock_holder::lock_row(lock_type& row_lock, bool exclusive) noexcept {
    return exclusive ?
            row_lock.write_lock().then([this] {
                _row_state = lock_state::exclusive;
            }) :
            row_lock.read_lock().then([this] {
                _row_state = lock_state::shared;
            });
}

future<row_locker::lock_holder>
row_locker::lock_pk(const dht::decorated_key& pk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking {} lock on entire partition {}", (exclusive ? "exclusive" : "shared"), pk);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    single_lock_stats &single_lock_stats = exclusive ? stats.exclusive_partition : stats.shared_partition;
    single_lock_stats.operations_currently_waiting_for_lock++;
    utils::latency_counter waiting_latency;
    waiting_latency.start();
    // Note: we rely on the fact that &i->first, the pointer to a key, never
    // becomes invalid (as long as the item is actually in the hash table),
    // even in the case of rehashing.
    auto holder = make_lw_shared<lock_holder>(this, &i->first, lock_state::unlocked);
    auto f = holder->lock_partition(i->second._partition_lock, exclusive);
    return f.then([this, pk = &i->first, exclusive, &single_lock_stats, waiting_latency = std::move(waiting_latency), holder = std::move(holder)] () mutable {
        waiting_latency.stop();
        single_lock_stats.estimated_waiting_for_lock.add(waiting_latency.latency());
        single_lock_stats.lock_acquisitions++;
        single_lock_stats.operations_currently_waiting_for_lock--;
        return std::move(*holder);
    });
}

future<row_locker::lock_holder>
row_locker::lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& cpk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    try {
        mylog.debug("taking shared lock on partition {}, and {} lock on row {} in it", pk, (exclusive ? "exclusive" : "shared"), cpk);
        auto i = _two_level_locks.try_emplace(pk, this).first;
        auto j = i->second._row_locks.find(cpk);
        if (j == i->second._row_locks.end()) {
            // Not yet locked, need to create the lock. This makes a copy of cpk.
            j = i->second._row_locks.emplace(cpk, lock_type()).first;
        }
        single_lock_stats &single_lock_stats = exclusive ? stats.exclusive_row : stats.shared_row;
        single_lock_stats.operations_currently_waiting_for_lock++;
        utils::latency_counter waiting_latency;
        waiting_latency.start();
        // Note: we rely on the fact that &i->first and &j->first, the pointers
        // to a key and row, never become invalid (as long as the item is actually
        // in the hash table), even in the case of rehashing.
        auto holder = make_lw_shared<lock_holder>(this, &i->first, &j->first, lock_state::unlocked);
        auto lock_partition = holder->lock_partition(i->second._partition_lock, false);
        auto lock_row = holder->lock_row(j->second, exclusive);
        return when_all_succeed(std::move(lock_partition), std::move(lock_row)).discard_result()
        .then([this, pk = &i->first, cpk = &j->first, exclusive, &single_lock_stats, waiting_latency = std::move(waiting_latency), holder = std::move(holder)] () mutable {
            waiting_latency.stop();
            single_lock_stats.estimated_waiting_for_lock.add(waiting_latency.latency());
            single_lock_stats.lock_acquisitions++;
            single_lock_stats.operations_currently_waiting_for_lock--;
            return std::move(*holder);
        });
    } catch (...) {
        return make_exception_future<row_locker::lock_holder>(std::current_exception());
    }
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
            // If _row_locks isn't empty it may contain locked as follows:
            // - This is an indication that lock_ck `lock_partition`
            //   failed on timeout while holding the row lock (https://github.com/scylladb/scylladb/issues/12168)
            //   In this case we must not erase the partition element that contains
            //   the row_locks map.
            // - Unlocked rows should never be left in the _row_locks map.
            //   since all lock operations are supposed to be tracked by lock_holder,
            //   so any failure should result in calling unlock.
            if (!row_locks.empty()) {
                for (auto it = row_locks.begin(); it != row_locks.end(); ) {
                    if (it->second.locked()) {
                        ++it;
                    } else {
                        mylog.debug("Unexpected stale unlocked row {}", it->first);
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
