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

#pragma once

// row_locker provides a mechanism needed by the Materialized Views code to
// lock clustering rows or entire partitions. The locks are shared/exclusive
// (a.k.a. read/write) locks, and locking a row always first locks the
// partition containing it with a shared lock.
//
// Each row_locker is local to a shard (obviously), and to one specific
// column_family. row_locker needs to know the column_family's schema, and
// if that schema is updated the upgrade() method should be called so that
// row_locker could release its shared-pointer to the old schema, and take
// the new.

#include <unordered_map>
#include <memory>

#include <seastar/core/rwlock.hh>
#include <seastar/core/future.hh>

#include "db/timeout_clock.hh"
#include "schema.hh"
#include "dht/i_partitioner.hh"
#include "query-request.hh"
#include "utils/estimated_histogram.hh"
#include "utils/histogram.hh"
#include "utils/latency.hh"

class row_locker {
public:
    struct single_lock_stats {
        uint64_t lock_acquisitions = 0;
        uint64_t operations_currently_waiting_for_lock = 0;
        utils::estimated_histogram estimated_waiting_for_lock;
    };
    struct stats {
        single_lock_stats exclusive_row;
        single_lock_stats shared_row;
        single_lock_stats exclusive_partition;
        single_lock_stats shared_partition;
    };
    // row_locker's locking functions lock_pk(), lock_ck() return a
    // "lock_holder" object. When the caller destroys the object it received,
    // the lock is released. The same type "lock_holder" is used regardless
    // of whether a row or partition was locked, for read or write.
    class lock_holder {
        row_locker* _locker;
        // The lock holder pointers to the partition and clustering keys,
        // which are stored inside the _two_level_locks hash table (we may
        // only drop them from the hash table when all the lock holders for
        // this partition or row are released).
        const dht::decorated_key* _partition;
        bool _partition_exclusive;
        const clustering_key_prefix* _row;
        bool _row_exclusive;
    public:
        lock_holder();
        lock_holder(row_locker* locker, const dht::decorated_key* pk, bool exclusive);
        lock_holder(row_locker* locker, const dht::decorated_key* pk, const clustering_key_prefix* cpk, bool exclusive);
        ~lock_holder();
        // Allow move (noexcept) but disallow copy
        lock_holder(lock_holder&&) noexcept;
        lock_holder& operator=(lock_holder&&) noexcept;
    };
private:
    schema_ptr _schema;
    using lock_type = basic_rwlock<db::timeout_clock>;
    struct two_level_lock {
        lock_type _partition_lock;
        struct clustering_key_prefix_less {
            // Since the schema object may change, we need to use the
            // row_locker's current schema every time.
            const row_locker* locker;
            clustering_key_prefix_less(const row_locker* rl) : locker(rl) {}
            bool operator()(const clustering_key_prefix& k1, const clustering_key_prefix& k2) const {
                return clustering_key_prefix::less_compare(*locker->_schema)(k1, k2);
            }
        };
        std::map<clustering_key_prefix, lock_type, clustering_key_prefix_less> _row_locks;
        two_level_lock(row_locker* locker)
            : _row_locks(locker) { }
    };
    struct decorated_key_hash {
        size_t operator()(const dht::decorated_key& k) const {
            return std::hash<dht::token>()(k.token());
        }
    };
    struct decorated_key_equals_comparator {
        const row_locker* locker;
        explicit decorated_key_equals_comparator(const row_locker* rl) : locker(rl) {}
        bool operator()(const dht::decorated_key& k1, const dht::decorated_key& k2) const {
            return k1.equal(*locker->_schema, k2);
        }
    };
    std::unordered_map<dht::decorated_key, two_level_lock, decorated_key_hash, decorated_key_equals_comparator> _two_level_locks;
    void unlock(const dht::decorated_key* pk, bool partition_exclusive, const clustering_key_prefix* cpk, bool row_exclusive);
public:
    // row_locker needs to know the column_family's schema because key
    // comparisons needs the schema.
    explicit row_locker(schema_ptr s);

    // If new_schema is different from the current schema, convert this
    // row_locker to use the new schema, and hold the shared pointer to the
    // new schema instead of the old schema. This is a trivial operation
    // requiring just comparison/assignment - the hash tables do not need
    // to be rebuilt on upgrade().
    void upgrade(schema_ptr new_schema);

    // Lock an entire partition with a shared or exclusive lock.
    // The key is assumed to belong to the schema saved by row_locker. If you
    // got a schema with the key, and not sure it's not a new version of the
    // schema, call upgrade() before taking the lock.
    future<lock_holder> lock_pk(const dht::decorated_key& pk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats);

    // Lock a clustering row with a shared or exclusive lock.
    // Also, first, takes a shared lock on the partition.
    // The key is assumed to belong to the schema saved by row_locker. If you
    // got a schema with the key, and not sure it's not a new version of the
    // schema, call upgrade() before taking the lock.
    future<lock_holder> lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& ckp, bool exclusive, db::timeout_clock::time_point timeout, stats& stats);

    bool empty() const { return _two_level_locks.empty(); }
};
