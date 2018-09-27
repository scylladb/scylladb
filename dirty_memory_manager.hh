/*
 * Copyright (C) 2017 ScyllaDB
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

#include <boost/intrusive/parent_from_member.hpp>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/semaphore.hh>
#include "database_fwd.hh"
#include "utils/logalloc.hh"

class dirty_memory_manager;

class sstable_write_permit final {
    friend class dirty_memory_manager;
    stdx::optional<semaphore_units<>> _permit;

    sstable_write_permit() noexcept = default;
    explicit sstable_write_permit(semaphore_units<>&& units) noexcept
            : _permit(std::move(units)) {
    }

public:
    sstable_write_permit(sstable_write_permit&&) noexcept = default;
    sstable_write_permit& operator=(sstable_write_permit&&) noexcept = default;

    static sstable_write_permit unconditional() {
        return sstable_write_permit();
    }
};

class flush_permit {
    friend class dirty_memory_manager;
    dirty_memory_manager* _manager;
    sstable_write_permit _sstable_write_permit;
    semaphore_units<> _background_permit;

    flush_permit(dirty_memory_manager* manager, sstable_write_permit&& sstable_write_permit, semaphore_units<>&& background_permit)
            : _manager(manager)
            , _sstable_write_permit(std::move(sstable_write_permit))
            , _background_permit(std::move(background_permit)) {
    }
public:
    flush_permit(flush_permit&&) noexcept = default;
    flush_permit& operator=(flush_permit&&) noexcept = default;

    sstable_write_permit release_sstable_write_permit() {
        return std::move(_sstable_write_permit);
    }

    future<flush_permit> reacquire_sstable_write_permit() &&;
};

class dirty_memory_manager: public logalloc::region_group_reclaimer {
    logalloc::region_group_reclaimer _real_dirty_reclaimer;
    // We need a separate boolean, because from the LSA point of view, pressure may still be
    // mounting, in which case the pressure flag could be set back on if we force it off.
    bool _db_shutdown_requested = false;

    database* _db;
    // The _real_region_group protects against actual dirty memory usage hitting the maximum. Usage
    // for this group is the real dirty memory usage of the system.
    logalloc::region_group _real_region_group;
    // The _virtual_region_group accounts for virtual memory usage. It is defined as the real dirty
    // memory usage minus bytes that were already written to disk.
    logalloc::region_group _virtual_region_group;

    // We would like to serialize the flushing of memtables. While flushing many memtables
    // simultaneously can sustain high levels of throughput, the memory is not freed until the
    // memtable is totally gone. That means that if we have throttled requests, they will stay
    // throttled for a long time. Even when we have virtual dirty, that only provides a rough
    // estimate, and we can't release requests that early.
    semaphore _flush_serializer;
    // We will accept a new flush before another one ends, once it is done with the data write.
    // That is so we can keep the disk always busy. But there is still some background work that is
    // left to be done. Mostly, update the caches and seal the auxiliary components of the SSTable.
    // This semaphore will cap the amount of background work that we have. Note that we're not
    // overly concerned about memtable memory, because dirty memory will put a limit to that. This
    // is mostly about dangling continuations. So that doesn't have to be a small number.
    static constexpr unsigned _max_background_work = 20;
    semaphore _background_work_flush_serializer = { _max_background_work };
    condition_variable _should_flush;
    int64_t _dirty_bytes_released_pre_accounted = 0;

    future<> flush_when_needed();

    future<> _waiting_flush;
    virtual void start_reclaiming() noexcept override;

    bool has_pressure() const {
        return over_soft_limit();
    }

    unsigned _extraneous_flushes = 0;

    seastar::metrics::metric_groups _metrics;
public:
    void setup_collectd(sstring namestr);

    future<> shutdown();

    // Limits and pressure conditions:
    // ===============================
    //
    // Virtual Dirty
    // -------------
    // We can't free memory until the whole memtable is flushed because we need to keep it in memory
    // until the end, but we can fake freeing memory. When we are done with an element of the
    // memtable, we will update the region group pretending memory just went down by that amount.
    //
    // Because the amount of memory that we pretend to free should be close enough to the actual
    // memory used by the memtables, that effectively creates two sub-regions inside the dirty
    // region group, of equal size. In the worst case, we will have <memtable_total_space> dirty
    // bytes used, and half of that already virtually freed.
    //
    // Hard Limit
    // ----------
    // The total space that can be used by memtables in each group is defined by the threshold, but
    // we will only allow the region_group to grow to half of that. This is because of virtual_dirty
    // as explained above. Because virtual dirty is implemented by reducing the usage in the
    // region_group directly on partition written, we want to throttle every time half of the memory
    // as seen by the region_group. To achieve that we need to set the hard limit (first parameter
    // of the region_group_reclaimer) to 1/2 of the user-supplied threshold
    //
    // Soft Limit
    // ----------
    // When the soft limit is hit, no throttle happens. The soft limit exists because we don't want
    // to start flushing only when the limit is hit, but a bit earlier instead. If we were to start
    // flushing only when the hard limit is hit, workloads in which the disk is fast enough to cope
    // would see latency added to some requests unnecessarily.
    //
    // We then set the soft limit to 80 % of the virtual dirty hard limit, which is equal to 40 % of
    // the user-supplied threshold.
    dirty_memory_manager(database& db, size_t threshold, double soft_limit, scheduling_group deferred_work_sg)
        : logalloc::region_group_reclaimer(threshold / 2, threshold * soft_limit / 2)
        , _real_dirty_reclaimer(threshold)
        , _db(&db)
        , _real_region_group(_real_dirty_reclaimer, deferred_work_sg)
        , _virtual_region_group(&_real_region_group, *this, deferred_work_sg)
        , _flush_serializer(1)
        , _waiting_flush(flush_when_needed()) {}

    dirty_memory_manager() : logalloc::region_group_reclaimer()
        , _db(nullptr)
        , _real_region_group(_real_dirty_reclaimer)
        , _virtual_region_group(&_real_region_group, *this)
        , _flush_serializer(1)
        , _waiting_flush(make_ready_future<>()) {}

    static dirty_memory_manager& from_region_group(logalloc::region_group *rg) {
        return *(boost::intrusive::get_parent_from_member(rg, &dirty_memory_manager::_virtual_region_group));
    }

    logalloc::region_group& region_group() {
        return _virtual_region_group;
    }

    const logalloc::region_group& region_group() const {
        return _virtual_region_group;
    }

    void revert_potentially_cleaned_up_memory(logalloc::region* from, int64_t delta) {
        _real_region_group.update(-delta);
        _virtual_region_group.update(delta);
        _dirty_bytes_released_pre_accounted -= delta;
    }

    void account_potentially_cleaned_up_memory(logalloc::region* from, int64_t delta) {
        _real_region_group.update(delta);
        _virtual_region_group.update(-delta);
        _dirty_bytes_released_pre_accounted += delta;
    }

    void pin_real_dirty_memory(int64_t delta) {
        _real_region_group.update(delta);
    }

    void unpin_real_dirty_memory(int64_t delta) {
        _real_region_group.update(-delta);
    }

    size_t real_dirty_memory() const {
        return _real_region_group.memory_used();
    }

    size_t virtual_dirty_memory() const {
        return _virtual_region_group.memory_used();
    }

    future<> flush_one(memtable_list& cf, flush_permit&& permit);

    future<flush_permit> get_flush_permit() {
        return get_units(_background_work_flush_serializer, 1).then([this] (auto&& units) {
            return this->get_flush_permit(std::move(units));
        });
    }

    bool has_extraneous_flushes_requested() const {
        return _extraneous_flushes > 0;
    }

    void start_extraneous_flush() {
        ++_extraneous_flushes;
    }

    void finish_extraneous_flush() {
        --_extraneous_flushes;
    }
private:
    future<flush_permit> get_flush_permit(semaphore_units<>&& background_permit) {
        return get_units(_flush_serializer, 1).then([this, background_permit = std::move(background_permit)] (auto&& units) mutable {
            return flush_permit(this, sstable_write_permit(std::move(units)), std::move(background_permit));
        });
    }

    friend class flush_permit;
};

extern thread_local dirty_memory_manager default_dirty_memory_manager;

