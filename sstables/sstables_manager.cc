/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

<<<<<<< HEAD
=======
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
>>>>>>> 6f58768c46 (sstables_manager: use maintenance scheduling group to run components reload fiber)
#include "log.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/partition_index_cache.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"

namespace sstables {

logging::logger smlogger("sstables_manager");

sstables_manager::sstables_manager(
<<<<<<< HEAD
    db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat, cache_tracker& ct, size_t available_memory, directory_semaphore& dir_sem)
    : _available_memory(available_memory), _large_data_handler(large_data_handler), _db_config(dbcfg), _features(feat), _cache_tracker(ct)
=======
    sstring name, db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat, cache_tracker& ct, size_t available_memory, directory_semaphore& dir_sem, noncopyable_function<locator::host_id()>&& resolve_host_id, scheduling_group maintenance_sg, storage_manager* shared)
    : _storage(shared)
    , _available_memory(available_memory)
    , _large_data_handler(large_data_handler), _db_config(dbcfg), _features(feat), _cache_tracker(ct)
>>>>>>> 79f6746298 (sstables_manager: add member to store maintenance scheduling group)
    , _sstable_metadata_concurrency_sem(
        max_count_sstable_metadata_concurrent_reads,
        max_memory_sstable_metadata_concurrent_reads(available_memory),
        "sstable_metadata_concurrency_sem",
        std::numeric_limits<size_t>::max())
    , _dir_semaphore(dir_sem)
<<<<<<< HEAD
=======
    , _resolve_host_id(std::move(resolve_host_id))
    , _maintenance_sg(std::move(maintenance_sg))
>>>>>>> 79f6746298 (sstables_manager: add member to store maintenance scheduling group)
{
}

sstables_manager::~sstables_manager() {
    assert(_closing);
    assert(_active.empty());
    assert(_undergoing_close.empty());
}

const locator::host_id& sstables_manager::get_local_host_id() const {
    return _db_config.host_id;
}

shared_sstable sstables_manager::make_sstable(schema_ptr schema,
        sstring dir,
        generation_type generation,
        sstable_version_types v,
        sstable_format_types f,
        gc_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size) {
    return make_lw_shared<sstable>(std::move(schema), std::move(dir), generation, v, f, get_large_data_handler(), *this, now, std::move(error_handler_gen), buffer_size);
}

sstable_writer_config sstables_manager::configure_writer(sstring origin) const {
    sstable_writer_config cfg;

    cfg.promoted_index_block_size = _db_config.column_index_size_in_kb() * 1024;
    cfg.promoted_index_auto_scale_threshold = (size_t)_db_config.column_index_auto_scale_threshold_in_kb() * 1024;
    if (!cfg.promoted_index_auto_scale_threshold) {
        cfg.promoted_index_auto_scale_threshold = std::numeric_limits<size_t>::max();
    }
    cfg.validation_level = _db_config.enable_sstable_key_validation()
            ? mutation_fragment_stream_validation_level::clustering_key
            : mutation_fragment_stream_validation_level::token;
    cfg.summary_byte_cost = summary_byte_cost(_db_config.sstable_summary_ratio());

    cfg.origin = std::move(origin);

    return cfg;
}

void sstables_manager::increment_total_reclaimable_memory_and_maybe_reclaim(sstable* sst) {
    _total_reclaimable_memory += sst->total_reclaimable_memory_size();

    size_t memory_reclaim_threshold = _available_memory * _db_config.components_memory_reclaim_threshold();
    if (_total_reclaimable_memory <= memory_reclaim_threshold) {
        // total memory used is within limit; no need to reclaim.
        return;
    }

    // Memory consumption has crossed threshold. Reclaim from the SSTable that
    // has the most reclaimable memory to get the total consumption under limit.
    auto sst_with_max_memory = std::max_element(_active.begin(), _active.end(), [](const sstable& sst1, const sstable& sst2) {
        return sst1.total_reclaimable_memory_size() < sst2.total_reclaimable_memory_size();
    });

    auto memory_reclaimed = sst_with_max_memory->reclaim_memory_from_components();
    _total_memory_reclaimed += memory_reclaimed;
    _total_reclaimable_memory -= memory_reclaimed;
    smlogger.info("Reclaimed {} bytes of memory from SSTable components. Total memory reclaimed so far is {} bytes", memory_reclaimed, _total_memory_reclaimed);
}

<<<<<<< HEAD
=======
size_t sstables_manager::get_memory_available_for_reclaimable_components() {
    size_t memory_reclaim_threshold = _available_memory * _db_config.components_memory_reclaim_threshold();
    return memory_reclaim_threshold - _total_reclaimable_memory;
}

future<> sstables_manager::components_reloader_fiber() {
    co_await coroutine::switch_to(_maintenance_sg);

    sstlog.trace("components_reloader_fiber start");
    while (true) {
        co_await _sstable_deleted_event.when();

        if (_closing) {
            co_return;
        }

        // Reload bloom filters from the smallest to largest so as to maximize
        // the number of bloom filters being reloaded.
        auto memory_available = get_memory_available_for_reclaimable_components();
        while (!_reclaimed.empty() && memory_available > 0) {
            auto sstable_to_reload = _reclaimed.begin();
            const size_t reclaimed_memory = sstable_to_reload->total_memory_reclaimed();
            if (reclaimed_memory > memory_available) {
                // cannot reload anymore sstables
                break;
            }

            // Increment the total memory before reloading to prevent any parallel
            // fibers from loading new bloom filters into memory.
            _total_reclaimable_memory += reclaimed_memory;
            _reclaimed.erase(sstable_to_reload);
            // Use a lw_shared_ptr to prevent the sstable from getting deleted when
            // the components are being reloaded.
            auto sstable_ptr = sstable_to_reload->shared_from_this();
            try {
                co_await sstable_ptr->reload_reclaimed_components();
            } catch (...) {
                // reload failed due to some reason
                sstlog.warn("Failed to reload reclaimed SSTable components : {}", std::current_exception());
                // revert back changes made before the reload
                _total_reclaimable_memory -= reclaimed_memory;
                _reclaimed.insert(*sstable_to_reload);
                break;
            }

            _total_memory_reclaimed -= reclaimed_memory;
            memory_available = get_memory_available_for_reclaimable_components();
        }
    }
}

>>>>>>> 6f58768c46 (sstables_manager: use maintenance scheduling group to run components reload fiber)
void sstables_manager::add(sstable* sst) {
    _active.push_back(*sst);
}

void sstables_manager::deactivate(sstable* sst) {
    // Remove SSTable from the reclaimable memory tracking
    _total_reclaimable_memory -= sst->total_reclaimable_memory_size();

    // At this point, sst has a reference count of zero, since we got here from
    // lw_shared_ptr_deleter<sstables::sstable>::dispose().
    _active.erase(_active.iterator_to(*sst));
    _undergoing_close.push_back(*sst);
    // guard against sstable::close_files() calling shared_from_this() and immediately destroying
    // the result, which will dispose of the sstable recursively
    auto ptr = sst->shared_from_this();
    (void)sst->destroy().finally([ptr] {
        // destruction of ptr will call maybe_done() and release close()
    });
}

void sstables_manager::remove(sstable* sst) {
    _undergoing_close.erase(_undergoing_close.iterator_to(*sst));
    delete sst;
    maybe_done();
}

void sstables_manager::maybe_done() {
    if (_closing && _active.empty() && _undergoing_close.empty()) {
        _done.set_value();
    }
}

future<> sstables_manager::close() {
    _closing = true;
    maybe_done();
    co_await _done.get_future();
    co_await _sstable_metadata_concurrency_sem.stop();
}

sstable_directory::components_lister sstables_manager::get_components_lister(std::filesystem::path dir) {
    return sstable_directory::components_lister(std::move(dir));
}

}   // namespace sstables
