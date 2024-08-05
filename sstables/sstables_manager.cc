/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
#include "log.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstables_registry.hh"
#include "sstables/partition_index_cache.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "utils/assert.hh"
#include "utils/s3/client.hh"
#include "exceptions/exceptions.hh"

namespace sstables {

logging::logger smlogger("sstables_manager");

sstables_manager::sstables_manager(
    sstring name, db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat, cache_tracker& ct, size_t available_memory, directory_semaphore& dir_sem,
    noncopyable_function<locator::host_id()>&& resolve_host_id, const abort_source& abort, scheduling_group maintenance_sg, storage_manager* shared)
    : _storage(shared)
    , _available_memory(available_memory)
    , _large_data_handler(large_data_handler), _db_config(dbcfg), _features(feat), _cache_tracker(ct)
    , _sstable_metadata_concurrency_sem(
        max_count_sstable_metadata_concurrent_reads,
        max_memory_sstable_metadata_concurrent_reads(available_memory),
        fmt::format("sstables_manager_{}", name),
        std::numeric_limits<size_t>::max(),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()),
        reader_concurrency_semaphore::register_metrics::no)
    , _dir_semaphore(dir_sem)
    , _resolve_host_id(std::move(resolve_host_id))
    , _maintenance_sg(std::move(maintenance_sg))
    , _abort(abort)
{
    _components_reloader_status = components_reloader_fiber();
}

sstables_manager::~sstables_manager() {
    SCYLLA_ASSERT(_closing);
    SCYLLA_ASSERT(_active.empty());
    SCYLLA_ASSERT(_undergoing_close.empty());
}

storage_manager::storage_manager(const db::config& cfg, config stm_cfg)
    : _s3_clients_memory(stm_cfg.s3_clients_memory)
    , _config_updater(this_shard_id() == 0 ? std::make_unique<config_updater>(cfg, *this) : nullptr)
{
    for (auto [ep, ecfg] : cfg.object_storage_config()) {
        _s3_endpoints.emplace(std::make_pair(std::move(ep), make_lw_shared<s3::endpoint_config>(std::move(ecfg))));
    }
}

future<> storage_manager::stop() {
    if (_config_updater) {
        co_await _config_updater->action.join();
    }

    for (auto ep : _s3_endpoints) {
        if (ep.second.client != nullptr) {
            co_await ep.second.client->close();
        }
    }
}

void storage_manager::update_config(const db::config& cfg) {
    for (auto [ep, ecfg] : cfg.object_storage_config()) {
        auto s3_cfg = make_lw_shared<s3::endpoint_config>(std::move(ecfg));
        auto [it, added] = _s3_endpoints.try_emplace(ep, std::move(s3_cfg));
        if (!added) {
            if (it->second.client != nullptr) {
                it->second.client->update_config(s3_cfg);
            }
            it->second.cfg = std::move(s3_cfg);
        }
    }
}

shared_ptr<s3::client> storage_manager::get_endpoint_client(sstring endpoint) {
    auto found = _s3_endpoints.find(endpoint);
    if (found == _s3_endpoints.end()) {
        smlogger.error("unable to find {} in configured object-storage endpoints", endpoint);
        throw std::invalid_argument(format("endpoint {} not found", endpoint));
    }
    auto& ep = found->second;
    if (ep.client == nullptr) {
        ep.client = s3::client::make(endpoint, ep.cfg, _s3_clients_memory, [ &ct = container() ] (std::string ep) {
            return ct.local().get_endpoint_client(ep);
        });
    }
    return ep.client;
}

bool storage_manager::is_known_endpoint(sstring endpoint) const {
    return _s3_endpoints.contains(endpoint);
}

storage_manager::config_updater::config_updater(const db::config& cfg, storage_manager& sstm)
    : action([&sstm, &cfg] () mutable {
        return sstm.container().invoke_on_all([&cfg] (auto& sstm) {
            sstm.update_config(cfg);
        });
    })
    , observer(cfg.object_storage_config.observe(action.make_observer()))
{}

locator::host_id sstables_manager::get_local_host_id() const {
    return _resolve_host_id();
}

bool sstables_manager::uuid_sstable_identifiers() const {
    return _features.uuid_sstable_identifiers;
}

shared_sstable sstables_manager::make_sstable(schema_ptr schema,
        sstring table_dir,
        const data_dictionary::storage_options& storage,
        generation_type generation,
        sstable_state state,
        sstable_version_types v,
        sstable_format_types f,
        gc_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size) {
    return make_lw_shared<sstable>(std::move(schema), std::move(table_dir), storage, generation, state, v, f, get_large_data_handler(), *this, now, std::move(error_handler_gen), buffer_size);
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
    _reclaimed.insert(*sst_with_max_memory);
    smlogger.info("Reclaimed {} bytes of memory from SSTable components. Total memory reclaimed so far is {} bytes", memory_reclaimed, _total_memory_reclaimed);
}

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

void sstables_manager::add(sstable* sst) {
    _active.push_back(*sst);
}

void sstables_manager::deactivate(sstable* sst) {
    // Remove SSTable from the reclaimable memory tracking
    _total_reclaimable_memory -= sst->total_reclaimable_memory_size();
    _total_memory_reclaimed -= sst->total_memory_reclaimed();
    _reclaimed.erase(*sst);

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
    _sstable_deleted_event.signal();
    maybe_done();
}

void sstables_manager::maybe_done() {
    if (_closing && _active.empty() && _undergoing_close.empty()) {
        _done.set_value();
    }
}

future<> sstables_manager::delete_atomically(std::vector<shared_sstable> ssts) {
    if (ssts.empty()) {
        co_return;
    }

    // All sstables here belong to the same table, thus they do live
    // in the same storage so it's OK to get the deleter from the
    // front element. The deleter implementation is welcome to check
    // that sstables from the vector really live in it.
    auto& storage = ssts.front()->get_storage();
    auto ctx = co_await storage.atomic_delete_prepare(ssts);

    co_await coroutine::parallel_for_each(ssts, [] (shared_sstable sst) {
        return sst->unlink(sstables::storage::sync_dir::no);
    });

    co_await storage.atomic_delete_complete(std::move(ctx));
}

future<> sstables_manager::close() {
    _closing = true;
    maybe_done();
    co_await _done.get_future();
    co_await _sstable_metadata_concurrency_sem.stop();
    // stop the components reload fiber
    _sstable_deleted_event.signal();
    co_await std::move(_components_reloader_status);
}

void sstables_manager::plug_sstables_registry(std::unique_ptr<sstables::sstables_registry> sr) noexcept {
    _sstables_registry = std::move(sr);
}

void sstables_manager::unplug_sstables_registry() noexcept {
    _sstables_registry.reset();
}

future<> sstables_manager::init_table_storage(const data_dictionary::storage_options& so, sstring dir) {
    return sstables::init_table_storage(so, dir);
}

future<> sstables_manager::init_keyspace_storage(const data_dictionary::storage_options& so, sstring dir) {
    return sstables::init_keyspace_storage(*this, so, dir);
}

future<> sstables_manager::destroy_table_storage(const data_dictionary::storage_options& so, sstring dir) {
    return sstables::destroy_table_storage(so, dir);
}

void sstables_manager::validate_new_keyspace_storage_options(const data_dictionary::storage_options& so) {
    std::visit(overloaded_functor {
        [] (const data_dictionary::storage_options::local&) {
        },
        [this] (const data_dictionary::storage_options::s3& so) {
            if (!_features.keyspace_storage_options) {
                throw exceptions::invalid_request_exception("Keyspace storage options not supported in the cluster");
            }
            // It's non-system keyspace
            if (!is_known_endpoint(so.endpoint)) {
                throw exceptions::configuration_exception(format("Endpoint {} not configured", so.endpoint));
            }
        }
    }, so.value);
}

void sstables_manager::on_unlink(sstable* sst) {
    // Remove the sst from manager's reclaimed list to prevent any attempts to reload its components.
    _reclaimed.erase(*sst);
}

sstables_registry::~sstables_registry() = default;

}   // namespace sstables
